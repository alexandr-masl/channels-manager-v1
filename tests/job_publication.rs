use channels_manager_v1::{
    contracts::messages::ClientTradeJob,
    rabbitmq::{PreparedPublication, RabbitError},
    signals::publication::{JobPublication, JobPublisher},
};
use futures_util::future::BoxFuture;
use std::{collections::VecDeque, sync::Mutex, time::Duration};

#[derive(Default)]
struct Sink {
    results: Mutex<VecDeque<Result<(), RabbitError>>>,
    bodies: Mutex<Vec<Vec<u8>>>,
}
impl JobPublisher for Sink {
    fn publish_job<'a>(
        &'a self,
        job: &'a PreparedPublication,
    ) -> BoxFuture<'a, Result<(), RabbitError>> {
        Box::pin(async move {
            self.bodies.lock().unwrap().push(job.body().to_vec());
            self.results.lock().unwrap().pop_front().unwrap_or(Ok(()))
        })
    }
}
fn job(message_id: i64) -> ClientTradeJob {
    let mut job: ClientTradeJob =
        serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
    job.message_id = message_id;
    job.idempotency_key = format!("job-{message_id}");
    job.trade_expires_at = Some(4102444800000);
    job
}
fn policy() -> JobPublication {
    JobPublication::new(2, Duration::from_millis(1))
}

#[tokio::test]
async fn confirms_all_jobs_with_exact_wire_bytes() {
    let jobs = vec![job(1), job(2)];
    let sink = Sink::default();
    assert_eq!(policy().publish(&jobs, &sink).await.unwrap(), 2);
    let bodies = sink.bodies.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    for (body, job) in bodies.iter().zip(jobs.iter()) {
        assert_eq!(*body, serde_json::to_vec(job).unwrap());
    }
}
#[tokio::test]
async fn definite_rejections_retry_only_current_job_with_unchanged_bytes() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [
        Ok(()),
        Err(RabbitError::Nack),
        Err(RabbitError::Unroutable),
        Ok(()),
    ]
    .into();
    assert_eq!(policy().publish(&[job(1), job(2)], &sink).await.unwrap(), 2);
    let bodies = sink.bodies.lock().unwrap();
    assert_eq!(bodies.len(), 4);
    assert_ne!(bodies[0], bodies[1]);
    assert_eq!(bodies[1], bodies[2]);
    assert_eq!(bodies[2], bodies[3]);
}
#[tokio::test]
async fn exhausted_retries_report_partial_count_and_stop_fanout() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [
        Ok(()),
        Err(RabbitError::Nack),
        Err(RabbitError::Nack),
        Err(RabbitError::Nack),
    ]
    .into();
    let failure = policy()
        .publish(&[job(1), job(2), job(3)], &sink)
        .await
        .unwrap_err();
    assert_eq!(failure.published_jobs, 1);
    assert_eq!(failure.error, RabbitError::Nack);
    assert_eq!(sink.bodies.lock().unwrap().len(), 4);
}
#[tokio::test]
async fn uncertainty_never_retries_or_starts_remaining_jobs() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [Ok(()), Err(RabbitError::PublishUncertain)].into();
    let failure = policy()
        .publish(&[job(1), job(2), job(3)], &sink)
        .await
        .unwrap_err();
    assert_eq!(failure.published_jobs, 1);
    assert_eq!(failure.error, RabbitError::PublishUncertain);
    assert_eq!(sink.bodies.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn expired_jobs_never_reach_broker_and_invalid_batch_sends_nothing() {
    let sink = Sink::default();
    let mut expired = job(1);
    expired.trade_expires_at = Some(1);
    assert_eq!(
        policy().publish(&[expired], &sink).await.unwrap_err().error,
        RabbitError::Expired
    );
    let mut invalid = job(2);
    invalid.trade_expires_at = None;
    assert_eq!(
        policy()
            .publish(&[job(1), invalid], &sink)
            .await
            .unwrap_err()
            .error,
        RabbitError::InvalidPayload
    );
    assert!(sink.bodies.lock().unwrap().is_empty());
}
#[tokio::test]
async fn expiry_during_retry_delay_stops_publication() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [Err(RabbitError::Nack)].into();
    let mut expiring = job(1);
    expiring.trade_expires_at = Some(mongodb::bson::DateTime::now().timestamp_millis() as u64 + 30);
    let failure = JobPublication::new(2, Duration::from_millis(60))
        .publish(&[expiring], &sink)
        .await
        .unwrap_err();
    assert_eq!(failure.error, RabbitError::Expired);
    assert_eq!(sink.bodies.lock().unwrap().len(), 1);
}
