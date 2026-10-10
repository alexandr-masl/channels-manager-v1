//! Confirmed, sequential fan-out. Credentials stay inside serialized publications.
use crate::{
    contracts::messages::ClientTradeJob,
    rabbitmq::{PreparedPublication, Publisher, RabbitError},
};
use futures_util::future::BoxFuture;
use std::time::Duration;

pub trait JobPublisher: Send + Sync {
    fn publish_job<'a>(
        &'a self,
        job: &'a PreparedPublication,
    ) -> BoxFuture<'a, Result<(), RabbitError>>;
}
impl JobPublisher for Publisher {
    fn publish_job<'a>(
        &'a self,
        job: &'a PreparedPublication,
    ) -> BoxFuture<'a, Result<(), RabbitError>> {
        Box::pin(self.publish(job))
    }
}
#[derive(Debug)]
pub struct PublicationFailure {
    pub published_jobs: usize,
    pub error: RabbitError,
}
#[derive(Clone)]
pub struct JobPublication {
    max_retries: u32,
    delay: Duration,
}
impl JobPublication {
    pub fn new(max_retries: u32, delay: Duration) -> Self {
        Self { max_retries, delay }
    }

    /// Serialize the entire batch before sending. Confirmed jobs are never retried
    /// in this invocation. Source redelivery can repeat them; no dedupe is implied.
    pub async fn publish(
        &self,
        jobs: &[ClientTradeJob],
        publisher: &impl JobPublisher,
    ) -> Result<usize, PublicationFailure> {
        let messages = jobs
            .iter()
            .map(PreparedPublication::client_job)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| PublicationFailure {
                published_jobs: 0,
                error,
            })?;
        for (published_jobs, message) in messages.iter().enumerate() {
            let mut retries = 0;
            loop {
                let now = mongodb::bson::DateTime::now()
                    .timestamp_millis()
                    .try_into()
                    .unwrap_or(u64::MAX);
                let result = if message.is_expired(now) {
                    Err(RabbitError::Expired)
                } else {
                    publisher.publish_job(message).await
                };
                match result {
                    Ok(()) => break,
                    Err(RabbitError::Nack | RabbitError::Unroutable | RabbitError::Timeout)
                        if retries < self.max_retries =>
                    {
                        retries += 1;
                        // Never hold an expired batch through a long configured retry delay.
                        let remaining = jobs[published_jobs]
                            .trade_expires_at
                            .unwrap()
                            .saturating_sub(now);
                        tokio::time::sleep(self.delay.min(Duration::from_millis(remaining))).await;
                    }
                    Err(error) => {
                        return Err(PublicationFailure {
                            published_jobs,
                            error,
                        });
                    }
                }
            }
        }
        Ok(messages.len())
    }
}
