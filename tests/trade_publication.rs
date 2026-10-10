use channels_manager_v1::{
    contracts::messages::{NewTradeMessage, trade_creation_id},
    rabbitmq::{PreparedPublication, RabbitError},
    trading::publication::{TradePublication, TradePublisher},
};
use futures_util::future::BoxFuture;
use std::{collections::VecDeque, sync::Mutex, time::Duration};
#[derive(Default)]
struct Sink {
    results: Mutex<VecDeque<Result<(), RabbitError>>>,
    bodies: Mutex<Vec<Vec<u8>>>,
}
impl TradePublisher for Sink {
    fn publish_trade<'a>(
        &'a self,
        message: &'a PreparedPublication,
    ) -> BoxFuture<'a, Result<(), RabbitError>> {
        Box::pin(async move {
            self.bodies.lock().unwrap().push(message.body().to_vec());
            self.results.lock().unwrap().pop_front().unwrap_or(Ok(()))
        })
    }
}
fn trade() -> NewTradeMessage {
    NewTradeMessage {
        expires_at: 4102444800000,
        trade_object: serde_json::json!({"id":trade_creation_id("job")}),
        client_data: serde_json::json!({"key":"secret"}),
    }
}
#[tokio::test]
async fn definite_failures_reuse_exact_trade_bytes() {
    for error in [
        RabbitError::Nack,
        RabbitError::Unroutable,
        RabbitError::Timeout,
    ] {
        let sink = Sink::default();
        *sink.results.lock().unwrap() = [Err(error), Ok(())].into();
        let message = trade();
        TradePublication::new(2, Duration::ZERO)
            .publish(&message, "job", &sink)
            .await
            .unwrap();
        let bodies = sink.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0], bodies[1]);
        assert_eq!(bodies[0], serde_json::to_vec(&message).unwrap());
    }
}
#[tokio::test]
async fn exhausted_definite_failures_are_bounded() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [Err(RabbitError::Nack); 4].into();
    assert_eq!(
        TradePublication::new(2, Duration::ZERO)
            .publish(&trade(), "job", &sink)
            .await,
        Err(RabbitError::Nack)
    );
    assert_eq!(sink.bodies.lock().unwrap().len(), 3);
}
#[tokio::test]
async fn uncertainty_stops_immediately() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [Err(RabbitError::PublishUncertain)].into();
    assert_eq!(
        TradePublication::new(2, Duration::ZERO)
            .publish(&trade(), "job", &sink)
            .await,
        Err(RabbitError::PublishUncertain)
    );
    assert_eq!(sink.bodies.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn expired_or_invalid_trade_is_never_sent() {
    let sink = Sink::default();
    let policy = TradePublication::new(2, Duration::ZERO);
    let mut message = trade();
    message.expires_at = 1;
    assert_eq!(
        policy.publish(&message, "job", &sink).await,
        Err(RabbitError::Expired)
    );
    assert_eq!(
        policy.publish(&trade(), "wrong-id", &sink).await,
        Err(RabbitError::InvalidPayload)
    );
    assert!(sink.bodies.lock().unwrap().is_empty());
}
#[tokio::test]
async fn retry_wait_is_bounded_by_expiry() {
    let sink = Sink::default();
    *sink.results.lock().unwrap() = [Err(RabbitError::Nack)].into();
    let mut message = trade();
    message.expires_at = mongodb::bson::DateTime::now().timestamp_millis() as u64 + 40;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(300),
            TradePublication::new(2, Duration::from_secs(10)).publish(&message, "job", &sink)
        )
        .await
        .unwrap(),
        Err(RabbitError::Expired)
    );
    assert_eq!(sink.bodies.lock().unwrap().len(), 1);
}
