//! Confirm final trades before source settlement; uncertain sends require recovery.
use crate::{
    contracts::messages::NewTradeMessage,
    rabbitmq::{PreparedPublication, Publisher, RabbitError},
};
use futures_util::future::BoxFuture;
use std::time::Duration;

pub trait TradePublisher: Send + Sync {
    fn publish_trade<'a>(
        &'a self,
        message: &'a PreparedPublication,
    ) -> BoxFuture<'a, Result<(), RabbitError>>;
}
impl TradePublisher for Publisher {
    fn publish_trade<'a>(
        &'a self,
        message: &'a PreparedPublication,
    ) -> BoxFuture<'a, Result<(), RabbitError>> {
        Box::pin(self.publish(message))
    }
}
#[derive(Clone)]
pub struct TradePublication {
    max_retries: u32,
    delay: Duration,
}
impl TradePublication {
    pub fn new(max_retries: u32, delay: Duration) -> Self {
        Self { max_retries, delay }
    }
    pub async fn publish(
        &self,
        trade: &NewTradeMessage,
        idempotency_key: &str,
        publisher: &impl TradePublisher,
    ) -> Result<(), RabbitError> {
        let message = PreparedPublication::trade(trade, idempotency_key)?;
        let mut retries = 0;
        loop {
            if message.is_expired(now_ms()) {
                return Err(RabbitError::Expired);
            }
            match publisher.publish_trade(&message).await {
                Err(RabbitError::Nack | RabbitError::Unroutable | RabbitError::Timeout)
                    if retries < self.max_retries =>
                {
                    retries += 1;
                    let remaining = trade.expires_at.saturating_sub(now_ms());
                    tokio::time::sleep(self.delay.min(Duration::from_millis(remaining))).await;
                }
                result => return result,
            }
        }
    }
}
pub(crate) fn now_ms() -> u64 {
    mongodb::bson::DateTime::now()
        .timestamp_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
