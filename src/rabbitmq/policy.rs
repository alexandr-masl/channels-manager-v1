//! Explicit execution outcomes; transport errors never guess whether a claim exists.
use super::{InboundDelivery, PreparedPublication, Publisher, RabbitError};
use crate::{
    config::RabbitMqConfig,
    contracts::{messages::DeadLetterMessage, rabbitmq::*},
};
use lapin::types::{AMQPValue, FieldTable};

#[derive(Clone, Copy, Debug)]
pub enum RetryReason {
    Timeout,
    DependencyUnavailable,
    ClaimUncertain,
    Contention,
}
impl RetryReason {
    fn code(self) -> &'static str {
        match self {
            Self::Timeout => "TIMEOUT",
            Self::DependencyUnavailable => "DEPENDENCY_UNAVAILABLE",
            Self::ClaimUncertain => "CLAIM_UNCERTAIN",
            Self::Contention => "CONTENTION",
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum DeliveryOutcome {
    Completed,
    Rejected,
    Suppressed,
    /// The handler records the terminal claim before returning this outcome.
    PostClaimTerminal,
    PreClaimRetry(RetryReason),
}
#[derive(Clone)]
pub struct DeliveryPolicy {
    max_attempts: u32,
    delay_ms: u64,
    queue: &'static str,
}
impl DeliveryPolicy {
    pub fn new(config: &RabbitMqConfig) -> Self {
        Self {
            max_attempts: config.retry_max_attempts.get(),
            delay_ms: config.retry_delay.as_millis() as u64,
            queue: config.input_queue,
        }
    }
    /// Publish the replacement before acknowledging. Failure faults the session,
    /// leaving the original unacknowledged for recovery with lifecycle backoff.
    pub async fn settle(
        &self,
        delivery: InboundDelivery,
        outcome: DeliveryOutcome,
        publisher: &Publisher,
    ) -> Result<(), RabbitError> {
        let DeliveryOutcome::PreClaimRetry(reason) = outcome else {
            return delivery.ack().await;
        };
        let publication = self.prepare(delivery.body(), delivery.properties(), reason)?;
        if let Err(error) = publisher.publish(&publication).await {
            delivery.fail(error);
            return Err(error);
        }
        delivery.ack().await
    }
    fn prepare(
        &self,
        body: &[u8],
        properties: &lapin::BasicProperties,
        reason: RetryReason,
    ) -> Result<PreparedPublication, RabbitError> {
        let mut headers = properties.headers().clone().unwrap_or_default();
        // Invalid counters go directly to diagnostics, never reset the retry budget.
        let current = attempt(&headers).unwrap_or(self.max_attempts as u64);
        let next = current.min(self.max_attempts as u64).saturating_add(1);
        let dead = next > self.max_attempts as u64;
        let now = mongodb::bson::DateTime::now();
        headers.insert(
            ORIGINAL_QUEUE_HEADER.into(),
            AMQPValue::LongString(self.queue.into()),
        );
        headers.insert(
            RETRY_ATTEMPT_HEADER.into(),
            AMQPValue::LongLongInt(next as i64),
        );
        headers.insert(
            RETRY_MAX_ATTEMPTS_HEADER.into(),
            AMQPValue::LongLongInt(self.max_attempts as i64),
        );
        headers.insert(
            RETRY_DELAY_HEADER.into(),
            AMQPValue::LongLongInt(if dead { 0 } else { self.delay_ms as i64 }),
        );
        if !headers.inner().contains_key(FIRST_FAILURE_HEADER) {
            headers.insert(
                FIRST_FAILURE_HEADER.into(),
                AMQPValue::LongLongInt(now.timestamp_millis()),
            );
        }
        headers.insert(
            LAST_FAILURE_HEADER.into(),
            AMQPValue::LongLongInt(now.timestamp_millis()),
        );
        headers.insert(
            LAST_ERROR_HEADER.into(),
            AMQPValue::LongString(reason.code().into()),
        );
        if dead {
            let message = DeadLetterMessage {
                original_queue: self.queue.into(),
                payload: serde_json::from_slice(body)
                    .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned().into()),
                error: reason.code().into(),
                attempt: next.min(u32::MAX as u64) as u32,
                max_attempts: self.max_attempts,
                dead_lettered_at: now
                    .try_to_rfc3339_string()
                    .map_err(|_| RabbitError::InvalidPayload)?,
            };
            let mut publication = PreparedPublication::dead_letter(&message)?;
            publication.set_retry_metadata(
                properties.message_id().as_ref().map(|id| id.as_str()),
                headers,
            );
            Ok(publication)
        } else {
            PreparedPublication::retry(
                body.to_vec(),
                properties.message_id().as_ref().map(|id| id.as_str()),
                headers,
            )
        }
    }
}
fn attempt(headers: &FieldTable) -> Option<u64> {
    match headers.inner().get(RETRY_ATTEMPT_HEADER) {
        None => Some(0),
        Some(AMQPValue::LongLongInt(n)) => (*n).try_into().ok(),
        Some(AMQPValue::LongInt(n)) => (*n).try_into().ok(),
        Some(AMQPValue::ShortInt(n)) => (*n).try_into().ok(),
        Some(AMQPValue::ShortShortInt(n)) => (*n).try_into().ok(),
        Some(AMQPValue::LongUInt(n)) => Some(*n as u64),
        Some(AMQPValue::ShortUInt(n)) => Some(*n as u64),
        Some(AMQPValue::ShortShortUInt(n)) => Some(*n as u64),
        Some(AMQPValue::LongString(n)) => n.to_string().parse().ok(),
        Some(AMQPValue::Double(n)) if n.is_finite() && *n >= 0.0 && n.fract() == 0.0 => {
            Some(*n as u64)
        }
        _ => None,
    }
}
