use super::{OperationGuard, RabbitError, Session};
use crate::contracts::{
    messages::{DeadLetterMessage, NewTradeMessage, PositionModeActionRequired, trade_creation_id},
    rabbitmq::*,
};
use lapin::{
    BasicProperties, Channel, Confirmation,
    options::BasicPublishOptions,
    types::{AMQPValue, FieldTable},
};
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Semaphore,
    time::{Instant, timeout_at},
};

#[derive(Clone, Copy)]
enum Destination {
    Trade,
    Admission,
    Retry,
    DeadLetter,
}

/// Prepared once, including headers and timestamps, and reused unchanged on retry.
/// Payloads can contain credentials, so this type deliberately omits Debug.
pub struct PreparedPublication {
    destination: Destination,
    body: Vec<u8>,
    properties: BasicProperties,
    expires_at: Option<u64>,
}
impl PreparedPublication {
    pub fn trade(message: &NewTradeMessage, idempotency_key: &str) -> Result<Self, RabbitError> {
        let id = trade_creation_id(idempotency_key);
        if idempotency_key.is_empty()
            || message.trade_object.get("id").and_then(|v| v.as_str()) != Some(&id)
        {
            return Err(RabbitError::InvalidPayload);
        }
        let mut headers = FieldTable::default();
        headers.insert(
            IDEMPOTENCY_HEADER.into(),
            AMQPValue::LongString(idempotency_key.into()),
        );
        Self::prepare(
            Destination::Trade,
            serde_json::to_vec(message).map_err(|_| RabbitError::InvalidPayload)?,
            Some(&id),
            headers,
            Some(message.expires_at),
        )
    }
    pub fn admission(message: &PositionModeActionRequired) -> Result<Self, RabbitError> {
        Self::prepare(
            Destination::Admission,
            serde_json::to_vec(message).map_err(|_| RabbitError::InvalidPayload)?,
            Some(&message.event_id),
            FieldTable::default(),
            None,
        )
    }
    pub fn retry(
        body: Vec<u8>,
        message_id: Option<&str>,
        headers: FieldTable,
    ) -> Result<Self, RabbitError> {
        Self::prepare(Destination::Retry, body, message_id, headers, None)
    }
    pub fn dead_letter(message: &DeadLetterMessage) -> Result<Self, RabbitError> {
        Self::prepare(
            Destination::DeadLetter,
            serde_json::to_vec(message).map_err(|_| RabbitError::InvalidPayload)?,
            None,
            FieldTable::default(),
            None,
        )
    }
    fn prepare(
        destination: Destination,
        body: Vec<u8>,
        id: Option<&str>,
        mut headers: FieldTable,
        expires_at: Option<u64>,
    ) -> Result<Self, RabbitError> {
        if id.is_some_and(|id| {
            id.len() > 255 || (id.is_empty() && !matches!(destination, Destination::Retry))
        }) {
            return Err(RabbitError::InvalidPayload);
        }
        let now = now_ms();
        headers.insert(
            PUBLISHED_AT_HEADER.into(),
            AMQPValue::LongLongInt(now as i64),
        );
        let mut properties = BasicProperties::default()
            .with_delivery_mode(1)
            .with_content_type("application/json".into())
            .with_timestamp(now / 1000)
            .with_headers(headers);
        if let Some(id) = id {
            properties = properties.with_message_id(id.into());
        }
        Ok(Self {
            destination,
            body,
            properties,
            expires_at,
        })
    }
    pub(super) fn set_retry_metadata(&mut self, id: Option<&str>, headers: FieldTable) {
        self.properties = self.properties.clone().with_headers(headers);
        if let Some(id) = id {
            self.properties = self.properties.clone().with_message_id(id.into());
        }
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone)]
pub struct Publisher {
    pub(super) channel: Channel,
    pub(super) session: Arc<Session>,
    pub(super) permits: Arc<Semaphore>,
    pub(super) capacity: u32,
    pub(super) timeout: Duration,
    pub(super) trade_queue: String,
    pub(super) retry_queue: String,
}
impl Publisher {
    pub async fn publish(&self, message: &PreparedPublication) -> Result<(), RabbitError> {
        self.session.check()?;
        if self.session.publishing_closed.load(Ordering::Acquire) {
            return Err(RabbitError::Unavailable);
        }
        let deadline = Instant::now() + self.timeout;
        let _permit = timeout_at(deadline, self.permits.acquire())
            .await
            .map_err(|_| RabbitError::Timeout)?
            .map_err(|_| RabbitError::Unavailable)?;
        self.session.check()?;
        if self.session.publishing_closed.load(Ordering::Acquire) {
            return Err(RabbitError::Unavailable);
        }
        if message.expires_at.is_some_and(|expiry| now_ms() >= expiry) {
            return Err(RabbitError::Expired);
        }
        let queue = match message.destination {
            Destination::Trade => &self.trade_queue,
            Destination::Admission => ADMISSION_EVENT_QUEUE,
            Destination::Retry => &self.retry_queue,
            Destination::DeadLetter => DEAD_LETTER_QUEUE,
        };
        let mut guard = OperationGuard {
            session: self.session.clone(),
            error: RabbitError::PublishUncertain,
            armed: true,
        };
        let confirmation = timeout_at(deadline, async {
            self.channel
                .basic_publish(
                    "".into(),
                    queue.into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    &message.body,
                    message.properties.clone(),
                )
                .await?
                .await
        })
        .await
        .map_err(|_| RabbitError::PublishUncertain)?
        .map_err(|_| RabbitError::PublishUncertain)?;
        let result = match confirmation {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_)) => {
                Err(RabbitError::Unroutable)
            }
            Confirmation::Nack(None) => Err(RabbitError::Nack),
            Confirmation::NotRequested => Err(RabbitError::PublishUncertain),
        };
        guard.armed = matches!(result, Err(RabbitError::PublishUncertain));
        result
    }
    pub(super) async fn flush(&self, limit: Duration) -> Result<(), RabbitError> {
        self.session
            .publishing_closed
            .store(true, Ordering::Release);
        let _all = tokio::time::timeout(limit, self.permits.acquire_many(self.capacity))
            .await
            .map_err(|_| RabbitError::Timeout)?
            .map_err(|_| RabbitError::Unavailable)?;
        Ok(())
    }
}
