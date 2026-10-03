//! RabbitMQ transport. Delivery retry policy and trade business logic are separate.
mod connection;
mod consumer;
mod publication;
mod transport;

pub use connection::{RabbitMq, queue_arguments};
pub use consumer::{InboundDelivery, RabbitConsumer};
pub use publication::{PreparedPublication, Publisher};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RabbitError {
    InvalidConfiguration,
    InvalidPayload,
    Topology,
    Unavailable,
    Timeout,
    Unroutable,
    Nack,
    PublishUncertain,
    SettlementUncertain,
    DeliveryAbandoned,
    ConsumerCancelled,
    Expired,
}
impl std::fmt::Display for RabbitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RabbitMQ: {self:?}")
    }
}
impl std::error::Error for RabbitError {}
impl From<RabbitError> for crate::runtime::Failure {
    fn from(error: RabbitError) -> Self {
        match error {
            RabbitError::InvalidConfiguration
            | RabbitError::InvalidPayload
            | RabbitError::Topology => Self::permanent("RABBITMQ_CONTRACT"),
            _ => Self::restart("RABBITMQ_UNAVAILABLE"),
        }
    }
}

pub(crate) struct Session {
    pub intake: CancellationToken,
    pub closed: CancellationToken,
    pub publishing_closed: AtomicBool,
    failure: watch::Sender<Option<RabbitError>>,
}
impl Session {
    fn new() -> Arc<Self> {
        let (failure, _) = watch::channel(None);
        Arc::new(Self {
            intake: CancellationToken::new(),
            closed: CancellationToken::new(),
            publishing_closed: AtomicBool::new(false),
            failure,
        })
    }
    pub fn fail(&self, error: RabbitError) {
        self.failure.send_if_modified(|slot| {
            if slot.is_none() {
                *slot = Some(error);
                true
            } else {
                false
            }
        });
        self.intake.cancel();
    }
    pub fn check(&self) -> Result<(), RabbitError> {
        if self.closed.is_cancelled() || self.failure.borrow().is_some() {
            Err(RabbitError::Unavailable)
        } else {
            Ok(())
        }
    }
    pub async fn wait_for_failure(&self) -> RabbitError {
        let mut receiver = self.failure.subscribe();
        loop {
            if let Some(error) = *receiver.borrow_and_update() {
                return error;
            }
            if receiver.changed().await.is_err() {
                return RabbitError::Unavailable;
            }
        }
    }
    fn stop(&self) {
        self.intake.cancel();
        self.closed.cancel();
        self.publishing_closed.store(true, Ordering::Release);
    }
}
struct OperationGuard {
    session: Arc<Session>,
    error: RabbitError,
    armed: bool,
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.session.fail(self.error);
        }
    }
}
