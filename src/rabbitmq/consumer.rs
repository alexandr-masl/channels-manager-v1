use super::{RabbitError, Session};
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Consumer,
    message::Delivery,
    options::{BasicAckOptions, BasicNackOptions},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct RabbitConsumer {
    pub(super) consumer: Consumer,
    pub(super) session: Arc<Session>,
    pub(super) permits: Arc<Semaphore>,
    pub(super) timeout: Duration,
}
impl RabbitConsumer {
    /// Cancellation-safe; a permit is retained until the returned delivery settles/drops.
    pub async fn next(&mut self) -> Result<Option<InboundDelivery>, RabbitError> {
        tokio::select! { biased;
            _=self.session.intake.cancelled()=> { self.session.check()?;Ok(None) },
            result=async {
                let permit=self.permits.clone().acquire_owned().await.map_err(|_|RabbitError::Unavailable)?;
                match self.consumer.next().await {
                    Some(Ok(delivery))=>Ok(Some(InboundDelivery { delivery,session:self.session.clone(),_permit:permit,timeout:self.timeout,settled:false })),
                    _=> { self.session.fail(RabbitError::ConsumerCancelled);Err(RabbitError::ConsumerCancelled) },
                }
            }=>result,
        }
    }
}
impl Drop for RabbitConsumer {
    fn drop(&mut self) {
        if !self.session.intake.is_cancelled() {
            self.session.fail(RabbitError::ConsumerCancelled);
        }
    }
}
/// A delivery is settled only by explicit ack/requeue. Dropping it faults the
/// session so shutdown returns unacknowledged work to the broker.
pub struct InboundDelivery {
    delivery: Delivery,
    session: Arc<Session>,
    _permit: OwnedSemaphorePermit,
    timeout: Duration,
    settled: bool,
}
impl InboundDelivery {
    pub fn body(&self) -> &[u8] {
        &self.delivery.data
    }
    pub fn properties(&self) -> &BasicProperties {
        &self.delivery.properties
    }
    pub fn redelivered(&self) -> bool {
        self.delivery.redelivered
    }
    pub async fn ack(mut self) -> Result<(), RabbitError> {
        self.settle(false).await
    }
    pub async fn requeue(mut self) -> Result<(), RabbitError> {
        self.settle(true).await
    }
    async fn settle(&mut self, requeue: bool) -> Result<(), RabbitError> {
        if self.session.closed.is_cancelled() {
            return Err(RabbitError::Unavailable);
        }
        tokio::time::timeout(self.timeout, async {
            if requeue {
                self.delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    })
                    .await
            } else {
                self.delivery.ack(BasicAckOptions { multiple: false }).await
            }
        })
        .await
        .map_err(|_| RabbitError::SettlementUncertain)?
        .map_err(|_| RabbitError::SettlementUncertain)?;
        self.settled = true;
        Ok(())
    }
}
impl Drop for InboundDelivery {
    fn drop(&mut self) {
        if !self.settled && !self.session.closed.is_cancelled() {
            self.session.fail(RabbitError::DeliveryAbandoned);
        }
    }
}
