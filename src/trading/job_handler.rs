use super::execution::{ClientTradeWorker, ProductionAdmission, WorkerOutcome};
use crate::{
    infrastructure::{DeliveryHandler, WorkerServices},
    rabbitmq::{DeliveryOutcome, InboundDelivery, RabbitError, RetryReason},
};
use futures_util::future::BoxFuture;

pub struct ClientTradeHandler;
impl DeliveryHandler for ClientTradeHandler {
    fn handle(
        &self,
        delivery: InboundDelivery,
        services: WorkerServices,
    ) -> BoxFuture<'static, Result<(), RabbitError>> {
        Box::pin(async move {
            let dependencies = ProductionAdmission {
                trades: &services.mongo.trades,
                exchange: &services.admission_client,
            };
            let outcome = ClientTradeWorker::new(&dependencies, services.operation_timeout)
                .prepare(delivery.body(), || {
                    mongodb::bson::DateTime::now()
                        .timestamp_millis()
                        .try_into()
                        .unwrap_or(u64::MAX)
                })
                .await;
            let settlement = match outcome {
                WorkerOutcome::Admitted(prepared) => {
                    println!(
                        "Client job admitted: channel_id={} message_id={} symbol={} positionConfiguration={} margin_mode={} expires_at_ms={} execution_enabled=false",
                        prepared.job.channel_id,
                        prepared.job.message_id,
                        prepared.job.symbol,
                        prepared.admission.position_configuration.as_str(),
                        prepared.settings.margin_mode,
                        prepared.job.trade_expires_at.unwrap()
                    );
                    DeliveryOutcome::Completed
                }
                WorkerOutcome::Rejected(reason) => {
                    println!("Client job rejected: reason={reason:?}");
                    DeliveryOutcome::Rejected
                }
                WorkerOutcome::Expired => {
                    println!("Client job expired");
                    DeliveryOutcome::Rejected
                }
                WorkerOutcome::Retryable(code) => {
                    println!("Client job retry: reason={code}");
                    DeliveryOutcome::PreClaimRetry(RetryReason::DependencyUnavailable)
                }
            };
            // Slice 3 is admission-only. There is deliberately no trade publisher call.
            services
                .delivery_policy
                .settle(delivery, settlement, &services.publisher)
                .await
        })
    }
}
