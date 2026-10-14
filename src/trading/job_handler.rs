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
                    use crate::exchanges::bingx::trade_builder::{TradeBuildError, build_trade};
                    match build_trade(
                        &prepared,
                        services.static_low_balance_fallback_ratio_futures,
                        super::publication::now_ms(),
                    ) {
                        Ok(trade) => match services
                            .trade_publication
                            .publish(&trade, &prepared.job.idempotency_key, &services.publisher)
                            .await
                        {
                            Ok(()) => {
                                println!(
                                    "Client trade published: channel_id={} message_id={} symbol={} positionConfiguration={} expires_at_ms={}",
                                    prepared.job.channel_id,
                                    prepared.job.message_id,
                                    prepared.job.symbol,
                                    prepared.admission.position_configuration.as_str(),
                                    trade.expires_at
                                );
                                DeliveryOutcome::Completed
                            }
                            Err(RabbitError::Expired) => {
                                println!("Client trade expired before publication");
                                DeliveryOutcome::Rejected
                            }
                            Err(
                                RabbitError::Nack | RabbitError::Unroutable | RabbitError::Timeout,
                            ) => {
                                println!(
                                    "Client trade publication retry: reason=publicationRejected"
                                );
                                DeliveryOutcome::PreClaimRetry(RetryReason::DependencyUnavailable)
                            }
                            // Dropping an unsettled delivery faults its session;
                            // recovery returns the source to the broker.
                            Err(error) => return Err(error),
                        },
                        Err(TradeBuildError::Expired) => {
                            println!("Client trade expired before construction");
                            DeliveryOutcome::Rejected
                        }
                        Err(TradeBuildError::Rejected(reason)) => {
                            println!("Client trade rejected: reason={reason}");
                            DeliveryOutcome::Rejected
                        }
                    }
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
            services
                .delivery_policy
                .settle(delivery, settlement, &services.publisher)
                .await
        })
    }
}
