use super::{ChannelOutcome, ChannelUpdateManager, IntakeOutcome, inspect_message};
use crate::{
    contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE,
    infrastructure::{DeliveryHandler, WorkerServices},
    rabbitmq::{DeliveryOutcome, InboundDelivery, RabbitError, RetryReason},
    signals::manager::{PreparationOutcome, SignalManager},
};
use futures_util::future::BoxFuture;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TelegramHandler;
impl DeliveryHandler for TelegramHandler {
    fn handle(
        &self,
        delivery: InboundDelivery,
        services: WorkerServices,
    ) -> BoxFuture<'static, Result<(), RabbitError>> {
        Box::pin(async move {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| RabbitError::Unavailable)?
                .as_millis() as u64;
            match inspect_message(delivery.body(), now) {
                IntakeOutcome::Received(message) => {
                    println!(
                        "Incoming {TELEGRAM_CHANNEL_QUEUE}: channel_id={} message_id={} source_created_at_ms={} text={:?}",
                        message.channel_id(),
                        message.message_id(),
                        message.source_created_at_ms(),
                        message.text()
                    );
                    let channel_id = message.channel_id();
                    let message_id = message.message_id();
                    match ChannelUpdateManager::new(&services.mongo)
                        .handle_channel_update(message, now)
                        .await
                    {
                        Ok(ChannelOutcome::Ready(context)) => {
                            println!(
                                "Signal parsed: channel_id={channel_id} message_id={message_id} result={}",
                                serde_json::to_string(&context.signal)
                                    .map_err(|_| RabbitError::InvalidPayload)?
                            );
                            println!(
                                "Channel context ready: channel_id={channel_id} message_id={message_id} eligible_accounts={} eligible_users={}",
                                context.clients.len(),
                                context
                                    .clients
                                    .iter()
                                    .map(|client| client.chat_id)
                                    .collect::<std::collections::BTreeSet<_>>()
                                    .len()
                            );
                            match SignalManager::new(
                                &services.mongo.trades,
                                services.market.as_ref(),
                            )
                            .prepare(*context, || {
                                mongodb::bson::DateTime::now()
                                    .timestamp_millis()
                                    .try_into()
                                    .unwrap_or(0)
                            })
                            .await
                            {
                                Ok(PreparationOutcome::Prepared(mut batch)) => {
                                    println!(
                                        "Signal prepared: {}",
                                        serde_json::to_string(&batch.summary)
                                            .map_err(|_| RabbitError::InvalidPayload)?
                                    );
                                    if let Some(sender) = &services.telegram_sender {
                                        match sender.send_accepted(channel_id, message_id).await {
                                            Ok(reply_id) => println!(
                                                "Signal notification sent: channel_id={channel_id} message_id={message_id} reply_id={reply_id}"
                                            ),
                                            Err(reason) => println!(
                                                "Signal notification failed: channel_id={channel_id} message_id={message_id} reason={reason:?}"
                                            ),
                                        }
                                    }
                                    if let Some(publication) = &services.job_publication {
                                        match publication
                                            .publish(&batch.jobs, &services.publisher)
                                            .await
                                        {
                                            Ok(count) => {
                                                batch.summary.published_jobs = count;
                                                println!(
                                                    "Signal published: {}",
                                                    serde_json::to_string(&batch.summary)
                                                        .map_err(|_| RabbitError::InvalidPayload)?
                                                );
                                            }
                                            Err(failure) => {
                                                eprintln!(
                                                    "Signal publication stopped: channel_id={channel_id} message_id={message_id} published_jobs={} prepared_jobs={} reason={:?}",
                                                    failure.published_jobs,
                                                    batch.jobs.len(),
                                                    failure.error
                                                );
                                                let outcome = match failure.error {
                                                    RabbitError::Expired
                                                    | RabbitError::InvalidPayload => {
                                                        DeliveryOutcome::Rejected
                                                    }
                                                    RabbitError::Nack
                                                    | RabbitError::Unroutable
                                                    | RabbitError::Timeout => {
                                                        DeliveryOutcome::PreClaimRetry(
                                                            RetryReason::DependencyUnavailable,
                                                        )
                                                    }
                                                    // A failed session cannot safely publish a replacement. Dropping
                                                    // the unsettled delivery lets lifecycle recovery return it.
                                                    error => return Err(error),
                                                };
                                                return services
                                                    .delivery_policy
                                                    .settle(delivery, outcome, &services.publisher)
                                                    .await;
                                            }
                                        }
                                    }
                                }
                                Ok(PreparationOutcome::Skipped(reason)) => println!(
                                    "Signal preparation skipped: channel_id={channel_id} message_id={message_id} reason={reason:?}"
                                ),
                                Ok(PreparationOutcome::Rejected(reason)) => println!(
                                    "Signal preparation rejected: channel_id={channel_id} message_id={message_id} reason={reason:?}"
                                ),
                                Err(error) => {
                                    eprintln!(
                                        "Signal preparation retry: channel_id={channel_id} message_id={message_id} reason={error:?}"
                                    );
                                    return services
                                        .delivery_policy
                                        .settle(
                                            delivery,
                                            DeliveryOutcome::PreClaimRetry(
                                                RetryReason::DependencyUnavailable,
                                            ),
                                            &services.publisher,
                                        )
                                        .await;
                                }
                            }
                        }
                        Ok(ChannelOutcome::Skipped(reason)) => println!(
                            "Channel update skipped: channel_id={channel_id} message_id={message_id} reason={reason:?}"
                        ),
                        Ok(ChannelOutcome::Rejected(reason)) => println!(
                            "Channel update rejected: channel_id={channel_id} message_id={message_id} reason={reason:?}"
                        ),
                        Err(error) => {
                            eprintln!(
                                "Channel update retry: channel_id={channel_id} message_id={message_id} reason={error}"
                            );
                            return services
                                .delivery_policy
                                .settle(
                                    delivery,
                                    DeliveryOutcome::PreClaimRetry(
                                        RetryReason::DependencyUnavailable,
                                    ),
                                    &services.publisher,
                                )
                                .await;
                        }
                    }
                }
                IntakeOutcome::Skipped(reason) => println!("Telegram intake skipped: {reason:?}"),
                IntakeOutcome::Rejected(reason) => println!("Telegram intake rejected: {reason:?}"),
            }
            // Enabled publication reaches here only after all jobs are confirmed.
            delivery.ack().await
        })
    }
}
