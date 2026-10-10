use super::{ChannelOutcome, ChannelUpdateManager, IntakeOutcome, inspect_message};
use crate::{
    contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE,
    infrastructure::{DeliveryHandler, WorkerServices},
    rabbitmq::{DeliveryOutcome, InboundDelivery, RabbitError, RetryReason},
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
            // Slice 3 ends at context logging. Future job publication must finish before ack.
            delivery.ack().await
        })
    }
}
