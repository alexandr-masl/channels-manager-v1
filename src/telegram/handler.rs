use super::{IntakeOutcome, inspect_message};
use crate::{
    contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE,
    infrastructure::{DeliveryHandler, WorkerServices},
    rabbitmq::{InboundDelivery, RabbitError},
    signals::{ParseOutcome, parse_signal},
};
use futures_util::future::BoxFuture;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TelegramHandler;
impl DeliveryHandler for TelegramHandler {
    fn handle(
        &self,
        delivery: InboundDelivery,
        _services: WorkerServices,
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
                    match parse_signal(message.text()) {
                        ParseOutcome::Parsed(signal) => println!(
                            "Signal parsed: channel_id={} message_id={} result={}",
                            message.channel_id(),
                            message.message_id(),
                            serde_json::to_string(&signal)
                                .map_err(|_| RabbitError::InvalidPayload)?
                        ),
                        ParseOutcome::NotSignal => println!("Signal skipped: NotSignal"),
                        ParseOutcome::Rejected(reason) => println!("Signal rejected: {reason:?}"),
                    }
                }
                IntakeOutcome::Skipped(reason) => println!("Telegram intake skipped: {reason:?}"),
                IntakeOutcome::Rejected(reason) => println!("Telegram intake rejected: {reason:?}"),
            }
            // Slice 2 ends at parsed-result logging. Future publication must finish before ack.
            delivery.ack().await
        })
    }
}
