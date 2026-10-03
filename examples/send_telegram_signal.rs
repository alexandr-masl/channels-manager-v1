//! cargo run --example send_telegram_signal -- [channel-id]
use channels_manager_v1::{config::AppConfig, contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE};
use lapin::{
    BasicProperties, Confirmation, Connection, ConnectionProperties, options::*, types::FieldTable,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

async fn send() -> Result<(), &'static str> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() > 1 {
        return Err("usage: cargo run --example send_telegram_signal -- [channel-id]");
    }
    let channel_id: i64 = args
        .first()
        .map(String::as_str)
        .unwrap_or("-1001596367704")
        .parse()
        .map_err(|_| "channel-id must be an integer")?;
    let config = AppConfig::from_env()
        .map_err(|_| "invalid configuration; run cargo run -- --check-config")?;
    let uri = config.rabbitmq.uri.expose();
    let parsed = url::Url::parse(uri).map_err(|_| "invalid RabbitMQ URL")?;
    if !matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")) {
        return Err("test sender requires a local RabbitMQ broker");
    }
    let connection = Connection::connect(uri, ConnectionProperties::default())
        .await
        .map_err(|_| "RabbitMQ connection failed")?;
    let result = async {
        let channel = connection.create_channel().await.map_err(|_| "RabbitMQ channel failed")?;
        let queue = channel.queue_declare(TELEGRAM_CHANNEL_QUEUE.into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default()).await.map_err(|_| "queue unavailable; start cargo run first")?;
        if queue.consumer_count() == 0 { return Err("no consumer; start cargo run first (nothing published)"); }
        channel.confirm_select(ConfirmSelectOptions::default()).await.map_err(|_| "confirms unavailable")?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| "clock unavailable")?;
        let message_id = now.as_millis() as u64;
        let payload = serde_json::to_vec(&serde_json::json!({
            "message_id": message_id, "date": now.as_secs(),
            "chat": {"id": channel_id, "type": "channel"},
            "text": include_str!("fixtures/ada-signal.txt").trim_end()
        })).map_err(|_| "cannot encode message")?;
        let confirmation = channel.basic_publish("".into(), TELEGRAM_CHANNEL_QUEUE.into(), BasicPublishOptions { mandatory: true, ..Default::default() }, &payload,
            BasicProperties::default().with_content_type("application/json".into()).with_delivery_mode(1).with_timestamp(now.as_secs()).with_message_id(message_id.to_string().into()).with_expiration("60000".into())
        ).await.map_err(|_| "publish outcome unknown; check before resending")?.await.map_err(|_| "confirmation missing; check before resending")?;
        match confirmation {
            Confirmation::Ack(None) => { println!("Broker confirmed ADA signal to {TELEGRAM_CHANNEL_QUEUE}; message_id={message_id}. Check the app terminal."); Ok(()) },
            Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_)) => Err("message was unroutable"),
            _ => Err("message was not confirmed; check before resending"),
        }
    }.await;
    let _ = connection.close(200, "test finished".into()).await;
    result
}
#[tokio::main]
async fn main() -> Result<(), &'static str> {
    tokio::time::timeout(Duration::from_secs(15), send())
        .await
        .map_err(|_| "sender timed out; check before resending")?
}
