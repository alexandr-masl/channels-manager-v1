use crate::config::RabbitMqConfig;
use serde_json::{Value, json};

pub const TELEGRAM_CHANNEL_QUEUE: &str = "tg_bot_channel_update";

pub const BINGX_FUTURES_QUEUE: &str = "satoshi-channel-updates.client-trade.bingx.futures";
pub const DEFAULT_TRADE_QUEUE: &str = "create-new-trusted-trade";
pub const ADMISSION_EVENT_QUEUE: &str = "tg_bot_bingx_position_mode_action_required";
pub const DEAD_LETTER_QUEUE: &str = "satoshi-channel-updates.dead-letter";
pub const RETRY_SUFFIX: &str = ".retry.delay";

pub const PUBLISHED_AT_HEADER: &str = "x-published-at-ms";
pub const IDEMPOTENCY_HEADER: &str = "idempotencyKey";
pub const RETRY_ATTEMPT_HEADER: &str = "x-retry-attempt";
pub const ORIGINAL_QUEUE_HEADER: &str = "x-original-queue";
pub const RETRY_MAX_ATTEMPTS_HEADER: &str = "x-retry-max-attempts";
pub const RETRY_DELAY_HEADER: &str = "x-retry-delay-ms";
pub const FIRST_FAILURE_HEADER: &str = "x-first-failure-at-ms";
pub const LAST_FAILURE_HEADER: &str = "x-last-failure-at-ms";
pub const LAST_ERROR_HEADER: &str = "x-last-error";

#[derive(Debug)]
pub struct QueueContract {
    pub name: String,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: Value,
}

pub fn queue_contracts(config: &RabbitMqConfig) -> Vec<QueueContract> {
    let mut queues: Vec<_> = [
        config.input_queue.to_owned(),
        config.output_queue.clone(),
        ADMISSION_EVENT_QUEUE.into(),
        DEAD_LETTER_QUEUE.into(),
        format!("{}{RETRY_SUFFIX}", config.input_queue),
    ]
    .into_iter()
    .map(|name| QueueContract {
        name,
        durable: false,
        exclusive: false,
        auto_delete: false,
        arguments: json!({}),
    })
    .collect();
    queues.last_mut().unwrap().arguments = json!({
        "x-message-ttl":config.retry_delay.as_millis() as u64,
        "x-dead-letter-exchange":"",
        "x-dead-letter-routing-key":config.input_queue,
    });
    if !queues.iter().any(|q| q.name == BINGX_FUTURES_QUEUE) {
        queues.push(QueueContract {
            name: BINGX_FUTURES_QUEUE.into(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: json!({}),
        });
    }
    queues
}

#[derive(Debug)]
pub struct TradePublishContract {
    pub exchange: &'static str,
    pub delivery_mode: u8,
    pub mandatory: bool,
    pub confirm: bool,
    pub content_type: &'static str,
}

pub const fn trade_publish_contract() -> TradePublishContract {
    TradePublishContract {
        exchange: "",
        delivery_mode: 1,
        mandatory: true,
        confirm: true,
        content_type: "application/json",
    }
}
