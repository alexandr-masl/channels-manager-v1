//! Wire envelopes. Nested trading schemas stay opaque until their logic migrates.
//! Payloads deliberately do not implement Debug: client data includes API secrets.
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const TRADE_EXPIRY_AFTER_ACCEPTANCE_MS: u64 = 60_000;
pub const SOURCE_MAX_AGE_MS: u64 = 600_000;
pub const SOURCE_FUTURE_TOLERANCE_MS: u64 = 120_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Provider {
    BingX,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Market {
    #[serde(rename = "futures")]
    Futures,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobType {
    #[serde(rename = "create_trade")]
    CreateTrade,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "u8", into = "u8")]
pub enum JobVersion {
    V1,
}

impl TryFrom<u8> for JobVersion {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value == 1 {
            Ok(Self::V1)
        } else {
            Err("unsupported client-trade job version")
        }
    }
}
impl From<JobVersion> for u8 {
    fn from(_: JobVersion) -> u8 {
        1
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTradeJob {
    pub job_type: JobType,
    pub version: JobVersion,
    #[serde(rename = "channelID")]
    pub channel_id: i64,
    pub message_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_expires_at: Option<u64>,
    pub signal_data: Value,
    pub channel_settings: Value,
    pub client: Value,
    // JSON.stringify omits undefined userConfig. Preserve explicit null as well.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_json"
    )]
    pub user_config: Option<Value>,
    pub market_data: Value,
    pub opened_trades: Vec<Value>,
    pub idempotency_key: String,
    pub partition_key: String,
    pub provider: Provider,
    pub market: Market,
    pub symbol: String,
}

fn present_json<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// Expiry is supplied by the upstream job. There is no clock-based default.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewTradeMessage {
    pub expires_at: u64,
    pub trade_object: Value,
    pub client_data: Value,
}

impl NewTradeMessage {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionRoute {
    #[serde(rename = "ORDER_LEDGER_V1")]
    Hedge,
    #[serde(rename = "ONE_WAY_V1")]
    OneWay,
}

impl ExecutionRoute {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hedge => "ORDER_LEDGER_V1",
            Self::OneWay => "ONE_WAY_V1",
        }
    }
}

pub struct TradeIdentity<'a> {
    pub channel_id: i64,
    pub message_id: i64,
    pub chat_id: i64,
    pub client_id: &'a str,
    pub symbol: &'a str,
}

impl TradeIdentity<'_> {
    pub fn idempotency_key(&self) -> String {
        format!(
            "auto-trade:{}:{}:{}:{}:BingX:futures:{}",
            self.channel_id, self.message_id, self.chat_id, self.client_id, self.symbol
        )
    }
    pub fn partition_key(&self) -> String {
        format!("client-symbol:{}:futures:{}", self.client_id, self.symbol)
    }
}

pub fn trade_creation_id(idempotency_key: &str) -> String {
    format!("{:x}", Sha256::digest(idempotency_key.as_bytes()))[..32].to_owned()
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SignalSide {
    #[serde(rename = "LONG")]
    Long,
    #[serde(rename = "SHORT")]
    Short,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum AdmissionEventType {
    #[serde(rename = "BINGX_FUTURES_POSITION_MODE_ACTION_REQUIRED")]
    PositionModeActionRequired,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum AdmissionReason {
    #[serde(rename = "MIGRATION_REQUIRED")]
    MigrationRequired,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PositionModeActionRequired {
    pub event_type: AdmissionEventType,
    pub event_id: String,
    pub chat_id: i64,
    pub exchange_client_id: String,
    pub provider: Provider,
    pub market: Market,
    pub normalized_symbol: String,
    pub channel_id: i64,
    pub signal_message_id: i64,
    pub signal_symbol: String,
    pub signal_side: SignalSide,
    pub reason_code: AdmissionReason,
    pub created_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterMessage {
    pub original_queue: String,
    pub payload: Value,
    pub error: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub dead_lettered_at: String,
}
