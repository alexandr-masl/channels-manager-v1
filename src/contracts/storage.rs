use serde_json::{Value, json};
use std::time::Duration;

pub const DEFAULT_LOCK_PREFIX: &str = "satoshi-channel-updates:locks:v1";
pub const DEFAULT_CACHE_PREFIX: &str = "satoshi-channel-updates:exchange-metadata:v1";
pub const REDIS_NOTIFICATION_CHANNEL: &str = "satoshiChannelCommand";
pub const ACCOUNT_LEASE_TTL: Duration = Duration::from_secs(30);
pub const ACCOUNT_LEASE_RENEWAL: Duration = Duration::from_secs(10);
pub const ACCOUNT_LEASE_COMMAND_TIMEOUT: Duration = Duration::from_secs(1);
pub const ACCOUNT_LEASE_ACQUISITION_WINDOW: Duration = Duration::from_secs(5);
pub const EXECUTION_DEADLINE: Duration = Duration::from_secs(90);
pub const EXECUTION_CLEANUP_DEADLINE: Duration = Duration::from_secs(2);

pub const CLAIMS_COLLECTION: &str = "bingx_futures_execution_claims";
pub const USER_CONFIGS_COLLECTION: &str = "user_configs";
pub const ACTIVE_TRADES_COLLECTION: &str = "trade_station_active_trades";
pub const ACCOUNTS_COLLECTION: &str = "users";
pub const CLAIMS_INDEX: &str = "uq_bingx_futures_execution_claim_work_id";
pub const CLAIM_WRITE_CONCERN: &str = "majority";
pub const CLAIM_JOURNALED: bool = true;
pub const CLAIM_READ_CONCERN: &str = "majority";
pub const CLAIM_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const CLAIM_OPERATION_DEADLINE: Duration = Duration::from_secs(6);
pub const CLAIM_TERMINAL_DEADLINE: Duration = Duration::from_secs(2);
pub const USER_NOTIFICATION_LIMIT: usize = 7;

#[derive(Debug)]
pub struct IndexContract {
    pub name: &'static str,
    pub keys: Value,
    pub unique: bool,
    pub expire_after_seconds: Option<u64>,
}

pub fn execution_claim_index() -> IndexContract {
    IndexContract {
        name: CLAIMS_INDEX,
        keys: json!({"workId":1}),
        unique: true,
        expire_after_seconds: None,
    }
}

pub fn account_lease_key(prefix: &str, exchange_client_id: &str) -> String {
    format!(
        "{prefix}:bingx_futures_account_admission:{}",
        encode_uri_component(&format!("BingX:futures:{exchange_client_id}"))
    )
}

pub fn bingx_metadata_key(prefix: &str, symbol: &str) -> String {
    format!(
        "{prefix}:bingx_symbol_metadata:{}",
        encode_uri_component(symbol)
    )
}

// JavaScript encodeURIComponent operates on UTF-8 bytes and leaves these exact
// ASCII characters unescaped (unlike form-urlencoding, spaces become %20).
fn encode_uri_component(value: &str) -> String {
    use std::fmt::Write;
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            write!(encoded, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    encoded
}
