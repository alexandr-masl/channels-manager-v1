//! Validate the worker boundary before invoking any external dependency.
use crate::{
    contracts::messages::{ClientTradeJob, TradeIdentity},
    signals::settings::{number, numeric_id, positive_number},
};
use serde_json::Value;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobValidationError {
    MissingExpiry,
    Expired,
    InvalidIdentity,
    InvalidClient,
    InvalidSymbol,
    InvalidSignal,
    InvalidMarketData,
}
/// Holds a reference to the unchanged wire job. Deliberately has no Debug implementation.
pub struct ValidatedJob<'a> {
    pub job: &'a ClientTradeJob,
    pub chat_id: i64,
    pub client_id: &'a str,
    pub expires_at: u64,
    pub normalized_symbol: String,
    pub requested_leverage: u32,
    pub is_long: bool,
}
pub fn validate_job(
    job: &ClientTradeJob,
    now_ms: u64,
) -> Result<ValidatedJob<'_>, JobValidationError> {
    let expires_at = job
        .trade_expires_at
        .filter(|v| *v > 0)
        .ok_or(JobValidationError::MissingExpiry)?;
    if now_ms >= expires_at {
        return Err(JobValidationError::Expired);
    }
    let client_id = nonempty(&job.client["clientId"]).ok_or(JobValidationError::InvalidClient)?;
    let chat_id = numeric_id(&job.client["chatId"])
        .filter(|n| *n > 0)
        .ok_or(JobValidationError::InvalidClient)?;
    if job.client["provider"] != "BingX"
        || nonempty(&job.client["key"]).is_none()
        || nonempty(&job.client["keySecret"]).is_none()
    {
        return Err(JobValidationError::InvalidClient);
    }
    let coin = job
        .symbol
        .strip_suffix("USDT")
        .filter(|c| {
            !c.is_empty()
                && c.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        })
        .ok_or(JobValidationError::InvalidSymbol)?;
    if job.signal_data["symbol"] != job.symbol {
        return Err(JobValidationError::InvalidSymbol);
    }
    if job.channel_id == 0
        || job.message_id <= 0
        || job.channel_id.unsigned_abs() > 9_007_199_254_740_991
        || job.message_id > 9_007_199_254_740_991
    {
        return Err(JobValidationError::InvalidIdentity);
    }
    let identity = TradeIdentity {
        channel_id: job.channel_id,
        message_id: job.message_id,
        chat_id,
        client_id,
        symbol: &job.symbol,
    };
    if job.idempotency_key != identity.idempotency_key()
        || job.partition_key != identity.partition_key()
    {
        return Err(JobValidationError::InvalidIdentity);
    }
    let signal = &job.signal_data;
    if signal["exchange_client"] != "_futures" {
        return Err(JobValidationError::InvalidSignal);
    }
    let is_long = signal["is_long"]
        .as_bool()
        .ok_or(JobValidationError::InvalidSignal)?;
    let leverage = signal["leverage"]
        .as_str()
        .map(|s| Value::String(s.strip_suffix(['x', 'X']).unwrap_or(s).to_owned()))
        .unwrap_or_else(|| signal["leverage"].clone());
    let leverage = positive_number(&leverage)
        .filter(|n| n.fract() == 0.0 && *n <= u32::MAX as f64)
        .ok_or(JobValidationError::InvalidSignal)?;
    for field in ["buy_targets", "sell_targets"] {
        if !signal[field]
            .as_array()
            .is_some_and(|ts| !ts.is_empty() && ts.iter().all(|t| positive_number(t).is_some()))
        {
            return Err(JobValidationError::InvalidSignal);
        }
    }
    positive_number(&signal["stop_loss"]).ok_or(JobValidationError::InvalidSignal)?;
    if signal
        .get("position")
        .is_some_and(|v| positive_number(v).is_none())
        || signal.get("breakOutEntry").is_some_and(|v| !v.is_boolean())
    {
        return Err(JobValidationError::InvalidSignal);
    }
    let snapshot = &job.market_data["bingXFutures"];
    let info = &snapshot["symbolInfo"];
    let normalized_symbol = format!("{coin}-USDT");
    if positive_number(&snapshot["currPrice"]).is_none()
        || info["symbol"] != normalized_symbol
        || number(&info["status"]) != Some(1.0)
        || ["tickSize", "lotSize", "minQty"]
            .iter()
            .any(|key| positive_number(&info[key]).is_none())
        || !number(&info["minNotional"]).is_some_and(|n| n >= 0.0)
        || info.get("err").is_some()
        || snapshot.get("err").is_some()
    {
        return Err(JobValidationError::InvalidMarketData);
    }
    Ok(ValidatedJob {
        job,
        chat_id,
        client_id,
        expires_at,
        normalized_symbol,
        requested_leverage: leverage as u32,
        is_long,
    })
}
fn nonempty(value: &Value) -> Option<&str> {
    value.as_str().filter(|s| !s.trim().is_empty())
}
