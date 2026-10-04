use serde::Serialize;

/// Decimal strings retain the original TypeScript wire representation.
#[derive(Debug, Serialize)]
pub struct TradingSignal {
    pub exchange_client: &'static str,
    pub symbol: String,
    pub base_currency: &'static str,
    pub coin: String,
    pub is_long: bool,
    pub buy_targets: Vec<String>,
    pub sell_targets: Vec<String>,
    pub stop_loss: String,
    pub leverage: String,
    /// Optional signal override as a balance fraction (0.5% → 0.005).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<f64>,
    /// Preserved for later STOP_LOSS_LIMIT entry-target construction.
    #[serde(rename = "breakOutEntry", skip_serializing_if = "Option::is_none")]
    pub breakout_entry: Option<bool>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum SignalError {
    MissingField,
    AmbiguousHeader,
    DuplicateField,
    InvalidNumber,
    InvalidPriceLevels,
    UnsupportedFormat,
}
#[derive(Debug)]
pub enum ParseOutcome {
    Parsed(TradingSignal),
    NotSignal,
    Rejected(SignalError),
}
