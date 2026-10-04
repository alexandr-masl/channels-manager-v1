//! Base USDT Futures grammar; no database or exchange access.
mod model;
mod parser;
mod validation;
pub use model::{ParseOutcome, SignalError, TradingSignal};
pub use parser::parse_signal;
