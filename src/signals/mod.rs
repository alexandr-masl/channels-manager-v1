//! Signal parsing and preparation; execution and publication are separate stages.
mod jobs;
pub mod manager;
mod model;
mod parser;
pub mod publication;
pub(crate) mod targets;
mod validation;
pub use model::{ParseOutcome, SignalError, TradingSignal};
pub use parser::parse_signal;
pub use targets::{PercentageTarget, ProfitTarget};

pub mod settings;
