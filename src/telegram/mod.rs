//! Raw Telegram intake; signal parsing and channel authorization follow separately.
mod handler;
mod message;
mod workflow;

pub use handler::TelegramHandler;
pub use message::TelegramMessage;
pub use workflow::{IntakeOutcome, RejectReason, SkipReason, inspect_message};
