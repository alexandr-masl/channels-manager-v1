//! Telegram intake, signal parsing, authorization and account context.
mod channel_update;
mod context;
mod handler;
mod message;
mod workflow;

pub use channel_update::{
    ChannelOutcome, ChannelReject, ChannelRepository, ChannelSkip, ChannelUpdateManager,
};
pub use context::{ChannelClient, ChannelContext};
pub use handler::TelegramHandler;
pub use message::TelegramMessage;
pub use workflow::{IntakeOutcome, RejectReason, SkipReason, inspect_message};
