use super::{TelegramMessage, message::Envelope};
use crate::contracts::messages::{SOURCE_FUTURE_TOLERANCE_MS, SOURCE_MAX_AGE_MS};

#[derive(Debug, PartialEq, Eq)]
pub enum RejectReason {
    InvalidEnvelope,
    InvalidIdentity,
    InvalidSourceTime,
    Stale,
    Future,
}
#[derive(Debug, PartialEq, Eq)]
pub enum SkipReason {
    NoText,
    Reply,
    NotChannel,
}
pub enum IntakeOutcome {
    Received(TelegramMessage),
    Skipped(SkipReason),
    Rejected(RejectReason),
}

/// Clock supplied by caller so source-time checks are deterministic in tests.
pub fn inspect_message(body: &[u8], now_ms: u64) -> IntakeOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope>(body) else {
        return IntakeOutcome::Rejected(RejectReason::InvalidEnvelope);
    };
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    if envelope.chat.id == 0
        || envelope.chat.id.unsigned_abs() > MAX_SAFE_INTEGER
        || envelope.message_id <= 0
        || envelope.message_id as u64 > MAX_SAFE_INTEGER
    {
        return IntakeOutcome::Rejected(RejectReason::InvalidIdentity);
    }
    let Some(source_ms) = envelope.date.checked_mul(1000).filter(|n| *n > 0) else {
        return IntakeOutcome::Rejected(RejectReason::InvalidSourceTime);
    };
    if source_ms > now_ms.saturating_add(SOURCE_FUTURE_TOLERANCE_MS) {
        return IntakeOutcome::Rejected(RejectReason::Future);
    }
    if now_ms.saturating_sub(source_ms) > SOURCE_MAX_AGE_MS {
        return IntakeOutcome::Rejected(RejectReason::Stale);
    }
    if envelope.chat.kind != "channel" {
        return IntakeOutcome::Skipped(SkipReason::NotChannel);
    }
    if envelope.reply_to_message.is_some() {
        return IntakeOutcome::Skipped(SkipReason::Reply);
    }
    let Some(text) = envelope.text.filter(|text| !text.trim().is_empty()) else {
        return IntakeOutcome::Skipped(SkipReason::NoText);
    };
    IntakeOutcome::Received(TelegramMessage {
        channel_id: envelope.chat.id,
        message_id: envelope.message_id,
        source_created_at_ms: source_ms,
        text,
    })
}
