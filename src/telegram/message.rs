use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct Envelope {
    pub message_id: i64,
    pub date: u64,
    pub chat: Chat,
    pub text: Option<String>,
    pub reply_to_message: Option<serde::de::IgnoredAny>,
}
#[derive(Deserialize)]
pub(super) struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
}

/// Structurally valid, fresh text. Authorization and signal validity are not implied.
pub struct TelegramMessage {
    pub(super) channel_id: i64,
    pub(super) message_id: i64,
    pub(super) source_created_at_ms: u64,
    pub(super) text: String,
}
impl TelegramMessage {
    pub fn channel_id(&self) -> i64 {
        self.channel_id
    }
    pub fn message_id(&self) -> i64 {
        self.message_id
    }
    pub fn source_created_at_ms(&self) -> u64 {
        self.source_created_at_ms
    }
    pub fn text(&self) -> &str {
        &self.text
    }
}
