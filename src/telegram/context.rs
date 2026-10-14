use super::TelegramMessage;
use crate::signals::TradingSignal;
use mongodb::bson::Document;

// No Debug/Serialize: this context contains exchange credentials and user settings.
pub struct ChannelContext {
    pub message: TelegramMessage,
    pub signal: TradingSignal,
    pub channel_settings: Document,
    pub clients: Vec<ChannelClient>,
}
pub struct ChannelClient {
    pub chat_id: i64,
    pub client_id: String,
    pub name: Option<String>,
    pub key: String,
    pub key_secret: String,
    /// Original user document for the existing client-job contract.
    pub user_config: Option<Document>,
    /// Matching private_channels entry; own_settings is resolved later.
    pub user_settings: Option<Document>,
}
