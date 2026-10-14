use super::{ChannelClient, ChannelContext, TelegramMessage};
use crate::{
    mongo::MongoError,
    signals::{ParseOutcome, SignalError, parse_signal},
};
use futures_util::future::BoxFuture;
use mongodb::bson::{Bson, DateTime, Document};
use std::collections::BTreeSet;

/// Database seam for deterministic workflow tests. Errors never mean an empty result.
pub trait ChannelRepository: Send + Sync {
    fn channel(&self, id: i64) -> BoxFuture<'_, Result<Option<Document>, MongoError>>;
    fn profiles(&self, id: i64) -> BoxFuture<'_, Result<Vec<Document>, MongoError>>;
    fn accounts(&self, ids: Vec<i64>) -> BoxFuture<'_, Result<Vec<Document>, MongoError>>;
    fn configs(&self, ids: Vec<i64>) -> BoxFuture<'_, Result<Vec<Document>, MongoError>>;
}
#[derive(Debug, PartialEq, Eq)]
pub enum ChannelSkip {
    UnauthorizedChannel,
    NotSignal,
    NoConnectedAccounts,
    NoEligibleAccounts,
}
#[derive(Debug, PartialEq, Eq)]
pub enum ChannelReject {
    Signal(SignalError),
    InvalidContext,
}
pub enum ChannelOutcome {
    Ready(Box<ChannelContext>),
    Skipped(ChannelSkip),
    Rejected(ChannelReject),
}
pub struct ChannelUpdateManager<'a, R: ?Sized> {
    repository: &'a R,
}
impl<'a, R: ChannelRepository + ?Sized> ChannelUpdateManager<'a, R> {
    pub fn new(repository: &'a R) -> Self {
        Self { repository }
    }
    pub async fn handle_channel_update(
        &self,
        message: TelegramMessage,
        now_ms: u64,
    ) -> Result<ChannelOutcome, MongoError> {
        let Some(channel_settings) = self.repository.channel(message.channel_id()).await? else {
            return Ok(ChannelOutcome::Skipped(ChannelSkip::UnauthorizedChannel));
        };
        if id(channel_settings.get("id")) != Some(message.channel_id()) {
            return Ok(ChannelOutcome::Rejected(ChannelReject::InvalidContext));
        }
        let signal = match parse_signal(message.text()) {
            ParseOutcome::Parsed(signal) => signal,
            ParseOutcome::NotSignal => return Ok(ChannelOutcome::Skipped(ChannelSkip::NotSignal)),
            ParseOutcome::Rejected(reason) => {
                return Ok(ChannelOutcome::Rejected(ChannelReject::Signal(reason)));
            }
        };
        let profiles = self.repository.profiles(message.channel_id()).await?;
        let Some(mut clients) = connected_clients(&profiles, message.channel_id()) else {
            return Ok(ChannelOutcome::Rejected(ChannelReject::InvalidContext));
        };
        if clients.is_empty() {
            return Ok(ChannelOutcome::Skipped(ChannelSkip::NoConnectedAccounts));
        }
        let users: BTreeSet<_> = clients.iter().map(|c| c.chat_id).collect();
        let accounts = self
            .repository
            .accounts(users.iter().copied().collect())
            .await?;
        let active: BTreeSet<_> = accounts
            .iter()
            .filter(|account| {
                account.get_bool("auto_trading") == Ok(true)
                    && expiry(account.get("valid_till")).is_some_and(|time| time > now_ms)
            })
            .filter_map(|account| id(account.get("tg_chat_id")))
            .collect();
        clients.retain(|client| active.contains(&client.chat_id));
        if clients.is_empty() {
            return Ok(ChannelOutcome::Skipped(ChannelSkip::NoEligibleAccounts));
        }
        let eligible_users: BTreeSet<_> = clients.iter().map(|c| c.chat_id).collect();
        let configs = self
            .repository
            .configs(eligible_users.into_iter().collect())
            .await?;
        for client in &mut clients {
            client.user_config = configs
                .iter()
                .find(|config| id(config.get("user")) == Some(client.chat_id))
                .cloned();
            client.user_settings = client
                .user_config
                .as_ref()
                .and_then(|config| config.get_array("private_channels").ok())
                .and_then(|channels| {
                    channels
                        .iter()
                        .filter_map(Bson::as_document)
                        .find(|ch| id(ch.get("id")) == Some(message.channel_id()))
                })
                .cloned();
        }
        Ok(ChannelOutcome::Ready(Box::new(ChannelContext {
            message,
            signal,
            channel_settings,
            clients,
        })))
    }
}
fn connected_clients(profiles: &[Document], channel_id: i64) -> Option<Vec<ChannelClient>> {
    let mut clients = Vec::new();
    for profile in profiles {
        for value in profile.get_array("exchangeClients").ok()? {
            let account = value.as_document()?;
            if id(account.get("connectedChannel")) != Some(channel_id)
                || account.get_str("provider") != Ok("BingX")
            {
                continue;
            }
            let chat_id = id(profile.get("userId")).filter(|id| *id > 0)?;
            let client_id = nonempty(account, "clientId")?;
            clients.push(ChannelClient {
                chat_id,
                client_id,
                name: account.get_str("name").ok().map(str::to_owned),
                key: nonempty(account, "api_key")?,
                key_secret: nonempty(account, "api_secret")?,
                user_config: None,
                user_settings: None,
            });
        }
    }
    Some(clients)
}
fn nonempty(doc: &Document, key: &str) -> Option<String> {
    doc.get_str(key)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
}
fn id(value: Option<&Bson>) -> Option<i64> {
    let number = match value? {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        Bson::Double(n)
            if n.is_finite() && n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0 =>
        {
            *n as i64
        }
        Bson::String(n) => n.parse().ok()?,
        _ => return None,
    };
    (number != 0 && number.unsigned_abs() <= 9_007_199_254_740_991).then_some(number)
}
fn expiry(value: Option<&Bson>) -> Option<u64> {
    match value? {
        Bson::DateTime(time) => time.timestamp_millis().try_into().ok(),
        Bson::String(time) => DateTime::parse_rfc3339_str(time)
            .ok()?
            .timestamp_millis()
            .try_into()
            .ok(),
        _ => None,
    }
}
