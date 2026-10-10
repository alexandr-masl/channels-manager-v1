use super::{ClaimStore, MongoError, MongoRole, error::bounded};
use crate::contracts::storage::*;
use futures_util::TryStreamExt;
use mongodb::{
    Collection, Database,
    bson::{DateTime, Document, doc},
};
use std::time::Duration;

#[derive(Clone)]
pub struct MongoRepositories {
    pub claims: ClaimStore,
    pub trades: TradeRepository,
    pub notifications: NotificationRepository,
    pub accounts: AccountRepository,
    pub channels: ChannelRepository,
}
impl MongoRepositories {
    pub(super) fn new(
        bot: &Database,
        trading: &Database,
        accounts: &Database,
        timeout: Duration,
    ) -> Self {
        Self {
            channels: ChannelRepository {
                channels: bot.collection(CHANNELS_COLLECTION),
                profiles: bot.collection(TRADING_PROFILES_COLLECTION),
                configs: bot.collection(USER_CONFIGS_COLLECTION),
                timeout,
            },
            claims: ClaimStore::new(bot),
            trades: TradeRepository {
                collection: trading.collection(ACTIVE_TRADES_COLLECTION),
                timeout,
            },
            notifications: NotificationRepository {
                collection: bot.collection(USER_CONFIGS_COLLECTION),
                timeout,
            },
            accounts: AccountRepository {
                collection: accounts.collection(ACCOUNTS_COLLECTION),
                timeout,
            },
        }
    }
}
#[derive(Clone)]
pub struct TradeRepository {
    collection: Collection<Document>,
    timeout: Duration,
}
impl TradeRepository {
    pub async fn get_opened_trades_by_users(
        &self,
        users: &[i64],
    ) -> Result<Vec<Document>, MongoError> {
        if users.is_empty() {
            return Ok(vec![]);
        }
        self.find(
            doc! {"state":"OPENED","chat_id":{"$in":users}},
            doc! {"_id":1,"chat_id":1,"symbol":1,"exchange_client":1,"exchangeClientId":1,"idempotencyKey":1,"auto_Trade":1},
        )
        .await
    }
    pub async fn get_trades_by_idempotency_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<Document>, MongoError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        self.find(
            doc! {"idempotencyKey":{"$in":keys}},
            doc! {"_id":1,"idempotencyKey":1,"state":1},
        )
        .await
    }
    pub async fn get_active_managed_futures_trades(
        &self,
        exchange_client_id: &str,
    ) -> Result<Vec<Document>, MongoError> {
        self.find(doc! {"exchangeClientId":exchange_client_id,"exchange_client":"_binance_futures_","state":{"$ne":"FINISHED"}},
            doc! {"_id":1,"id":1,"exchangeClientId":1,"exchange_client":1,"symbol":1,"state":1,"creationStatus":1,"is_long":1,"tradeLeverage":1,"positionConfiguration":1}).await
    }
    async fn find(
        &self,
        filter: Document,
        projection: Document,
    ) -> Result<Vec<Document>, MongoError> {
        bounded(MongoRole::TradingStation, self.timeout, async {
            self.collection
                .find(filter)
                .projection(projection)
                .max_time(self.timeout)
                .await?
                .try_collect()
                .await
        })
        .await
    }
}
#[derive(Clone)]
pub struct NotificationRepository {
    collection: Collection<Document>,
    timeout: Duration,
}
impl NotificationRepository {
    /// No upsert: only existing users receive notifications, retaining the newest seven.
    pub async fn add_user_notification(
        &self,
        chat_id: i64,
        notification: &str,
    ) -> Result<bool, MongoError> {
        let time = DateTime::now()
            .try_to_rfc3339_string()
            .expect("current date has an RFC3339 representation");
        bounded(MongoRole::Bot, self.timeout, async {
            self.collection.update_one(doc! {"user":chat_id}, doc! {"$push":{"lastNotifications":{"$each":[{"info":notification,"time":time}],"$slice":-(USER_NOTIFICATION_LIMIT as i32)}}}).await
        }).await.map(|result| result.matched_count == 1)
    }
}
#[derive(Clone)]
pub struct AccountRepository {
    collection: Collection<Document>,
    timeout: Duration,
}
impl AccountRepository {
    /// Matches the upstream lookup; the caller classifies valid_till expiry.
    pub async fn get_auto_trading_accounts(
        &self,
        chat_ids: &[i64],
    ) -> Result<Vec<Document>, MongoError> {
        if chat_ids.is_empty() {
            return Ok(vec![]);
        }
        bounded(MongoRole::AccountValidator, self.timeout, async {
            self.collection
                .find(doc! {"tg_chat_id":{"$in":chat_ids},"auto_trading":true})
                .projection(doc! {"tg_chat_id":1,"valid_till":1,"auto_trading":1})
                .max_time(self.timeout)
                .await?
                .try_collect()
                .await
        })
        .await
    }
}

#[derive(Clone)]
pub struct ChannelRepository {
    channels: Collection<Document>,
    profiles: Collection<Document>,
    configs: Collection<Document>,
    timeout: Duration,
}
impl ChannelRepository {
    pub async fn get_channel(&self, channel_id: i64) -> Result<Option<Document>, MongoError> {
        bounded(MongoRole::Bot, self.timeout, async {
            self.channels
                .find_one(doc! {"id": channel_id})
                .max_time(self.timeout)
                .await
        })
        .await
    }
    pub async fn get_profiles_by_channel(
        &self,
        channel_id: i64,
    ) -> Result<Vec<Document>, MongoError> {
        bounded(MongoRole::Bot, self.timeout, async {
            self.profiles.find(doc! {"exchangeClients.connectedChannel":channel_id})
                .projection(doc! {"userId":1,"exchangeClients.clientId":1,"exchangeClients.provider":1,"exchangeClients.name":1,"exchangeClients.api_key":1,"exchangeClients.api_secret":1,"exchangeClients.connectedChannel":1})
                .max_time(self.timeout).await?.try_collect().await
        }).await
    }
    pub async fn get_configs_by_users(&self, users: &[i64]) -> Result<Vec<Document>, MongoError> {
        if users.is_empty() {
            return Ok(vec![]);
        }
        bounded(MongoRole::Bot, self.timeout, async {
            self.configs
                .find(doc! {"user":{"$in":users}})
                .projection(doc! {"user":1,"private_channels":1})
                .max_time(self.timeout)
                .await?
                .try_collect()
                .await
        })
        .await
    }
}
impl crate::telegram::ChannelRepository for MongoRepositories {
    fn channel(
        &self,
        id: i64,
    ) -> futures_util::future::BoxFuture<'_, Result<Option<Document>, MongoError>> {
        Box::pin(self.channels.get_channel(id))
    }
    fn profiles(
        &self,
        id: i64,
    ) -> futures_util::future::BoxFuture<'_, Result<Vec<Document>, MongoError>> {
        Box::pin(self.channels.get_profiles_by_channel(id))
    }
    fn accounts(
        &self,
        ids: Vec<i64>,
    ) -> futures_util::future::BoxFuture<'_, Result<Vec<Document>, MongoError>> {
        Box::pin(async move { self.accounts.get_auto_trading_accounts(&ids).await })
    }
    fn configs(
        &self,
        ids: Vec<i64>,
    ) -> futures_util::future::BoxFuture<'_, Result<Vec<Document>, MongoError>> {
        Box::pin(async move { self.channels.get_configs_by_users(&ids).await })
    }
}
