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
}
impl MongoRepositories {
    pub(super) fn new(
        bot: &Database,
        trading: &Database,
        accounts: &Database,
        timeout: Duration,
    ) -> Self {
        Self {
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
