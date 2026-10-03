use super::{MongoError, MongoErrorKind, MongoRepositories, error::bounded};
use crate::config::MongoConfig;
use mongodb::{Client, Database, bson::doc, options::ClientOptions};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MongoRole {
    Bot,
    TradingStation,
    AccountValidator,
}
const ROLES: [MongoRole; 3] = [
    MongoRole::Bot,
    MongoRole::TradingStation,
    MongoRole::AccountValidator,
];
struct Pool {
    client: Client,
    database: Database,
}

pub struct MongoConnections {
    config: MongoConfig,
    timeout: Duration,
    pools: [Option<Pool>; 3],
    repositories: Option<MongoRepositories>,
    connected: bool,
}
impl MongoConnections {
    pub fn new(config: MongoConfig, timeout: Duration) -> Self {
        Self {
            config,
            timeout,
            pools: [None, None, None],
            repositories: None,
            connected: false,
        }
    }
    /// Own each pool before pinging so cancelled/partial startup can be cleaned up.
    pub async fn connect(&mut self) -> Result<(), MongoError> {
        self.connected = false;
        if let Some(repos) = &self.repositories {
            repos.claims.invalidate();
        }
        for (index, role) in ROLES.into_iter().enumerate() {
            if self.pools[index].is_none() {
                let uri = match role {
                    MongoRole::Bot => &self.config.bot_uri,
                    MongoRole::TradingStation => &self.config.trade_station_uri,
                    MongoRole::AccountValidator => &self.config.account_validator_uri,
                };
                let mut options = bounded(role, self.timeout, async {
                    ClientOptions::parse(uri.expose()).await
                })
                .await?;
                options.max_pool_size = Some(self.config.max_pool_size.get());
                options.server_selection_timeout = Some(self.config.server_selection_timeout);
                options.connect_timeout = Some(self.timeout);
                let name = options
                    .default_database
                    .clone()
                    .unwrap_or_else(|| "test".into());
                let client =
                    Client::with_options(options).map_err(|e| MongoError::driver(role, e))?;
                let database = client.database(&name);
                self.pools[index] = Some(Pool { client, database });
            }
            let pool = self.pools[index].as_ref().expect("pool just initialized");
            bounded(role, self.timeout, async {
                pool.database.run_command(doc! {"ping":1}).await
            })
            .await?;
        }
        if self.repositories.is_none() {
            self.repositories = Some(MongoRepositories::new(
                &self.pools[0].as_ref().unwrap().database,
                &self.pools[1].as_ref().unwrap().database,
                &self.pools[2].as_ref().unwrap().database,
                self.timeout,
            ));
        }
        self.connected = true;
        Ok(())
    }
    pub fn database_name(&self, role: MongoRole) -> Option<&str> {
        self.pools[role as usize]
            .as_ref()
            .map(|p| p.database.name())
    }
    pub fn repositories(&self) -> Result<MongoRepositories, MongoError> {
        self.repositories
            .as_ref()
            .filter(|_| self.connected)
            .cloned()
            .ok_or_else(|| MongoError::new(MongoRole::Bot, MongoErrorKind::NotConnected))
    }
    pub async fn initialize_indexes(&self) -> Result<(), MongoError> {
        self.repositories()?.claims.initialize(self.timeout).await
    }
    /// Recheck existing pools without resetting a verified claim index.
    pub async fn verify_connections(&self) -> Result<(), MongoError> {
        if !self.connected {
            return Err(MongoError::new(
                MongoRole::Bot,
                MongoErrorKind::NotConnected,
            ));
        }
        for (index, role) in ROLES.into_iter().enumerate() {
            let pool = self.pools[index]
                .as_ref()
                .ok_or_else(|| MongoError::new(role, MongoErrorKind::NotConnected))?;
            bounded(role, self.timeout, async {
                pool.database.run_command(doc! {"ping":1}).await
            })
            .await?;
        }
        Ok(())
    }
    /// Called after worker drain. Immediate driver shutdown also invalidates cloned handles.
    pub async fn close(&mut self) -> Result<(), MongoError> {
        self.connected = false;
        if let Some(repos) = self.repositories.take() {
            repos.claims.invalidate();
        }
        let mut failure = None;
        for (index, role) in ROLES.into_iter().enumerate() {
            if let Some(pool) = &self.pools[index] {
                if tokio::time::timeout(
                    self.timeout,
                    pool.client.clone().shutdown().immediate(true),
                )
                .await
                .is_err()
                {
                    failure.get_or_insert(MongoError::new(role, MongoErrorKind::Timeout));
                } else {
                    self.pools[index] = None;
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
