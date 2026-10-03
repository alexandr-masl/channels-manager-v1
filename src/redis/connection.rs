use super::{LeaseManager, MetadataCache, RedisError};
use crate::{
    config::{RedisConfig, RedisTimeouts, RuntimeConfig},
    runtime::Backoff,
};
use redis::{AsyncConnectionConfig, Client, Cmd, FromRedisValue, aio::MultiplexedConnection};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(super) struct Status {
    pub epoch: u64,
    pub ready: bool,
    pub accepting: bool,
    pub failure: Option<RedisError>,
}

pub(super) struct Connection {
    client: Client,
    timeouts: RedisTimeouts,
    slot: Mutex<Option<MultiplexedConnection>>,
    pub closed: CancellationToken,
    pub status: watch::Sender<Status>,
    required: bool,
}
impl Connection {
    fn new(config: &RedisConfig, required: bool) -> Result<Arc<Self>, RedisError> {
        let client = Client::open((config.host.as_str(), config.port.get()))
            .map_err(|_| RedisError::InvalidConfiguration)?;
        let (status, _) = watch::channel(Status {
            epoch: 0,
            ready: false,
            accepting: false,
            failure: None,
        });
        Ok(Arc::new(Self {
            client,
            timeouts: if required {
                config.locks.clone()
            } else {
                config.cache.clone()
            },
            slot: Mutex::new(None),
            closed: CancellationToken::new(),
            status,
            required,
        }))
    }
    /// The future owns the checked-out connection. Cancellation drops it rather than
    /// reusing a socket with an unknown command outcome. No command is replayed.
    pub async fn query<T: FromRedisValue>(
        &self,
        command: &Cmd,
        budget: Duration,
    ) -> Result<T, RedisError> {
        if self.closed.is_cancelled() {
            return Err(RedisError::Closed);
        }
        let operation = async {
            let mut slot = self.slot.lock().await;
            // Cancellation while only waiting for the slot has sent no command.
            let mut guard = FailureGuard {
                connection: self,
                armed: true,
            };
            let mut conn = match slot.take() {
                Some(conn) => conn,
                None => self
                    .client
                    .get_multiplexed_async_connection_with_config(
                        &AsyncConnectionConfig::new()
                            .set_connection_timeout(Some(self.timeouts.connect))
                            .set_response_timeout(Some(self.timeouts.command)),
                    )
                    .await
                    .map_err(|_| RedisError::Unavailable)?,
            };
            let value = command
                .query_async(&mut conn)
                .await
                .map_err(|_| RedisError::Unavailable)?;
            if self.closed.is_cancelled() {
                return Err(RedisError::Closed);
            }
            *slot = Some(conn);
            guard.armed = false;
            Ok(value)
        };
        tokio::select! {
            biased;
            _ = self.closed.cancelled() => Err(RedisError::Closed),
            result = tokio::time::timeout(budget, operation) => result.unwrap_or(Err(RedisError::Timeout)),
        }
    }
    fn fail(&self) {
        if self.required {
            self.status.send_modify(|s| {
                s.epoch = s.epoch.wrapping_add(1);
                s.ready = false;
                s.accepting = false;
                s.failure.get_or_insert(RedisError::Unavailable);
            });
        }
    }
    fn stop(&self) {
        if !self.closed.is_cancelled() {
            self.closed.cancel();
            self.fail();
        }
        if let Ok(mut slot) = self.slot.try_lock() {
            slot.take();
        }
    }
    async fn close(&self) {
        self.stop();
        self.slot.lock().await.take();
    }
    pub fn budget(&self) -> Duration {
        self.timeouts.connect + self.timeouts.command
    }
    pub fn lease_budget(&self) -> Duration {
        self.timeouts
            .command
            .min(crate::contracts::storage::ACCOUNT_LEASE_COMMAND_TIMEOUT)
    }
}
struct FailureGuard<'a> {
    connection: &'a Connection,
    armed: bool,
}
impl Drop for FailureGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.connection.fail();
        }
    }
}

pub struct RedisConnections {
    config: RedisConfig,
    runtime: RuntimeConfig,
    locks: Arc<Connection>,
    cache: Option<Arc<Connection>>,
    monitor: Option<JoinHandle<()>>,
}
impl RedisConnections {
    pub fn new(config: RedisConfig, runtime: &RuntimeConfig) -> Result<Self, RedisError> {
        runtime
            .validate()
            .map_err(|_| RedisError::InvalidConfiguration)?;
        let locks = Connection::new(&config, true)?;
        let cache = if config.cache_enabled {
            Some(Connection::new(&config, false)?)
        } else {
            None
        };
        Ok(Self {
            config,
            runtime: runtime.clone(),
            locks,
            cache,
            monitor: None,
        })
    }
    pub fn locks(&self) -> LeaseManager {
        LeaseManager::new(self.locks.clone(), self.config.lock_prefix.clone())
    }
    pub fn cache(&self) -> MetadataCache {
        MetadataCache::new(
            self.cache.clone(),
            self.config.cache_prefix.clone(),
            self.config.metadata_cache_ttl,
        )
    }
    pub fn is_connected(&self) -> bool {
        self.locks.status.borrow().ready
    }
    pub async fn connect_required(&mut self) -> Result<(), RedisError> {
        if self.locks.closed.is_cancelled() {
            *self = Self::new(self.config.clone(), &self.runtime)?;
        }
        let epoch = self.locks.status.borrow().epoch;
        let result = self
            .locks
            .query::<String>(&redis::cmd("PING"), self.locks.budget())
            .await;
        if self.monitor.is_none() {
            let conn = self.locks.clone();
            let backoff =
                Backoff::new(&self.runtime).map_err(|_| RedisError::InvalidConfiguration)?;
            self.monitor = Some(tokio::spawn(async move {
                let mut attempt = 0u32;
                loop {
                    let delay = if attempt == 0 {
                        Duration::from_secs(1)
                    } else {
                        backoff.next(attempt)
                    };
                    tokio::select! { biased; _=conn.closed.cancelled()=>break, _=tokio::time::sleep(delay)=>{} }
                    let epoch = conn.status.borrow().epoch;
                    if conn
                        .query::<String>(&redis::cmd("PING"), conn.budget())
                        .await
                        .is_ok()
                    {
                        conn.status.send_if_modified(|s| {
                            if s.ready || s.epoch != epoch || conn.closed.is_cancelled() {
                                false
                            } else {
                                s.ready = true;
                                true
                            }
                        });
                        attempt = 0;
                    } else {
                        attempt = attempt.saturating_add(1);
                    }
                }
            }));
        }
        result?;
        let mut admitted = false;
        self.locks.status.send_modify(|s| {
            if s.epoch == epoch {
                s.ready = true;
                s.accepting = true;
                s.failure = None;
                admitted = true;
            }
        });
        if admitted {
            Ok(())
        } else {
            Err(RedisError::Unavailable)
        }
    }
    /// Failure stays latched through background reconnection until initialization succeeds.
    pub async fn wait_for_failure(&self) -> RedisError {
        let mut status = self.locks.status.subscribe();
        loop {
            if let Some(error) = status.borrow_and_update().failure {
                return error;
            }
            if status.changed().await.is_err() {
                return RedisError::Closed;
            }
        }
    }
    /// Stop new acquisitions while existing lease renewals continue through drain.
    pub fn quiesce(&self) {
        self.locks.status.send_modify(|s| s.accepting = false);
    }
    pub async fn close(&mut self) {
        self.locks.stop();
        if let Some(cache) = &self.cache {
            cache.stop();
        }
        if let Some(task) = self.monitor.take() {
            task.abort();
            let _ = task.await;
        }
        self.locks.close().await;
        if let Some(cache) = &self.cache {
            cache.close().await;
        }
    }
}
impl Drop for RedisConnections {
    fn drop(&mut self) {
        self.locks.stop();
        if let Some(cache) = &self.cache {
            cache.stop();
        }
        if let Some(task) = &self.monitor {
            task.abort();
        }
    }
}
