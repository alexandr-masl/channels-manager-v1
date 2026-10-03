use super::{RedisError, connection::Connection};
use crate::contracts::storage::*;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const VERIFY: &str = "if redis.call('GET',KEYS[1]) == ARGV[1] then return 1 end return 0";
const RENEW: &str = "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end return 0";
const RELEASE: &str =
    "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0";
// Keep local validity slightly shorter than Redis's TTL for clock/round-trip margin.
const EXPIRY_MARGIN: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct LeaseManager {
    connection: Arc<Connection>,
    prefix: String,
}
impl LeaseManager {
    pub(super) fn new(connection: Arc<Connection>, prefix: String) -> Self {
        Self { connection, prefix }
    }
    pub async fn acquire(&self, exchange_client_id: &str) -> Result<AccountLease, RedisError> {
        if exchange_client_id.is_empty()
            || exchange_client_id.trim() != exchange_client_id
            || exchange_client_id.chars().any(char::is_control)
        {
            return Err(RedisError::InvalidIdentity);
        }
        let key = account_lease_key(&self.prefix, exchange_client_id);
        let token = Uuid::new_v4().to_string();
        let deadline = Instant::now() + ACCOUNT_LEASE_ACQUISITION_WINDOW;
        loop {
            let status = *self.connection.status.borrow();
            if !status.accepting
                || !status.ready
                || status.failure.is_some()
                || self.connection.closed.is_cancelled()
            {
                return Err(RedisError::Unavailable);
            }
            let started = Instant::now();
            if started >= deadline {
                return Err(RedisError::Contended);
            }
            let mut cmd = redis::cmd("SET");
            cmd.arg(&key)
                .arg(&token)
                .arg("PX")
                .arg(ACCOUNT_LEASE_TTL.as_millis() as u64)
                .arg("NX");
            let result = tokio::time::timeout_at(
                deadline,
                self.connection
                    .query::<Option<String>>(&cmd, self.connection.lease_budget()),
            )
            .await
            .map_err(|_| RedisError::Timeout)??;
            if result.as_deref() == Some("OK") {
                let current = *self.connection.status.borrow();
                if current.epoch != status.epoch
                    || !current.accepting
                    || Instant::now() >= started + ACCOUNT_LEASE_TTL - EXPIRY_MARGIN
                {
                    return Err(RedisError::LeaseLost);
                }
                return Ok(AccountLease::new(
                    self.connection.clone(),
                    key,
                    token,
                    status.epoch,
                    started,
                ));
            }
            // Only known contention is retried. Unknown SET outcomes never grant ownership.
            let delay = Duration::from_millis(rand::random_range(50..=150));
            tokio::time::sleep_until((Instant::now() + delay).min(deadline)).await;
        }
    }
}

struct LeaseState {
    connection: Arc<Connection>,
    key: String,
    token: String,
    epoch: u64,
    valid_until: watch::Sender<Instant>,
    lost: CancellationToken,
}
impl LeaseState {
    fn check(&self) -> Result<(), RedisError> {
        if self.lost.is_cancelled()
            || self.connection.closed.is_cancelled()
            || self.connection.status.borrow().epoch != self.epoch
            || Instant::now() >= *self.valid_until.borrow()
        {
            self.lost.cancel();
            return Err(RedisError::LeaseLost);
        }
        Ok(())
    }
    async fn script(&self, script: &str) -> Result<i64, RedisError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script)
            .arg(1)
            .arg(&self.key)
            .arg(&self.token)
            .arg(ACCOUNT_LEASE_TTL.as_millis() as u64);
        self.connection
            .query(&cmd, self.connection.lease_budget())
            .await
    }
    async fn lost(&self) {
        let mut status = self.connection.status.subscribe();
        let mut expiry = self.valid_until.subscribe();
        loop {
            if self.check().is_err() {
                return;
            }
            let until = *expiry.borrow_and_update();
            tokio::select! {
                biased;
                _=self.lost.cancelled()=>return,
                _=self.connection.closed.cancelled()=>{},
                _=status.changed()=>{},
                _=expiry.changed()=>{},
                _=tokio::time::sleep_until(until)=>{},
            }
        }
    }
    async fn renew(&self) -> Result<(), RedisError> {
        self.check()?;
        let started = Instant::now();
        let old_expiry = *self.valid_until.borrow();
        let result = tokio::time::timeout_at(old_expiry, self.script(RENEW)).await;
        if !matches!(result, Ok(Ok(1))) || self.check().is_err() {
            self.lost.cancel();
            return Err(RedisError::LeaseLost);
        }
        self.valid_until
            .send_replace(started + ACCOUNT_LEASE_TTL - EXPIRY_MARGIN);
        Ok(())
    }
}

/// Owns its renewal task. Drop stops renewals; explicit release performs bounded cleanup.
/// It cannot undo side effects already accepted by a remote service.
pub struct AccountLease {
    state: Arc<LeaseState>,
    renewal: Option<JoinHandle<()>>,
    released: bool,
}
impl AccountLease {
    fn new(
        connection: Arc<Connection>,
        key: String,
        token: String,
        epoch: u64,
        started: Instant,
    ) -> Self {
        let (valid_until, _) = watch::channel(started + ACCOUNT_LEASE_TTL - EXPIRY_MARGIN);
        let state = Arc::new(LeaseState {
            connection,
            key,
            token,
            epoch,
            valid_until,
            lost: CancellationToken::new(),
        });
        let worker = state.clone();
        let renewal = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                Instant::now() + ACCOUNT_LEASE_RENEWAL,
                ACCOUNT_LEASE_RENEWAL,
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased; _=worker.lost()=>break, _=interval.tick()=>{} }
                if worker.renew().await.is_err() {
                    break;
                }
            }
        });
        Self {
            state,
            renewal: Some(renewal),
            released: false,
        }
    }
    pub async fn assert_owned(&self) -> Result<(), RedisError> {
        self.state.check()?;
        let expiry = *self.state.valid_until.borrow();
        if !matches!(
            tokio::time::timeout_at(expiry, self.state.script(VERIFY)).await,
            Ok(Ok(1))
        ) || self.state.check().is_err()
        {
            self.state.lost.cancel();
            return Err(RedisError::LeaseLost);
        }
        Ok(())
    }
    pub async fn lost(&self) {
        self.state.lost().await;
    }
    /// Drops a pending protected future on ownership loss. The future must not detach tasks;
    /// callers still verify ownership immediately before irreversible external operations.
    pub async fn run<T>(&self, work: impl Future<Output = T>) -> Result<T, RedisError> {
        self.assert_owned().await?;
        tokio::select! {
            biased;
            _=self.lost()=>Err(RedisError::LeaseLost),
            value=work=> { self.state.check()?; Ok(value) },
        }
    }
    pub async fn release(&mut self) -> bool {
        if self.released {
            return false;
        }
        self.released = true;
        self.state.lost.cancel();
        // A renewal already sent to Redis is bounded. Let it finish rather than
        // cancelling its command and invalidating unrelated account leases.
        // Retain the handle while awaiting so cancellation never detaches it.
        if let Some(task) = self.renewal.as_mut() {
            let _ = task.await;
        }
        self.renewal.take();
        matches!(self.state.script(RELEASE).await, Ok(1))
    }
}
impl Drop for AccountLease {
    fn drop(&mut self) {
        self.state.lost.cancel();
        if let Some(task) = &self.renewal {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::AppConfig, redis::RedisConnections};

    #[tokio::test(start_paused = true)]
    async fn expired_local_lease_cannot_resume_protected_work() {
        let config = AppConfig::from_lookup(|key| match key {
            "RABBIT_MQ" => Some("amqp://localhost".into()),
            "REDIS" => Some("localhost".into()),
            "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
                Some("mongodb://localhost/test".into())
            }
            _ => None,
        })
        .unwrap();
        let manager = RedisConnections::new(config.redis, &config.runtime).unwrap();
        let lease = AccountLease::new(
            manager.locks().connection,
            "key".into(),
            "token".into(),
            0,
            Instant::now(),
        );
        // Model a process that resumes after its renewal task stopped running.
        lease.renewal.as_ref().unwrap().abort();
        tokio::time::advance(ACCOUNT_LEASE_TTL).await;
        assert_eq!(
            lease
                .run(async { panic!("expired work must never be polled") })
                .await,
            Err(RedisError::LeaseLost)
        );
    }
}
