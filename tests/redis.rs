use channels_manager_v1::{
    config::AppConfig,
    contracts::storage::*,
    redis::{RedisConnections, RedisError},
};
use redis::{FromRedisValue, aio::MultiplexedConnection};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::time::{sleep, timeout};

struct Server {
    child: Option<Child>,
    port: u16,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}
impl Server {
    async fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut server = Self { child: None, port };
        server.restart().await;
        server
    }
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    async fn restart(&mut self) {
        self.child = Some(
            Command::new(
                std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into()),
            )
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("install redis-server or set REDIS_SERVER_BIN"),
        );
        timeout(Duration::from_secs(5), async {
            loop {
                if self
                    .client()
                    .get_multiplexed_async_connection()
                    .await
                    .is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    fn client(&self) -> redis::Client {
        redis::Client::open(("127.0.0.1", self.port)).unwrap()
    }
    fn config(&self) -> AppConfig {
        AppConfig::from_lookup(|k| match k {
            "RABBIT_MQ" => Some("amqp://localhost".into()),
            "REDIS" => Some("127.0.0.1".into()),
            "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
                Some("mongodb://localhost/test".into())
            }
            "REDIS_CLIENT_PORT" => Some(self.port.to_string()),
            "STARTUP_RETRY_DELAY_MS" => Some("50".into()),
            "STARTUP_RETRY_MAX_DELAY_MS" => Some("200".into()),
            "REDIS_LOCK_COMMAND_TIMEOUT_MS"
            | "REDIS_CACHE_COMMAND_TIMEOUT_MS"
            | "REDIS_LOCK_CONNECT_TIMEOUT_MS"
            | "REDIS_CACHE_CONNECT_TIMEOUT_MS" => Some("100".into()),
            "EXCHANGE_METADATA_CACHE_TTL_MS" => Some("100".into()),
            _ => None,
        })
        .unwrap()
    }
    fn manager(&self) -> RedisConnections {
        let c = self.config();
        RedisConnections::new(c.redis, &c.runtime).unwrap()
    }
}
async fn command<T: FromRedisValue>(conn: &mut MultiplexedConnection, args: &[&str]) -> T {
    redis::cmd(args[0])
        .arg(&args[1..])
        .query_async(conn)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn cross_pod_locks_renew_cancel_and_release_only_the_owner() {
    let server = Server::start().await;
    let mut first = server.manager();
    let mut second = server.manager();
    first.connect_required().await.unwrap();
    second.connect_required().await.unwrap();
    drop(first.locks().acquire("dropped").await.unwrap());
    let mut conn = server
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let mut lease = first.locks().acquire("account-1").await.unwrap();
    let key = account_lease_key(DEFAULT_LOCK_PREFIX, "account-1");
    let ttl: i64 = command(&mut conn, &["PTTL", &key]).await;
    assert!((28_000..=30_000).contains(&ttl));
    assert!(matches!(
        timeout(Duration::from_secs(6), second.locks().acquire("account-1"))
            .await
            .unwrap(),
        Err(RedisError::Contended)
    ));
    let mut unrelated = second.locks().acquire("account-2").await.unwrap();
    assert!(unrelated.release().await);
    // Actual timer renewal extends TTL, including during the drain phase.
    first.quiesce();
    assert!(matches!(
        first.locks().acquire("new").await,
        Err(RedisError::Unavailable)
    ));
    sleep(Duration::from_secs(6)).await;
    let ttl: i64 = command(&mut conn, &["PTTL", &key]).await;
    assert!(ttl > 25_000, "renewal did not refresh TTL: {ttl}");
    let dropped_ttl: i64 = command(
        &mut conn,
        &["PTTL", &account_lease_key(DEFAULT_LOCK_PREFIX, "dropped")],
    )
    .await;
    assert!(
        dropped_ttl < 20_000,
        "dropped lease kept renewing: {dropped_ttl}"
    );
    assert_eq!(lease.run(async { 7 }).await.unwrap(), 7);

    // Ownership replacement cancels protected work and cannot be renewed/deleted by the old token.
    let _: String = command(&mut conn, &["SET", &key, "replacement", "PX", "30000"]).await;
    assert_eq!(lease.assert_owned().await, Err(RedisError::LeaseLost));
    let polled = std::sync::atomic::AtomicBool::new(false);
    assert_eq!(
        lease
            .run(async {
                polled.store(true, std::sync::atomic::Ordering::SeqCst);
            })
            .await,
        Err(RedisError::LeaseLost)
    );
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!lease.release().await);
    assert_eq!(
        command::<String>(&mut conn, &["GET", &key]).await,
        "replacement"
    );
    assert!(!lease.release().await);

    first.close().await;
    second.close().await;
    first.close().await;
    assert!(matches!(
        first.locks().acquire("closed").await,
        Err(RedisError::Unavailable)
    ));
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn outage_is_latched_recovers_without_traffic_and_invalidates_old_leases() {
    let mut server = Server::start().await;
    let mut manager = server.manager();
    manager.connect_required().await.unwrap();
    let lease = manager.locks().acquire("outage").await.unwrap();
    server.stop();
    timeout(Duration::from_secs(3), manager.wait_for_failure())
        .await
        .unwrap();
    assert_eq!(
        timeout(
            Duration::from_secs(2),
            lease.run(std::future::pending::<()>())
        )
        .await
        .unwrap(),
        Err(RedisError::LeaseLost)
    );
    assert!(matches!(
        manager.locks().acquire("blocked").await,
        Err(RedisError::Unavailable)
    ));
    server.restart().await;
    timeout(Duration::from_secs(3), async {
        while !manager.is_connected() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Recovery alone does not silently reopen the coordinator's intake gate.
    assert!(matches!(
        manager.locks().acquire("blocked").await,
        Err(RedisError::Unavailable)
    ));
    manager.connect_required().await.unwrap();
    assert_eq!(lease.assert_owned().await, Err(RedisError::LeaseLost));
    let mut fresh = manager.locks().acquire("outage").await.unwrap();
    assert!(fresh.release().await);
    let active = manager.locks().acquire("shutdown").await.unwrap();
    manager.close().await;
    assert_eq!(active.assert_owned().await, Err(RedisError::LeaseLost));
    // Initial startup outage also leaves a monitor that can recover without work.
    server.stop();
    assert!(manager.connect_required().await.is_err());
    server.restart().await;
    timeout(Duration::from_secs(3), async {
        while !manager.is_connected() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    manager.connect_required().await.unwrap();
    let mut startup_lease = manager.locks().acquire("recovered-startup").await.unwrap();
    assert!(startup_lease.release().await);
    manager.close().await;
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn optional_metadata_cache_uses_shared_json_ttl_and_api_fallback() {
    let mut server = Server::start().await;
    let mut first = server.manager();
    let mut second = server.manager();
    let value = json!({"symbol":"BTC-USDT","stepSize":0.1});
    assert_eq!(
        first
            .cache()
            .get_or_load("BTC-USDT", || async { Ok::<_, ()>(value.clone()) })
            .await
            .unwrap(),
        value
    );
    assert_eq!(
        second
            .cache()
            .get_or_load("BTC-USDT", || async { Err::<serde_json::Value, _>(()) })
            .await
            .unwrap(),
        value
    );
    sleep(Duration::from_millis(150)).await;
    assert!(
        second
            .cache()
            .get_or_load("BTC-USDT", || async { Err::<serde_json::Value, _>(()) })
            .await
            .is_err()
    );
    let mut conn = server
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = bingx_metadata_key(DEFAULT_CACHE_PREFIX, "BAD");
    let _: String = command(&mut conn, &["SET", &key, "invalid-json"]).await;
    assert_eq!(
        first
            .cache()
            .get_or_load("BAD", || async { Ok::<_, ()>(value.clone()) })
            .await
            .unwrap(),
        value
    );
    let rejected = json!({"err":"API failed"});
    first
        .cache()
        .get_or_load("ERR", || async { Ok::<_, ()>(rejected) })
        .await
        .unwrap();
    assert_eq!(
        command::<i64>(
            &mut conn,
            &["EXISTS", &bingx_metadata_key(DEFAULT_CACHE_PREFIX, "ERR")]
        )
        .await,
        0
    );
    let mut config = server.config();
    config.redis.cache_enabled = false;
    let mut disabled = RedisConnections::new(config.redis, &config.runtime).unwrap();
    let before = command::<String>(&mut conn, &["CLIENT", "LIST"])
        .await
        .lines()
        .count();
    assert_eq!(
        disabled
            .cache()
            .get_or_load("DISABLED", || async { Ok::<_, ()>(value.clone()) })
            .await
            .unwrap(),
        value
    );
    let after = command::<String>(&mut conn, &["CLIENT", "LIST"])
        .await
        .lines()
        .count();
    assert_eq!(before, after, "disabled cache opened a connection");
    server.stop();
    assert_eq!(
        first
            .cache()
            .get_or_load("FALLBACK", || async { Ok::<_, ()>(value.clone()) })
            .await
            .unwrap(),
        value
    );
    first.close().await;
    second.close().await;
    disabled.close().await;
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn unknown_acquisition_and_cancelled_commands_fail_closed() {
    let server = Server::start().await;
    let mut manager = server.manager();
    manager.connect_required().await.unwrap();
    let mut conn = server
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: String = command(&mut conn, &["CLIENT", "PAUSE", "300", "ALL"]).await;
    let result = timeout(Duration::from_secs(1), manager.locks().acquire("unknown"))
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(RedisError::Timeout | RedisError::Unavailable)
    ));
    timeout(Duration::from_millis(100), manager.wait_for_failure())
        .await
        .unwrap();
    assert!(matches!(
        manager.locks().acquire("gated").await,
        Err(RedisError::Unavailable)
    ));
    sleep(Duration::from_millis(400)).await;
    manager.connect_required().await.unwrap();
    let _: String = command(&mut conn, &["CLIENT", "PAUSE", "300", "ALL"]).await;
    assert!(
        timeout(
            Duration::from_millis(20),
            manager.locks().acquire("cancelled")
        )
        .await
        .is_err()
    );
    timeout(Duration::from_millis(100), manager.wait_for_failure())
        .await
        .unwrap();
    manager.close().await;
    sleep(Duration::from_millis(400)).await;
    manager.connect_required().await.unwrap();
    let mut lease = manager.locks().acquire("after-reopen").await.unwrap();
    assert!(lease.release().await);
    manager.close().await;
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn automatic_renewal_loss_drops_protected_future() {
    let server = Server::start().await;
    let mut manager = server.manager();
    manager.connect_required().await.unwrap();
    let lease = manager.locks().acquire("protected").await.unwrap();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let mut conn = server
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let (result, ()) = tokio::join!(
        timeout(
            Duration::from_secs(12),
            lease.run(async {
                let _flag = DropFlag(dropped.clone());
                std::future::pending::<()>().await;
            })
        ),
        async {
            sleep(Duration::from_millis(100)).await;
            let _: i64 = command(
                &mut conn,
                &["DEL", &account_lease_key(DEFAULT_LOCK_PREFIX, "protected")],
            )
            .await;
        }
    );
    assert_eq!(result.unwrap(), Err(RedisError::LeaseLost));
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    manager.close().await;
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn cache_failure_is_independent_of_required_locks() {
    let server = Server::start().await;
    let mut manager = server.manager();
    manager.connect_required().await.unwrap();
    let mut conn = server
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    // WRONGTYPE affects only this cache GET; the required lock socket stays usable.
    let key = bingx_metadata_key(DEFAULT_CACHE_PREFIX, "WRONGTYPE");
    let _: i64 = command(&mut conn, &["LPUSH", &key, "item"]).await;
    assert_eq!(
        manager
            .cache()
            .get_or_load("WRONGTYPE", || async { Ok::<_, ()>(json!({"api":true})) })
            .await
            .unwrap(),
        json!({"api":true})
    );
    assert!(
        timeout(Duration::from_millis(50), manager.wait_for_failure())
            .await
            .is_err()
    );
    let mut lease = manager.locks().acquire("still-available").await.unwrap();
    assert!(lease.release().await);
    manager.close().await;
}

#[tokio::test]
#[ignore = "starts an isolated local Redis; requires loopback access"]
async fn releasing_during_renewal_does_not_invalidate_other_accounts() {
    let server = Server::start().await;
    let mut config = server.config();
    config.redis.locks.command = Duration::from_secs(1);
    let mut manager = RedisConnections::new(config.redis, &config.runtime).unwrap();
    manager.connect_required().await.unwrap();
    let mut first = manager.locks().acquire("first").await.unwrap();
    let second = manager.locks().acquire("second").await.unwrap();
    let mut conn = server
        .client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    sleep(Duration::from_millis(9900)).await;
    let _: String = command(&mut conn, &["CLIENT", "PAUSE", "500", "ALL"]).await;
    sleep(Duration::from_millis(200)).await;
    assert!(first.release().await);
    assert_eq!(second.assert_owned().await, Ok(()));
    assert!(
        timeout(Duration::from_millis(50), manager.wait_for_failure())
            .await
            .is_err()
    );
    manager.close().await;
}
