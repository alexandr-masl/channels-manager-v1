use channels_manager_v1::config::AppConfig;
use std::{collections::HashMap, time::Duration};

fn environment() -> HashMap<String, String> {
    [
        ("RABBIT_MQ", "amqp://user:secret@localhost:5672/%2f"),
        ("MONGO_PATH", "mongodb://localhost:27017/bot"),
        (
            "TRADE_STATION_MONGO_PATH",
            "mongodb://localhost:27017/trading",
        ),
        ("REDIS", "localhost"),
        (
            "ACCOUNT_VALIDATOR_MONGO_PATH",
            "mongodb://localhost:27017/accounts",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v.into()))
    .collect()
}

#[test]
fn defaults_match_the_typescript_worker() {
    let env = environment();
    let config = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap();
    assert_eq!(config.rabbitmq.prefetch.get(), 2);
    assert_eq!(config.rabbitmq.retry_max_attempts.get(), 5);
    assert_eq!(config.rabbitmq.retry_delay, Duration::from_secs(1));
    assert_eq!(config.rabbitmq.output_queue, "create-new-trusted-trade");
    assert_eq!(config.rabbitmq.reconnect.base, Duration::from_millis(500));
    assert_eq!(config.rabbitmq.reconnect.max, Duration::from_secs(30));
    assert_eq!(config.rabbitmq.reconnect.jitter_ratio, 0.2);
    assert_eq!(config.redis.port.get(), 6379);
    assert_eq!(config.redis.cache.command, Duration::from_millis(500));
    assert!(config.redis.cache_enabled);
    assert_eq!(config.mongo.max_pool_size.get(), 10);
    assert_eq!(
        config.mongo.account_validator_uri.expose(),
        "mongodb://localhost:27017/accounts"
    );
    assert_eq!(
        config.runtime.shutdown_drain_timeout,
        Duration::from_secs(10)
    );
    assert_eq!(config.runtime.operation_timeout, Duration::from_secs(10));
    assert_eq!(config.runtime.shutdown_timeout, Duration::from_secs(30));
    assert_eq!(
        config.runtime.startup_retry_max_delay,
        Duration::from_secs(30)
    );
    assert_eq!(config.runtime.startup_retry_jitter_ratio, 0.2);
}

#[test]
fn runtime_limits_are_validated_before_startup() {
    for (key, value) in [
        ("RUNTIME_OPERATION_TIMEOUT_MS", "0"),
        ("SHUTDOWN_TIMEOUT_MS", "10000"),
        ("STARTUP_RETRY_MAX_DELAY_MS", "1000"),
        ("STARTUP_RETRY_JITTER_RATIO", "NaN"),
    ] {
        let mut env = environment();
        env.insert(key.into(), value.into());
        let error = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap_err();
        assert_eq!(error.setting, key);
    }
}

#[test]
fn shared_api_cache_can_be_disabled_and_rejects_invalid_flags() {
    let mut env = environment();
    env.insert("EXCHANGE_METADATA_CACHE_ENABLED".into(), "false".into());
    assert!(
        !AppConfig::from_lookup(|k| env.get(k).cloned())
            .unwrap()
            .redis
            .cache_enabled
    );
    env.insert("EXCHANGE_METADATA_CACHE_ENABLED".into(), "maybe".into());
    assert_eq!(
        AppConfig::from_lookup(|k| env.get(k).cloned())
            .unwrap_err()
            .setting,
        "EXCHANGE_METADATA_CACHE_ENABLED"
    );
}

#[test]
fn existing_environment_names_override_defaults() {
    let mut env = environment();
    for (k, v) in [
        ("CONSUMER_PREFETCH", "8"),
        ("REDIS_CLIENT_PORT", "6380"),
        ("RABBITMQ_QUEUE", " custom-trades "),
        ("RABBITMQ_RETRY_DELAY_MS", "2000"),
        ("REDIS_LOCK_COMMAND_TIMEOUT_MS", "750"),
        ("PROCESSING_LOCK_PREFIX", "shared:locks"),
        ("SERVICE_REVISION", "revision-123"),
    ] {
        env.insert(k.into(), v.into());
    }
    let config = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap();
    assert_eq!(config.rabbitmq.prefetch.get(), 8);
    assert_eq!(config.rabbitmq.output_queue, "custom-trades");
    assert_eq!(config.rabbitmq.retry_delay, Duration::from_secs(2));
    assert_eq!(config.redis.port.get(), 6380);
    assert_eq!(config.redis.locks.command, Duration::from_millis(750));
    assert_eq!(config.redis.lock_prefix, "shared:locks");
    assert_eq!(config.runtime.service_revision, "revision-123");
}

#[test]
fn required_values_must_be_present_and_nonblank() {
    for key in [
        "RABBIT_MQ",
        "MONGO_PATH",
        "TRADE_STATION_MONGO_PATH",
        "ACCOUNT_VALIDATOR_MONGO_PATH",
        "REDIS",
    ] {
        for value in [None, Some(""), Some("  ")] {
            let mut env = environment();
            env.remove(key);
            if let Some(v) = value {
                env.insert(key.into(), v.into());
            }
            let error = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap_err();
            assert_eq!(error.setting, key);
        }
    }
}

#[test]
fn malformed_explicit_values_fail_instead_of_falling_back() {
    for (key, value) in [
        ("CONSUMER_PREFETCH", "0"),
        ("CONSUMER_PREFETCH", "65536"),
        ("CONSUMER_PREFETCH", "1.5"),
        ("CONSUMER_PREFETCH", "-1"),
        ("REDIS_CLIENT_PORT", "65536"),
        ("REDIS_CLIENT_PORT", "0"),
        ("REDIS_CACHE_COMMAND_TIMEOUT_MS", ""),
        ("REDIS_LOCK_CONNECT_TIMEOUT_MS", "NaN"),
        ("SHUTDOWN_DRAIN_TIMEOUT_MS", "18446744073709551615"),
        ("RABBITMQ_RETRY_MAX_ATTEMPTS", "0"),
        ("RABBITMQ_RETRY_DELAY_MS", "2147483648"),
        ("RABBITMQ_RECONNECT_JITTER_RATIO", "NaN"),
        ("RABBITMQ_RECONNECT_JITTER_RATIO", "1.1"),
        ("RABBITMQ_RECONNECT_JITTER_RATIO", "-0.1"),
        ("PROCESSING_LOCK_PREFIX", " "),
        ("RABBIT_MQ", "http://localhost"),
        ("RABBIT_MQ", "amqp:///"),
        ("MONGO_PATH", "postgres://localhost/bot"),
        ("MONGO_PATH", "mongodb:///bot"),
        (
            "ACCOUNT_VALIDATOR_MONGO_PATH",
            "postgres://localhost/accounts",
        ),
        ("REDIS", "redis://localhost"),
        ("REDIS", "localhost:6379"),
    ] {
        let mut env = environment();
        env.insert(key.into(), value.into());
        let error = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap_err();
        assert_eq!(error.setting, key, "value: {value}");
    }
}

#[test]
fn reconnect_max_cannot_be_less_than_base() {
    let mut env = environment();
    env.insert("RABBITMQ_RECONNECT_BASE_MS".into(), "5000".into());
    env.insert("RABBITMQ_RECONNECT_MAX_MS".into(), "1000".into());
    assert!(AppConfig::from_lookup(|k| env.get(k).cloned()).is_err());
}

#[test]
fn worker_queue_selection_cannot_consume_other_providers() {
    for value in [
        "all",
        "",
        "satoshi-channel-updates.client-trade.binance.futures",
        "satoshi-channel-updates.client-trade.bingx.futures,satoshi-channel-updates.client-trade.bingx.spot",
    ] {
        let mut env = environment();
        env.insert("CLIENT_TRADE_WORKER_QUEUES".into(), value.into());
        assert!(AppConfig::from_lookup(|k| env.get(k).cloned()).is_err());
    }
}

#[test]
fn supports_tls_mongo_seed_lists_srv_and_ipv6() {
    let mut env = environment();
    env.insert(
        "RABBIT_MQ".into(),
        "amqps://user:p%40ss@broker:5671/vhost".into(),
    );
    env.insert(
        "MONGO_PATH".into(),
        "mongodb://user:p%40ss@host1:27017,[::1]:27018/bot?replicaSet=rs0".into(),
    );
    env.insert(
        "TRADE_STATION_MONGO_PATH".into(),
        "mongodb+srv://cluster.example.com/trading".into(),
    );
    env.insert("REDIS".into(), "::1".into());
    let config = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap();
    assert_eq!(config.mongo.bot_uri.expose(), env["MONGO_PATH"]);
}

#[test]
fn debug_and_validation_errors_do_not_expose_credentials() {
    let mut env = environment();
    let config = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap();
    assert!(!format!("{config:?}").contains("secret"));
    env.insert(
        "RABBIT_MQ".into(),
        "https://user:VERY_PRIVATE@localhost".into(),
    );
    let error = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap_err();
    assert!(!format!("{error:?} {error}").contains("VERY_PRIVATE"));
}

#[test]
fn example_environment_is_valid_and_every_setting_is_recognized() {
    let env: HashMap<String, String> = include_str!("../.env.example")
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (key, value) = line.split_once('=').unwrap();
            (key.into(), value.into())
        })
        .collect();
    let read = std::cell::RefCell::new(std::collections::HashSet::new());
    AppConfig::from_lookup(|key| {
        read.borrow_mut().insert(key.to_owned());
        env.get(key).cloned()
    })
    .unwrap();
    assert!(env.keys().all(|key| read.borrow().contains(key)));
}

#[test]
fn queue_override_rejects_collisions_reserved_names_and_long_utf8() {
    for value in [
        "satoshi-channel-updates.client-trade.bingx.futures".to_owned(),
        "satoshi-channel-updates.dead-letter".to_owned(),
        "tg_bot_bingx_position_mode_action_required".to_owned(),
        "some.retry.delay".to_owned(),
        "amq.reserved".to_owned(),
        "é".repeat(128),
        "queue\nname".to_owned(),
    ] {
        let mut env = environment();
        env.insert("RABBITMQ_QUEUE".into(), value);
        let error = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap_err();
        assert_eq!(error.setting, "RABBITMQ_QUEUE");
    }
    let mut env = environment();
    env.insert("RABBITMQ_QUEUE".into(), "  ".into());
    let config = AppConfig::from_lookup(|k| env.get(k).cloned()).unwrap();
    assert_eq!(config.rabbitmq.output_queue, "create-new-trusted-trade");
}

#[test]
fn shared_redis_prefixes_cannot_be_silently_trimmed() {
    for key in ["PROCESSING_LOCK_PREFIX", "EXCHANGE_METADATA_CACHE_PREFIX"] {
        let mut env = environment();
        env.insert(key.into(), " shared:prefix ".into());
        assert!(
            AppConfig::from_lookup(|k| env.get(k).cloned()).is_err(),
            "{key}"
        );
    }
}

#[test]
fn redis_ipv6_brackets_must_be_balanced_and_are_removed_for_the_driver() {
    for value in ["[[::1]]", "[::1", "::1]", "[::1]]"] {
        let mut env = environment();
        env.insert("REDIS".into(), value.into());
        assert!(
            AppConfig::from_lookup(|k| env.get(k).cloned()).is_err(),
            "{value}"
        );
    }
    let mut env = environment();
    env.insert("REDIS".into(), "[::1]".into());
    assert_eq!(
        AppConfig::from_lookup(|k| env.get(k).cloned())
            .unwrap()
            .redis
            .host,
        "::1"
    );
}
