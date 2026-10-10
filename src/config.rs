//! Startup configuration. Parsing performs no network or filesystem operations.
mod read;

use crate::contracts::{
    rabbitmq::{BINGX_FUTURES_QUEUE, DEFAULT_TRADE_QUEUE},
    storage::{DEFAULT_CACHE_PREFIX, DEFAULT_LOCK_PREFIX},
};
use read::Reader;
use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

/// Connection strings may contain credentials. Access must be explicit.
#[derive(Clone)]
pub struct ConnectionString(String);

impl ConnectionString {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ConnectionString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConfigError {
    pub setting: &'static str,
    pub reason: &'static str,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.setting, self.reason)
    }
}
impl std::error::Error for ConfigError {}

#[derive(Debug)]
pub struct AppConfig {
    pub telegram_intake_enabled: bool,
    pub client_trade_worker_enabled: bool,
    pub client_trade_worker_prefetch: NonZeroU16,
    pub telegram_bot_token: Option<ConnectionString>,
    pub telegram_api_base_url: String,
    pub client_trade_job_fanout_enabled: bool,
    pub bingx_public_api_base_url: String,
    pub rabbitmq: RabbitMqConfig,
    pub redis: RedisConfig,
    pub mongo: MongoConfig,
    pub runtime: RuntimeConfig,
}

#[derive(Debug, Clone)]
pub struct RabbitMqConfig {
    pub uri: ConnectionString,
    pub input_queue: &'static str,
    pub output_queue: String,
    /// Also bounds the worker's unacknowledged in-flight deliveries.
    pub prefetch: NonZeroU16,
    pub heartbeat_seconds: NonZeroU16,
    pub retry_max_attempts: NonZeroU32,
    pub retry_delay: Duration,
    pub reconnect: ReconnectConfig,
    pub publish_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    pub base: Duration,
    pub max: Duration,
    pub jitter_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: NonZeroU16,
    pub locks: RedisTimeouts,
    pub cache_enabled: bool,
    pub cache: RedisTimeouts,
    pub lock_prefix: String,
    pub cache_prefix: String,
    pub metadata_cache_ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct RedisTimeouts {
    pub connect: Duration,
    pub command: Duration,
}

#[derive(Debug, Clone)]
pub struct MongoConfig {
    pub bot_uri: ConnectionString,
    pub trade_station_uri: ConnectionString,
    pub account_validator_uri: ConnectionString,
    pub max_pool_size: NonZeroU32,
    pub server_selection_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub startup_retry_delay: Duration,
    pub startup_retry_max_delay: Duration,
    pub startup_retry_jitter_ratio: f64,
    pub operation_timeout: Duration,
    pub shutdown_drain_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub service_revision: String,
}

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (setting, value) in [
            ("STARTUP_RETRY_DELAY_MS", self.startup_retry_delay),
            ("STARTUP_RETRY_MAX_DELAY_MS", self.startup_retry_max_delay),
            ("RUNTIME_OPERATION_TIMEOUT_MS", self.operation_timeout),
            ("SHUTDOWN_DRAIN_TIMEOUT_MS", self.shutdown_drain_timeout),
            ("SHUTDOWN_TIMEOUT_MS", self.shutdown_timeout),
        ] {
            if value < Duration::from_millis(1) || value > Duration::from_millis(i32::MAX as u64) {
                return Err(ConfigError {
                    setting,
                    reason: "must be milliseconds between 1 and 2147483647",
                });
            }
        }
        if self.startup_retry_max_delay < self.startup_retry_delay {
            return Err(ConfigError {
                setting: "STARTUP_RETRY_MAX_DELAY_MS",
                reason: "must be at least STARTUP_RETRY_DELAY_MS",
            });
        }
        if !self.startup_retry_jitter_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.startup_retry_jitter_ratio)
        {
            return Err(ConfigError {
                setting: "STARTUP_RETRY_JITTER_RATIO",
                reason: "must be a finite number between 0 and 1",
            });
        }
        if self.shutdown_timeout <= self.shutdown_drain_timeout {
            return Err(ConfigError {
                setting: "SHUTDOWN_TIMEOUT_MS",
                reason: "must exceed SHUTDOWN_DRAIN_TIMEOUT_MS to allow resource cleanup",
            });
        }
        Ok(())
    }
}

impl AppConfig {
    /// Read optional .env.local in the working directory, with process overrides.
    /// Parse into a map instead of mutating the environment after Tokio starts.
    pub fn from_env() -> Result<Self, ConfigError> {
        let file_error = || ConfigError {
            setting: ".env.local",
            reason: "could not read or parse environment file",
        };
        let mut local = std::collections::HashMap::new();
        match dotenvy::from_path_iter(".env.local") {
            Ok(entries) => {
                for entry in entries {
                    let (key, value) = entry.map_err(|_| file_error())?;
                    local.entry(key).or_insert(value);
                }
            }
            Err(error) if error.not_found() => {}
            Err(_) => return Err(file_error()),
        }
        // Non-Unicode values are invalid, rather than silently treated as unset.
        Self::parse(Reader(|key| match std::env::var(key) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(local.get(key).cloned()),
            Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError {
                setting: key,
                reason: "must be valid Unicode",
            }),
        }))
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Self::parse(Reader(|key| Ok(lookup(key))))
    }

    fn parse(
        reader: Reader<impl Fn(&'static str) -> Result<Option<String>, ConfigError>>,
    ) -> Result<Self, ConfigError> {
        let telegram_bot_token = (reader.0)("SATOSHI_TG_TOKEN")?
            .map(|token| {
                if crate::telegram::sender::valid_token(&token) {
                    Ok(ConnectionString(token))
                } else {
                    Err(ConfigError {
                        setting: "SATOSHI_TG_TOKEN",
                        reason: "must be a valid bot token",
                    })
                }
            })
            .transpose()?;
        let telegram_api_base_url =
            reader.text("TELEGRAM_API_BASE_URL", Some("https://api.telegram.org"))?;
        if !crate::telegram::sender::valid_endpoint(&telegram_api_base_url) {
            return Err(ConfigError {
                setting: "TELEGRAM_API_BASE_URL",
                reason: "must be Telegram HTTPS or a numeric HTTP loopback test origin",
            });
        }
        let bingx_public_api_base_url = reader.text(
            "BINGX_PUBLIC_API_BASE_URL",
            Some("https://open-api.bingx.com"),
        )?;
        if bingx_public_api_base_url != "https://open-api.bingx.com" {
            let valid = url::Url::parse(&bingx_public_api_base_url).is_ok_and(|url| {
                let loopback = match url.host() {
                    Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                    Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                    _ => false,
                };
                loopback
                    && url.scheme() == "http"
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
            });
            if !valid {
                return Err(ConfigError {
                    setting: "BINGX_PUBLIC_API_BASE_URL",
                    reason: "must be the BingX HTTPS origin or a numeric HTTP loopback test origin",
                });
            }
        }
        let uri = reader.uri("RABBIT_MQ", false)?;
        let bot_uri = reader.uri("MONGO_PATH", true)?;
        let trade_station_uri = reader.uri("TRADE_STATION_MONGO_PATH", true)?;
        let account_validator_uri = reader.uri("ACCOUNT_VALIDATOR_MONGO_PATH", true)?;
        let host = reader.host("REDIS")?;
        let input = reader.text("CLIENT_TRADE_WORKER_QUEUES", Some(BINGX_FUTURES_QUEUE))?;
        if input != BINGX_FUTURES_QUEUE {
            return Err(ConfigError {
                setting: "CLIENT_TRADE_WORKER_QUEUES",
                reason: "must select only satoshi-channel-updates.client-trade.bingx.futures",
            });
        }
        let output_queue = reader.queue("RABBITMQ_QUEUE", DEFAULT_TRADE_QUEUE)?;
        // A shared output/input queue would route published trades back as jobs.
        if [
            BINGX_FUTURES_QUEUE,
            crate::contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE,
            crate::contracts::rabbitmq::ADMISSION_EVENT_QUEUE,
            crate::contracts::rabbitmq::DEAD_LETTER_QUEUE,
        ]
        .contains(&output_queue.as_str())
            || output_queue.ends_with(crate::contracts::rabbitmq::RETRY_SUFFIX)
        {
            return Err(ConfigError {
                setting: "RABBITMQ_QUEUE",
                reason: "must not overlap input, admission, retry, or dead-letter queues",
            });
        }
        let reconnect = ReconnectConfig {
            base: reader.duration("RABBITMQ_RECONNECT_BASE_MS", 500)?,
            max: reader.duration("RABBITMQ_RECONNECT_MAX_MS", 30000)?,
            jitter_ratio: reader.ratio("RABBITMQ_RECONNECT_JITTER_RATIO", 0.2)?,
        };
        if reconnect.max < reconnect.base {
            return Err(ConfigError {
                setting: "RABBITMQ_RECONNECT_MAX_MS",
                reason: "must be at least RABBITMQ_RECONNECT_BASE_MS",
            });
        }
        let runtime = RuntimeConfig {
            startup_retry_delay: reader.duration("STARTUP_RETRY_DELAY_MS", 5000)?,
            startup_retry_max_delay: reader.duration("STARTUP_RETRY_MAX_DELAY_MS", 30000)?,
            startup_retry_jitter_ratio: reader.ratio("STARTUP_RETRY_JITTER_RATIO", 0.2)?,
            operation_timeout: reader.duration("RUNTIME_OPERATION_TIMEOUT_MS", 10000)?,
            shutdown_drain_timeout: reader.duration("SHUTDOWN_DRAIN_TIMEOUT_MS", 10000)?,
            shutdown_timeout: reader.duration("SHUTDOWN_TIMEOUT_MS", 30000)?,
            service_revision: reader.text("SERVICE_REVISION", Some("channels-manager-v1"))?,
        };
        runtime.validate()?;
        Ok(Self {
            telegram_intake_enabled: reader.boolean("TELEGRAM_INTAKE_ENABLED", true)?,
            client_trade_worker_enabled: reader.boolean("CLIENT_TRADE_WORKER_ENABLED", false)?,
            client_trade_worker_prefetch: reader.port("CLIENT_TRADE_WORKER_PREFETCH", 2)?,
            telegram_bot_token,
            telegram_api_base_url,
            client_trade_job_fanout_enabled: reader
                .boolean("CLIENT_TRADE_JOB_FANOUT_ENABLED", false)?,
            bingx_public_api_base_url,
            rabbitmq: RabbitMqConfig {
                uri,
                input_queue: BINGX_FUTURES_QUEUE,
                output_queue,
                prefetch: reader.port("CONSUMER_PREFETCH", 2)?,
                heartbeat_seconds: reader.port("RABBITMQ_HEARTBEAT_SECONDS", 30)?,
                retry_max_attempts: reader.positive("RABBITMQ_RETRY_MAX_ATTEMPTS", 5)?,
                retry_delay: reader.duration("RABBITMQ_RETRY_DELAY_MS", 1000)?,
                reconnect,
                publish_timeout: Duration::from_secs(5),
            },
            redis: RedisConfig {
                host,
                port: reader.port("REDIS_CLIENT_PORT", 6379)?,
                locks: RedisTimeouts {
                    connect: reader.duration("REDIS_LOCK_CONNECT_TIMEOUT_MS", 1000)?,
                    command: reader.duration("REDIS_LOCK_COMMAND_TIMEOUT_MS", 1000)?,
                },
                cache_enabled: reader.boolean("EXCHANGE_METADATA_CACHE_ENABLED", true)?,
                cache: RedisTimeouts {
                    connect: reader.duration("REDIS_CACHE_CONNECT_TIMEOUT_MS", 1000)?,
                    command: reader.duration("REDIS_CACHE_COMMAND_TIMEOUT_MS", 500)?,
                },
                lock_prefix: reader.prefix("PROCESSING_LOCK_PREFIX", DEFAULT_LOCK_PREFIX)?,
                cache_prefix: reader
                    .prefix("EXCHANGE_METADATA_CACHE_PREFIX", DEFAULT_CACHE_PREFIX)?,
                metadata_cache_ttl: reader.duration("EXCHANGE_METADATA_CACHE_TTL_MS", 3600000)?,
            },
            mongo: MongoConfig {
                bot_uri,
                trade_station_uri,
                account_validator_uri,
                max_pool_size: NonZeroU32::new(10).unwrap(),
                server_selection_timeout: Duration::from_secs(5),
            },
            runtime,
        })
    }
}
