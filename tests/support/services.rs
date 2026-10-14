#![allow(dead_code)]
use channels_manager_v1::config::AppConfig;
use lapin::{Connection, ConnectionProperties};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::time::timeout;
// Own each child immediately, including partially completed startup.
struct StartingChild(Option<Child>);
impl StartingChild {
    fn release(mut self) -> Child {
        self.0.take().unwrap()
    }
}
impl Drop for StartingChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
pub struct Stores {
    pub mongo: Child,
    pub redis: Child,
    pub mongo_port: u16,
    pub redis_port: u16,
}
impl Drop for Stores {
    fn drop(&mut self) {
        let _ = self.mongo.kill();
        let _ = self.mongo.wait();
        let _ = self.redis.kill();
        let _ = self.redis.wait();
    }
}
impl Stores {
    pub async fn start(directory: &std::path::Path) -> Self {
        let mongo_port = free_port();
        let redis_port = free_port();
        let path = directory.join("mongo");
        std::fs::create_dir_all(&path).unwrap();
        let mongo = Command::new(std::env::var("MONGOD_BIN").unwrap_or_else(|_| "mongod".into()))
            .args(["--bind_ip", "127.0.0.1", "--port", &mongo_port.to_string()])
            .arg("--dbpath")
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mongo = StartingChild(Some(mongo));
        let redis = Command::new(
            std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into()),
        )
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &redis_port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
        let stores = Self {
            mongo: mongo.release(),
            redis,
            mongo_port,
            redis_port,
        };
        timeout(Duration::from_secs(10), async {
            loop {
                if tokio::net::TcpStream::connect(("127.0.0.1", mongo_port))
                    .await
                    .is_ok()
                    && tokio::net::TcpStream::connect(("127.0.0.1", redis_port))
                        .await
                        .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        stores
    }
    pub fn config(&self, broker: &Broker) -> AppConfig {
        AppConfig::from_lookup(|key| match key {
            "RABBIT_MQ" => Some(broker.uri()),
            "REDIS" => Some("127.0.0.1".into()),
            "REDIS_CLIENT_PORT" => Some(self.redis_port.to_string()),
            "MONGO_PATH" => Some(format!("mongodb://127.0.0.1:{}/bot", self.mongo_port)),
            "TRADE_STATION_MONGO_PATH" => {
                Some(format!("mongodb://127.0.0.1:{}/trading", self.mongo_port))
            }
            "ACCOUNT_VALIDATOR_MONGO_PATH" => {
                Some(format!("mongodb://127.0.0.1:{}/accounts", self.mongo_port))
            }
            "STARTUP_RETRY_DELAY_MS" => Some("50".into()),
            "STARTUP_RETRY_MAX_DELAY_MS" => Some("100".into()),
            _ => None,
        })
        .unwrap()
    }
}
pub struct Broker {
    pub process: Child,
    pub command: Command,
    pub directory: PathBuf,
    pub port: u16,
    pub epmd_port: u16,
}
impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = Command::new("epmd")
            .env("ERL_EPMD_PORT", self.epmd_port.to_string())
            .arg("-kill")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
impl Broker {
    pub async fn start() -> Self {
        let port = free_port();
        let epmd_port = free_port();
        let dist_port = free_port();
        let directory =
            std::env::temp_dir().join(format!("channels-rabbit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("rabbitmq.conf"),
            format!("listeners.tcp.1 = 127.0.0.1:{port}\nloopback_users.guest = true\n"),
        )
        .unwrap();
        std::fs::write(directory.join("env.conf"), "").unwrap();
        std::fs::write(directory.join("plugins"), "[].\n").unwrap();
        let log = std::fs::File::create(directory.join("startup.log")).unwrap();
        let mut command = Command::new(
            std::env::var("RABBITMQ_SERVER_BIN").unwrap_or_else(|_| "rabbitmq-server".into()),
        );
        let process = command
            .env("RABBITMQ_CONF_ENV_FILE", directory.join("env.conf"))
            .env("RABBITMQ_CONFIG_FILE", directory.join("rabbitmq.conf"))
            .env("RABBITMQ_MNESIA_BASE", directory.join("data"))
            .env("RABBITMQ_LOG_BASE", directory.join("logs"))
            .env("RABBITMQ_PID_FILE", directory.join("pid"))
            .env("RABBITMQ_ENABLED_PLUGINS_FILE", directory.join("plugins"))
            .env("RABBITMQ_NODENAME", format!("channels_{port}@localhost"))
            .env("RABBITMQ_NODE_PORT", port.to_string())
            .env("RABBITMQ_DIST_PORT", dist_port.to_string())
            .env("ERL_EPMD_PORT", epmd_port.to_string())
            .env("ERL_EPMD_ADDRESS", "127.0.0.1")
            .env(
                "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
                "+S 2:2 +A 2 -setcookie isolated_channels_test",
            )
            .env("RABBITMQ_ALLOW_INPUT", "1")
            .env("RABBITMQ_SERVER_START_ARGS", "-noshell -noinput")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let broker = Self {
            process,
            command,
            directory,
            port,
            epmd_port,
        };
        broker.wait_ready().await;
        broker
    }
    pub async fn wait_ready(&self) {
        timeout(Duration::from_secs(40), async {
            loop {
                if let Ok(Ok(conn)) = timeout(
                    Duration::from_secs(1),
                    Connection::connect(&self.uri(), ConnectionProperties::default()),
                )
                .await
                {
                    conn.close(200, "OK".into()).await.unwrap();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "RabbitMQ startup failed: {}",
                std::fs::read_to_string(self.directory.join("startup.log")).unwrap()
            )
        });
    }
    pub fn uri(&self) -> String {
        format!("amqp://guest:guest@127.0.0.1:{}/%2f", self.port)
    }
    pub fn config(&self) -> AppConfig {
        AppConfig::from_lookup(|key| match key {
            "RABBIT_MQ" => Some(self.uri()),
            "REDIS" => Some("127.0.0.1".into()),
            "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
                Some("mongodb://localhost/test".into())
            }
            "RABBITMQ_RETRY_DELAY_MS" => Some("100".into()),
            _ => None,
        })
        .unwrap()
    }
}
