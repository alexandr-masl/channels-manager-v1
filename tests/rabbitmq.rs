use channels_manager_v1::{
    config::AppConfig,
    contracts::{
        messages::{NewTradeMessage, trade_creation_id},
        rabbitmq::*,
    },
    rabbitmq::{PreparedPublication, RabbitError, RabbitMq},
};
use channels_manager_v1::{
    infrastructure::{DeliveryHandler, Infrastructure, WorkerServices},
    rabbitmq::InboundDelivery,
    runtime::{Lifecycle, Phase},
};
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::FieldTable};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

struct Stores {
    mongo: Child,
    redis: Child,
    mongo_port: u16,
    redis_port: u16,
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
    async fn start(directory: &std::path::Path) -> Self {
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
            mongo,
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
    fn config(&self, broker: &Broker) -> AppConfig {
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
struct Handler {
    active: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    done: Arc<AtomicUsize>,
    gate: Arc<tokio::sync::Semaphore>,
    fail: bool,
    dropped: Arc<AtomicBool>,
}
impl DeliveryHandler for Handler {
    fn handle(
        &self,
        delivery: InboundDelivery,
        services: WorkerServices,
    ) -> futures_util::future::BoxFuture<'static, Result<(), RabbitError>> {
        let active = self.active.clone();
        let max = self.max.clone();
        let done = self.done.clone();
        let gate = self.gate.clone();
        let fail = self.fail;
        let dropped = self.dropped.clone();
        Box::pin(async move {
            struct Guard {
                active: Arc<AtomicUsize>,
                dropped: Arc<AtomicBool>,
                slow: bool,
            }
            impl Drop for Guard {
                fn drop(&mut self) {
                    if self.slow {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    self.active.fetch_sub(1, Ordering::SeqCst);
                    self.dropped.store(true, Ordering::SeqCst);
                }
            }
            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(count, Ordering::SeqCst);
            let guard = Guard {
                active,
                dropped,
                slow: fail && delivery.body() == b"sibling",
            };
            if fail && delivery.body() == b"sibling" {
                std::future::pending::<()>().await;
            }
            gate.acquire().await.unwrap().forget();
            if fail {
                return Err(RabbitError::InvalidPayload);
            }
            services
                .publisher
                .publish(&PreparedPublication::trade(&trade(), "work")?)
                .await?;
            delivery.ack().await?;
            done.fetch_add(1, Ordering::SeqCst);
            drop(guard);
            Ok(())
        })
    }
}
async fn running(phases: &mut tokio::sync::watch::Receiver<Phase>) {
    timeout(Duration::from_secs(10), async {
        while *phases.borrow_and_update() != Phase::Running {
            phases.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn seed(channel: &lapin::Channel, body: &[u8]) {
    channel
        .basic_publish(
            "".into(),
            BINGX_FUTURES_QUEUE.into(),
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB and Redis; requires loopback access"]
async fn concrete_lifecycle_bounds_workers_and_joins_cleanup() {
    let broker = Broker::start().await;
    let stores = Stores::start(&broker.directory).await;
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // Infrastructure-only startup must leave backlog untouched.
    let config = stores.config(&broker);
    let runtime = config.runtime.clone();
    let lifecycle = Lifecycle::new(Infrastructure::new(config, None).unwrap(), runtime).unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    seed(&channel, b"one").await;
    seed(&channel, b"two").await;
    seed(&channel, b"three").await;
    let queue = channel
        .queue_declare(
            BINGX_FUTURES_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(queue.consumer_count(), 0);
    assert_eq!(queue.message_count(), 3);
    stop.cancel();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let active = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(Handler {
        active: active.clone(),
        max: max.clone(),
        done: done.clone(),
        gate: gate.clone(),
        fail: false,
        dropped: dropped.clone(),
    });
    let config = stores.config(&broker);
    let runtime = config.runtime.clone();
    let lifecycle =
        Lifecycle::new(Infrastructure::new(config, Some(handler)).unwrap(), runtime).unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    timeout(Duration::from_secs(3), async {
        while active.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(max.load(Ordering::SeqCst), 2);
    gate.add_permits(3);
    timeout(Duration::from_secs(3), async {
        while done.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);

    // A failed handler must join a sibling's destructor before teardown completes.
    dropped.store(false, Ordering::SeqCst);
    gate.forget_permits(gate.available_permits());
    seed(&channel, b"sibling").await;
    seed(&channel, b"fail").await;
    let handler = Arc::new(Handler {
        active: active.clone(),
        max,
        done,
        gate: gate.clone(),
        fail: true,
        dropped: dropped.clone(),
    });
    let config = stores.config(&broker);
    let runtime = config.runtime.clone();
    let lifecycle =
        Lifecycle::new(Infrastructure::new(config, Some(handler)).unwrap(), runtime).unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    timeout(Duration::from_secs(3), async {
        while active.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.add_permits(1);
    timeout(Duration::from_secs(3), async {
        while *phases.borrow_and_update() == Phase::Running {
            phases.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    stop.cancel();
    let _ = timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "coordinator completed before sibling cleanup"
    );
    channel
        .queue_purge(BINGX_FUTURES_QUEUE.into(), QueuePurgeOptions::default())
        .await
        .unwrap();
    seed(&channel, b"sibling").await;
    let handler = Arc::new(Handler {
        active: active.clone(),
        max: Arc::new(AtomicUsize::new(0)),
        done: Arc::new(AtomicUsize::new(0)),
        gate: Arc::new(tokio::sync::Semaphore::new(0)),
        fail: true,
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let config = stores.config(&broker);
    let mut runtime = config.runtime.clone();
    runtime.shutdown_drain_timeout = Duration::from_millis(50);
    let lifecycle =
        Lifecycle::new(Infrastructure::new(config, Some(handler)).unwrap(), runtime).unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    timeout(Duration::from_secs(3), async {
        while active.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    let error = timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .shutdown
            .iter()
            .any(|e| e.step == channels_manager_v1::runtime::ShutdownStep::Drain)
    );
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "forced abort did not join handler cleanup"
    );
    admin.close(200, "OK".into()).await.unwrap();
}

struct Broker {
    process: Child,
    command: Command,
    directory: PathBuf,
    port: u16,
    epmd_port: u16,
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
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
impl Broker {
    async fn start() -> Self {
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
    async fn wait_ready(&self) {
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
    fn uri(&self) -> String {
        format!("amqp://guest:guest@127.0.0.1:{}/%2f", self.port)
    }
    fn config(&self) -> AppConfig {
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
fn trade() -> NewTradeMessage {
    NewTradeMessage {
        expires_at: 4102444800000,
        trade_object: json!({"id":trade_creation_id("work"),"symbol":"BTC-USDT"}),
        client_data: json!({"clientId":"account","provider":"BingX"}),
    }
}

#[test]
fn prepared_trade_preserves_identity_expiry_and_bytes() {
    let mut input = trade();
    let prepared = PreparedPublication::trade(&input, "work").unwrap();
    let bytes = prepared.body().to_vec();
    input.expires_at = 1;
    input.trade_object["symbol"] = json!("changed");
    assert_eq!(prepared.body(), bytes);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["expires_at"],
        4102444800000u64
    );
    assert!(matches!(
        PreparedPublication::trade(&input, "other"),
        Err(RabbitError::InvalidPayload)
    ));
}

#[tokio::test]
#[ignore = "starts an isolated local RabbitMQ; requires loopback access"]
async fn rabbitmq_stage5_contracts() {
    let mut broker = Broker::start().await;
    let config = broker.config();
    let mut rabbit = RabbitMq::new(config.rabbitmq.clone(), config.runtime.operation_timeout);
    assert!(rabbit.publisher().is_err());
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    assert!(rabbit.heartbeat_seconds() > 0);
    let publisher = rabbit.publisher().unwrap();
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // Equivalent redeclarations verify durability/exclusivity/deletion/arguments.
    for queue in queue_contracts(&config.rabbitmq) {
        channel
            .queue_declare(
                queue.name.into(),
                QueueDeclareOptions::default(),
                channels_manager_v1::rabbitmq::queue_arguments(&queue.arguments).unwrap(),
            )
            .await
            .unwrap();
    }
    let publication = PreparedPublication::trade(&trade(), "work").unwrap();
    let mut expired = trade();
    expired.expires_at = 1;
    assert_eq!(
        publisher
            .publish(&PreparedPublication::trade(&expired, "work").unwrap())
            .await,
        Err(RabbitError::Expired)
    );
    publisher.publish(&publication).await.unwrap();
    publisher.publish(&publication).await.unwrap();
    for _ in 0..2 {
        let delivery = channel
            .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.data, publication.body());
        assert_eq!(delivery.properties.delivery_mode(), &Some(1));
        assert_eq!(
            delivery
                .properties
                .content_type()
                .as_ref()
                .unwrap()
                .as_str(),
            "application/json"
        );
        assert_eq!(
            delivery.properties.message_id().as_ref().unwrap().as_str(),
            trade_creation_id("work")
        );
        delivery.ack(BasicAckOptions::default()).await.unwrap();
    }
    channel
        .queue_delete(DEFAULT_TRADE_QUEUE.into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        publisher.publish(&publication).await,
        Err(RabbitError::Unroutable)
    );
    channel
        .queue_declare(
            DEFAULT_TRADE_QUEUE.into(),
            QueueDeclareOptions::default(),
            channels_manager_v1::rabbitmq::queue_arguments(
                &json!({"x-max-length":1,"x-overflow":"reject-publish"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    publisher.publish(&publication).await.unwrap();
    assert_eq!(
        publisher.publish(&publication).await,
        Err(RabbitError::Nack)
    );
    channel
        .queue_delete(DEFAULT_TRADE_QUEUE.into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    channel
        .queue_declare(
            DEFAULT_TRADE_QUEUE.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let mut consumer = rabbit.start_consumer().await.unwrap();
    publisher
        .publish(
            &PreparedPublication::retry(b"delayed".to_vec(), None, FieldTable::default()).unwrap(),
        )
        .await
        .unwrap();
    let delayed = timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(delayed.body(), b"delayed");
    delayed.ack().await.unwrap();
    for n in 0..3 {
        channel
            .basic_publish(
                "".into(),
                BINGX_FUTURES_QUEUE.into(),
                BasicPublishOptions::default(),
                n.to_string().as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let first = timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        timeout(Duration::from_millis(100), consumer.next())
            .await
            .is_err()
    );
    first.ack().await.unwrap();
    let third = timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    second.ack().await.unwrap();
    third.requeue().await.unwrap();
    let redelivery = timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(redelivery.redelivered());
    redelivery.ack().await.unwrap();
    rabbit.quiesce();
    assert!(consumer.next().await.unwrap().is_none());
    rabbit.stop_consumers().await.unwrap();
    rabbit.flush().await.unwrap();
    rabbit.close().await.unwrap();
    rabbit.close().await.unwrap();
    assert_eq!(
        publisher.publish(&publication).await,
        Err(RabbitError::Unavailable)
    );
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    let mut cancelled = rabbit.start_consumer().await.unwrap();
    channel
        .queue_delete(BINGX_FUTURES_QUEUE.into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), cancelled.next())
            .await
            .unwrap()
            .is_err()
    );
    timeout(Duration::from_secs(2), rabbit.wait_for_failure())
        .await
        .unwrap();
    rabbit.close().await.unwrap();
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    let idle = rabbit.publisher().unwrap();
    broker.process.kill().unwrap();
    broker.process.wait().unwrap();
    timeout(Duration::from_secs(2), rabbit.wait_for_failure())
        .await
        .unwrap();
    assert_eq!(
        idle.publish(&publication).await,
        Err(RabbitError::Unavailable)
    );
    rabbit.close().await.unwrap();
}

#[tokio::test]
#[ignore = "starts isolated RabbitMQ and a loopback fault proxy"]
async fn lost_confirmation_is_uncertain_and_blocks_blind_replay() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let broker = Broker::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let upstream = broker.port;
    let blocked = Arc::new(AtomicBool::new(false));
    let block = blocked.clone();
    let resume = Arc::new(tokio::sync::Notify::new());
    let resumed = resume.clone();
    let proxy = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await.unwrap();
        let mut server = tokio::net::TcpStream::connect(("127.0.0.1", upstream))
            .await
            .unwrap();
        let (mut cr, mut cw) = client.split();
        let (mut sr, mut sw) = server.split();
        let inbound = tokio::io::copy(&mut cr, &mut sw);
        let outbound = async {
            let mut bytes = [0u8; 8192];
            loop {
                let count = sr.read(&mut bytes).await?;
                if count == 0 {
                    break;
                }
                if block.load(Ordering::SeqCst) {
                    resumed.notified().await;
                }
                cw.write_all(&bytes[..count]).await?;
            }
            Ok::<_, std::io::Error>(())
        };
        tokio::select! {_=inbound=>{},_=outbound=>{}}
    });
    struct Abort(tokio::task::JoinHandle<()>);
    impl Drop for Abort {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _proxy = Abort(proxy);
    let uri = format!("amqp://guest:guest@127.0.0.1:{port}/%2f");
    let mut config = AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some(uri.clone()),
        "REDIS" => Some("localhost".into()),
        "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        _ => None,
    })
    .unwrap();
    config.rabbitmq.publish_timeout = Duration::from_millis(100);
    let mut rabbit = RabbitMq::new(config.rabbitmq, Duration::from_secs(2));
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    let publisher = rabbit.publisher().unwrap();
    let message = PreparedPublication::trade(&trade(), "work").unwrap();
    blocked.store(true, Ordering::SeqCst);
    assert_eq!(
        timeout(Duration::from_secs(1), publisher.publish(&message))
            .await
            .unwrap(),
        Err(RabbitError::PublishUncertain)
    );
    assert_eq!(
        publisher.publish(&message).await,
        Err(RabbitError::Unavailable)
    );
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    let queue = channel
        .queue_declare(
            DEFAULT_TRADE_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        queue.message_count(),
        1,
        "uncertain publication was replayed"
    );
    blocked.store(false, Ordering::SeqCst);
    resume.notify_one();
    rabbit.close().await.unwrap();
    admin.close(200, "OK".into()).await.unwrap();
}

#[tokio::test]
#[ignore = "uses a loopback server to stall the AMQP handshake"]
async fn stalled_handshake_is_bounded_and_releases_socket() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some(format!("amqp://127.0.0.1:{port}/%2f")),
        "REDIS" => Some("localhost".into()),
        "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        _ => None,
    })
    .unwrap();
    let mut rabbit = RabbitMq::new(config.rabbitmq, Duration::from_millis(100));
    let connect = rabbit.connect_and_declare();
    let (result, socket) = tokio::join!(connect, listener.accept());
    let (mut socket, _) = socket.unwrap();
    assert_eq!(result, Err(RabbitError::Timeout));
    rabbit.close().await.unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(1), socket.read_to_end(&mut bytes))
        .await
        .expect("timed-out handshake retained socket")
        .unwrap();
}

#[tokio::test]
#[ignore = "starts isolated RabbitMQ; requires loopback access"]
async fn bounded_delivery_retries_preserve_payload_then_dead_letter() {
    use channels_manager_v1::rabbitmq::{DeliveryOutcome, DeliveryPolicy, RetryReason};
    let broker = Broker::start().await;
    let mut config = broker.config();
    config.rabbitmq.retry_max_attempts = 2.try_into().unwrap();
    let policy = DeliveryPolicy::new(&config.rabbitmq);
    let mut rabbit = RabbitMq::new(config.rabbitmq, config.runtime.operation_timeout);
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    let publisher = rabbit.publisher().unwrap();
    let mut consumer = rabbit.start_consumer().await.unwrap();
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let body = b" {\"expires_at\":123, \"value\":true} ";
    channel
        .basic_publish(
            "".into(),
            BINGX_FUTURES_QUEUE.into(),
            BasicPublishOptions::default(),
            body,
            BasicProperties::default().with_message_id("".into()),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let mut first_failure = None;
    for attempt in 0..=2 {
        let delivery = timeout(Duration::from_secs(3), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(delivery.body(), body);
        assert_eq!(
            delivery
                .properties()
                .message_id()
                .as_ref()
                .unwrap()
                .as_str(),
            ""
        );
        if attempt > 0 {
            let headers = delivery.properties().headers().as_ref().unwrap();
            assert_eq!(
                headers.inner().get(RETRY_ATTEMPT_HEADER),
                Some(&lapin::types::AMQPValue::LongLongInt(attempt))
            );
            let first = headers.inner().get(FIRST_FAILURE_HEADER).unwrap().clone();
            if let Some(previous) = &first_failure {
                assert_eq!(previous, &first);
            }
            first_failure = Some(first);
        }
        policy
            .settle(
                delivery,
                DeliveryOutcome::PreClaimRetry(RetryReason::Timeout),
                &publisher,
            )
            .await
            .unwrap();
    }
    let dead = channel
        .basic_get(DEAD_LETTER_QUEUE.into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&dead.data).unwrap();
    assert_eq!(value["attempt"], 3);
    assert_eq!(value["maxAttempts"], 2);
    assert_eq!(value["payload"]["expires_at"], 123);
    assert_eq!(value["error"], "TIMEOUT");
    dead.ack(BasicAckOptions::default()).await.unwrap();
    for outcome in [
        DeliveryOutcome::Completed,
        DeliveryOutcome::Rejected,
        DeliveryOutcome::Suppressed,
        DeliveryOutcome::PostClaimTerminal,
    ] {
        seed(&channel, b"terminal").await;
        let delivery = consumer.next().await.unwrap().unwrap();
        policy.settle(delivery, outcome, &publisher).await.unwrap();
    }
    assert!(
        timeout(Duration::from_millis(200), consumer.next())
            .await
            .is_err()
    );
    rabbit.close().await.unwrap();
}

#[tokio::test]
#[ignore = "starts isolated RabbitMQ; requires loopback access"]
async fn failed_retry_or_dead_letter_keeps_original_for_recovery() {
    use channels_manager_v1::rabbitmq::{DeliveryOutcome, DeliveryPolicy, RetryReason};
    let broker = Broker::start().await;
    let config = broker.config();
    let policy = DeliveryPolicy::new(&config.rabbitmq);
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for (queue, attempt) in [
        (format!("{BINGX_FUTURES_QUEUE}{RETRY_SUFFIX}"), 0),
        (DEAD_LETTER_QUEUE.into(), 5),
    ] {
        let mut rabbit = RabbitMq::new(config.rabbitmq.clone(), config.runtime.operation_timeout);
        rabbit.connect_and_declare().await.unwrap();
        rabbit.initialize_publisher().await.unwrap();
        let publisher = rabbit.publisher().unwrap();
        let mut consumer = rabbit.start_consumer().await.unwrap();
        channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await
            .unwrap();
        let mut headers = FieldTable::default();
        headers.insert(
            RETRY_ATTEMPT_HEADER.into(),
            lapin::types::AMQPValue::LongInt(attempt),
        );
        channel
            .basic_publish(
                "".into(),
                BINGX_FUTURES_QUEUE.into(),
                BasicPublishOptions::default(),
                b"poison",
                BasicProperties::default().with_headers(headers),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        let delivery = consumer.next().await.unwrap().unwrap();
        assert_eq!(
            policy
                .settle(
                    delivery,
                    DeliveryOutcome::PreClaimRetry(RetryReason::DependencyUnavailable),
                    &publisher
                )
                .await,
            Err(RabbitError::Unroutable)
        );
        assert!(consumer.next().await.is_err(), "failure must gate intake");
        rabbit.close().await.unwrap();
        let original = channel
            .basic_get(BINGX_FUTURES_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert!(original.redelivered);
        assert_eq!(original.data, b"poison");
        original.ack(BasicAckOptions::default()).await.unwrap();
    }
}

struct PolicyHandler;
impl DeliveryHandler for PolicyHandler {
    fn handle(
        &self,
        delivery: InboundDelivery,
        services: WorkerServices,
    ) -> futures_util::future::BoxFuture<'static, Result<(), RabbitError>> {
        Box::pin(async move {
            services
                .publisher
                .publish(&PreparedPublication::trade(&trade(), "work")?)
                .await?;
            services
                .delivery_policy
                .settle(
                    delivery,
                    channels_manager_v1::rabbitmq::DeliveryOutcome::Completed,
                    &services.publisher,
                )
                .await
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB and Redis; requires loopback access"]
async fn lifecycle_recovers_idle_disconnect_and_consumer_cancellation() {
    let mut broker = Broker::start().await;
    let mut stores = Stores::start(&broker.directory).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let (disconnect, receiver) = tokio::sync::watch::channel(0u32);
    let proxy_stop = CancellationToken::new();
    let stop_proxy = proxy_stop.clone();
    let broker_port = broker.port;
    let proxy = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _=stop_proxy.cancelled()=>break,
                Some(_)=connections.join_next(),if !connections.is_empty()=>{},
                accepted=listener.accept()=> {
                    let (mut client,_) = accepted.unwrap();
                    let mut changed=receiver.clone();
                    changed.borrow_and_update();
                    connections.spawn(async move {
                        let mut server=tokio::net::TcpStream::connect(("127.0.0.1",broker_port)).await.unwrap();
                        tokio::select! {
                            _=changed.changed()=>{},
                            _=tokio::io::copy_bidirectional(&mut client,&mut server)=>{},
                        }
                    });
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });
    let mut config = stores.config(&broker);
    config.rabbitmq.uri = channels_manager_v1::config::AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some(format!("amqp://guest:guest@127.0.0.1:{proxy_port}/%2f")),
        "REDIS" => Some("127.0.0.1".into()),
        "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        _ => None,
    })
    .unwrap()
    .rabbitmq
    .uri;
    let runtime = config.runtime.clone();
    let lifecycle = Lifecycle::new(
        Infrastructure::new(config, Some(Arc::new(PolicyHandler))).unwrap(),
        runtime,
    )
    .unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    let mut admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let mut channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for cycle in 1..=3 {
        if cycle == 1 {
            disconnect.send_replace(cycle);
        } else if cycle == 2 {
            channel
                .queue_delete(BINGX_FUTURES_QUEUE.into(), QueueDeleteOptions::default())
                .await
                .unwrap();
        } else {
            broker.process.kill().unwrap();
            broker.process.wait().unwrap();
            timeout(Duration::from_secs(5), async {
                loop {
                    phases.changed().await.unwrap();
                    if *phases.borrow_and_update() != Phase::Running {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            broker.process = broker.command.spawn().unwrap();
            broker.wait_ready().await;
            admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
                .await
                .unwrap();
            channel = admin.create_channel().await.unwrap();
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .unwrap();
        }
        timeout(Duration::from_secs(10), async {
            if cycle < 3 {
                loop {
                    phases.changed().await.unwrap();
                    if *phases.borrow_and_update() != Phase::Running {
                        break;
                    }
                }
            }
            running(&mut phases).await;
        })
        .await
        .unwrap();
        // Recovery completed while idle, before any publication or job arrives.
        let queue = channel
            .queue_declare(
                BINGX_FUTURES_QUEUE.into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        assert_eq!(queue.consumer_count(), 1);
        seed(&channel, b"job after recovery").await;
        let result = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(message) = channel
                    .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
                    .await
                    .unwrap()
                {
                    break message;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            result.data,
            PreparedPublication::trade(&trade(), "work").unwrap().body()
        );
        result.ack(BasicAckOptions::default()).await.unwrap();
    }
    // Required Redis loss must cancel intake and preserve queued work until recovery.
    stores.redis.kill().unwrap();
    stores.redis.wait().unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            phases.changed().await.unwrap();
            if *phases.borrow_and_update() != Phase::Running {
                break;
            }
        }
        loop {
            let queue = channel
                .queue_declare(
                    BINGX_FUTURES_QUEUE.into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();
            if queue.consumer_count() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    seed(&channel, b"backlog during required outage").await;
    assert!(
        channel
            .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );
    stores.redis =
        Command::new(std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into()))
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &stores.redis_port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
    running(&mut phases).await;
    let result = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(message) = channel
                .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
                .await
                .unwrap()
            {
                break message;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    result.ack(BasicAckOptions::default()).await.unwrap();
    stop.cancel();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    proxy_stop.cancel();
    proxy.await.unwrap();
}

#[tokio::test]
#[ignore = "starts isolated RabbitMQ; requires loopback access"]
async fn malformed_retry_metadata_cannot_reset_budget() {
    use channels_manager_v1::rabbitmq::{DeliveryOutcome, DeliveryPolicy, RetryReason};
    use lapin::types::AMQPValue;
    let broker = Broker::start().await;
    let config = broker.config();
    let policy = DeliveryPolicy::new(&config.rabbitmq);
    let mut rabbit = RabbitMq::new(config.rabbitmq, config.runtime.operation_timeout);
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    let publisher = rabbit.publisher().unwrap();
    let mut consumer = rabbit.start_consumer().await.unwrap();
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for counter in [
        AMQPValue::LongInt(-1),
        AMQPValue::Double(1.5),
        AMQPValue::LongString("broken".into()),
        AMQPValue::LongLongInt(i64::MAX),
    ] {
        let mut headers = FieldTable::default();
        headers.insert(RETRY_ATTEMPT_HEADER.into(), counter);
        headers.insert(RETRY_MAX_ATTEMPTS_HEADER.into(), AMQPValue::LongInt(999));
        headers.insert("custom".into(), AMQPValue::LongString("keep".into()));
        let properties = BasicProperties::default()
            .with_message_id("stable-job".into())
            .with_headers(headers);
        channel
            .basic_publish(
                "".into(),
                BINGX_FUTURES_QUEUE.into(),
                BasicPublishOptions::default(),
                b"invalid json",
                properties,
            )
            .await
            .unwrap()
            .await
            .unwrap();
        let delivery = consumer.next().await.unwrap().unwrap();
        policy
            .settle(
                delivery,
                DeliveryOutcome::PreClaimRetry(RetryReason::ClaimUncertain),
                &publisher,
            )
            .await
            .unwrap();
        let dead = channel
            .basic_get(DEAD_LETTER_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&dead.data).unwrap();
        assert_eq!(value["payload"], "invalid json");
        assert_eq!(value["maxAttempts"], 5);
        assert_eq!(
            dead.properties.message_id().as_ref().unwrap().as_str(),
            "stable-job"
        );
        assert_eq!(
            dead.properties
                .headers()
                .as_ref()
                .unwrap()
                .inner()
                .get("custom"),
            Some(&AMQPValue::LongString("keep".into()))
        );
        dead.ack(BasicAckOptions::default()).await.unwrap();
    }
    assert!(
        timeout(Duration::from_millis(200), consumer.next())
            .await
            .is_err()
    );
    rabbit.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB and Redis; requires loopback access"]
async fn startup_outage_preserves_backlog_until_required_dependency_recovers() {
    let broker = Broker::start().await;
    let mut stores = Stores::start(&broker.directory).await;
    let config = stores.config(&broker);
    let mut setup = RabbitMq::new(config.rabbitmq.clone(), config.runtime.operation_timeout);
    setup.connect_and_declare().await.unwrap();
    setup.close().await.unwrap();
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    seed(&channel, b"backlog before startup").await;
    stores.redis.kill().unwrap();
    stores.redis.wait().unwrap();
    let runtime = config.runtime.clone();
    let lifecycle = Lifecycle::new(
        Infrastructure::new(config, Some(Arc::new(PolicyHandler))).unwrap(),
        runtime,
    )
    .unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    timeout(Duration::from_secs(10), async {
        loop {
            let phase = *phases.borrow_and_update();
            assert_ne!(phase, Phase::Running);
            if matches!(
                phase,
                Phase::Retrying {
                    stage: channels_manager_v1::runtime::StartupStage::RedisRequired,
                    ..
                }
            ) {
                break;
            }
            phases.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let queue = channel
        .queue_declare(
            BINGX_FUTURES_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(queue.consumer_count(), 0);
    assert_eq!(queue.message_count(), 1);
    assert!(
        channel
            .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );
    stores.redis =
        Command::new(std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into()))
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &stores.redis_port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
    running(&mut phases).await;
    let result = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(message) = channel
                .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
                .await
                .unwrap()
            {
                break message;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        result.data,
        PreparedPublication::trade(&trade(), "work").unwrap().body()
    );
    result.ack(BasicAckOptions::default()).await.unwrap();
    stop.cancel();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB and Redis; requires loopback access"]
async fn binary_default_pipeline_publishes_without_enable_flags() {
    binary_signal(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated services; requires loopback access"]
async fn binary_telegram_notification_failure_does_not_block_jobs() {
    binary_signal(true, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated services; requires loopback access"]
async fn binary_dual_consumers_publish_final_trades() {
    binary_signal(true, false).await;
}

async fn binary_signal(send_notification: bool, notification_error: bool) {
    use std::io::{BufRead, BufReader};
    let broker = Broker::start().await;
    let stores = Stores::start(&broker.directory).await;
    let mongo = mongodb::Client::with_uri_str(format!("mongodb://127.0.0.1:{}", stores.mongo_port))
        .await
        .unwrap();
    let bot = mongo.database("bot");
    bot.collection::<mongodb::bson::Document>("mcr_channels")
        .insert_one(mongodb::bson::doc! {"id":-1001596367704_i64,"default_quantity":0.1,"default_buy_targets":[{"fraction":1}],"default_sell_targets":[{"fraction":1}],"strategy":"basic"})
        .await
        .unwrap();
    bot.collection::<mongodb::bson::Document>("tradingprofiles").insert_one(mongodb::bson::doc! {
        "userId":42.0, "exchangeClients":[{"clientId":"integration-account", "provider":"BingX", "connectedChannel":-1001596367704_f64,
        "api_key":"must-not-log-api-key", "api_secret":"must-not-log-api-secret"}]
    }).await.unwrap();
    mongo.database("accounts").collection::<mongodb::bson::Document>("users").insert_one(mongodb::bson::doc! {
        "tg_chat_id":42.0, "auto_trading":true, "valid_till":mongodb::bson::DateTime::from_millis(mongodb::bson::DateTime::now().timestamp_millis()+60000)
    }).await.unwrap();
    let quote_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let quote_endpoint = format!("http://{}", quote_listener.local_addr().unwrap());
    let quotes = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for _ in 0..9 {
            let (mut socket, _) = quote_listener.accept().await.unwrap();
            let mut bytes = vec![0; 8192];
            let count = socket.read(&mut bytes).await.unwrap();
            let request = String::from_utf8_lossy(&bytes[..count]);
            let switching = request.starts_with("POST ");
            assert!(switching || request.starts_with("GET "));
            if switching {
                assert!(
                    request.contains("/openApi/swap/v1/positionSide/dual?dualSidePosition=true&")
                        || request.contains(
                            "/openApi/swap/v2/trade/leverage?symbol=ADA-USDT&leverage=3&side=LONG&"
                        )
                );
            }
            let route = request
                .split_whitespace()
                .nth(1)
                .unwrap()
                .split('?')
                .next()
                .unwrap();
            let data = match route {
                "/openApi/swap/v2/quote/contracts" => {
                    json!([{"symbol":"ADA-USDT","status":1,"pricePrecision":4,"quantityPrecision":1,"tradeMinUSDT":"5"}])
                }
                "/openApi/swap/v1/ticker/price" => json!({"symbol":"ADA-USDT","price":"0.2570"}),
                "/openApi/swap/v1/positionSide/dual" if switching => json!({}),
                "/openApi/swap/v1/positionSide/dual" => json!({"dualSidePosition":false}),
                "/openApi/swap/v2/user/positions" => json!([]),
                "/openApi/swap/v2/trade/openOrders" => json!({"orders":[]}),
                "/openApi/swap/v3/user/balance" => {
                    json!([{"asset":"USDT","availableMargin":"100"}])
                }
                "/openApi/swap/v2/trade/leverage" if switching => {
                    json!({"symbol":"ADA-USDT","leverage":3})
                }
                "/openApi/swap/v2/trade/leverage" => {
                    json!({"longLeverage":5,"shortLeverage":3,"maxLongLeverage":50,"maxShortLeverage":50})
                }
                _ => panic!("unexpected read route"),
            };
            let body = json!({"code":0,"data":data}).to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let mut telegram_env = Vec::new();
    let notification = if send_notification {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        telegram_env.push(("SATOSHI_TG_TOKEN", "123:integration_secret".to_string()));
        telegram_env.push((
            "TELEGRAM_API_BASE_URL",
            format!("http://{}", listener.local_addr().unwrap()),
        ));
        Some(tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 2048];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                let request = String::from_utf8_lossy(&bytes);
                if let Some((headers, body)) = request.split_once("\r\n\r\n") {
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if body.len() >= length {
                        break;
                    }
                }
            }
            let request = String::from_utf8_lossy(&bytes);
            assert!(request.starts_with("POST /bot123:integration_secret/sendMessage "));
            let payload: serde_json::Value =
                serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(payload["chat_id"], -1001596367704_i64);
            assert_eq!(payload["reply_parameters"]["message_id"], 7);
            assert_eq!(payload["text"], "created ✅");
            let (status, body) = if notification_error {
                (403, r#"{"ok":false,"description":"integration_secret"}"#)
            } else {
                (200, r#"{"ok":true,"result":{"message_id":123}}"#)
            };
            socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }))
    } else {
        None
    };
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_channels-manager-v1"))
            .current_dir(&broker.directory)
            .env_clear()
            .envs(telegram_env)
            .env("BINGX_PUBLIC_API_BASE_URL", quote_endpoint)
            .env("RABBIT_MQ", broker.uri())
            .env("REDIS", "127.0.0.1")
            .env("REDIS_CLIENT_PORT", stores.redis_port.to_string())
            .env(
                "MONGO_PATH",
                format!("mongodb://127.0.0.1:{}/bot", stores.mongo_port),
            )
            .env(
                "TRADE_STATION_MONGO_PATH",
                format!("mongodb://127.0.0.1:{}/trading", stores.mongo_port),
            )
            .env(
                "ACCOUNT_VALIDATOR_MONGO_PATH",
                format!("mongodb://127.0.0.1:{}/accounts", stores.mongo_port),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (lines_tx, mut lines) = tokio::sync::mpsc::unbounded_channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if lines_tx.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    timeout(Duration::from_secs(10), async {
        while !lines
            .recv()
            .await
            .unwrap()
            .contains("Listening for messages on tg_bot_channel_update")
        {}
    })
    .await
    .unwrap();
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    channel
        .queue_declare(
            BINGX_FUTURES_QUEUE.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let body = serde_json::to_vec(&json!({"message_id":7,"date":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),"chat":{"id":-1001596367704i64,"type":"channel"},"text":include_str!("../examples/fixtures/ada-signal.txt").trim_end()})).unwrap();
    channel
        .basic_publish(
            "".into(),
            TELEGRAM_CHANNEL_QUEUE.into(),
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let line = timeout(Duration::from_secs(3), async {
        loop {
            let line = lines.recv().await.unwrap();
            if line.starts_with("Incoming tg_bot_channel_update:") {
                break line;
            }
        }
    })
    .await
    .unwrap();
    assert!(line.contains("ADA/USDT"));
    assert!(line.contains("0.2570"));
    assert!(line.contains("-1001596367704"));
    let parsed = timeout(Duration::from_secs(3), lines.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(parsed.starts_with("Signal parsed:"));
    let result: serde_json::Value =
        serde_json::from_str(parsed.split_once("result=").unwrap().1).unwrap();
    assert_eq!(result["symbol"], "ADAUSDT");
    assert_eq!(result["buy_targets"], json!(["0.2570"]));
    assert_eq!(result["sell_targets"], json!(["0.26", "0.27", "0.28"]));
    assert_eq!(result["stop_loss"], "0.18");
    assert_eq!(result["leverage"], "3x");
    let context_log = timeout(Duration::from_secs(3), lines.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(context_log.starts_with("Channel context ready:"));
    assert!(context_log.contains("eligible_accounts=1 eligible_users=1"));
    for log in [&line, &parsed, &context_log] {
        assert!(!log.contains("must-not-log"));
        assert!(!log.contains("integration-account"));
    }
    let prepared = timeout(Duration::from_secs(5), lines.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(prepared.starts_with("Signal prepared:"));
    let summary: serde_json::Value =
        serde_json::from_str(prepared.strip_prefix("Signal prepared: ").unwrap()).unwrap();
    assert_eq!(summary["prepared_jobs"], 1);
    assert_eq!(summary["published_jobs"], 0);
    assert_eq!(summary["symbol"], "ADAUSDT");
    assert_eq!(
        summary["expires_at_ms"].as_u64().unwrap() - summary["accepted_at_ms"].as_u64().unwrap(),
        60000
    );
    assert!(!prepared.contains("must-not-log"));

    if send_notification {
        let log = timeout(Duration::from_secs(5), lines.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            log.starts_with(if notification_error {
                "Signal notification failed:"
            } else {
                "Signal notification sent:"
            }),
            "{log}"
        );
        assert!(!log.contains("integration_secret"));
        timeout(Duration::from_secs(3), notification.unwrap())
            .await
            .unwrap()
            .unwrap();
    }
    let (published, admitted) = timeout(Duration::from_secs(5), async {
        let mut published = None;
        let mut admitted = None;
        loop {
            let log = lines.recv().await.unwrap();
            assert!(!log.contains("must-not-log"));
            if log.starts_with("Signal published:") {
                published = Some(log.clone());
            }
            if log.starts_with("Client trade published:") {
                admitted = Some(log);
            }
            if admitted.is_some()
                && let Some(published) = published.take()
            {
                break (published, admitted);
            }
        }
    })
    .await
    .unwrap();
    if let Some(admitted) = admitted {
        assert!(admitted.contains("positionConfiguration=ORDER_LEDGER_V1"));
    }
    assert!(published.starts_with("Signal published: "), "{published}");
    let report: serde_json::Value =
        serde_json::from_str(published.strip_prefix("Signal published: ").unwrap()).unwrap();
    assert_eq!(report["published_jobs"], 1);
    assert!(!published.contains("must-not-log"));

    let message = channel
        .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
        .await
        .unwrap()
        .expect("confirmed final trade queued");
    let trade: serde_json::Value = serde_json::from_slice(&message.data).unwrap();
    assert_eq!(trade["expires_at"], summary["expires_at_ms"]);
    assert_eq!(
        trade["trade_object"]["positionConfiguration"]["accountingModel"],
        "ORDER_LEDGER_V1"
    );
    assert_eq!(trade["client_data"]["clientId"], "integration-account");
    assert_eq!(trade["client_data"]["key"], "must-not-log-api-key");
    assert_eq!(trade["client_data"]["keySecret"], "must-not-log-api-secret");
    assert_eq!(trade.as_object().unwrap().len(), 3);
    assert!(
        trade["trade_object"]["id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(message.properties.delivery_mode(), &Some(1));
    assert_eq!(
        message.properties.content_type().as_ref().unwrap().as_str(),
        "application/json"
    );
    message.ack(BasicAckOptions::default()).await.unwrap();

    timeout(Duration::from_secs(3), quotes)
        .await
        .unwrap()
        .unwrap();
    for (body, expected) in [
        (br#"{"text":"private-payload-without-envelope"}"#.to_vec(), "Telegram intake rejected: InvalidEnvelope"),
        (serde_json::to_vec(&json!({"message_id":8,"date":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),"chat":{"id":-1001596367704i64,"type":"channel"}})).unwrap(), "Telegram intake skipped: NoText"),
    ] {
        channel.basic_publish("".into(), TELEGRAM_CHANNEL_QUEUE.into(), BasicPublishOptions::default(), &body, BasicProperties::default()).await.unwrap().await.unwrap();
        let line = timeout(Duration::from_secs(3), lines.recv()).await.unwrap().unwrap();
        assert_eq!(line, expected);
        assert!(!line.contains("private-payload"));
    }
    // Graceful drain completes settlement before closing the channel.
    Command::new("kill")
        .args(["-INT", &process.0.id().to_string()])
        .status()
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    reader.join().unwrap();
    assert!(
        channel
            .basic_get(TELEGRAM_CHANNEL_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        channel
            .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        channel
            .basic_get(BINGX_FUTURES_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "starts isolated RabbitMQ; requires loopback access"]
async fn client_job_route_confirms_expiry_and_partial_failure_retry_source() {
    use channels_manager_v1::{
        contracts::messages::ClientTradeJob,
        rabbitmq::{DeliveryOutcome, DeliveryPolicy, RetryReason},
        signals::publication::{JobPublication, JobPublisher},
    };
    use futures_util::future::BoxFuture;
    let broker = Broker::start().await;
    let mut config = broker.config();
    config.rabbitmq.input_queue = TELEGRAM_CHANNEL_QUEUE;
    let mut rabbit = RabbitMq::new(config.rabbitmq.clone(), config.runtime.operation_timeout);
    rabbit.connect_and_declare().await.unwrap();
    rabbit.initialize_publisher().await.unwrap();
    let publisher = rabbit.publisher().unwrap();
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let mut consumer = rabbit.start_consumer().await.unwrap();
    channel
        .basic_publish(
            "".into(),
            TELEGRAM_CHANNEL_QUEUE.into(),
            BasicPublishOptions::default(),
            b"original-signal",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    struct LoseRoute {
        publisher: channels_manager_v1::rabbitmq::Publisher,
        channel: lapin::Channel,
        calls: AtomicUsize,
    }
    impl JobPublisher for LoseRoute {
        fn publish_job<'a>(
            &'a self,
            message: &'a PreparedPublication,
        ) -> BoxFuture<'a, Result<(), RabbitError>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    let received = self
                        .channel
                        .basic_get(BINGX_FUTURES_QUEUE.into(), BasicGetOptions::default())
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(received.data, message.body());
                    assert_eq!(received.properties.delivery_mode(), &Some(1));
                    assert_eq!(
                        received.properties.message_id().as_ref().unwrap().as_str(),
                        "job-key"
                    );
                    let headers = received.properties.headers().as_ref().unwrap();
                    assert!(headers.inner().contains_key(PUBLISHED_AT_HEADER));
                    assert!(headers.inner().contains_key(IDEMPOTENCY_HEADER));
                    received.ack(BasicAckOptions::default()).await.unwrap();
                    self.channel
                        .queue_delete(BINGX_FUTURES_QUEUE.into(), QueueDeleteOptions::default())
                        .await
                        .unwrap();
                }
                self.publisher.publish(message).await
            })
        }
    }
    let sink = LoseRoute {
        publisher: publisher.clone(),
        channel,
        calls: AtomicUsize::new(0),
    };
    let mut job: ClientTradeJob =
        serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
    job.idempotency_key = "job-key".into();
    job.trade_expires_at = Some(1);
    assert_eq!(
        publisher
            .publish(&PreparedPublication::client_job(&job).unwrap())
            .await,
        Err(RabbitError::Expired)
    );
    job.trade_expires_at = Some(4102444800000);
    let duplicate = serde_json::from_value(serde_json::to_value(&job).unwrap()).unwrap();
    let failure = JobPublication::new(1, Duration::from_millis(1))
        .publish(&[job, duplicate], &sink)
        .await
        .unwrap_err();
    assert_eq!(failure.published_jobs, 1);
    assert_eq!(failure.error, RabbitError::Unroutable);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 3);
    DeliveryPolicy::new(&config.rabbitmq)
        .settle(
            delivery,
            DeliveryOutcome::PreClaimRetry(RetryReason::DependencyUnavailable),
            &publisher,
        )
        .await
        .unwrap();
    let retry = timeout(Duration::from_secs(3), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(retry.body(), b"original-signal");
    assert_eq!(
        retry
            .properties()
            .headers()
            .as_ref()
            .unwrap()
            .inner()
            .get(ORIGINAL_QUEUE_HEADER)
            .unwrap(),
        &lapin::types::AMQPValue::LongString(TELEGRAM_CHANNEL_QUEUE.into())
    );
    retry.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB, Redis and HTTP; requires loopback access"]
async fn dual_consumers_retry_to_own_queue_and_recover_without_duplicate_consumers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let broker = Broker::start().await;
    let stores = Stores::start(&broker.directory).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = stores.config(&broker);
    config.bingx_public_api_base_url = format!("http://{}", listener.local_addr().unwrap());
    config.telegram_intake_enabled = true;
    config.rabbitmq.retry_max_attempts = std::num::NonZeroU32::new(1).unwrap();
    config.rabbitmq.retry_delay = Duration::from_millis(50);
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let http = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            let _ = socket.read(&mut bytes).await;
            counted.fetch_add(1, Ordering::SeqCst);
            let _ = socket
                .write_all(
                    b"HTTP/1.1 503 Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .await;
        }
    });
    let runtime = config.runtime.clone();
    let lifecycle =
        Lifecycle::new(Infrastructure::for_application(config).unwrap(), runtime).unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for queue in [TELEGRAM_CHANNEL_QUEUE, BINGX_FUTURES_QUEUE] {
        let state = channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        assert_eq!(state.consumer_count(), 1);
    }
    let body = include_bytes!("fixtures/admission-job.json");
    seed(&channel, body).await;
    let dead = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(message) = channel
                .basic_get(DEAD_LETTER_QUEUE.into(), BasicGetOptions::default())
                .await
                .unwrap()
            {
                break message;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let diagnostic: serde_json::Value = serde_json::from_slice(&dead.data).unwrap();
    assert_eq!(diagnostic["originalQueue"], BINGX_FUTURES_QUEUE);
    assert_eq!(
        diagnostic["payload"],
        serde_json::from_slice::<serde_json::Value>(body).unwrap()
    );
    assert_eq!(diagnostic["attempt"], 2);
    dead.ack(BasicAckOptions::default()).await.unwrap();
    assert!(requests.load(Ordering::SeqCst) >= 2);
    let calls = requests.load(Ordering::SeqCst);
    // Invalid and expired jobs settle without touching the exchange.
    seed(&channel, b"not a job").await;
    let mut expired: serde_json::Value = serde_json::from_slice(body).unwrap();
    expired["tradeExpiresAt"] = json!(1);
    seed(&channel, &serde_json::to_vec(&expired).unwrap()).await;
    // Deleting either role's queue forces coordinated recovery of both roles.
    for queue in [TELEGRAM_CHANNEL_QUEUE, BINGX_FUTURES_QUEUE] {
        channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await
            .unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                phases.changed().await.unwrap();
                if *phases.borrow_and_update() != Phase::Running {
                    break;
                }
            }
            running(&mut phases).await;
        })
        .await
        .unwrap();
        for queue in [TELEGRAM_CHANNEL_QUEUE, BINGX_FUTURES_QUEUE] {
            let state = channel
                .queue_declare(
                    queue.into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                state.consumer_count(),
                1,
                "duplicate role consumer after recovery"
            );
        }
    }
    stop.cancel();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), calls);
    for queue in [
        TELEGRAM_CHANNEL_QUEUE,
        BINGX_FUTURES_QUEUE,
        DEFAULT_TRADE_QUEUE,
    ] {
        let state = channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        assert_eq!(state.consumer_count(), 0);
        assert_eq!(state.message_count(), 0);
    }
    http.abort();
    let _ = http.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB, Redis and HTTP; requires loopback access"]
async fn worker_only_cutover_preserves_intake_backlog_and_publishes_both_routes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let broker = Broker::start().await;
    let stores = Stores::start(&broker.directory).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = stores.config(&broker);
    config.bingx_public_api_base_url = format!("http://{}", listener.local_addr().unwrap());
    config.telegram_intake_enabled = false;
    config.rabbitmq.output_queue = "cutover-trades".into();
    let one_way = Arc::new(AtomicBool::new(false));
    let mode = one_way.clone();
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let http = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 8192];
            let n = socket.read(&mut bytes).await.unwrap();
            let request = String::from_utf8_lossy(&bytes[..n]);
            assert!(
                request.starts_with("GET "),
                "existing Hedge and legacy One-Way require no writes"
            );
            counted.fetch_add(1, Ordering::SeqCst);
            let route = request
                .split_whitespace()
                .nth(1)
                .unwrap()
                .split('?')
                .next()
                .unwrap();
            let data = match route {
                "/openApi/swap/v1/positionSide/dual" => {
                    json!({"dualSidePosition":!mode.load(Ordering::SeqCst)})
                }
                "/openApi/swap/v2/user/positions" if mode.load(Ordering::SeqCst) => {
                    json!([{"positionId":"other-position","symbol":"ETH-USDT","positionSide":"BOTH","positionAmt":"1"}])
                }
                "/openApi/swap/v2/user/positions" => json!([]),
                "/openApi/swap/v2/trade/openOrders" => json!({"orders":[]}),
                "/openApi/swap/v3/user/balance" => {
                    json!([{"asset":"USDT","availableMargin":"1000"}])
                }
                "/openApi/swap/v2/trade/leverage" => {
                    json!({"longLeverage":10,"shortLeverage":10,"maxLongLeverage":50,"maxShortLeverage":50})
                }
                _ => panic!("unexpected route"),
            };
            let body = json!({"code":0,"data":data}).to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let admin = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = admin.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    channel
        .queue_declare(
            TELEGRAM_CHANNEL_QUEUE.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    channel
        .basic_publish(
            "".into(),
            TELEGRAM_CHANNEL_QUEUE.into(),
            BasicPublishOptions::default(),
            b"intake-owned-by-typescript",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let runtime = config.runtime.clone();
    let lifecycle =
        Lifecycle::new(Infrastructure::for_application(config).unwrap(), runtime).unwrap();
    let mut phases = lifecycle.subscribe();
    let stop = CancellationToken::new();
    let task = tokio::spawn(lifecycle.run(stop.clone()));
    running(&mut phases).await;
    for (index, route) in ["ORDER_LEDGER_V1", "ONE_WAY_V1"].iter().enumerate() {
        one_way.store(index == 1, Ordering::SeqCst);
        let mut job: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/admission-job.json")).unwrap();
        let message_id = 7 + index as i64;
        job["messageId"] = json!(message_id);
        job["idempotencyKey"] = json!(format!(
            "auto-trade:-100:{message_id}:42:account-1:BingX:futures:BTCUSDT"
        ));
        job["tradeExpiresAt"] = json!(mongodb::bson::DateTime::now().timestamp_millis() + 60000);
        job["marketData"]["bingXFutures"]["symbolInfo"]["minNotional"] = json!(5);
        seed(&channel, &serde_json::to_vec(&job).unwrap()).await;
        let message = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(message) = channel
                    .basic_get("cutover-trades".into(), BasicGetOptions::default())
                    .await
                    .unwrap()
                {
                    break message;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&message.data).unwrap();
        assert_eq!(envelope["expires_at"], job["tradeExpiresAt"]);
        assert_eq!(
            envelope["trade_object"]["id"],
            trade_creation_id(job["idempotencyKey"].as_str().unwrap())
        );
        assert_eq!(
            envelope["trade_object"]["positionConfiguration"]["accountingModel"],
            *route
        );
        assert_eq!(envelope["client_data"], job["client"]);
        message.ack(BasicAckOptions::default()).await.unwrap();
    }
    let intake = channel
        .queue_declare(
            TELEGRAM_CHANNEL_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(intake.consumer_count(), 0);
    assert_eq!(intake.message_count(), 1);
    assert_eq!(requests.load(Ordering::SeqCst), 10);
    stop.cancel();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let worker = channel
        .queue_declare(
            BINGX_FUTURES_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(worker.consumer_count(), 0);
    assert_eq!(
        worker.message_count(),
        0,
        "confirmed jobs must be acknowledged before drain completes"
    );
    http.abort();
    let _ = http.await;
}
