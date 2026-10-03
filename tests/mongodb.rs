use channels_manager_v1::{
    config::AppConfig,
    contracts::storage::*,
    mongo::{ClaimInput, ClaimOutcome, MongoConnections, MongoRole},
};
use futures_util::future::join_all;
use mongodb::{
    Client, IndexModel,
    bson::{DateTime, Document, doc},
    options::{ClientOptions, IndexOptions},
};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Server {
    process: Child,
    directory: PathBuf,
    port: u16,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
impl Server {
    async fn start() -> (Self, Client) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let directory =
            std::env::temp_dir().join(format!("channels-manager-mongo-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let process = Command::new(std::env::var("MONGOD_BIN").unwrap_or_else(|_| "mongod".into()))
            .args([
                "--replSet",
                "stage3",
                "--bind_ip",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--setParameter",
                "enableTestCommands=1",
            ])
            .arg("--dbpath")
            .arg(&directory)
            .arg("--logpath")
            .arg(directory.join("mongod.log"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("install mongod or set MONGOD_BIN");
        let server = Self {
            process,
            directory,
            port,
        };
        let mut options =
            ClientOptions::parse(format!("mongodb://127.0.0.1:{port}/?directConnection=true"))
                .await
                .unwrap();
        options.server_selection_timeout = Some(Duration::from_millis(200));
        let client = Client::with_options(options).unwrap();
        tokio::time::timeout(Duration::from_secs(15),async {
            loop {
                if client.database("admin").run_command(doc!{"ping":1}).await.is_ok() { break; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            client.database("admin").run_command(doc!{"replSetInitiate":{"_id":"stage3","members":[{"_id":0,"host":format!("127.0.0.1:{port}")}]}}).await.unwrap();
            loop {
                if client.database("admin").run_command(doc!{"hello":1}).await.is_ok_and(|d|d.get_bool("isWritablePrimary")==Ok(true)) { break; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.expect("isolated replica set did not start");
        (server, client)
    }
    fn config(&self) -> AppConfig {
        AppConfig::from_lookup(|key| match key {
            "RABBIT_MQ" => Some("amqp://localhost".into()),
            "REDIS" => Some("localhost".into()),
            "MONGO_PATH" => Some(self.uri("bot")),
            "TRADE_STATION_MONGO_PATH" => Some(self.uri("trading")),
            "ACCOUNT_VALIDATOR_MONGO_PATH" => Some(self.uri("accounts")),
            _ => None,
        })
        .unwrap()
    }
    fn uri(&self, database: &str) -> String {
        format!(
            "mongodb://127.0.0.1:{}/{database}?replicaSet=stage3&retryWrites=false",
            self.port
        )
    }
}

fn input() -> ClaimInput {
    serde_json::from_value(serde_json::json!({
        "workId":"auto-trade:-100:7:42:account-1:BingX:futures:BTCUSDT", "inputHash":"canonical-hash",
        "channelId":-100,"signalMessageId":7,"sourceCreatedAt":"2026-10-03T10:00:00.000Z",
        "chatId":42,"exchangeClientId":"account-1","provider":"BingX","market":"futures",
        "parserCanonicalSymbol":"BTCUSDT","normalizedSymbol":"BTC-USDT","signalSide":"LONG",
        "canonicalSignalData":{"symbol":"BTCUSDT","leverage":10}
    })).unwrap()
}

#[tokio::test]
#[ignore = "starts an isolated local mongod replica set; requires loopback access"]
async fn mongodb_stage3_contracts() {
    let (server, admin) = Server::start().await;
    let config = server.config();
    let mut mongo = MongoConnections::new(config.mongo.clone(), config.runtime.operation_timeout);
    assert!(mongo.repositories().is_err());
    mongo.connect().await.unwrap();
    mongo.connect().await.unwrap();
    assert_eq!(mongo.database_name(MongoRole::Bot), Some("bot"));
    assert_eq!(
        mongo.database_name(MongoRole::TradingStation),
        Some("trading")
    );
    assert_eq!(
        mongo.database_name(MongoRole::AccountValidator),
        Some("accounts")
    );
    let repos = mongo.repositories().unwrap();
    let work = input();
    assert!(matches!(
        repos.claims.claim(&work, "test").await,
        ClaimOutcome::Unavailable { .. }
    ));
    mongo.initialize_indexes().await.unwrap();
    mongo.initialize_indexes().await.unwrap();
    assert!(matches!(
        repos.claims.claim(&work, " ").await,
        ClaimOutcome::Unavailable {
            code: "INVALID_PRODUCER_REVISION"
        }
    ));
    let results = join_all((0..12).map(|_| repos.claims.claim(&work, "test"))).await;
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, ClaimOutcome::Created { .. }))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, ClaimOutcome::Duplicate))
            .count(),
        11
    );
    let owner = results
        .into_iter()
        .find_map(|r| {
            if let ClaimOutcome::Created { owner_token } = r {
                Some(owner_token)
            } else {
                None
            }
        })
        .unwrap();
    let mut changed = input();
    changed.input_hash = "different".into();
    assert!(matches!(
        repos.claims.claim(&changed, "test").await,
        ClaimOutcome::Conflict
    ));
    let mut changed = input();
    changed.normalized_symbol = "ETH-USDT".into();
    assert!(matches!(
        repos.claims.claim(&changed, "test").await,
        ClaimOutcome::Conflict
    ));
    assert!(
        !repos
            .claims
            .record_terminal(&work.work_id, "wrong-owner", "COMPLETED", "OK")
            .await
    );
    assert!(
        repos
            .claims
            .record_terminal(&work.work_id, &owner, "COMPLETED", "OK")
            .await
    );
    let stored = admin
        .database("bot")
        .collection::<Document>(CLAIMS_COLLECTION)
        .find_one(doc! {"workId":&work.work_id})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .get_document("terminal")
            .unwrap()
            .get_str("outcome")
            .unwrap(),
        "COMPLETED"
    );
    assert!(stored.get_datetime("claimedAt").is_ok());
    assert!(stored.get_str("_id").is_ok());

    // Matches Node's JSON comparison when a BSON double represents an integer.
    admin.database("bot").collection::<Document>(CLAIMS_COLLECTION).update_one(doc!{"workId":&work.work_id},doc!{"$set":{"canonicalSignalData.leverage":10.0,"channelId":-100.0,"signalMessageId":7.0,"chatId":42.0}}).await.unwrap();
    assert!(matches!(
        repos.claims.claim(&work, "test").await,
        ClaimOutcome::Duplicate
    ));

    // The insert is applied but the write concern response is uncertain.
    admin.database("admin").run_command(doc!{"configureFailPoint":"failCommand","mode":{"times":1},"data":{"failCommands":["insert"],"writeConcernError":{"code":64,"errmsg":"test acknowledgement lost"}}}).await.unwrap();
    let mut uncertain = input();
    uncertain.work_id = "uncertain-work".into();
    assert!(matches!(
        repos.claims.claim(&uncertain, "test").await,
        ClaimOutcome::Uncertain { .. }
    ));
    assert!(matches!(
        repos.claims.claim(&uncertain, "test").await,
        ClaimOutcome::Duplicate
    ));

    let users = admin
        .database("bot")
        .collection::<Document>(USER_CONFIGS_COLLECTION);
    users
        .insert_one(doc! {"user":42,"lastNotifications":[]})
        .await
        .unwrap();
    for n in 0..10 {
        assert!(
            repos
                .notifications
                .add_user_notification(42, &format!("message-{n}"))
                .await
                .unwrap()
        );
    }
    assert!(
        !repos
            .notifications
            .add_user_notification(999, "no upsert")
            .await
            .unwrap()
    );
    let user = users.find_one(doc! {"user":42}).await.unwrap().unwrap();
    let notifications = user.get_array("lastNotifications").unwrap();
    assert_eq!(notifications.len(), 7);
    assert_eq!(
        notifications[0]
            .as_document()
            .unwrap()
            .get_str("info")
            .unwrap(),
        "message-3"
    );
    assert!(
        notifications[0]
            .as_document()
            .unwrap()
            .get_str("time")
            .unwrap()
            .contains('T')
    );

    let trades = admin
        .database("trading")
        .collection::<Document>(ACTIVE_TRADES_COLLECTION);
    trades.insert_many([
        doc!{"id":"open","exchangeClientId":"account-1","exchange_client":"_binance_futures_","state":"OPENED","idempotencyKey":"key","chat_id":42,"sensitive":"hidden"},
        doc!{"id":"pending","exchangeClientId":"account-1","exchange_client":"_binance_futures_","state":"CREATING"},
        doc!{"id":"done","exchangeClientId":"account-1","exchange_client":"_binance_futures_","state":"FINISHED"},
        doc!{"id":"other","exchangeClientId":"account-2","exchange_client":"_binance_futures_","state":"OPENED"},
    ]).await.unwrap();
    assert_eq!(
        repos
            .trades
            .get_active_managed_futures_trades("account-1")
            .await
            .unwrap()
            .len(),
        2
    );
    let existing = repos
        .trades
        .get_trades_by_idempotency_keys(&["key".into()])
        .await
        .unwrap();
    assert_eq!(existing.len(), 1);
    assert!(!existing[0].contains_key("sensitive"));
    assert!(
        repos
            .trades
            .get_trades_by_idempotency_keys(&[])
            .await
            .unwrap()
            .is_empty()
    );

    admin.database("accounts").collection::<Document>(ACCOUNTS_COLLECTION).insert_many([
        doc!{"tg_chat_id":42,"auto_trading":true,"valid_till":DateTime::from_millis(4102444800000),"psswrd":"hidden"},
        doc!{"tg_chat_id":43,"auto_trading":false},
        doc!{"tg_chat_id":44,"auto_trading":true,"valid_till":DateTime::from_millis(0)},
    ]).await.unwrap();
    let accounts = repos
        .accounts
        .get_auto_trading_accounts(&[42, 43, 44])
        .await
        .unwrap();
    assert_eq!(accounts.len(), 2); // Expiry classification stays with the caller, matching TS.
    assert!(accounts.iter().all(|a| !a.contains_key("psswrd")));
    assert!(
        repos
            .accounts
            .get_auto_trading_accounts(&[])
            .await
            .unwrap()
            .is_empty()
    );

    // An incompatible TTL index blocks claim admission.
    let claims = admin
        .database("bot")
        .collection::<Document>(CLAIMS_COLLECTION);
    claims.drop_index(CLAIMS_INDEX).await.unwrap();
    claims
        .create_index(
            IndexModel::builder()
                .keys(doc! {"workId":1})
                .options(
                    IndexOptions::builder()
                        .name(CLAIMS_INDEX.to_owned())
                        .unique(true)
                        .expire_after(Duration::ZERO)
                        .build(),
                )
                .build(),
        )
        .await
        .unwrap();
    assert!(mongo.initialize_indexes().await.is_err());
    assert!(matches!(
        repos.claims.claim(&work, "test").await,
        ClaimOutcome::Unavailable { .. }
    ));

    mongo.close().await.unwrap();
    mongo.close().await.unwrap();
    assert!(mongo.repositories().is_err());
    assert!(
        repos
            .trades
            .get_trades_by_idempotency_keys(&["key".into()])
            .await
            .is_err()
    );
    claims.drop_index(CLAIMS_INDEX).await.unwrap();
    mongo.connect().await.unwrap();
    mongo.initialize_indexes().await.unwrap();
    assert!(matches!(
        mongo
            .repositories()
            .unwrap()
            .claims
            .claim(&work, "test")
            .await,
        ClaimOutcome::Duplicate
    ));
    mongo.close().await.unwrap();
    // A failed connection attempt retains owned pools for cleanup and retry.
    admin
        .database("admin")
        .run_command(doc! {
            "configureFailPoint":"failCommand","mode":{"times":1},
            "data":{"failCommands":["ping"],"errorCode":13}
        })
        .await
        .unwrap();
    let error = mongo.connect().await.unwrap_err();
    assert_eq!(error.role, MongoRole::Bot);
    assert!(mongo.repositories().is_err());
    assert_eq!(mongo.database_name(MongoRole::Bot), Some("bot"));
    mongo.connect().await.unwrap();
    let recovered = mongo.repositories().unwrap();
    assert!(matches!(
        recovered.claims.claim(&work, "test").await,
        ClaimOutcome::Unavailable { .. }
    ));
    mongo.initialize_indexes().await.unwrap();
    assert!(matches!(
        recovered.claims.claim(&work, "test").await,
        ClaimOutcome::Duplicate
    ));
    mongo.close().await.unwrap();

    // Cancel during a ping: the pool is already owned and can still be closed.
    admin
        .database("admin")
        .run_command(doc! {
            "configureFailPoint":"failCommand","mode":{"times":1},
            "data":{"failCommands":["ping"],"blockConnection":true,"blockTimeMS":1000}
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), mongo.connect())
            .await
            .is_err()
    );
    assert_eq!(mongo.database_name(MongoRole::Bot), Some("bot"));
    assert!(mongo.repositories().is_err());
    mongo.close().await.unwrap();

    // Failure in the third connection must not lose the first two pools.
    let config = AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some("amqp://localhost".into()),
        "REDIS" => Some("localhost".into()),
        "MONGO_PATH" => Some(server.uri("bot")),
        "TRADE_STATION_MONGO_PATH" => Some(server.uri("trading")),
        "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some(format!("{}&maxPoolSize=invalid", server.uri("accounts")))
        }
        _ => None,
    })
    .unwrap();
    let mut partial = MongoConnections::new(config.mongo, Duration::from_secs(3));
    assert_eq!(
        partial.connect().await.unwrap_err().role,
        MongoRole::AccountValidator
    );
    assert_eq!(partial.database_name(MongoRole::Bot), Some("bot"));
    assert_eq!(
        partial.database_name(MongoRole::TradingStation),
        Some("trading")
    );
    assert!(partial.repositories().is_err());
    partial.close().await.unwrap();
    assert!(partial.database_name(MongoRole::Bot).is_none());
    assert!(partial.database_name(MongoRole::TradingStation).is_none());
    admin.shutdown().await;
}

#[tokio::test]
async fn invalid_driver_options_are_redacted() {
    let config = AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some("amqp://localhost".into()),
        "REDIS" => Some("localhost".into()),
        "MONGO_PATH" => {
            Some("mongodb://private-user:private-password@localhost/bot?maxPoolSize=invalid".into())
        }
        "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        _ => None,
    })
    .unwrap();
    let mut mongo = MongoConnections::new(config.mongo, Duration::from_secs(1));
    let error = mongo.connect().await.unwrap_err();
    assert_eq!(
        error.kind,
        channels_manager_v1::mongo::MongoErrorKind::InvalidConfiguration
    );
    assert_eq!(format!("{error}"), "MongoDB Bot: InvalidConfiguration");
    assert!(!format!("{error:?}").contains("private"));
    mongo.close().await.unwrap();
}
