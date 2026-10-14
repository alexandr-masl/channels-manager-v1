use super::{
    scripted_exchange::{Expected, Script},
    services::{Broker, Stores},
};
use channels_manager_v1::contracts::rabbitmq::*;
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::FieldTable};
use mongodb::bson::{DateTime, Document, doc};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::timeout;
pub const CHANNEL: i64 = -1001596367704;

#[derive(Clone, Copy, Debug)]
pub enum Signal {
    Ada,
    Sol,
    Missing,
}
impl Signal {
    fn text(self) -> &'static str {
        match self {
            Self::Ada => include_str!("../fixtures/signals/ada-3x.txt"),
            Self::Sol => include_str!("../fixtures/signals/sol-50x.txt"),
            Self::Missing => include_str!("../fixtures/signals/ada-no-leverage.txt"),
        }
    }
    fn symbol(self) -> &'static str {
        if matches!(self, Self::Sol) {
            "SOL-USDT"
        } else {
            "ADA-USDT"
        }
    }
    fn price(self) -> &'static str {
        match self {
            Self::Ada => "0.217",
            Self::Sol => "72.00",
            Self::Missing => "0.2570",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    Hedge,
    Switch,
    OneWay,
}
pub struct Scenario {
    pub name: &'static str,
    pub signal: Signal,
    pub mode: Mode,
    pub current_leverage: u32,
    pub channel_leverage: Option<Value>,
    pub overrides: Vec<Option<Value>>,
    pub resolved: Vec<u32>,
    pub blocked: bool,
    pub post_fails: bool,
    pub telegram_fails: bool,
    pub expired: bool,
}
impl Scenario {
    pub fn base(name: &'static str, signal: Signal, leverage: u32) -> Self {
        Self {
            name,
            signal,
            mode: Mode::Hedge,
            current_leverage: leverage,
            channel_leverage: Some(json!(3)),
            overrides: vec![None],
            resolved: vec![leverage],
            blocked: false,
            post_fails: false,
            telegram_fails: false,
            expired: false,
        }
    }
    fn publishes(&self) -> bool {
        !self.resolved.is_empty() && !self.blocked && !self.post_fails && !self.expired
    }
}
fn response(data: Value) -> Value {
    json!({"code":0,"data":data})
}
fn http_script(s: &Scenario) -> Vec<Vec<Expected>> {
    if s.expired {
        return vec![];
    }
    let symbol = s.signal.symbol();
    let mut phases = vec![
        vec![Expected::new(
            "GET",
            "/openApi/swap/v2/quote/contracts",
            &[("symbol", symbol)],
            false,
            response(
                json!([{"symbol":symbol,"status":1,"pricePrecision":4,"quantityPrecision":1,"tradeMinUSDT":"1"}]),
            ),
        )],
        vec![Expected::new(
            "GET",
            "/openApi/swap/v1/ticker/price",
            &[("symbol", symbol)],
            false,
            response(json!({"symbol":symbol,"price":s.signal.price()})),
        )],
    ];
    if s.resolved.is_empty() {
        return phases;
    }
    let mut telegram = Expected::new(
        "POST",
        "/bot123:fixture/sendMessage",
        &[],
        false,
        json!({"ok":true,"result":{"message_id":123}}),
    );
    telegram.body = Some(
        json!({"chat_id":CHANNEL,"text":"created ✅","reply_parameters":{"message_id":7,"allow_sending_without_reply":false}}),
    );
    if s.telegram_fails {
        telegram.status = 403;
        telegram.response = json!({"ok":false});
    }
    phases.push(vec![telegram]);
    for leverage in &s.resolved {
        let positions = if s.blocked {
            json!([{"positionId":"occupied","symbol":symbol,"positionSide":"LONG","positionAmt":"1"}])
        } else if s.mode == Mode::OneWay {
            json!([{"positionId":"other","symbol":"ETH-USDT","positionSide":"BOTH","positionAmt":"1"}])
        } else {
            json!([])
        };
        phases.push(vec![
            Expected::new("GET","/openApi/swap/v1/positionSide/dual",&[],true,response(json!({"dualSidePosition":s.mode==Mode::Hedge}))),
            Expected::new("GET","/openApi/swap/v2/user/positions",&[],true,response(positions)),
            Expected::new("GET","/openApi/swap/v2/trade/openOrders",&[],true,response(json!({"orders":[]}))),
            Expected::new("GET","/openApi/swap/v3/user/balance",&[],true,response(json!([{"asset":"USDT","availableMargin":"1000"}]))),
            Expected::new("GET","/openApi/swap/v2/trade/leverage",&[("symbol",symbol)],true,response(json!({"longLeverage":s.current_leverage,"shortLeverage":s.current_leverage,"maxLongLeverage":100,"maxShortLeverage":100}))),
        ]);
        if s.mode == Mode::Switch {
            phases.push(vec![Expected::new(
                "POST",
                "/openApi/swap/v1/positionSide/dual",
                &[("dualSidePosition", "true")],
                true,
                response(json!({})),
            )]);
        }
        if *leverage != s.current_leverage && !s.blocked {
            let mut post = Expected::new(
                "POST",
                "/openApi/swap/v2/trade/leverage",
                &[
                    ("symbol", symbol),
                    ("leverage", &leverage.to_string()),
                    (
                        "side",
                        if s.mode == Mode::OneWay {
                            "BOTH"
                        } else {
                            "LONG"
                        },
                    ),
                ],
                true,
                response(json!({"symbol":symbol,"leverage":leverage})),
            );
            if s.post_fails {
                post.status = 400;
                post.response = json!({"code":100001});
            }
            phases.push(vec![post]);
        }
    }
    phases
}
struct Process {
    child: Child,
    journal: Arc<Mutex<Vec<String>>>,
    readers: Vec<std::thread::JoinHandle<()>>,
}
impl Process {
    fn start(broker: &Broker, stores: &Stores, http: &Script) -> Self {
        let mut child = crate::authorization_process::command(&broker.directory)
            .env("BINGX_PUBLIC_API_BASE_URL", &http.endpoint)
            .env("TELEGRAM_API_BASE_URL", &http.endpoint)
            .env("SATOSHI_TG_TOKEN", "123:fixture")
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
            .env("CLIENT_TRADE_WORKER_PREFETCH", "1")
            .env("CONSUMER_PREFETCH", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let journal = Arc::new(Mutex::new(Vec::new()));
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let mut readers = vec![];
        for pipe in [
            Box::new(stdout) as Box<dyn std::io::Read + Send>,
            Box::new(stderr),
        ] {
            let j = journal.clone();
            readers.push(std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines() {
                    j.lock().unwrap().push(line.unwrap());
                }
            }));
        }
        Self {
            child,
            journal,
            readers,
        }
    }
    async fn wait_for(&self, predicate: impl Fn(&[String]) -> bool) {
        timeout(Duration::from_secs(15), async {
            loop {
                if predicate(&self.journal.lock().unwrap()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "binary did not reach expected terminal event: {:?}",
                self.journal.lock().unwrap()
            )
        });
    }
    async fn stop(&mut self) {
        assert!(
            Command::new("kill")
                .args(["-INT", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("graceful drain timed out");
        for reader in self.readers.drain(..) {
            reader.join().unwrap();
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("binary journal: {:?}", self.journal.lock().unwrap());
        }
    }
}

pub async fn run(broker: &Broker, s: Scenario) {
    let directory = broker.directory.join(s.name);
    std::fs::create_dir_all(&directory).unwrap();
    let stores = Stores::start(&directory).await;
    let mongo = mongodb::Client::with_uri_str(format!("mongodb://127.0.0.1:{}", stores.mongo_port))
        .await
        .unwrap();
    let bot = mongo.database("bot");
    let mut channel = doc! {"id":CHANNEL,"default_quantity":0.1,"default_buy_targets":[{"fraction":0.5},{"fraction":0.5}],"default_sell_targets":[{"fraction":0.5},{"fraction":0.5}],"strategy":"basic","futures":{"active":true}};
    if let Some(leverage) = &s.channel_leverage {
        channel
            .get_document_mut("futures")
            .unwrap()
            .insert("leverage", mongodb::bson::to_bson(leverage).unwrap());
    }
    bot.collection::<Document>("mcr_channels")
        .insert_one(channel)
        .await
        .unwrap();
    for (index, over) in s.overrides.iter().enumerate() {
        let user = 42 + index as i64;
        let account = format!("account-{user}");
        bot.collection::<Document>("tradingprofiles").insert_one(doc!{"userId":user,"exchangeClients":[{"clientId":account,"provider":"BingX","connectedChannel":CHANNEL,"api_key":"fixture-key","api_secret":"fixture-secret"}]}).await.unwrap();
        mongo.database("accounts").collection::<Document>("users").insert_one(doc!{"tg_chat_id":user,"auto_trading":true,"valid_till":DateTime::from_millis(DateTime::now().timestamp_millis()+120000)}).await.unwrap();
        if let Some(leverage) = over {
            bot.collection::<Document>("user_configs").insert_one(doc!{"user":user,"private_channels":[{"id":CHANNEL,"own_settings":true,"futures":{"active":true,"leverage":mongodb::bson::to_bson(leverage).unwrap()}}]}).await.unwrap();
        }
    }
    // Exercise the managed-trade repository with an unrelated, completed row.
    mongo.database("trading").collection::<Document>("trade_station_active_trades").insert_one(doc!{"_id":"historical","state":"FINISHED","chat_id":42,"exchangeClientId":"account-42","exchange_client":"_binance_futures_","symbol":"ETH-USDT"}).await.unwrap();
    let http = Script::start(http_script(&s)).await;
    let mut process = Process::start(broker, &stores, &http);
    process
        .wait_for(|lines| {
            lines
                .iter()
                .any(|l| l.contains("Listening for messages on tg_bot_channel_update"))
        })
        .await;
    let connection = Connection::connect(&broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let channel = connection.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let started = DateTime::now().timestamp_millis();
    let body = if s.expired {
        let mut job: Value =
            serde_json::from_str(include_str!("../fixtures/admission-job.json")).unwrap();
        job["tradeExpiresAt"] = json!(started - 1);
        serde_json::to_vec(&job).unwrap()
    } else {
        serde_json::to_vec(&json!({"message_id":7,"date":started/1000,"chat":{"id":CHANNEL,"type":"channel"},"text":s.signal.text().trim_end()})).unwrap()
    };
    let confirmed = channel
        .basic_publish(
            "".into(),
            if s.expired {
                BINGX_FUTURES_QUEUE
            } else {
                TELEGRAM_CHANNEL_QUEUE
            }
            .into(),
            BasicPublishOptions {
                mandatory: true,
                ..Default::default()
            },
            &body,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        confirmed.is_ack() && confirmed.take_message().is_none(),
        "mandatory input was not confirmed/routed"
    );
    let count = s.resolved.len();
    process
        .wait_for(|lines| {
            if s.expired {
                lines.iter().any(|l| l == "Client job expired")
            } else if count == 0 {
                lines
                    .iter()
                    .any(|l| l.starts_with("Signal preparation rejected:"))
            } else {
                lines
                    .iter()
                    .filter(|l| {
                        l.starts_with("Client trade published:")
                            || l.starts_with("Client job rejected:")
                            || l.starts_with("Client trade rejected:")
                    })
                    .count()
                    == count
            }
        })
        .await;
    process.stop().await;
    http.assert_complete();
    let journal = process.journal.lock().unwrap().clone();
    let expected_rejection = if s.blocked {
        Some("leverageChangeBlocked")
    } else if s.post_fails {
        Some("leverageChangeFailed")
    } else if !s.expired && count == 0 {
        Some("NoValidLeverage")
    } else {
        None
    };
    if let Some(reason) = expected_rejection {
        assert!(
            journal.iter().any(|line| line.contains(reason)),
            "{} did not reject for {reason}: {journal:?}",
            s.name
        );
    }

    let summary = journal.iter().find_map(|l| {
        l.strip_prefix("Signal published: ")
            .and_then(|v| serde_json::from_str::<Value>(v).ok())
    });
    if !s.expired && count > 0 {
        let summary = summary.as_ref().expect("signal publication summary");
        assert_eq!(summary["published_jobs"], count);
        assert!(summary["accepted_at_ms"].as_i64().unwrap() >= started);
        assert!(summary["accepted_at_ms"].as_i64().unwrap() <= DateTime::now().timestamp_millis());
        assert_eq!(
            summary["expires_at_ms"].as_i64().unwrap(),
            summary["accepted_at_ms"].as_i64().unwrap() + 60000
        );
    }
    if matches!(s.signal, Signal::Sol) {
        let parsed: Value = serde_json::from_str(
            journal
                .iter()
                .find(|l| l.starts_with("Signal parsed:"))
                .unwrap()
                .split_once("result=")
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(parsed["buy_targets"], json!(["71", "72"]));
        assert_eq!(parsed["sell_targets"], json!(["88", "90", "92", "94"]));
        assert_eq!(parsed["position"], 0.005);
    }
    let mut outputs = Vec::new();
    while let Some(message) = channel
        .basic_get(DEFAULT_TRADE_QUEUE.into(), BasicGetOptions::default())
        .await
        .unwrap()
    {
        assert_eq!(
            message.properties.content_type().as_ref().unwrap().as_str(),
            "application/json"
        );
        assert_eq!(message.properties.delivery_mode(), &Some(1));
        outputs.push(serde_json::from_slice::<Value>(&message.data).unwrap());
        message.ack(BasicAckOptions::default()).await.unwrap();
    }
    assert_eq!(
        outputs.len(),
        if s.publishes() { count } else { 0 },
        "{}: {outputs:?}; HTTP {}",
        s.name,
        http.journal()
    );
    outputs.sort_by_key(|o| o["trade_object"]["chat_id"].as_i64().unwrap());
    for (index, actual) in outputs.iter().enumerate() {
        let mut expected = expected(&s, index);
        expected["expires_at"] = summary.as_ref().unwrap()["expires_at_ms"].clone();
        assert_eq!(*actual, expected, "complete final envelope for {}", s.name);
        let mut sanitized = actual.clone();
        sanitized["client_data"]["key"] = json!("[redacted]");
        sanitized["client_data"]["keySecret"] = json!("[redacted]");
        println!("{}: {}", s.name, sanitized);
    }
    for queue in [
        TELEGRAM_CHANNEL_QUEUE.to_owned(),
        BINGX_FUTURES_QUEUE.to_owned(),
        DEFAULT_TRADE_QUEUE.to_owned(),
        DEAD_LETTER_QUEUE.to_owned(),
        format!("{TELEGRAM_CHANNEL_QUEUE}{RETRY_SUFFIX}"),
        format!("{BINGX_FUTURES_QUEUE}{RETRY_SUFFIX}"),
    ] {
        let state = channel
            .queue_declare(
                queue.clone().into(),
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
            0,
            "consumers left after drain: {queue}"
        );
        assert_eq!(state.message_count(), 0, "unexpected queued work: {queue}");
    }
    // No admission diagnostics expected for these leverage failures.
    assert!(
        channel
            .basic_get(ADMISSION_EVENT_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );
    connection.close(200, "done".into()).await.unwrap();
    println!(
        "PASS {}: {} final envelopes; {}",
        s.name,
        outputs.len(),
        http.journal()
    );
}
fn expected(s: &Scenario, index: usize) -> Value {
    let fixture = match s.signal {
        Signal::Ada => include_str!("../fixtures/signals/ada-3x.expected.json"),
        Signal::Sol => include_str!("../fixtures/signals/sol-50x.expected.json"),
        Signal::Missing => match s.resolved[index] {
            3 => include_str!("../fixtures/signals/ada-fallback-3x.expected.json"),
            5 => include_str!("../fixtures/signals/ada-fallback-5x.expected.json"),
            7 => include_str!("../fixtures/signals/ada-fallback-7x.expected.json"),
            _ => panic!("missing expected fixture"),
        },
    };
    let mut value: Value = serde_json::from_str(fixture).unwrap();
    if s.mode == Mode::OneWay {
        value["trade_object"]["positionConfiguration"]["accountingModel"] = json!("ONE_WAY_V1");
    }
    value["trade_object"]["auto_Trade"]["usingChannelConfig"] = json!(s.overrides[index].is_none());
    value
}
