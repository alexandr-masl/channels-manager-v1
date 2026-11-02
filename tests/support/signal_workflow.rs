use super::{
    scripted_exchange::{Expected, Script},
    services::{Broker, Stores},
};
use channels_manager_v1::contracts::rabbitmq::*;
use channels_manager_v1::trading::notifications::UserFailure;
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::FieldTable};
use mongodb::bson::{DateTime, Document, doc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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
    Xlm,
    Eth,
}
impl Signal {
    fn text(self) -> &'static str {
        match self {
            Self::Ada => include_str!("../fixtures/signals/ada-3x.txt"),
            Self::Sol => include_str!("../fixtures/signals/sol-50x.txt"),
            Self::Missing => include_str!("../fixtures/signals/ada-no-leverage.txt"),
            Self::Xlm => include_str!("../fixtures/signals/xlm-8x.txt"),
            Self::Eth => include_str!("../fixtures/signals/eth-short-1x.txt"),
        }
    }
    fn symbol(self) -> &'static str {
        match self {
            Self::Sol => "SOL-USDT",
            Self::Xlm => "XLM-USDT",
            Self::Eth => "ETH-USDT",
            _ => "ADA-USDT",
        }
    }
    fn price(self) -> &'static str {
        match self {
            Self::Ada => "0.217",
            Self::Sol => "72.00",
            Self::Missing => "0.2570",
            Self::Xlm => "0.188920",
            Self::Eth => "1000.00",
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
    pub malformed: bool,
    pub balance: &'static str,
    pub max_leverage: u32,
    pub history_write_failures: bool,
    pub targets: bool,
    pub stop: bool,
    pub decorated: bool,
    pub raw_text: Option<&'static str>,
    pub channel_terminal: Option<&'static str>,
    pub rejection_reason: Option<&'static str>,
    pub percentage: Option<PercentageExpectation>,
    pub allocation: Option<AllocationExpectation>,
}
pub struct PercentageExpectation {
    pub parsed: Value,
    pub prices: Vec<Vec<&'static str>>,
    /// Per-user selected entries: price, fraction, executable quantity.
    pub entries: Vec<Vec<(&'static str, f64, f64)>>,
}
/// Manually specified allocation boundaries, independent of the production planner.
pub struct AllocationExpectation {
    pub balances: Vec<&'static str>,
    pub min_qty: f64,
    pub configured_fractions: Vec<f64>,
    pub quantities: Vec<f64>,
    /// None means this account must not publish a final trade.
    pub profits: Vec<Option<Vec<(&'static str, f64)>>>,
    pub failures: Vec<Option<&'static str>>,
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
            malformed: false,
            balance: "1000",
            max_leverage: 100,
            history_write_failures: false,
            targets: true,
            stop: true,
            decorated: false,
            raw_text: None,
            channel_terminal: None,
            rejection_reason: None,
            percentage: None,
            allocation: None,
        }
    }
    fn publishes(&self) -> bool {
        !self.resolved.is_empty()
            && !self.blocked
            && !self.post_fails
            && !self.expired
            && !self.malformed
            && self.balance != "0"
            && self
                .resolved
                .iter()
                .all(|value| *value <= self.max_leverage)
            && self.channel_terminal.is_none()
    }
    fn failure_for(&self, index: usize) -> Option<&'static str> {
        self.allocation.as_ref().map_or_else(
            || self.failure_reason(),
            |expected| expected.failures[index],
        )
    }
    fn publishes_for(&self, index: usize) -> bool {
        self.publishes()
            && self
                .allocation
                .as_ref()
                .is_none_or(|expected| expected.profits[index].is_some())
    }
    fn failure_reason(&self) -> Option<&'static str> {
        if self.blocked {
            Some("leverageChangeBlocked")
        } else if self.resolved.iter().any(|value| *value > self.max_leverage) {
            Some("leverageLimitExceeded")
        } else if self.balance == "0" {
            Some("insufficientBalance")
        } else {
            None
        }
    }
    fn text(&self) -> String {
        if let Some(text) = self.raw_text {
            return text.to_owned();
        }
        let text = self
            .signal
            .text()
            .lines()
            .filter(|line| {
                let label = line.to_ascii_uppercase();
                (self.targets || !label.contains("TG")) && (self.stop || !label.contains("SL"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        if self.decorated {
            format!(
                "VANERIX SINGLE_TARGET_MARKET — TEST ONLY\nReference VQA-83827.1-83987.2\n{}\nCommentary: BREAKOUT remains a possibility at 84087.2\n— Team 83707.1",
                text.replacen('\n', "\nCampaign reference 99999\n", 1)
            )
        } else {
            text
        }
    }
}
fn response(data: Value) -> Value {
    json!({"code":0,"data":data})
}
fn http_script(s: &Scenario) -> Vec<Vec<Expected>> {
    if s.expired || s.malformed || s.channel_terminal.is_some() {
        return vec![];
    }
    let symbol = s.signal.symbol();
    let metadata = if let Some(expected) = &s.allocation {
        json!([{"symbol":symbol,"status":1,"pricePrecision":2,"quantityPrecision":2,"size":"0.01","tradeMinQuantity":expected.min_qty,"tradeMinUSDT":"10"}])
    } else {
        json!([{"symbol":symbol,"status":1,"pricePrecision":4,"quantityPrecision":1,"tradeMinUSDT":"1"}])
    };
    let mut phases = vec![
        vec![Expected::new(
            "GET",
            "/openApi/swap/v2/quote/contracts",
            &[("symbol", symbol)],
            false,
            response(metadata),
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
    for (index, leverage) in s.resolved.iter().enumerate() {
        let balance = s
            .allocation
            .as_ref()
            .map_or(s.balance, |expected| expected.balances[index]);
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
            Expected::new("GET","/openApi/swap/v3/user/balance",&[],true,response(json!([{"asset":"USDT","availableMargin":balance}]))),
            Expected::new("GET","/openApi/swap/v2/trade/leverage",&[("symbol",symbol)],true,response(json!({"longLeverage":s.current_leverage,"shortLeverage":s.current_leverage,"maxLongLeverage":s.max_leverage,"maxShortLeverage":s.max_leverage}))),
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
        if *leverage != s.current_leverage && s.failure_reason().is_none() {
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
                            if s.percentage
                                .as_ref()
                                .is_some_and(|expected| expected.parsed["is_long"] == false)
                            {
                                "SHORT"
                            } else {
                                "LONG"
                            }
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
    if let Some(expected) = &s.percentage {
        channel.insert(
            "default_buy_targets",
            expected.entries[0]
                .iter()
                .map(|(_, fraction, _)| doc! {"fraction":fraction})
                .collect::<Vec<_>>(),
        );
        channel.insert("default_sell_targets", vec![doc! {"fraction":0.25}; 4]);
    }
    if let Some(expected) = &s.allocation {
        channel.insert(
            "default_sell_targets",
            expected
                .configured_fractions
                .iter()
                .map(|fraction| doc! {"fraction":fraction})
                .collect::<Vec<_>>(),
        );
    }
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
        bot.collection::<Document>("tradingprofiles").insert_one(doc!{"userId":user,"exchangeClients":[{"clientId":account,"name":"BingX-1","provider":"BingX","connectedChannel":CHANNEL,"api_key":"fixture-key","api_secret":"fixture-secret"}]}).await.unwrap();
        mongo.database("accounts").collection::<Document>("users").insert_one(doc!{"tg_chat_id":user,"auto_trading":true,"valid_till":DateTime::from_millis(DateTime::now().timestamp_millis()+120000)}).await.unwrap();
        if let Some(leverage) = over {
            bot.collection::<Document>("user_configs").insert_one(doc!{"user":user,"private_channels":[{"id":CHANNEL,"own_settings":true,"futures":{"active":true,"leverage":mongodb::bson::to_bson(leverage).unwrap()}}]}).await.unwrap();
        } else {
            bot.collection::<Document>("user_configs")
                .insert_one(doc! {"user":user,"lastNotifications":[{"info":"existing notice","time":"original"}]})
                .await
                .unwrap();
        }
    }
    if s.history_write_failures {
        mongo
            .database("admin")
            .run_command(doc! {
                "configureFailPoint":"failCommand", "mode":{"times":2},
                "data":{"failCommands":["update"],"errorCode":2}
            })
            .await
            .unwrap();
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
    let body = if s.malformed {
        b"{invalid job".to_vec()
    } else if s.expired {
        let mut job: Value =
            serde_json::from_str(include_str!("../fixtures/admission-job.json")).unwrap();
        job["tradeExpiresAt"] = json!(started - 1);
        serde_json::to_vec(&job).unwrap()
    } else {
        serde_json::to_vec(&json!({"message_id":7,"date":started/1000,"chat":{"id":CHANNEL,"type":"channel"},"text":s.text()})).unwrap()
    };
    let confirmed = channel
        .basic_publish(
            "".into(),
            if s.expired || s.malformed {
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
            if let Some(terminal) = s.channel_terminal {
                lines.iter().any(|line| line.starts_with(terminal))
            } else if s.malformed {
                lines.iter().any(|l| l.starts_with("Client job rejected:"))
            } else if s.expired {
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
    let failure_count = (0..count)
        .filter(|index| s.failure_for(*index).is_some())
        .count();
    if failure_count > 0 {
        process
            .wait_for(|lines| {
                lines
                    .iter()
                    .filter(|line| line.starts_with("user_failure_notification_saved:"))
                    .count()
                    == failure_count
            })
            .await;
    }
    process.stop().await;
    http.assert_complete();
    let journal = process.journal.lock().unwrap().clone();
    if let Some(expected) = &s.allocation {
        for index in 0..count {
            let account = format!("account-{}", 42 + index);
            if let Some(reason) = expected.failures[index] {
                assert!(
                    journal
                        .iter()
                        .any(|line| line.starts_with("Client trade rejected:")
                            && line.contains(&format!("account_id={}", json!(account)))
                            && line.contains(&format!("reason={reason}")))
                );
            }
            let diagnostics: Vec<Value> = journal
                .iter()
                .filter_map(|line| {
                    line.strip_prefix("Profit allocation rejected: ")
                        .or_else(|| line.strip_prefix("Profit targets reduced: "))
                        .and_then(|text| serde_json::from_str::<Value>(text).ok())
                        .filter(|value| value["account_id"] == account)
                })
                .collect();
            let reduced = s.targets
                && expected.profits[index].as_ref().is_some_and(|profits| {
                    profits.len() < expected.configured_fractions.len().min(4)
                });
            let exhausted = expected.failures[index] == Some("insufficientTargetBalance");
            assert_eq!(diagnostics.len(), usize::from(reduced || exhausted));
            if let Some(diagnostic) = diagnostics.first() {
                assert_eq!(diagnostic["channel_id"], CHANNEL);
                assert_eq!(diagnostic["message_id"], 7);
                assert_eq!(diagnostic["account_name"], "BingX-1");
                assert_eq!(
                    diagnostic["original_count"],
                    expected.configured_fractions.len().min(4)
                );
                assert_eq!(
                    diagnostic["final_count"],
                    expected.profits[index].as_ref().map_or(0, Vec::len)
                );
                if exhausted {
                    assert_eq!(diagnostic["constraint"], "minNotional");
                    assert_eq!(diagnostic["quantity"], "0.01");
                    assert_eq!(diagnostic["notional"], "9");
                    assert_eq!(diagnostic["minimum"], "10");
                }
                let text = diagnostic.to_string();
                assert!(!text.contains("fixture-key") && !text.contains("fixture-secret"));
            }
        }
    }
    let expected_rejection = if let Some(reason) = s.rejection_reason {
        Some(reason)
    } else if let Some(reason) = s.failure_reason() {
        Some(reason)
    } else if s.post_fails {
        Some("leverageChangeFailed")
    } else if !s.expired && !s.malformed && s.channel_terminal.is_none() && count == 0 {
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
    for index in 0..s.overrides.len() {
        let user = bot
            .collection::<Document>("user_configs")
            .find_one(doc! {"user":42 + index as i64})
            .await
            .unwrap()
            .unwrap();
        let history = user
            .get_array("lastNotifications")
            .cloned()
            .unwrap_or_default();
        let existing = usize::from(s.overrides[index].is_none());
        if existing == 1 {
            assert_eq!(
                history[0].as_document().unwrap(),
                &doc! {"info":"existing notice","time":"original"}
            );
        }
        assert_eq!(
            history.len(),
            existing + usize::from(s.failure_for(index).is_some()),
            "history for {}",
            s.name
        );
        if let Some(reason) = s.failure_for(index) {
            let notice = history[existing].as_document().unwrap();
            assert_eq!(
                notice.get_str("info").unwrap(),
                UserFailure::from_reason(reason)
                    .unwrap()
                    .format(s.signal.symbol(), "BingX-1")
            );
            let user_id = 42 + index as i64;
            let job_key = format!(
                "auto-trade:{CHANNEL}:7:{user_id}:account-{user_id}:BingX:futures:{}",
                s.signal.symbol().replace('-', "")
            );
            let event_id = format!(
                "{:x}",
                Sha256::digest(format!("user-failure:{job_key}:{reason}").as_bytes())
            );
            assert_eq!(notice.get_str("eventId").unwrap(), event_id);
            let recorded_at = DateTime::parse_rfc3339_str(notice.get_str("time").unwrap())
                .unwrap()
                .timestamp_millis();
            assert!(recorded_at >= started && recorded_at <= DateTime::now().timestamp_millis());
        }
    }
    if s.history_write_failures {
        let state = mongo
            .database("admin")
            .run_command(doc! {"configureFailPoint":"failCommand","mode":"off"})
            .await
            .unwrap();
        assert_eq!(
            state.get_i32("count"),
            Ok(2),
            "both injected history failures must occur"
        );
        assert!(
            !journal
                .iter()
                .any(|line| line.starts_with("Client job retry:")
                    || line.starts_with("user_failure_notification_save_failed:"))
        );
    }

    let summary = journal.iter().find_map(|l| {
        l.strip_prefix("Signal published: ")
            .and_then(|v| serde_json::from_str::<Value>(v).ok())
    });
    if !s.expired && !s.malformed && s.channel_terminal.is_none() && count > 0 {
        let summary = summary.as_ref().expect("signal publication summary");
        assert_eq!(summary["published_jobs"], count);
        assert!(summary["accepted_at_ms"].as_i64().unwrap() >= started);
        assert!(summary["accepted_at_ms"].as_i64().unwrap() <= DateTime::now().timestamp_millis());
        assert_eq!(
            summary["expires_at_ms"].as_i64().unwrap(),
            summary["accepted_at_ms"].as_i64().unwrap() + 60000
        );
    }
    if let Some(terminal) = s.channel_terminal {
        let reason = if let Some(reason) = s.rejection_reason {
            reason
        } else if terminal.ends_with("skipped:") {
            "NotSignal"
        } else {
            "AmbiguousInstruction"
        };
        assert!(
            journal
                .iter()
                .any(|line| line.starts_with(terminal) && line.contains(reason)),
            "{}: {journal:?}",
            s.name
        );
        assert!(summary.is_none());
        assert!(
            !journal
                .iter()
                .any(|line| line.starts_with("Signal parsed:"))
        );
    }
    if matches!(s.signal, Signal::Ada) && !s.expired && !s.malformed && s.channel_terminal.is_none()
    {
        let line = journal
            .iter()
            .find(|line| line.starts_with("Signal parsed:"))
            .unwrap();
        let parsed: Value = serde_json::from_str(line.split_once("result=").unwrap().1).unwrap();
        let mut expected = json!({"exchange_client":"_futures","symbol":"ADAUSDT","base_currency":"USDT","coin":"ADA","is_long":true,"buy_targets":["0.217"],"sell_targets":["0.22","0.23","0.24"],"stop_loss":"0.18","leverage":"3x"});
        if !s.targets {
            expected["sell_targets"] = json!([]);
        }
        if !s.stop {
            expected.as_object_mut().unwrap().remove("stop_loss");
        }
        assert_eq!(parsed, expected, "extracted data for {}", s.name);
        if s.decorated {
            assert!(line.contains("ignored_lines=5 "), "{line}");
        }
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
    if let Some(expected) = &s.percentage {
        let parsed: Value = serde_json::from_str(
            journal
                .iter()
                .find(|line| line.starts_with("Signal parsed:"))
                .unwrap()
                .split_once("result=")
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(parsed, expected.parsed, "percentage intent for {}", s.name);
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
        (0..count).filter(|index| s.publishes_for(*index)).count(),
        "{}: {outputs:?}; HTTP {}",
        s.name,
        http.journal()
    );
    outputs.sort_by_key(|o| o["trade_object"]["chat_id"].as_i64().unwrap());
    assert_eq!(
        outputs
            .iter()
            .map(|output| output["trade_object"]["chat_id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        (0..count)
            .filter(|index| s.publishes_for(*index))
            .map(|index| 42 + index as i64)
            .collect::<Vec<_>>(),
        "published account identities for {}",
        s.name
    );
    for actual in &outputs {
        let index = (actual["trade_object"]["chat_id"].as_i64().unwrap() - 42) as usize;
        assert!(
            index < count && s.publishes_for(index),
            "unexpected account output: {actual}"
        );
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
    if let Some(allocation) = &s.allocation {
        return expected_allocation(s, allocation, index);
    }
    if let Some(percentage) = &s.percentage {
        return expected_percentage(s, percentage, index);
    }
    let fixture = match s.signal {
        Signal::Ada => include_str!("../fixtures/signals/ada-3x.expected.json"),
        Signal::Sol => include_str!("../fixtures/signals/sol-50x.expected.json"),
        Signal::Missing => match s.resolved[index] {
            3 => include_str!("../fixtures/signals/ada-fallback-3x.expected.json"),
            5 => include_str!("../fixtures/signals/ada-fallback-5x.expected.json"),
            7 => include_str!("../fixtures/signals/ada-fallback-7x.expected.json"),
            _ => panic!("missing expected fixture"),
        },
        Signal::Xlm | Signal::Eth => unreachable!("custom expectation required"),
    };
    let mut value: Value = serde_json::from_str(fixture).unwrap();
    value["client_data"]["name"] = json!("BingX-1");
    value["trade_object"]["auto_Trade"]["exchangeClientName"] = json!("BingX-1");
    if !s.targets {
        value["trade_object"]["sell_targets"] = json!([]);
    }
    if !s.stop {
        value["trade_object"]
            .as_object_mut()
            .unwrap()
            .remove("stop_loss");
    }
    if s.mode == Mode::OneWay {
        value["trade_object"]["positionConfiguration"]["accountingModel"] = json!("ONE_WAY_V1");
    }
    value["trade_object"]["auto_Trade"]["usingChannelConfig"] = json!(s.overrides[index].is_none());
    value
}

fn expected_percentage(s: &Scenario, expected: &PercentageExpectation, index: usize) -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("../fixtures/signals/ada-3x.expected.json")).unwrap();
    let user = 42 + index as i64;
    let key = format!("auto-trade:{CHANNEL}:7:{user}:account-{user}:BingX:futures:XLMUSDT");
    let id = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()))[..32].to_owned();
    let long = expected.parsed["is_long"] == true;
    let quantity: f64 = expected.entries[index]
        .iter()
        .map(|(_, _, quantity)| quantity)
        .sum();
    let notional: f64 = expected.entries[index]
        .iter()
        .map(|(price, _, quantity)| price.parse::<f64>().unwrap() * quantity)
        .sum();
    let price = 0.18892;
    let margin = 5.0; // 0.5% of the scripted 1000 USDT balance.
    let requested = margin * s.resolved[index] as f64;
    let trade = &mut value["trade_object"];
    trade["id"] = json!(id(&key));
    trade["symbol"] = json!("XLM-USDT");
    trade["chat_id"] = json!(user);
    trade["is_long"] = json!(long);
    trade["quantity"] = json!(quantity);
    trade["used_coins"]["wished_quantity"] = json!(quantity);
    trade["precisions"] = json!({"symbol":"XLM-USDT","priceRangeUp":5,"priceRangeDown":0.5,"tickSize":0.0001,"minQty":1.0/price,"maxQty":100000.0/price,"minNotional":1.0,"lotSize":0.1});
    trade["buy_targets"] = json!(expected.entries[index].iter().enumerate().map(|(i,(price,fraction,quantity))| json!({"price":price,"symbol":"XLM-USDT","clientOrderId":id(&format!("{key}:BT:{i}")),"opened":true,"status":"new","role":"BT","side":if long {"BUY"}else{"SELL"},"type":"LIMIT","quantity":quantity,"fraction":fraction})).collect::<Vec<_>>());
    if expected.entries[index].len() == 1 {
        trade["buy_targets"][0]["fraction"] = json!(1);
    }
    trade["sell_targets"] = json!(expected.prices[index].iter().enumerate().map(|(i,price)| json!({"price":price,"symbol":"XLM-USDT","clientOrderId":id(&format!("{key}:ST:{i}")),"opened":true,"status":"new","role":"ST","side":if long {"SELL"}else{"BUY"},"type":"LIMIT","fraction":0.25})).collect::<Vec<_>>());
    trade["positionSizing"] = json!({"mode":"dynamic","source":"signal","requestedQuoteAmount":margin,"effectiveQuoteAmount":margin,"balanceFallbackApplied":false,"fallbackUsageRatio":null,"marginQuoteAmount":margin,"notionalQuoteAmount":requested,"rawTradeQuantity":requested/price,"executableTradeQuantity":quantity,"executableNotionalQuoteAmount":notional,"display":"0.5% signal override","balanceFraction":0.005});
    trade["auto_Trade"]["usingChannelConfig"] = json!(s.overrides[index].is_none());
    trade["auto_Trade"]["exchangeClientName"] = json!("BingX-1");
    trade["tradeLeverage"] = json!(format!("{}x", s.resolved[index]));
    trade["idempotencyKey"] = json!(key);
    trade["routing"]["symbol"] = json!("XLMUSDT");
    trade["routing"]["partitionKey"] =
        json!(format!("client-symbol:account-{user}:futures:XLMUSDT"));
    trade.as_object_mut().unwrap().remove("stop_loss");
    value["client_data"]["chatId"] = json!(user);
    value["client_data"]["clientId"] = json!(format!("account-{user}"));
    value["client_data"]["name"] = json!("BingX-1");
    value
}

fn expected_allocation(s: &Scenario, expected: &AllocationExpectation, index: usize) -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("../fixtures/signals/ada-3x.expected.json")).unwrap();
    let user = 42 + index as i64;
    let key = format!("auto-trade:{CHANNEL}:7:{user}:account-{user}:BingX:futures:ETHUSDT");
    let id = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()))[..32].to_owned();
    let quantity = expected.quantities[index];
    let margin = expected.balances[index].parse::<f64>().unwrap() * 0.1;
    let trade = &mut value["trade_object"];
    trade["id"] = json!(id(&key));
    trade["symbol"] = json!("ETH-USDT");
    trade["chat_id"] = json!(user);
    trade["is_long"] = json!(false);
    trade["quantity"] = json!(quantity);
    trade["used_coins"]["wished_quantity"] = json!(quantity);
    trade["precisions"] = json!({"symbol":"ETH-USDT","priceRangeUp":5,"priceRangeDown":0.5,"tickSize":0.01,"minQty":expected.min_qty,"maxQty":100.0,"minNotional":10.0,"lotSize":0.01});
    trade["buy_targets"] = json!([{"price":"1000.00","symbol":"ETH-USDT","clientOrderId":id(&format!("{key}:BT:0")),"opened":true,"status":"new","role":"BT","side":"SELL","type":"LIMIT","quantity":quantity,"fraction":1}]);
    trade["sell_targets"] = json!(expected.profits[index].as_ref().unwrap().iter().enumerate().map(|(i,(price,fraction))| json!({"price":price,"symbol":"ETH-USDT","clientOrderId":id(&format!("{key}:ST:{i}")),"opened":true,"status":"new","role":"ST","side":"BUY","type":"LIMIT","fraction":fraction})).collect::<Vec<_>>());
    if s.stop {
        trade["stop_loss"] = json!({"price":"1100.00","symbol":"ETH-USDT","clientOrderId":id(&format!("{key}:SL:0")),"opened":true,"status":"new","role":"SL","side":"BUY","type":"STOP_LOSS_LIMIT"});
    } else {
        trade.as_object_mut().unwrap().remove("stop_loss");
    }
    trade["positionSizing"] = json!({"mode":"dynamic","source":"channel","requestedQuoteAmount":margin,"effectiveQuoteAmount":margin,"balanceFallbackApplied":false,"fallbackUsageRatio":null,"marginQuoteAmount":margin,"notionalQuoteAmount":margin,"rawTradeQuantity":margin/1000.0,"executableTradeQuantity":quantity,"executableNotionalQuoteAmount":quantity*1000.0,"display":"10%","balanceFraction":0.1});
    trade["auto_Trade"]["usingChannelConfig"] = json!(true);
    trade["auto_Trade"]["exchangeClientName"] = json!("BingX-1");
    trade["tradeLeverage"] = json!("1x");
    trade["idempotencyKey"] = json!(key);
    trade["routing"]["symbol"] = json!("ETHUSDT");
    trade["routing"]["partitionKey"] =
        json!(format!("client-symbol:account-{user}:futures:ETHUSDT"));
    value["client_data"]["chatId"] = json!(user);
    value["client_data"]["clientId"] = json!(format!("account-{user}"));
    value["client_data"]["name"] = json!("BingX-1");
    value
}
