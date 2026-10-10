use channels_manager_v1::{
    exchanges::bingx::market_data::{MarketDataError, MarketDataProvider, MarketSnapshot},
    signals::manager::{OpenTradeRepository, PreparationError, PreparationOutcome, SignalManager},
    signals::{ParseOutcome, parse_signal},
    telegram::{ChannelClient, ChannelContext, IntakeOutcome, inspect_message},
};
use futures_util::future::BoxFuture;
use mongodb::bson::{DateTime, Document, doc, oid::ObjectId};
use serde_json::json;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
const NOW: u64 = 1_791_056_304_000;
struct Trades {
    docs: Vec<Document>,
    fail: bool,
    users: Mutex<Vec<i64>>,
}
impl OpenTradeRepository for Trades {
    fn opened_trades(
        &self,
        users: Vec<i64>,
    ) -> BoxFuture<'_, Result<Vec<Document>, PreparationError>> {
        Box::pin(async move {
            *self.users.lock().unwrap() = users;
            if self.fail {
                Err(PreparationError::DatabaseUnavailable)
            } else {
                Ok(self.docs.clone())
            }
        })
    }
}
struct Market {
    calls: AtomicUsize,
    error: Option<MarketDataError>,
}
impl MarketDataProvider for Market {
    fn snapshot<'a>(
        &'a self,
        symbol: &'a str,
    ) -> BoxFuture<'a, Result<MarketSnapshot, MarketDataError>> {
        Box::pin(async move {
            assert_eq!(symbol, "BTCUSDT");
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.error {
                return Err(error);
            }
            Ok(MarketSnapshot {
                curr_price: json!("85000.5"),
                symbol_info: json!({"symbol":"BTC-USDT","status":1,"tickSize":0.1,"lotSize":0.001,"minQty":0.001,"minNotional":5}),
            })
        })
    }
}
fn fixtures() -> (Trades, Market, ChannelContext) {
    let text = "BTCUSDT BREAKOUT SHORT\nENTRY 84550-84650\nTG1 83595\nLEVERAGE 5x\nPOSITION SIZE 0.5%\nSL 86347";
    let body = serde_json::to_vec(
        &json!({"message_id":7,"date":NOW/1000,"chat":{"id":-100,"type":"channel"},"text":text}),
    )
    .unwrap();
    let IntakeOutcome::Received(message) = inspect_message(&body, NOW) else {
        panic!("message")
    };
    let ParseOutcome::Parsed(signal) = parse_signal(text) else {
        panic!("signal")
    };
    let context = ChannelContext {
        message,
        signal,
        channel_settings: doc! {"id":-100_i64,"_id":ObjectId::parse_str("0123456789abcdef01234567").unwrap(),"createdAt":DateTime::from_millis(NOW as i64)},
        clients: vec![
            ChannelClient {
                chat_id: 42,
                client_id: "a".into(),
                name: Some("account".into()),
                key: "secret-key".into(),
                key_secret: "secret-value".into(),
                user_config: Some(
                    doc! {"user":42.0,"private_channels":[{"id":-100.0,"own_settings":true}]},
                ),
                user_settings: None,
            },
            ChannelClient {
                chat_id: 42,
                client_id: "b".into(),
                name: None,
                key: "secret-key".into(),
                key_secret: "secret-value".into(),
                user_config: None,
                user_settings: None,
            },
            ChannelClient {
                chat_id: 43,
                client_id: "c".into(),
                name: None,
                key: "secret-key".into(),
                key_secret: "secret-value".into(),
                user_config: None,
                user_settings: None,
            },
        ],
    };
    (
        Trades {
            docs: vec![
                doc! {"chat_id":42.0,"symbol":"SOLUSDT"},
                doc! {"chat_id":43_i64,"symbol":"ETHUSDT"},
            ],
            fail: false,
            users: Mutex::new(vec![]),
        },
        Market {
            calls: AtomicUsize::new(0),
            error: None,
        },
        context,
    )
}
#[tokio::test]
async fn prepares_wire_jobs_with_shared_market_and_per_user_trades() {
    let (trades, market, context) = fixtures();
    let PreparationOutcome::Prepared(batch) = SignalManager::new(&trades, &market)
        .prepare(context, || NOW + 1000)
        .await
        .unwrap()
    else {
        panic!("prepared")
    };
    assert_eq!(batch.jobs.len(), 3);
    assert_eq!(market.calls.load(Ordering::SeqCst), 1);
    assert_eq!(*trades.users.lock().unwrap(), vec![42, 43]);
    let value = serde_json::to_value(&batch.jobs[0]).unwrap();
    assert_eq!(
        value,
        json!({
            "jobType":"create_trade","version":1,"channelID":-100,"messageId":7,
            "sourceCreatedAt":"2026-10-03T19:38:24.000Z","tradeExpiresAt":NOW+61000,
            "signalData":{"exchange_client":"_futures","symbol":"BTCUSDT","base_currency":"USDT","coin":"BTC","is_long":false,"buy_targets":["84550","84650"],"sell_targets":["83595"],"stop_loss":"86347","leverage":"5x","position":0.005,"breakOutEntry":true},
            "channelSettings":{"id":-100,"_id":"0123456789abcdef01234567","createdAt":"2026-10-03T19:38:24.000Z"},
            "client":{"chatId":42,"clientId":"a","name":"account","provider":"BingX","key":"secret-key","keySecret":"secret-value"},
            "userConfig":{"user":42,"private_channels":[{"id":-100,"own_settings":true}]},
            "marketData":{"bingXFutures":{"currPrice":"85000.5","symbolInfo":{"symbol":"BTC-USDT","status":1,"tickSize":0.1,"lotSize":0.001,"minQty":0.001,"minNotional":5}}},
            "openedTrades":[{"chat_id":42,"symbol":"SOLUSDT"}],
            "idempotencyKey":"auto-trade:-100:7:42:a:BingX:futures:BTCUSDT","partitionKey":"client-symbol:a:futures:BTCUSDT","provider":"BingX","market":"futures","symbol":"BTCUSDT"
        })
    );
    let second = serde_json::to_value(&batch.jobs[1]).unwrap();
    assert!(second.get("userConfig").is_none());
    assert!(second["client"].get("name").is_none());
    assert_eq!(batch.jobs[2].opened_trades[0]["symbol"], "ETHUSDT");
    let log = serde_json::to_string(&batch.summary).unwrap();
    for secret in [
        "secret-key",
        "secret-value",
        "keySecret",
        "private_channels",
    ] {
        assert!(!log.contains(secret));
    }
}
#[tokio::test]
async fn empty_accounts_do_not_load_dependencies() {
    let (trades, market, mut context) = fixtures();
    context.clients.clear();
    assert!(matches!(
        SignalManager::new(&trades, &market)
            .prepare(context, || NOW)
            .await
            .unwrap(),
        PreparationOutcome::Skipped(_)
    ));
    assert!(trades.users.lock().unwrap().is_empty());
    assert_eq!(market.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn dependency_errors_retry_and_unsupported_symbols_reject() {
    let (mut trades, market, context) = fixtures();
    trades.fail = true;
    assert!(matches!(
        SignalManager::new(&trades, &market)
            .prepare(context, || NOW)
            .await,
        Err(PreparationError::DatabaseUnavailable)
    ));
    for error in [
        MarketDataError::Unavailable,
        MarketDataError::InvalidResponse,
    ] {
        let (trades, mut market, context) = fixtures();
        market.error = Some(error);
        assert!(
            SignalManager::new(&trades, &market)
                .prepare(context, || NOW)
                .await
                .is_err()
        );
    }
    let (trades, mut market, context) = fixtures();
    market.error = Some(MarketDataError::UnsupportedSymbol);
    assert!(matches!(
        SignalManager::new(&trades, &market)
            .prepare(context, || NOW)
            .await
            .unwrap(),
        PreparationOutcome::Rejected(_)
    ));
}
#[tokio::test]
async fn expiry_clock_runs_after_preparation_and_has_no_global_capacity_gate() {
    let (mut trades, market, context) = fixtures();
    trades.docs = vec![doc! {"chat_id":42_i64}; 100];
    let PreparationOutcome::Prepared(batch) = SignalManager::new(&trades, &market)
        .prepare(context, || {
            assert_eq!(market.calls.load(Ordering::SeqCst), 1);
            NOW + 2000
        })
        .await
        .unwrap()
    else {
        panic!("prepared")
    };
    assert!(
        batch
            .jobs
            .iter()
            .all(|j| j.trade_expires_at == Some(NOW + 62000))
    );
    assert_eq!(batch.jobs[0].opened_trades.len(), 100);
}

#[tokio::test]
async fn invalid_context_and_clock_do_not_create_jobs() {
    for now in [0, u64::MAX] {
        let (trades, market, context) = fixtures();
        assert!(matches!(
            SignalManager::new(&trades, &market)
                .prepare(context, || now)
                .await
                .unwrap(),
            PreparationOutcome::Rejected(_)
        ));
    }
    let (mut trades, market, context) = fixtures();
    trades.docs = vec![doc! {"chat_id":"invalid"}];
    assert!(matches!(
        SignalManager::new(&trades, &market)
            .prepare(context, || NOW)
            .await
            .unwrap(),
        PreparationOutcome::Rejected(_)
    ));
}
