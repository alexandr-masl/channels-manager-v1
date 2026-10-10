use channels_manager_v1::{
    config::AppConfig,
    exchanges::bingx::market_data::{BingxMarketData, MarketDataError, MarketDataProvider},
    redis::{MetadataCache, RedisConnections},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn cache() -> MetadataCache {
    let mut config = AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some("amqp://localhost".into()),
        "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        "REDIS" => Some("localhost".into()),
        _ => None,
    })
    .unwrap();
    config.redis.cache_enabled = false;
    RedisConnections::new(config.redis, &config.runtime)
        .unwrap()
        .cache()
}
fn contract() -> Value {
    json!({"code":0,"data":[{"symbol":"BTC-USDT","status":1,"pricePrecision":2,"quantityPrecision":3,"tradeMinQuantity":"0.002","tradeMinUSDT":"5"}]})
}
async fn server(
    responses: Vec<(u16, String)>,
    delay: Duration,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = vec![];
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 8192];
            let n = socket.read(&mut bytes).await.unwrap();
            requests.push(String::from_utf8_lossy(&bytes[..n]).to_string());
            tokio::time::sleep(delay).await;
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
        requests
    });
    (url, task)
}
#[tokio::test]
async fn fetches_public_routes_and_normalizes_contract_without_credentials() {
    let (url, task) = server(
        vec![
            (200, contract().to_string()),
            (
                200,
                json!({"code":0,"data":{"symbol":"BTC-USDT","price":"123.45"}}).to_string(),
            ),
        ],
        Duration::ZERO,
    )
    .await;
    let api =
        BingxMarketData::with_loopback_endpoint(cache(), Duration::from_secs(1), &url).unwrap();
    let snapshot = api.snapshot("BTCUSDT").await.unwrap();
    assert_eq!(snapshot.curr_price, json!("123.45"));
    assert_eq!(snapshot.symbol_info["tickSize"], json!(0.01));
    assert_eq!(snapshot.symbol_info["lotSize"], json!(0.001));
    assert_eq!(snapshot.symbol_info["minQty"], json!(0.002));
    assert_eq!(snapshot.symbol_info["minNotional"], json!(5.0));
    let requests = task.await.unwrap();
    assert!(requests[0].starts_with("GET /openApi/swap/v2/quote/contracts?symbol=BTC-USDT "));
    assert!(requests[1].starts_with("GET /openApi/swap/v1/ticker/price?symbol=BTC-USDT "));
    assert!(
        requests
            .iter()
            .all(|r| !r.to_lowercase().contains("api-key") && !r.contains("signature="))
    );
}
#[tokio::test]
async fn rejects_status_api_errors_inactive_and_invalid_metadata() {
    let mut inactive = contract();
    inactive["data"][0]["status"] = json!(0);
    let mut invalid = contract();
    invalid["data"][0]["size"] = json!("NaN");
    for (status, body, expected) in [
        (503, contract().to_string(), MarketDataError::Unavailable),
        (302, String::new(), MarketDataError::Unavailable),
        (
            200,
            json!({"code":100,"msg":"secret"}).to_string(),
            MarketDataError::Unavailable,
        ),
        (
            200,
            inactive.to_string(),
            MarketDataError::UnsupportedSymbol,
        ),
        (200, invalid.to_string(), MarketDataError::InvalidResponse),
        (200, "invalid json".into(), MarketDataError::InvalidResponse),
        (200, "x".repeat(1_048_577), MarketDataError::InvalidResponse),
    ] {
        let (url, task) = server(vec![(status, body)], Duration::ZERO).await;
        let api =
            BingxMarketData::with_loopback_endpoint(cache(), Duration::from_secs(1), &url).unwrap();
        assert_eq!(api.snapshot("BTCUSDT").await.err(), Some(expected));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn rejects_bad_prices_and_bounds_request_time() {
    for price in [json!(0), json!("NaN"), json!(-1), Value::Null] {
        let (url, task) = server(
            vec![
                (200, contract().to_string()),
                (200, json!({"code":0,"data":{"price":price}}).to_string()),
            ],
            Duration::ZERO,
        )
        .await;
        let api =
            BingxMarketData::with_loopback_endpoint(cache(), Duration::from_secs(1), &url).unwrap();
        assert_eq!(
            api.snapshot("BTCUSDT").await.err(),
            Some(MarketDataError::InvalidResponse)
        );
        task.await.unwrap();
    }
    let (url, task) = server(
        vec![(200, contract().to_string())],
        Duration::from_millis(100),
    )
    .await;
    let api =
        BingxMarketData::with_loopback_endpoint(cache(), Duration::from_millis(10), &url).unwrap();
    assert_eq!(
        api.snapshot("BTCUSDT").await.err(),
        Some(MarketDataError::Unavailable)
    );
    task.await.unwrap();
}
#[test]
fn endpoint_override_only_allows_loopback_without_credentials() {
    for url in [
        "https://example.com",
        "http://localhost",
        "http://user:password@127.0.0.1",
        "http://127.0.0.1/path",
    ] {
        assert!(
            BingxMarketData::with_loopback_endpoint(cache(), Duration::from_secs(1), url).is_err()
        );
    }
}

#[tokio::test]
async fn valid_shared_metadata_skips_contract_request_and_invalid_cache_falls_back() {
    use channels_manager_v1::contracts::storage::{DEFAULT_CACHE_PREFIX, bingx_metadata_key};
    use std::process::{Child, Command, Stdio};
    struct RedisServer(Child);
    impl Drop for RedisServer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let _server = RedisServer(
        Command::new(std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into()))
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = redis::Client::open(("127.0.0.1", port)).unwrap();
    let mut connection = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(c) = client.get_multiplexed_async_connection().await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let config = AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some("amqp://localhost".into()),
        "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        "REDIS" => Some("127.0.0.1".into()),
        "REDIS_CLIENT_PORT" => Some(port.to_string()),
        _ => None,
    })
    .unwrap();
    let mut manager = RedisConnections::new(config.redis, &config.runtime).unwrap();
    let metadata = json!({"symbol":"BTC-USDT","status":1,"tickSize":0.01,"lotSize":0.001,"minQty":0.002,"minNotional":5.0});
    let key = bingx_metadata_key(DEFAULT_CACHE_PREFIX, "futures:BTC-USDT");
    for valid in [true, false] {
        let stored = if valid {
            metadata.clone()
        } else {
            json!({"symbol":"BTC-USDT","status":0})
        };
        redis::cmd("SET")
            .arg(&key)
            .arg(stored.to_string())
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let price = (200, json!({"code":0,"data":{"price":"123.45"}}).to_string());
        let responses = if valid {
            vec![price]
        } else {
            vec![(200, contract().to_string()), price]
        };
        let (url, task) = server(responses, Duration::ZERO).await;
        let api =
            BingxMarketData::with_loopback_endpoint(manager.cache(), Duration::from_secs(1), &url)
                .unwrap();
        let snapshot = api.snapshot("BTCUSDT").await.unwrap();
        assert_eq!(snapshot.symbol_info["tickSize"], metadata["tickSize"]);
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), if valid { 1 } else { 2 });
        if valid {
            assert!(requests[0].contains("/ticker/price?"));
        }
    }
    manager.close().await;
}

#[tokio::test]
async fn rejects_malformed_or_non_usdt_symbols_before_network() {
    let api = BingxMarketData::with_loopback_endpoint(
        cache(),
        Duration::from_secs(1),
        "http://127.0.0.1:1",
    )
    .unwrap();
    for symbol in [
        "",
        "USDT",
        "BTCUSD",
        "btcUSDT",
        "BTC/USDT",
        "BTC--USDT",
        "BTC-USDT-USDT",
        "BTCUSDT?x=1",
    ] {
        assert_eq!(
            api.snapshot(symbol).await.err(),
            Some(MarketDataError::UnsupportedSymbol),
            "{symbol}"
        );
    }
}

#[tokio::test]
async fn explicit_symbol_errors_are_terminal_but_other_api_errors_retry() {
    for message in [
        "invalid symbol",
        "symbol not found",
        "symbol does not exist",
        "symbol not available",
        "Invalid Symbol: BTC-USDT",
    ] {
        let (url, task) = server(
            vec![(200, json!({"code":100400,"msg":message}).to_string())],
            Duration::ZERO,
        )
        .await;
        let api =
            BingxMarketData::with_loopback_endpoint(cache(), Duration::from_secs(1), &url).unwrap();
        assert_eq!(
            api.snapshot("BTCUSDT").await.err(),
            Some(MarketDataError::UnsupportedSymbol),
            "{message}"
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_status_is_retryable_response_error() {
    for status in [Value::Null, json!("unknown"), json!({}), json!(true)] {
        let mut data = contract();
        data["data"][0]["status"] = status;
        let (url, task) = server(vec![(200, data.to_string())], Duration::ZERO).await;
        let api =
            BingxMarketData::with_loopback_endpoint(cache(), Duration::from_secs(1), &url).unwrap();
        assert_eq!(
            api.snapshot("BTCUSDT").await.err(),
            Some(MarketDataError::InvalidResponse)
        );
        task.await.unwrap();
    }
}
