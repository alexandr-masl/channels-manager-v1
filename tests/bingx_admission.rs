use channels_manager_v1::exchanges::bingx::{
    admission::evaluate_admission,
    client::{AccountEvidence, BingxReadClient},
};
use serde_json::{Value, json};
use std::time::Duration;
fn account(mode: bool) -> AccountEvidence {
    AccountEvidence {
        mode: json!({"dualSidePosition":mode}),
        positions: json!([]),
        orders: json!({"orders":[]}),
        balance: json!([{"asset":"USDT","availableMargin":"100"}]),
        leverage: json!({"longLeverage":5,"shortLeverage":8,"maxLongLeverage":10,"maxShortLeverage":20}),
    }
}
fn admit(a: &AccountEvidence, managed: &[Value]) -> Result<&'static str, &'static str> {
    evaluate_admission(a, managed, "account", "BTC-USDT", true, 5)
        .map(|a| a.position_configuration.as_str())
        .map_err(|e| e.code)
}
#[test]
fn hedge_and_flat_oneway_policy() {
    assert_eq!(admit(&account(true), &[]), Ok("ORDER_LEDGER_V1"));
    assert_eq!(admit(&account(false), &[]), Err("migrationRequired"));
}
#[test]
fn legacy_requires_other_symbol_activity_and_both_leverages() {
    let mut a = account(false);
    a.positions =
        json!([{"positionId":"p","symbol":"ETH-USDT","positionSide":"BOTH","positionAmt":"1"}]);
    assert_eq!(admit(&a, &[]), Err("leverageChangeRequired"));
    a.leverage["shortLeverage"] = json!(5);
    assert_eq!(admit(&a, &[]), Ok("ONE_WAY_V1"));
    a.positions[0]["symbol"] = json!("BTC-USDT");
    assert_eq!(admit(&a, &[]), Err("migrationRequired"));
}
fn owner() -> Value {
    json!({"id":"t","exchangeClientId":"account","exchange_client":"_binance_futures_","symbol":"BTC-USDT","state":"OPENED","is_long":true,"tradeLeverage":"5x"})
}
#[test]
fn ownership_fails_closed() {
    let a = account(true);
    assert!(admit(&a, &[owner()]).is_ok());
    let mut o = owner();
    o["tradeLeverage"] = json!(6);
    assert_eq!(admit(&a, &[o.clone()]), Err("leverageConflict"));
    o["tradeLeverage"] = Value::Null;
    assert_eq!(admit(&a, &[o]), Ok("ORDER_LEDGER_V1"));
    let mut o = owner();
    o["state"] = json!("UNKNOWN");
    assert_eq!(admit(&a, &[o]), Ok("ORDER_LEDGER_V1"));
}
#[test]
fn validates_all_evidence_and_balance() {
    let mut a = account(true);
    a.balance[0]["availableMargin"] = json!("0");
    assert_eq!(admit(&a, &[]), Err("insufficientBalance"));
    a = account(true);
    a.positions = json!([{"positionAmt":0}]);
    assert_eq!(admit(&a, &[]), Err("incompleteExchangeEvidence"));
    a = account(true);
    a.leverage["maxLongLeverage"] = json!(4);
    assert_eq!(admit(&a, &[]), Err("leverageLimitExceeded"));
}
#[test]
fn endpoint_allowlist() {
    for bad in [
        "http://open-api.bingx.com",
        "https://example.com",
        "http://localhost:1234",
        "http://127.0.0.1:1234/path",
        "https://user:secret@open-api.bingx.com",
    ] {
        assert!(BingxReadClient::new(bad, Duration::from_secs(1)).is_err());
    }
    assert!(BingxReadClient::new("http://127.0.0.1:1234", Duration::from_secs(1)).is_ok());
}

#[tokio::test]
async fn signed_client_uses_only_five_get_routes_and_verifies_signatures() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut routes = std::collections::HashSet::new();
        for _ in 0..5 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8(buf[..n].to_vec()).unwrap();
            assert!(request.starts_with("GET "));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-bx-apikey: fixture-key")
            );
            let target = request.split_whitespace().nth(1).unwrap();
            let (path, query) = target.split_once('?').unwrap();
            let (signed, signature) = query.rsplit_once("&signature=").unwrap();
            let mut mac = Hmac::<Sha256>::new_from_slice(b"fixture-secret").unwrap();
            mac.update(signed.as_bytes());
            assert_eq!(signature, format!("{:x}", mac.finalize().into_bytes()));
            assert!(signed.contains("recvWindow=5000&timestamp="));
            if path.ends_with("leverage") {
                assert!(signed.starts_with("symbol=BTC-USDT&"));
            } else {
                assert!(!signed.contains("symbol="));
            }
            assert!(routes.insert(path.to_owned()));
            let a = account(true);
            let data = match path {
                "/openApi/swap/v1/positionSide/dual" => a.mode,
                "/openApi/swap/v2/user/positions" => a.positions,
                "/openApi/swap/v2/trade/openOrders" => a.orders,
                "/openApi/swap/v3/user/balance" => a.balance,
                "/openApi/swap/v2/trade/leverage" => a.leverage,
                _ => panic!("unexpected route"),
            };
            let body = json!({"code":0,"data":data}).to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let client =
        BingxReadClient::new(&format!("http://{address}"), Duration::from_secs(2)).unwrap();
    let evidence = client
        .read_account("fixture-key", "fixture-secret", "BTC-USDT")
        .await
        .unwrap();
    assert!(admit(&evidence, &[]).is_ok());
    server.await.unwrap();
}

#[tokio::test]
async fn transport_errors_are_bounded_and_redacted() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    for (status, body, retryable) in [
        (429, "secret provider detail", true),
        (401, "secret provider detail", false),
        (200, "not json secret", false),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0; 8192];
            let _ = stream.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        });
        let client =
            BingxReadClient::new(&format!("http://{addr}"), Duration::from_millis(200)).unwrap();
        let error = client
            .read_account("fixture-key", "fixture-secret", "BTC-USDT")
            .await
            .unwrap_err();
        assert_eq!(error.retryable, retryable);
        let debug = format!("{error:?}");
        assert!(
            !debug.contains("secret")
                && !debug.contains("127.0.0.1")
                && !debug.contains("signature")
        );
        server.await.unwrap();
    }
}

#[test]
fn malformed_leverage_strings_and_duplicate_owners_fail_closed() {
    let mut a = account(true);
    a.leverage["longLeverage"] = json!("5.0");
    assert_eq!(admit(&a, &[]), Err("incompleteExchangeEvidence"));
    assert_eq!(
        admit(&account(true), &[owner(), owner()]),
        Err("incompleteManagedEvidence")
    );
    let mut o = owner();
    o["tradeLeverage"] = json!("5xx");
    assert_eq!(admit(&account(true), &[o]), Ok("ORDER_LEDGER_V1"));
}
#[test]
fn closing_ownership_and_side_specific_limits_are_preserved() {
    let mut o = owner();
    o["state"] = json!("CLOSING");
    o["tradeLeverage"] = json!(6);
    assert_eq!(admit(&account(true), &[o]), Err("leverageConflict"));
    let mut a = account(true);
    a.leverage["maxShortLeverage"] = json!(1);
    assert!(admit(&a, &[]).is_ok());
    let mut o = owner();
    o["is_long"] = json!(false);
    o["tradeLeverage"] = json!(8);
    assert!(admit(&a, &[o]).is_ok());
}

#[tokio::test]
async fn account_reads_timeout_when_server_never_responds() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BingxReadClient::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        Duration::from_millis(40),
    )
    .unwrap();
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.read_account("fixture-key", "fixture-secret", "BTC-USDT"),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().retryable);
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn malformed_orders_and_unknown_mode_are_rejected() {
    let mut a = account(true);
    a.mode = json!({"dualSidePosition":"unknown"});
    assert_eq!(admit(&a, &[]), Err("incompleteExchangeEvidence"));
    a = account(true);
    let order = json!({"orderId":"o","symbol":"ETH-USDT","side":"BUY","positionSide":"LONG","type":"LIMIT","origQty":"1"});
    a.orders = json!([order.clone(), order]);
    assert_eq!(admit(&a, &[]), Err("incompleteExchangeEvidence"));
}

#[test]
fn historical_and_contradictory_lifecycles_do_not_block_mode() {
    for (state, status) in [
        ("EXECUTING", None),
        ("ERROR", None),
        ("UNKNOWN", None),
        ("NEW", Some("COMPLETE")),
        ("OPENED", Some("FAILED")),
        ("FINISHED", None),
    ] {
        let mut o = owner();
        o["state"] = json!(state);
        o["tradeLeverage"] = json!(99);
        if let Some(status) = status {
            o["creationStatus"] = json!(status);
        }
        assert_eq!(
            admit(&account(true), &[o.clone()]),
            Ok("ORDER_LEDGER_V1"),
            "{state}"
        );
        assert_eq!(
            admit(&account(false), &[o]),
            Err("migrationRequired"),
            "{state}"
        );
    }
}
#[test]
fn retained_rows_without_ownership_still_block_oneway_mode() {
    let mut o = owner();
    o.as_object_mut().unwrap().remove("is_long");
    o.as_object_mut().unwrap().remove("tradeLeverage");
    assert_eq!(admit(&account(true), &[o.clone()]), Ok("ORDER_LEDGER_V1"));
    assert_eq!(admit(&account(false), &[o]), Err("migrationRequired"));
}
#[test]
fn mode_blocker_takes_precedence_over_owner_conflict() {
    let mut o = owner();
    o["tradeLeverage"] = json!(99);
    assert_eq!(admit(&account(false), &[o]), Err("migrationRequired"));
}

#[test]
fn zero_positions_are_validated_then_excluded_before_duplicate_check() {
    let mut a = account(true);
    let zero =
        json!({"positionId":"p","symbol":"ETH-USDT","positionSide":"LONG","positionAmt":"0"});
    a.positions = json!([zero.clone(), zero]);
    assert!(admit(&a, &[]).is_ok());
    a.positions[0]["positionId"] = Value::Null;
    assert_eq!(admit(&a, &[]), Err("incompleteExchangeEvidence"));
}
#[test]
fn invalid_retained_ownership_values_are_diagnostic_only() {
    for value in [
        json!(null),
        json!("5xx"),
        json!("6 x"),
        json!(-1),
        json!("6.0"),
    ] {
        let mut o = owner();
        o["tradeLeverage"] = value;
        assert!(admit(&account(true), &[o]).is_ok());
    }
    let mut o = owner();
    o["is_long"] = json!("true");
    o["tradeLeverage"] = json!(99);
    assert!(admit(&account(true), &[o]).is_ok());
}

#[test]
fn mode_matrix_distinguishes_migration_from_blocked_and_legacy_accounts() {
    use channels_manager_v1::exchanges::bingx::admission::{ModeDecision, evaluate_position_mode};
    let decide = |a: &AccountEvidence, rows: &[Value]| {
        evaluate_position_mode(a, rows, "account", "BTC-USDT")
    };
    assert_eq!(
        decide(&account(false), &[]).unwrap(),
        ModeDecision::MigrateToHedge
    );
    assert_eq!(decide(&account(true), &[]).unwrap(), ModeDecision::Hedge);
    for orders in [false, true] {
        for (symbol, blocked) in [("BTC-USDT", true), ("ETH-USDT", false)] {
            let mut a = account(false);
            if orders {
                a.orders = json!([{"orderId":"o","symbol":symbol,"side":"BUY","positionSide":"BOTH","type":"LIMIT","origQty":"1"}]);
            } else {
                a.positions = json!([{"positionId":"p","symbol":symbol,"positionSide":"BOTH","positionAmt":"1"}]);
            }
            let result = decide(&a, &[]);
            if blocked {
                assert_eq!(result.unwrap_err().code, "migrationRequired");
            } else {
                assert_eq!(result.unwrap(), ModeDecision::LegacyOneWay);
            }
        }
    }
    for state in ["NEW", "OPENED", "CLOSING", "ERROR", "EXECUTING", "FINISHED"] {
        let mut row = owner();
        row["state"] = json!(state);
        let result = decide(&account(false), &[row]);
        if matches!(state, "NEW" | "OPENED") {
            assert_eq!(result.unwrap_err().code, "migrationRequired");
        } else {
            assert_eq!(result.unwrap(), ModeDecision::MigrateToHedge);
        }
    }
}
