use channels_manager_v1::{
    contracts::messages::{ClientTradeJob, ExecutionRoute, NewTradeMessage},
    exchanges::bingx::{
        client::{AccountEvidence, BingxReadClient},
        migration::{AdmissionAttempt, admit_with_migration},
    },
    trading::job::validate_job,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn account(hedge: bool) -> AccountEvidence {
    AccountEvidence {
        mode: json!({"dualSidePosition":hedge}),
        positions: json!([]),
        orders: json!([]),
        balance: json!([{"asset":"USDT","availableMargin":"100"}]),
        leverage: json!({"longLeverage":10,"shortLeverage":10,"maxLongLeverage":50,"maxShortLeverage":50}),
    }
}
fn managed(symbol: &str, state: &str) -> Value {
    json!({"id":"t","exchangeClientId":"account-1","exchange_client":"_binance_futures_","symbol":symbol,"state":state,"is_long":true,"tradeLeverage":"10x"})
}
async fn server(
    status: u16,
    body: &'static str,
    delay: Duration,
) -> (
    BingxReadClient,
    Arc<Mutex<Vec<String>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = BingxReadClient::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        Duration::from_millis(100),
    )
    .unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let handle = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 8192];
            let n = stream.read(&mut bytes).await.unwrap();
            seen.lock()
                .unwrap()
                .push(String::from_utf8(bytes[..n].to_vec()).unwrap());
            tokio::time::sleep(delay).await;
            let _ = stream.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
        }
    });
    (client, requests, handle)
}
#[tokio::test]
async fn matrix_switches_only_eligible_accounts_and_sets_trade_configuration() {
    for (a, rows, expected, writes) in [
        (account(true), vec![], Some(ExecutionRoute::Hedge), 0),
        (account(false), vec![], Some(ExecutionRoute::Hedge), 1),
        (account(false), vec![managed("BTC-USDT", "OPENED")], None, 0),
        (
            account(false),
            vec![managed("ETH-USDT", "NEW")],
            Some(ExecutionRoute::OneWay),
            0,
        ),
        (
            account(false),
            vec![managed("BTC-USDT", "CLOSING")],
            Some(ExecutionRoute::Hedge),
            1,
        ),
    ] {
        let (client, requests, task) = server(200, r#"{"code":0,"data":{}}"#, Duration::ZERO).await;
        let job: ClientTradeJob =
            serde_json::from_str(include_str!("fixtures/admission-job.json")).unwrap();
        let validated = validate_job(&job, 1).unwrap();
        let attempt = AdmissionAttempt::new(Duration::from_secs(1));
        let result = admit_with_migration(&client, &validated, a, &rows, &attempt).await;
        if let Some(route) = expected {
            let admitted = result.unwrap();
            assert_eq!(admitted.position_configuration, route);
            let mut trade = json!({"id":"stable-id","positionConfiguration":"stale"});
            admitted.apply_to_trade_object(trade.as_object_mut().unwrap());
            let envelope = NewTradeMessage {
                expires_at: validated.expires_at,
                trade_object: trade,
                client_data: json!({}),
            };
            let wire = serde_json::to_value(envelope).unwrap();
            assert_eq!(
                wire["trade_object"]["positionConfiguration"]["accountingModel"],
                serde_json::to_value(route).unwrap()
            );
            assert_eq!(wire["trade_object"]["id"], "stable-id");
        } else {
            assert_eq!(result.unwrap_err().code, "migrationRequired");
        }
        let seen = requests.lock().unwrap();
        assert_eq!(seen.len(), writes, "no confirmation GET or repeat POST");
        for request in seen.iter() {
            assert!(request.starts_with("POST /openApi/swap/v1/positionSide/dual?dualSidePosition=true&recvWindow=5000&timestamp="));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-bx-apikey: fixture-key")
            );
            let query = request
                .split_whitespace()
                .nth(1)
                .unwrap()
                .split_once('?')
                .unwrap()
                .1;
            let (signed, signature) = query.rsplit_once("&signature=").unwrap();
            use hmac::{Hmac, Mac};
            use sha2::Sha256;
            let mut mac = Hmac::<Sha256>::new_from_slice(b"fixture-secret").unwrap();
            mac.update(signed.as_bytes());
            assert_eq!(signature, format!("{:x}", mac.finalize().into_bytes()));
        }
        task.abort();
    }
}
#[tokio::test]
async fn switch_failures_are_terminal_and_never_read_back_or_retry() {
    for (status, body, delay) in [
        (
            200,
            r#"{"code":123,"msg":"fixture-secret"}"#,
            Duration::ZERO,
        ),
        (503, "fixture-secret", Duration::ZERO),
        (200, "invalid fixture-secret", Duration::ZERO),
        (200, r#"{"code":0,"data":[]}"#, Duration::ZERO),
        (200, r#"{"code":0,"data":{}}"#, Duration::from_millis(200)),
    ] {
        let (client, requests, task) = server(status, body, delay).await;
        let job = serde_json::from_str(include_str!("fixtures/admission-job.json")).unwrap();
        let validated = validate_job(&job, 1).unwrap();
        let result = admit_with_migration(
            &client,
            &validated,
            account(false),
            &[],
            &AdmissionAttempt::new(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(result.unwrap_err().code, "modeSwitchFailed");
        assert_eq!(requests.lock().unwrap().len(), 1);
        task.abort();
    }
}
#[tokio::test]
async fn expired_attempt_never_switches() {
    let (client, requests, task) = server(200, r#"{"code":0,"data":{}}"#, Duration::ZERO).await;
    let job = serde_json::from_str(include_str!("fixtures/admission-job.json")).unwrap();
    let validated = validate_job(&job, 1).unwrap();
    let result = admit_with_migration(
        &client,
        &validated,
        account(false),
        &[],
        &AdmissionAttempt::new(Duration::ZERO),
    )
    .await;
    assert_eq!(result.unwrap_err().code, "admissionDeadlineExceeded");
    assert!(requests.lock().unwrap().is_empty());
    task.abort();
}
