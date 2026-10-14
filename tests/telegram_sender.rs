use channels_manager_v1::telegram::sender::{SendError, TelegramSender};
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn server(
    status: u16,
    body: String,
    delay: Duration,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut chunk = [0; 2048];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&request);
            if let Some((headers, body)) = text.split_once("\r\n\r\n") {
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
        tokio::time::sleep(delay).await;
        let _ = socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
        String::from_utf8(request).unwrap()
    });
    (endpoint, task)
}
#[tokio::test]
async fn sends_exact_created_reply_without_polling() {
    let (endpoint, task) = server(
        200,
        json!({"ok":true,"result":{"message_id":42}}).to_string(),
        Duration::ZERO,
    )
    .await;
    let sender = TelegramSender::new("123:fake_secret", &endpoint, Duration::from_secs(1)).unwrap();
    assert_eq!(sender.send_accepted(-1001596367704, 4388).await, Ok(42));
    let request = task.await.unwrap();
    assert!(request.starts_with("POST /bot123:fake_secret/sendMessage "));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request.split_once("\r\n\r\n").unwrap().1)
            .unwrap(),
        json!({
            "chat_id":-1001596367704_i64,"text":"created ✅","reply_parameters":{"message_id":4388,"allow_sending_without_reply":false}
        })
    );
}
#[tokio::test]
async fn errors_are_sanitized_and_requests_are_not_retried() {
    for (status, body, expected) in [
        (
            403,
            json!({"ok":false,"description":"fake_secret"}).to_string(),
            SendError::Rejected(403),
        ),
        (
            429,
            json!({"ok":false,"parameters":{"retry_after":5}}).to_string(),
            SendError::RateLimited,
        ),
        (500, "fake_secret".into(), SendError::UnknownOutcome),
        (
            200,
            "not json fake_secret".into(),
            SendError::UnknownOutcome,
        ),
        (
            200,
            json!({"ok":true}).to_string(),
            SendError::UnknownOutcome,
        ),
        (200, "x".repeat(65537), SendError::UnknownOutcome),
    ] {
        let (endpoint, task) = server(status, body, Duration::ZERO).await;
        let sender =
            TelegramSender::new("123:fake_secret", &endpoint, Duration::from_secs(1)).unwrap();
        let error = sender.send_accepted(-100, 1).await.unwrap_err();
        assert_eq!(error, expected);
        assert!(!format!("{error:?}").contains("fake_secret"));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn timeout_is_uncertain_and_redirects_are_not_followed() {
    let (endpoint, task) = server(200, "{}".into(), Duration::from_millis(100)).await;
    let sender =
        TelegramSender::new("123:fake_secret", &endpoint, Duration::from_millis(20)).unwrap();
    assert_eq!(
        sender.send_accepted(-100, 1).await,
        Err(SendError::UnknownOutcome)
    );
    task.await.unwrap();
    let (endpoint, task) = server(302, "{}".into(), Duration::ZERO).await;
    let sender = TelegramSender::new("123:fake_secret", &endpoint, Duration::from_secs(1)).unwrap();
    assert_eq!(
        sender.send_accepted(-100, 1).await,
        Err(SendError::Rejected(302))
    );
    task.await.unwrap();
}
#[tokio::test]
async fn validates_token_endpoint_and_reply_before_io() {
    for token in [
        "",
        "abc:secret",
        "123:secret/path",
        "123:",
        "123:secret?foo",
    ] {
        assert!(
            TelegramSender::new(token, "https://api.telegram.org", Duration::from_secs(1)).is_err()
        );
    }
    for endpoint in [
        "http://api.telegram.org",
        "https://evil.example",
        "http://user@127.0.0.1",
        "http://127.0.0.1/path",
    ] {
        assert!(TelegramSender::new("123:secret", endpoint, Duration::from_secs(1)).is_err());
    }
    let sender =
        TelegramSender::new("123:secret", "http://127.0.0.1:1", Duration::from_secs(1)).unwrap();
    assert_eq!(
        sender.send_accepted(-100, 0).await,
        Err(SendError::InvalidMessage)
    );
    assert_eq!(
        sender.send_accepted(0, 1).await,
        Err(SendError::InvalidMessage)
    );
}
