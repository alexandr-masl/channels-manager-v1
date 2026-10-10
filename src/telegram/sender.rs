//! Outbound-only Bot API client. Never polls updates or changes webhooks.
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    InvalidConfiguration,
    InvalidMessage,
    Rejected(u16),
    RateLimited,
    UnknownOutcome,
}
// URL contains the token: never derive Debug or expose reqwest errors.
pub struct TelegramSender {
    client: reqwest::Client,
    url: String,
}
pub(crate) fn valid_token(token: &str) -> bool {
    token.split_once(':').is_some_and(|(id, secret)| {
        !id.is_empty()
            && id.bytes().all(|b| b.is_ascii_digit())
            && !secret.is_empty()
            && secret
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    })
}
pub(crate) fn valid_endpoint(endpoint: &str) -> bool {
    endpoint == "https://api.telegram.org"
        || url::Url::parse(endpoint).is_ok_and(|url| {
            let loopback = match url.host() {
                Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                _ => false,
            };
            loopback
                && url.scheme() == "http"
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none()
        })
}
impl TelegramSender {
    /// Production uses Telegram HTTPS; numeric HTTP loopback is for isolated tests.
    pub fn new(token: &str, endpoint: &str, timeout: Duration) -> Result<Self, SendError> {
        if !valid_token(token) || !valid_endpoint(endpoint) || timeout.is_zero() {
            return Err(SendError::InvalidConfiguration);
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| SendError::InvalidConfiguration)?;
        Ok(Self {
            client,
            url: format!("{}/bot{token}/sendMessage", endpoint.trim_end_matches('/')),
        })
    }
    /// One attempt only. Unknown outcomes may already have delivered a message.
    pub async fn send_accepted(&self, channel_id: i64, message_id: i64) -> Result<i64, SendError> {
        if channel_id >= 0 || message_id <= 0 || message_id > i32::MAX as i64 {
            return Err(SendError::InvalidMessage);
        }
        let body = json!({"chat_id":channel_id,"text":"created ✅",
            "reply_parameters":{"message_id":message_id,"allow_sending_without_reply":false}})
        .to_string();
        let mut response = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| SendError::UnknownOutcome)?;
        let status = response.status();
        if status.as_u16() == 429 {
            return Err(SendError::RateLimited);
        }
        if status.is_server_error() {
            return Err(SendError::UnknownOutcome);
        }
        if !status.is_success() {
            return Err(SendError::Rejected(status.as_u16()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| SendError::UnknownOutcome)?
        {
            if body.len() + chunk.len() > 65536 {
                return Err(SendError::UnknownOutcome);
            }
            body.extend_from_slice(&chunk);
        }
        let response: Value =
            serde_json::from_slice(&body).map_err(|_| SendError::UnknownOutcome)?;
        if response["ok"] != true {
            return Err(SendError::UnknownOutcome);
        }
        response["result"]["message_id"]
            .as_i64()
            .filter(|id| *id > 0)
            .ok_or(SendError::UnknownOutcome)
    }
}
