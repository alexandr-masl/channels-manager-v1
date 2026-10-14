//! Signed account reads and Hedge mode switching. Errors contain static codes, never URLs or provider bodies.
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadError {
    pub code: &'static str,
    pub retryable: bool,
}
impl ReadError {
    fn invalid() -> Self {
        Self {
            code: "invalidExchangeResponse",
            retryable: false,
        }
    }
    fn transport() -> Self {
        Self {
            code: "exchangeReadUnavailable",
            retryable: true,
        }
    }
}
#[derive(Clone)]
pub struct BingxReadClient {
    client: reqwest::Client,
    base: String,
}
#[derive(Debug, Clone)]
pub struct AccountEvidence {
    pub mode: Value,
    pub positions: Value,
    pub orders: Value,
    pub balance: Value,
    pub leverage: Value,
}
impl BingxReadClient {
    pub fn new(base: &str, timeout: Duration) -> Result<Self, ReadError> {
        let url = url::Url::parse(base).map_err(|_| ReadError::invalid())?;
        let local = match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        let official = url.scheme() == "https"
            && url.host_str() == Some("open-api.bingx.com")
            && url.port_or_known_default() == Some(443);
        if !(official || local && url.scheme() == "http")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || timeout.is_zero()
        {
            return Err(ReadError::invalid());
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| ReadError::transport())?;
        Ok(Self {
            client,
            base: base.trim_end_matches('/').to_owned(),
        })
    }
    pub async fn read_account(
        &self,
        api_key: &str,
        secret: &str,
        symbol: &str,
    ) -> Result<AccountEvidence, ReadError> {
        if !super::admission::valid_symbol(symbol) || api_key.is_empty() || secret.is_empty() {
            return Err(ReadError::invalid());
        }
        let (mode, positions, orders, balance, leverage) = tokio::try_join!(
            self.get("/openApi/swap/v1/positionSide/dual", api_key, secret, None),
            self.get("/openApi/swap/v2/user/positions", api_key, secret, None),
            self.get("/openApi/swap/v2/trade/openOrders", api_key, secret, None),
            self.get("/openApi/swap/v3/user/balance", api_key, secret, None),
            self.get(
                "/openApi/swap/v2/trade/leverage",
                api_key,
                secret,
                Some(symbol)
            )
        )?;
        Ok(AccountEvidence {
            mode,
            positions,
            orders,
            balance,
            leverage,
        })
    }
    async fn get(
        &self,
        path: &str,
        key: &str,
        secret: &str,
        symbol: Option<&str>,
    ) -> Result<Value, ReadError> {
        let parameters = symbol.map(|s| format!("symbol={s}&")).unwrap_or_default();
        self.request(reqwest::Method::GET, path, key, secret, &parameters)
            .await
    }
    pub async fn switch_to_hedge(&self, key: &str, secret: &str) -> Result<(), ReadError> {
        let result = self
            .request(
                reqwest::Method::POST,
                "/openApi/swap/v1/positionSide/dual",
                key,
                secret,
                "dualSidePosition=true&",
            )
            .await;
        match result {
            Ok(data) if data.is_object() => Ok(()),
            _ => Err(ReadError {
                code: "modeSwitchFailed",
                retryable: false,
            }),
        }
    }
    pub async fn set_leverage(
        &self,
        key: &str,
        secret: &str,
        symbol: &str,
        leverage: u32,
        side: &str,
    ) -> Result<(), ReadError> {
        let failure = || ReadError {
            code: "leverageChangeFailed",
            retryable: false,
        };
        if !super::admission::valid_symbol(symbol)
            || leverage == 0
            || !matches!(side, "LONG" | "SHORT" | "BOTH")
        {
            return Err(failure());
        }
        let data = self
            .request(
                reqwest::Method::POST,
                "/openApi/swap/v2/trade/leverage",
                key,
                secret,
                &format!("symbol={symbol}&leverage={leverage}&side={side}&"),
            )
            .await
            .map_err(|_| failure())?;
        let returned = data["leverage"]
            .as_u64()
            .or_else(|| data["leverage"].as_str()?.parse().ok());
        if data["symbol"].as_str() != Some(symbol) || returned != Some(leverage as u64) {
            return Err(failure());
        }
        Ok(())
    }
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        key: &str,
        secret: &str,
        parameters: &str,
    ) -> Result<Value, ReadError> {
        if key.is_empty() || secret.is_empty() {
            return Err(ReadError::invalid());
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ReadError::invalid())?
            .as_millis();
        let query = format!("{parameters}recvWindow=5000&timestamp={timestamp}");
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| ReadError::invalid())?;
        mac.update(query.as_bytes());
        let signature = format!("{:x}", mac.finalize().into_bytes());
        let mut key =
            reqwest::header::HeaderValue::from_str(key).map_err(|_| ReadError::invalid())?;
        key.set_sensitive(true);
        let mut response = self
            .client
            .request(
                method,
                format!("{}{path}?{query}&signature={signature}", self.base),
            )
            .header("X-BX-APIKEY", key)
            .send()
            .await
            .map_err(|_| ReadError::transport())?;
        let status = response.status();
        if !status.is_success() {
            return Err(ReadError {
                code: "exchangeReadRejected",
                retryable: status.is_server_error()
                    || status.as_u16() == 429
                    || status.as_u16() == 408,
            });
        }
        const MAX: usize = 1_048_576;
        if response.content_length().is_some_and(|n| n > MAX as u64) {
            return Err(ReadError::invalid());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ReadError::transport())? {
            if chunk.len() > MAX - bytes.len() {
                return Err(ReadError::invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(ReadError::transport());
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| ReadError::invalid())?;
        if body.get("err").is_some() {
            return Err(ReadError::invalid());
        }
        match body["code"].as_i64() {
            Some(0) => body
                .get("data")
                .filter(|v| !v.is_null())
                .cloned()
                .ok_or_else(ReadError::invalid),
            Some(code) => Err(ReadError {
                code: "exchangeReadRejected",
                retryable: matches!(code, 429 | 100410 | 100500 | 100503),
            }),
            None => Err(ReadError::invalid()),
        }
    }
}
