//! Public BingX futures quotes. This adapter never sends account credentials.
use crate::redis::MetadataCache;
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::time::Duration;

pub struct MarketSnapshot {
    pub curr_price: Value,
    pub symbol_info: Value,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataError {
    Unavailable,
    InvalidResponse,
    UnsupportedSymbol,
}
pub trait MarketDataProvider: Send + Sync {
    fn snapshot<'a>(
        &'a self,
        symbol: &'a str,
    ) -> BoxFuture<'a, Result<MarketSnapshot, MarketDataError>>;
}
#[derive(Clone)]
pub struct BingxMarketData {
    client: reqwest::Client,
    cache: MetadataCache,
    endpoint: String,
}
impl BingxMarketData {
    pub fn new(cache: MetadataCache, timeout: Duration) -> Result<Self, MarketDataError> {
        Self::build(cache, timeout, "https://open-api.bingx.com".into())
    }
    /// Test harness override: only numeric loopback hosts and a bare HTTP origin.
    pub fn with_loopback_endpoint(
        cache: MetadataCache,
        timeout: Duration,
        endpoint: &str,
    ) -> Result<Self, MarketDataError> {
        let url = url::Url::parse(endpoint).map_err(|_| MarketDataError::InvalidResponse)?;
        let loopback = match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if !loopback
            || url.scheme() != "http"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(MarketDataError::InvalidResponse);
        }
        Self::build(cache, timeout, endpoint.trim_end_matches('/').into())
    }
    fn build(
        cache: MetadataCache,
        timeout: Duration,
        endpoint: String,
    ) -> Result<Self, MarketDataError> {
        if timeout.is_zero() {
            return Err(MarketDataError::InvalidResponse);
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| MarketDataError::Unavailable)?;
        Ok(Self {
            client,
            cache,
            endpoint,
        })
    }
    async fn get(&self, path: &str, symbol: &str) -> Result<Value, MarketDataError> {
        const MAX_BYTES: usize = 1_048_576;
        let mut response = self
            .client
            .get(format!("{}{path}", self.endpoint))
            .query(&[("symbol", symbol)])
            .send()
            .await
            .map_err(|_| MarketDataError::Unavailable)?;
        if !response.status().is_success() {
            return Err(MarketDataError::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BYTES as u64)
        {
            return Err(MarketDataError::InvalidResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| MarketDataError::Unavailable)?
        {
            if chunk.len() > MAX_BYTES - bytes.len() {
                return Err(MarketDataError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| MarketDataError::InvalidResponse)?;
        match number(&value["code"]) {
            Some(0.0) => Ok(value),
            Some(_) => {
                let message = value["msg"]
                    .as_str()
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                // Keep the TypeScript symbol-support classification contract.
                if [
                    "symbol not found",
                    "invalid symbol",
                    "does not exist",
                    "not available",
                ]
                .iter()
                .any(|phrase| message.contains(phrase))
                {
                    Err(MarketDataError::UnsupportedSymbol)
                } else {
                    Err(MarketDataError::Unavailable)
                }
            }
            None => Err(MarketDataError::InvalidResponse),
        }
    }
    async fn metadata(&self, symbol: &str) -> Result<Value, MarketDataError> {
        let response = self.get("/openApi/swap/v2/quote/contracts", symbol).await?;
        let contracts = response["data"]
            .as_array()
            .ok_or(MarketDataError::InvalidResponse)?;
        let mut info = contracts
            .iter()
            .find(|c| c["symbol"].as_str() == Some(symbol))
            .cloned()
            .ok_or(MarketDataError::UnsupportedSymbol)?;
        let status = number(&info["status"]).ok_or(MarketDataError::InvalidResponse)?;
        if status != 1.0 {
            return Err(MarketDataError::UnsupportedSymbol);
        }
        let tick_size = precision_step(&info["pricePrecision"])?;
        let lot_size = if truthy(&info["size"]) {
            positive(&info["size"])?
        } else {
            precision_step(&info["quantityPrecision"])?
        };
        let min_qty = if truthy(&info["tradeMinQuantity"]) {
            positive(&info["tradeMinQuantity"])?
        } else {
            lot_size
        };
        let min_notional = if truthy(&info["tradeMinUSDT"]) {
            number(&info["tradeMinUSDT"])
                .filter(|n| *n >= 0.0)
                .ok_or(MarketDataError::InvalidResponse)?
        } else {
            0.0
        };
        info["tickSize"] = tick_size.into();
        info["lotSize"] = lot_size.into();
        info["minQty"] = min_qty.into();
        info["minNotional"] = min_notional.into();
        Ok(info)
    }
}
impl MarketDataProvider for BingxMarketData {
    fn snapshot<'a>(
        &'a self,
        symbol: &'a str,
    ) -> BoxFuture<'a, Result<MarketSnapshot, MarketDataError>> {
        Box::pin(async move {
            let exchange_symbol = exchange_symbol(symbol)?;
            let symbol = exchange_symbol.as_str();
            let cached = self
                .cache
                .get_or_load(&format!("futures:{symbol}"), || self.metadata(symbol))
                .await?;
            let symbol_info = if valid_metadata(&cached, symbol) {
                cached
            } else {
                self.metadata(symbol).await?
            };
            let response = self.get("/openApi/swap/v1/ticker/price", symbol).await?;
            let curr_price = response["data"]["price"].clone();
            positive(&curr_price)?;
            if response["data"]
                .get("symbol")
                .is_some_and(|s| s.as_str() != Some(symbol))
            {
                return Err(MarketDataError::InvalidResponse);
            }
            Ok(MarketSnapshot {
                curr_price,
                symbol_info,
            })
        })
    }
}
// Parser symbols omit the futures market separator; Redis and BingX use it.
fn exchange_symbol(symbol: &str) -> Result<String, MarketDataError> {
    let base = symbol
        .strip_suffix("-USDT")
        .or_else(|| symbol.strip_suffix("USDT"))
        .ok_or(MarketDataError::UnsupportedSymbol)?;
    if base.is_empty()
        || symbol.len() > 64
        || !base
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err(MarketDataError::UnsupportedSymbol);
    }
    Ok(format!("{base}-USDT"))
}
fn number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
        .filter(|n| n.is_finite())
}
fn positive(value: &Value) -> Result<f64, MarketDataError> {
    number(value)
        .filter(|n| *n > 0.0)
        .ok_or(MarketDataError::InvalidResponse)
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::String(s) => !s.is_empty(),
        Value::Number(n) => n.as_f64() != Some(0.0),
        _ => true,
    }
}
fn precision_step(value: &Value) -> Result<f64, MarketDataError> {
    let precision = if truthy(value) {
        number(value).ok_or(MarketDataError::InvalidResponse)?
    } else {
        0.0
    };
    if precision.fract() != 0.0 || !(0.0..=18.0).contains(&precision) {
        return Err(MarketDataError::InvalidResponse);
    }
    Ok(1.0 / 10_f64.powi(precision as i32))
}
fn valid_metadata(info: &Value, symbol: &str) -> bool {
    info["symbol"].as_str() == Some(symbol)
        && number(&info["status"]) == Some(1.0)
        && ["tickSize", "lotSize", "minQty"]
            .iter()
            .all(|key| positive(&info[key]).is_ok())
        && number(&info["minNotional"]).is_some_and(|n| n >= 0.0)
        && info.get("err").is_none()
}
