//! Pure target and precision rules ported from the TypeScript targets helpers.
use super::trade_builder::TradeBuildError;
use crate::{contracts::messages::trade_creation_id, signals::settings::positive_number};
use serde_json::{Value, json};
type Result<T> = std::result::Result<T, TradeBuildError>;
pub(super) fn positive(value: &Value) -> Result<f64> {
    positive_number(value).ok_or(TradeBuildError::Rejected("invalidTradeNumber"))
}
pub(super) fn finite(value: f64) -> Result<f64> {
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(TradeBuildError::Rejected("invalidCalculatedAmount"))
    }
}
fn spelling(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
fn decimal_places(s: &str) -> Result<usize> {
    let lower = s.to_ascii_lowercase();
    let (coefficient, exponent) = lower.split_once('e').unwrap_or((&lower, "0"));
    let exponent: i32 = exponent
        .parse()
        .map_err(|_| TradeBuildError::Rejected("invalidPrecision"))?;
    let digits = coefficient.split_once('.').map_or(0, |(_, d)| d.len()) as i32;
    let digits = (digits - exponent).max(0);
    if digits > 18 {
        return Err(TradeBuildError::Rejected("unsupportedPrecision"));
    }
    Ok(digits as usize)
}
pub(super) struct Precision {
    pub wire: Value,
    pub current: f64,
    pub min_qty: f64,
    pub min_notional: f64,
    max_qty: f64,
    lot: f64,
    lot_digits: usize,
    price_digits: usize,
}
impl Precision {
    pub fn new(info: &Value, current: &Value) -> Result<Self> {
        let price = positive(current)?;
        let min_notional = positive(&info["minNotional"])?;
        let min_qty = finite((min_notional / price).max(positive(&info["minQty"])?))?;
        let max_qty = finite(
            positive_number(&info["maxQty"]).map_or(100000.0 / price, |n| n.min(100000.0 / price)),
        )?;
        let lot = positive(&info["lotSize"])?;
        let current_text = spelling(current);
        // Original prices preserve the current quote's decimal width. Integer JSON
        // quotes have no width; use the exchange tick's width in that case.
        let price_digits = if current_text.contains('.') || current_text.contains(['e', 'E']) {
            decimal_places(&current_text)?
        } else {
            decimal_places(&spelling(&info["tickSize"]))?
        };
        Ok(Self {
            wire: json!({"symbol":info["symbol"],"priceRangeUp":5,"priceRangeDown":0.5,"tickSize":info["tickSize"],"minQty":min_qty,"maxQty":max_qty,"minNotional":min_notional,"lotSize":info["lotSize"]}),
            current: price,
            min_qty,
            min_notional,
            max_qty,
            lot,
            lot_digits: decimal_places(&lot.to_string())?,
            price_digits,
        })
    }
    pub fn price(&self, raw: &Value) -> Result<String> {
        let mut price = positive(raw)?;
        if price.fract() == 0.0 && self.current < 1.0 {
            let reference = format!("{:.18}", self.current);
            let fraction = reference.split_once('.').unwrap().1;
            let zeros = fraction.bytes().take_while(|c| *c == b'0').count();
            let digits = format!("{price:.0}");
            price = format!("0.{}{digits}", "0".repeat(zeros))
                .parse()
                .map_err(|_| TradeBuildError::Rejected("invalidPrice"))?;
        }
        let factor = 10_f64.powi(self.price_digits as i32);
        let truncated = (price * factor).floor() / factor;
        finite(truncated)?;
        Ok(format!("{:.*}", self.price_digits, truncated))
    }
    pub fn quantity(&self, raw: f64) -> Result<f64> {
        finite(raw)?;
        let ratio = raw / self.lot;
        let tolerance = f64::EPSILON * ratio.abs().max(1.0) * 4.0;
        let rounded = (ratio + tolerance).floor() * self.lot;
        let result = format!("{:.*}", self.lot_digits, rounded)
            .parse()
            .map_err(|_| TradeBuildError::Rejected("invalidQuantity"))?;
        finite(result)
    }
    pub fn validate(&self, price: f64, quantity: f64) -> Result<()> {
        finite(quantity)?;
        let notional = finite(price * quantity)?;
        if quantity < self.min_qty || quantity > self.max_qty || notional < self.min_notional {
            return Err(TradeBuildError::Rejected("orderOutsideExchangeLimits"));
        }
        Ok(())
    }
    pub fn minimum(&self, sells: usize, buys: usize, pro: bool) -> (f64, f64) {
        let mut factor = buys as f64 * 1.05;
        if pro && sells == 1 {
            factor = 3.5;
        }
        if sells > buys {
            factor = sells as f64 * 1.05;
        }
        (self.min_qty * factor, self.min_notional * factor)
    }
}
fn target(
    key: &str,
    index: usize,
    role: &str,
    symbol: &str,
    price: String,
    is_long: bool,
) -> Value {
    json!({"price":price,"symbol":symbol,"clientOrderId":trade_creation_id(&format!("{key}:{role}:{index}")),"opened":true,"status":"new","role":role,"side":if (role=="BT")==is_long {"BUY"}else{"SELL"},"type":if role=="SL"{"STOP_LOSS_LIMIT"}else{"LIMIT"}})
}
pub(super) fn entries(
    signal: &Value,
    config: &Value,
    precision: &Precision,
    quantity: f64,
    key: &str,
    symbol: &str,
    is_long: bool,
) -> Result<Vec<Value>> {
    let mut prices = signal["buy_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidEntryTargets"))?
        .iter()
        .map(positive)
        .collect::<Result<Vec<_>>>()?;
    prices.sort_by(f64::total_cmp);
    let low = *prices
        .first()
        .ok_or(TradeBuildError::Rejected("missingEntryTargets"))?;
    let high = *prices.last().unwrap();
    let fractions = config["default_buy_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidEntryFractions"))?;
    let definitions = if prices.len() > 1 && matches!(fractions.len(), 2 | 3) {
        let mut values = vec![
            (low, fractions[0]["fraction"].clone()),
            (high, fractions[1]["fraction"].clone()),
        ];
        if fractions.len() == 3 {
            let range: f64 = format!("{:.6}", high - low)
                .parse()
                .map_err(|_| TradeBuildError::Rejected("invalidEntryRange"))?;
            values.push((high - range / 2.0, fractions[2]["fraction"].clone()));
        }
        values
    } else {
        vec![(if is_long { high } else { low }, json!(1))]
    };
    definitions
        .into_iter()
        .enumerate()
        .map(|(i, (price, fraction))| {
            let price = precision.price(&json!(price))?;
            let qty = precision.quantity(quantity * positive(&fraction)?)?;
            precision.validate(price.parse().unwrap(), qty)?;
            let mut t = target(key, i, "BT", symbol, price, is_long);
            t["quantity"] = json!(qty);
            t["fraction"] = json!(fraction);
            if signal["breakOutEntry"] == true {
                t["type"] = json!("STOP_LOSS_LIMIT");
            }
            Ok(t)
        })
        .collect()
}
pub(super) struct ExitContext<'a> {
    pub signal: &'a Value,
    pub config: &'a Value,
    pub precision: &'a Precision,
    pub quantity: f64,
    pub notional: f64,
    pub entries: &'a [Value],
    pub pro: bool,
    pub key: &'a str,
    pub symbol: &'a str,
    pub is_long: bool,
}
pub(super) fn exits(c: ExitContext<'_>) -> Result<(Vec<Value>, Value)> {
    let ExitContext {
        signal,
        config,
        precision,
        quantity,
        notional,
        entries,
        pro,
        key,
        symbol,
        is_long,
    } = c;
    let prices = signal["sell_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidProfitTargets"))?;
    let buys = signal["buy_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidEntryTargets"))?
        .len();
    let configured = config["default_sell_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidProfitFractions"))?;
    let enough = |sells| {
        let (q, n) = precision.minimum(sells, buys, pro);
        quantity >= q && notional >= n
    };
    let reduced = if enough(prices.len()) {
        None
    } else {
        // Recheck entry-only minimums when no reduced target count fits, just
        // like TypeScript. Its single-signal-target branch still creates one TP.
        let count = (1..prices.len()).rev().find(|n| enough(*n)).unwrap_or(0);
        if !enough(count) {
            return Err(TradeBuildError::Rejected("insufficientTargetBalance"));
        }
        Some(count)
    };
    let count = if prices.len() == 1 {
        1
    } else {
        reduced.unwrap_or(configured.len()).min(prices.len())
    };
    if count == 0 {
        return Err(TradeBuildError::Rejected("missingProfitTargets"));
    }
    let equal = reduced.is_some() || configured.len() > prices.len();
    let high = entries
        .iter()
        .map(|e| positive(&e["price"]))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max_by(f64::total_cmp)
        .ok_or(TradeBuildError::Rejected("missingEntryTargets"))?;
    let mut profits = Vec::new();
    for (i, raw) in prices.iter().take(count).enumerate() {
        let fraction = if count == 1 {
            1.0
        } else if equal {
            let base = 100 / count;
            if i == count - 1 {
                (100 - base * (count - 1)) as f64 / 100.0
            } else {
                base as f64 / 100.0
            }
        } else {
            positive(&configured[i]["fraction"])?
        };
        let price = precision.price(raw)?;
        let n: f64 = price.parse().unwrap();
        precision.validate(precision.current, quantity * fraction)?;
        if (is_long && n <= high) || (!is_long && n >= high) {
            return Err(TradeBuildError::Rejected("profitInsideEntryZone"));
        }
        let mut t = target(key, i, "ST", symbol, price, is_long);
        t["fraction"] = if count == 1 || equal {
            json!(fraction)
        } else {
            configured[i]["fraction"].clone()
        };
        profits.push(t);
    }
    let stop_price = precision.price(&signal["stop_loss"])?;
    let stop: f64 = stop_price.parse().unwrap();
    let low = entries
        .iter()
        .map(|e| positive(&e["price"]))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .min_by(f64::total_cmp)
        .unwrap();
    if (is_long && stop > low) || (!is_long && stop < high) {
        return Err(TradeBuildError::Rejected("stopInsideEntryZone"));
    }
    precision.validate(precision.current, quantity)?;
    Ok((profits, target(key, 0, "SL", symbol, stop_price, is_long)))
}
