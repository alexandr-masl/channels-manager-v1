//! Pure target and precision rules ported from the TypeScript targets helpers.
use super::{
    profit_allocation::{self, AllocationError, Diagnostic},
    trade_builder::TradeBuildError,
};
use crate::contracts::messages::ClientTradeJob;
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
        self.format_price(price)
    }
    // Calculated prices are actual prices, never the legacy integer shorthand.
    fn format_price(&self, price: f64) -> Result<String> {
        finite(price)?;
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
    pub job: &'a ClientTradeJob,
    pub config: &'a Value,
    pub precision: &'a Precision,
    pub quantity: f64,
    pub entries: &'a [Value],
    pub pro: bool,
    pub key: &'a str,
    pub symbol: &'a str,
    pub is_long: bool,
    pub leverage: u32,
}

fn profit_entry_reference(entries: &[Value], current: f64) -> Result<f64> {
    let prices: Vec<_> = entries
        .iter()
        .filter_map(|entry| {
            positive_number(&entry["price"])
                .map(|price| (price, positive_number(&entry["fraction"])))
        })
        .collect();
    if prices.is_empty() {
        return finite(current);
    }
    let (weighted, fractions) = prices
        .iter()
        .filter_map(|(price, fraction)| fraction.map(|fraction| (price * fraction, fraction)))
        .fold((0.0, 0.0), |(sum, weights), (price, fraction)| {
            (sum + price, weights + fraction)
        });
    finite(if fractions > 0.0 {
        weighted / fractions
    } else {
        prices.iter().map(|(price, _)| price).sum::<f64>() / prices.len() as f64
    })
}

pub(super) fn exits(c: ExitContext<'_>) -> Result<(Vec<Value>, Option<Value>)> {
    let ExitContext {
        signal,
        job,
        config,
        precision,
        quantity,
        entries,
        pro,
        key,
        symbol,
        is_long,
        leverage,
    } = c;
    let prices = signal["sell_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidProfitTargets"))?;
    let entry_prices = entries
        .iter()
        .map(|e| positive(&e["price"]))
        .collect::<Result<Vec<_>>>()?;
    let high = entry_prices
        .iter()
        .copied()
        .max_by(f64::total_cmp)
        .ok_or(TradeBuildError::Rejected("missingEntryTargets"))?;
    let low = entry_prices.iter().copied().min_by(f64::total_cmp).unwrap();
    let reference = if prices.first().is_some_and(|raw| raw["type"] == "percent") {
        Some(profit_entry_reference(entries, precision.current)?)
    } else {
        None
    };
    // Resolve every supplied price before allocation may omit later targets.
    let mut resolved = Vec::new();
    let mut previous = None;
    for raw in prices {
        let price = if let Some(reference) = reference {
            let movement = positive(&raw["value"])? / (leverage as f64 * 100.0);
            let calculated = finite(
                reference
                    * if is_long {
                        1.0 + movement
                    } else {
                        1.0 - movement
                    },
            )?;
            let tolerance = f64::EPSILON * reference.abs().max(1.0) * 4.0;
            precision.format_price(calculated + tolerance)?
        } else {
            precision.price(raw)?
        };
        let n: f64 = price.parse().unwrap();
        if (is_long && n <= high) || (!is_long && n >= low) {
            return Err(TradeBuildError::Rejected("profitInsideEntryZone"));
        }
        if previous.is_some_and(|previous| {
            if is_long {
                n <= previous
            } else {
                n >= previous
            }
        }) {
            return Err(TradeBuildError::Rejected("invalidProfitTargetOrder"));
        }
        previous = Some(n);
        resolved.push(price);
    }
    let stop = if let Some(raw_stop) = signal.get("stop_loss") {
        let price = precision.price(raw_stop)?;
        let stop: f64 = price.parse().unwrap();
        if (is_long && stop > low) || (!is_long && stop < high) {
            return Err(TradeBuildError::Rejected("stopInsideEntryZone"));
        }
        precision.validate(precision.current, quantity)?;
        Some(target(key, 0, "SL", symbol, price, is_long))
    } else {
        None
    };
    if prices.is_empty() {
        return Ok((Vec::new(), stop));
    }
    let configured = config["default_sell_targets"]
        .as_array()
        .ok_or(TradeBuildError::Rejected("invalidProfitFractions"))?;
    let original_count = configured.len().min(prices.len());
    let allocation = match profit_allocation::plan(
        entries,
        &resolved,
        configured,
        &precision.wire,
        pro,
    ) {
        Ok(allocation) => allocation,
        Err(error) => {
            let (reason, diagnostic) = match &error {
                AllocationError::Minimum(d) => ("insufficientTargetBalance", Some(d)),
                AllocationError::Maximum(d) => ("orderOutsideExchangeLimits", Some(d)),
                AllocationError::Invalid(detail) => {
                    println!(
                        "Profit allocation invalid: channel_id={} message_id={} account_id={} detail={detail}",
                        job.channel_id, job.message_id, job.client["clientId"]
                    );
                    ("invalidProfitAllocation", None)
                }
            };
            if let Some(diagnostic) = diagnostic {
                log_allocation(
                    "Profit allocation rejected",
                    job,
                    original_count,
                    0,
                    diagnostic,
                );
            }
            return Err(TradeBuildError::Rejected(reason));
        }
    };
    if let Some(diagnostic) = &allocation.reduction {
        log_allocation(
            "Profit targets reduced",
            job,
            original_count,
            allocation.fractions.len(),
            diagnostic,
        );
    }
    let profits = resolved
        .into_iter()
        .zip(allocation.fractions)
        .enumerate()
        .map(|(i, (price, fraction))| {
            let mut target = target(key, i, "ST", symbol, price, is_long);
            target["fraction"] = fraction;
            target
        })
        .collect();
    Ok((profits, stop))
}

fn log_allocation(
    event: &str,
    job: &ClientTradeJob,
    original_count: usize,
    final_count: usize,
    diagnostic: &Diagnostic,
) {
    println!(
        "{event}: {}",
        json!({
            "channel_id":job.channel_id,"message_id":job.message_id,
            "account_id":job.client["clientId"],"account_name":job.client["name"],
            "symbol":job.symbol,"original_count":original_count,"final_count":final_count,
            "constraint":diagnostic.constraint,"target_index":diagnostic.target_index,
            "quantity":diagnostic.quantity,"notional":diagnostic.notional,
            "minimum":diagnostic.minimum,"maximum":diagnostic.maximum,
        })
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profit_reference_matches_original_fallbacks() {
        assert_eq!(
            profit_entry_reference(
                &[
                    json!({"price":"100","fraction":0.25}),
                    json!({"price":"120","fraction":0.75})
                ],
                90.
            )
            .unwrap(),
            115.
        );
        assert_eq!(
            profit_entry_reference(&[json!({"price":"100"}), json!({"price":"120"})], 90.).unwrap(),
            110.
        );
        assert_eq!(
            profit_entry_reference(&[json!({"price":"bad","fraction":1})], 90.).unwrap(),
            90.
        );
        assert_eq!(profit_entry_reference(&[], 90.).unwrap(), 90.);
        assert_eq!(
            profit_entry_reference(
                &[
                    json!({"price":"100","fraction":0.5}),
                    json!({"price":"120","fraction":0})
                ],
                90.
            )
            .unwrap(),
            100.
        );
    }
}
