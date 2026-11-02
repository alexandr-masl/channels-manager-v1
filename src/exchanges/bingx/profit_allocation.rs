//! Pure feasibility checks using the same bounded decimal cap/floor arithmetic
//! as Trading Station. Planned quantities are never published as order sizes.
use serde_json::{Value, json};
use std::{cmp::Ordering, fmt};

#[derive(Debug)]
pub(super) struct Allocation {
    pub fractions: Vec<Value>,
    pub reduction: Option<Diagnostic>,
}

/// Contains sizing values only; never account data or original job payloads.
#[derive(Debug, Clone)]
pub(super) struct Diagnostic {
    pub constraint: &'static str,
    pub target_index: usize,
    pub quantity: String,
    pub notional: String,
    pub minimum: String,
    pub maximum: String,
}

#[derive(Debug)]
pub(super) enum AllocationError {
    Minimum(Diagnostic),
    Maximum(Diagnostic),
    Invalid(&'static str),
}

type Result<T> = std::result::Result<T, AllocationError>;
const ARITHMETIC: AllocationError =
    AllocationError::Invalid("profit allocation exceeds exact arithmetic limits");

/// Nonnegative plain decimals, bounded to u128 units and 38 decimal places.
/// Unsupported representations fail rather than approximating exchange limits.
#[derive(Clone, Debug)]
struct Decimal {
    units: u128,
    scale: u32,
}
impl Decimal {
    fn zero() -> Self {
        Self { units: 0, scale: 0 }
    }
    fn parse(text: &str) -> Result<Self> {
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        if whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
            || (text.contains('.') && fraction.is_empty())
        {
            return Err(AllocationError::Invalid(
                "invalid profit allocation decimal",
            ));
        }
        let fraction = fraction.trim_end_matches('0');
        if fraction.len() > 38 {
            return Err(ARITHMETIC);
        }
        let units = whole
            .bytes()
            .chain(fraction.bytes())
            .try_fold(0u128, |n, b| {
                n.checked_mul(10)?.checked_add(u128::from(b - b'0'))
            })
            .ok_or(ARITHMETIC)?;
        Self::normalized(units, fraction.len() as u32)
    }
    fn wire(v: &Value) -> Result<Self> {
        match v {
            Value::String(s) => Self::parse(s),
            Value::Number(n) => Self::parse(&n.to_string()),
            _ => Err(AllocationError::Invalid(
                "missing profit allocation decimal",
            )),
        }
    }
    fn positive(v: &Value) -> Result<Self> {
        let n = Self::wire(v)?;
        if n.units == 0 {
            return Err(AllocationError::Invalid(
                "profit allocation requires positive values",
            ));
        }
        Ok(n)
    }
    fn normalized(mut units: u128, mut scale: u32) -> Result<Self> {
        while scale > 0 && units.is_multiple_of(10) {
            units /= 10;
            scale -= 1;
        }
        if scale > 38 {
            return Err(ARITHMETIC);
        }
        Ok(Self { units, scale })
    }
    fn at(&self, scale: u32) -> Result<u128> {
        self.units
            .checked_mul(10u128.checked_pow(scale - self.scale).ok_or(ARITHMETIC)?)
            .ok_or(ARITHMETIC)
    }
    fn cmp(&self, b: &Self) -> Result<Ordering> {
        let s = self.scale.max(b.scale);
        Ok(self.at(s)?.cmp(&b.at(s)?))
    }
    fn add(&self, b: &Self) -> Result<Self> {
        let s = self.scale.max(b.scale);
        Self::normalized(self.at(s)?.checked_add(b.at(s)?).ok_or(ARITHMETIC)?, s)
    }
    fn sub(&self, b: &Self) -> Result<Self> {
        let s = self.scale.max(b.scale);
        Self::normalized(self.at(s)?.checked_sub(b.at(s)?).ok_or(ARITHMETIC)?, s)
    }
    fn mul(&self, b: &Self) -> Result<Self> {
        Self::normalized(
            self.units.checked_mul(b.units).ok_or(ARITHMETIC)?,
            self.scale + b.scale,
        )
    }
    fn allocate(&self, fraction: &Self, remaining: &Self, lot: &Self) -> Result<Self> {
        let requested = self.mul(fraction)?;
        let scale = requested.scale.max(remaining.scale).max(lot.scale);
        let capped = requested.at(scale)?.min(remaining.at(scale)?);
        let lot_units = lot.at(scale)?;
        Self::normalized(capped / lot_units * lot_units, scale)
    }
}
impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut text = format!("{:0width$}", self.units, width = self.scale as usize + 1);
        if self.scale > 0 {
            text.insert(text.len() - self.scale as usize, '.');
        }
        f.write_str(&text)
    }
}

struct Limits {
    min: Decimal,
    max: Decimal,
    notional: Decimal,
    lot: Decimal,
}
impl Limits {
    fn diagnostic(
        &self,
        constraint: &'static str,
        i: usize,
        q: &Decimal,
        n: &Decimal,
        min: &Decimal,
    ) -> Diagnostic {
        Diagnostic {
            constraint,
            target_index: i,
            quantity: q.to_string(),
            notional: n.to_string(),
            minimum: min.to_string(),
            maximum: self.max.to_string(),
        }
    }
}

fn equal_fractions(count: usize) -> Vec<Value> {
    let percent = 100 / count;
    (0..count)
        .map(|i| {
            let weight = if i + 1 == count {
                100 - percent * (count - 1)
            } else {
                percent
            };
            json!(weight as f64 / 100.0)
        })
        .collect()
}

fn check_candidate(
    total: &Decimal,
    entry_notional: &Decimal,
    prices: &[Decimal],
    fractions: &[Value],
    limits: &Limits,
    pro: bool,
) -> Result<()> {
    let mut remaining = total.clone();
    let mut minimum = None;
    let mut maximum = None;
    for (i, (price, fraction)) in prices.iter().zip(fractions).enumerate() {
        let quantity = total.allocate(&Decimal::wire(fraction)?, &remaining, &limits.lot)?;
        remaining = remaining.sub(&quantity)?;
        let notional = quantity.mul(price)?;
        if quantity.cmp(&limits.max)? == Ordering::Greater {
            maximum.get_or_insert_with(|| {
                limits.diagnostic("maxQty", i, &quantity, &notional, &limits.min)
            });
        }
        if quantity.units == 0 || quantity.cmp(&limits.min)? == Ordering::Less {
            minimum.get_or_insert_with(|| {
                limits.diagnostic("minQty", i, &quantity, &notional, &limits.min)
            });
        } else if notional.cmp(&limits.notional)? == Ordering::Less {
            minimum.get_or_insert_with(|| {
                limits.diagnostic("minNotional", i, &quantity, &notional, &limits.notional)
            });
        }
    }
    if pro && fractions.len() == 1 {
        let multiplier = Decimal::parse("3.5")?;
        let min_qty = limits.min.mul(&multiplier)?;
        let min_notional = limits.notional.mul(&multiplier)?;
        if total.cmp(&min_qty)? == Ordering::Less {
            minimum.get_or_insert_with(|| {
                limits.diagnostic("proMinQty", 0, total, entry_notional, &min_qty)
            });
        } else if entry_notional.cmp(&min_notional)? == Ordering::Less {
            minimum.get_or_insert_with(|| {
                limits.diagnostic("proMinNotional", 0, total, entry_notional, &min_notional)
            });
        }
    }
    if let Some(d) = maximum {
        return Err(AllocationError::Maximum(d));
    }
    if let Some(d) = minimum {
        return Err(AllocationError::Minimum(d));
    }
    Ok(())
}

pub(super) fn plan(
    entries: &[Value],
    prices: &[String],
    configured: &[Value],
    precisions: &Value,
    pro: bool,
) -> Result<Allocation> {
    if entries.is_empty() || prices.is_empty() || configured.is_empty() {
        return Err(AllocationError::Invalid("empty profit allocation inputs"));
    }
    let limits = Limits {
        min: Decimal::positive(&precisions["minQty"])?,
        max: Decimal::positive(&precisions["maxQty"])?,
        notional: Decimal::positive(&precisions["minNotional"])?,
        lot: Decimal::positive(&precisions["lotSize"])?,
    };
    if limits.min.cmp(&limits.max)? == Ordering::Greater {
        return Err(AllocationError::Invalid(
            "inconsistent profit quantity limits",
        ));
    }
    // Validate all inputs before fallback can omit any configured targets.
    let one = Decimal::parse("1")?;
    for config in configured {
        let fraction = Decimal::positive(&config["fraction"])?;
        if fraction.cmp(&one)? == Ordering::Greater {
            return Err(AllocationError::Invalid("profit fraction exceeds one"));
        }
    }
    let prices = prices
        .iter()
        .map(|p| {
            let n = Decimal::parse(p)?;
            if n.units == 0 {
                return Err(AllocationError::Invalid("profit price must be positive"));
            }
            Ok(n)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut total = Decimal::zero();
    let mut entry_notional = Decimal::zero();
    for entry in entries {
        let quantity = Decimal::positive(&entry["quantity"])?;
        let price = Decimal::positive(&entry["price"])?;
        total = total.add(&quantity)?;
        entry_notional = entry_notional.add(&quantity.mul(&price)?)?;
    }
    let count = configured.len().min(prices.len());
    let mut first_failure = None;
    let mut max_failure = None;
    let mut last_minimum = None;
    for n in (1..=count).rev() {
        let fractions = if n == 1 || n < count || prices.len() < configured.len() {
            equal_fractions(n)
        } else {
            configured
                .iter()
                .take(n)
                .map(|c| c["fraction"].clone())
                .collect()
        };
        match check_candidate(
            &total,
            &entry_notional,
            &prices[..n],
            &fractions,
            &limits,
            pro,
        ) {
            Ok(()) => {
                return Ok(Allocation {
                    fractions,
                    reduction: first_failure,
                });
            }
            Err(AllocationError::Minimum(d)) => {
                first_failure.get_or_insert_with(|| d.clone());
                last_minimum = Some(d);
            }
            Err(AllocationError::Maximum(d)) => {
                first_failure.get_or_insert_with(|| d.clone());
                max_failure = Some(d);
            }
            Err(e) => return Err(e),
        }
    }
    if let Some(d) = max_failure {
        return Err(AllocationError::Maximum(d));
    }
    Err(AllocationError::Minimum(
        last_minimum.expect("nonempty candidates"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn metadata() -> Value {
        json!({"minQty":"0.01","maxQty":"100","minNotional":"1","lotSize":"0.01"})
    }
    fn configured() -> Vec<Value> {
        [0.4, 0.3, 0.2, 0.1].map(|f| json!({"fraction":f})).to_vec()
    }
    fn prices() -> Vec<String> {
        ["1000", "1100", "1200", "1300"].map(str::to_owned).to_vec()
    }
    fn entry(q: &str) -> Vec<Value> {
        vec![json!({"quantity":q,"price":"1000"})]
    }
    #[test]
    fn am3_selects_largest_executable_target_count() {
        for (q, count) in [
            ("0.01", 1),
            ("0.02", 2),
            ("0.03", 2),
            ("0.04", 3),
            ("0.05", 3),
            ("0.06", 3),
            ("0.07", 3),
            ("0.08", 3),
            ("0.09", 3),
            ("0.10", 4),
        ] {
            let result = plan(&entry(q), &prices(), &configured(), &metadata(), false).unwrap();
            assert_eq!(result.fractions.len(), count, "quantity {q}");
            assert_eq!(result.reduction.is_some(), count < 4);
        }
    }
    #[test]
    fn preserves_configured_wire_and_count_ceiling() {
        let c = vec![json!({"fraction":"0.40"}), json!({"fraction":0.6})];
        let a = plan(&entry("0.10"), &prices(), &c, &metadata(), false).unwrap();
        assert_eq!(a.fractions, vec![json!("0.40"), json!(0.6)]);
        assert!(a.reduction.is_none());
        let a = plan(&entry("0.10"), &prices(), &c[..1], &metadata(), false).unwrap();
        assert_eq!(a.fractions, vec![json!(1.0)]);
        let a = plan(
            &entry("0.10"),
            &prices()[..3],
            &configured(),
            &metadata(),
            false,
        )
        .unwrap();
        assert_eq!(a.fractions, vec![json!(0.33), json!(0.33), json!(0.34)]);
    }
    #[test]
    fn exact_entry_sum_and_remainder_caps() {
        let entries = vec![
            json!({"quantity":"0.01","price":"1000"}),
            json!({"quantity":"0.06","price":"1000"}),
        ];
        let mut m = metadata();
        m["minQty"] = json!("0.07");
        assert!(plan(&entries, &prices(), &configured()[..1], &m, false).is_ok());
        let c = vec![json!({"fraction":0.8}), json!({"fraction":0.8})];
        assert!(plan(&entry("0.05"), &prices(), &c, &metadata(), false).is_ok());
    }
    #[test]
    fn target_notional_and_effective_quantity_are_both_required() {
        let mut m = metadata();
        m["minNotional"] = json!("10");
        assert!(matches!(
            plan(
                &entry("0.01"),
                &["900".into()],
                &configured()[..1],
                &m,
                false
            ),
            Err(AllocationError::Minimum(_))
        ));
        assert!(
            plan(
                &entry("0.01"),
                &["1100".into()],
                &configured()[..1],
                &m,
                false
            )
            .is_ok()
        );
        m["minQty"] = json!("0.02");
        assert!(matches!(
            plan(
                &entry("0.01"),
                &["10000".into()],
                &configured()[..1],
                &m,
                false
            ),
            Err(AllocationError::Minimum(_))
        ));
    }
    #[test]
    fn pro_thresholds_are_exact_and_independent() {
        let mut m = metadata();
        m["minNotional"] = json!("10");
        m["lotSize"] = json!("0.001");
        let c = &configured()[..1];
        assert!(plan(&entry("0.02"), &prices(), c, &m, false).is_ok());
        assert!(matches!(
            plan(&entry("0.02"), &prices(), c, &m, true),
            Err(AllocationError::Minimum(_))
        ));
        assert!(plan(&entry("0.035"), &prices(), c, &m, true).is_ok());
        assert!(matches!(
            plan(
                &[json!({"quantity":"0.034","price":"2000"})],
                &prices(),
                c,
                &m,
                true
            ),
            Err(AllocationError::Minimum(_))
        ));
        assert!(matches!(
            plan(
                &[json!({"quantity":"0.035","price":"999"})],
                &prices(),
                c,
                &m,
                true
            ),
            Err(AllocationError::Minimum(_))
        ));
    }
    #[test]
    fn invalid_and_maximum_failures_remain_internal() {
        let mut m = metadata();
        m["maxQty"] = json!("0.005");
        m["minQty"] = json!("0.001");
        assert!(matches!(
            plan(&entry("0.10"), &prices(), &configured(), &m, false),
            Err(AllocationError::Maximum(_))
        ));
        for bad in [
            json!(0),
            json!(-0.1),
            json!(1.1),
            json!("1e-2"),
            json!(null),
        ] {
            let c = vec![json!({"fraction":bad})];
            assert!(matches!(
                plan(&entry("0.10"), &prices(), &c, &metadata(), false),
                Err(AllocationError::Invalid(_))
            ));
        }
        let mut m = metadata();
        m["lotSize"] = json!(0);
        assert!(matches!(
            plan(&entry("0.10"), &prices(), &configured(), &m, false),
            Err(AllocationError::Invalid(_))
        ));
        assert!(matches!(
            plan(
                &entry("340282366920938463463374607431768211456"),
                &prices(),
                &configured(),
                &metadata(),
                false
            ),
            Err(AllocationError::Invalid(_))
        ));
        assert!(matches!(
            plan(
                &entry("0.000000000000000000000000000000000000001"),
                &prices(),
                &configured(),
                &metadata(),
                false
            ),
            Err(AllocationError::Invalid(_))
        ));
    }
    #[test]
    fn increasing_basic_positions_remain_executable() {
        for q in 1..=100 {
            assert!(
                plan(
                    &entry(&format!("{}.{:02}", q / 100, q % 100)),
                    &prices(),
                    &configured(),
                    &metadata(),
                    false
                )
                .is_ok()
            );
        }
    }

    fn simulated(total: &str, fractions: &[&str]) -> (Vec<String>, String) {
        let total = Decimal::parse(total).unwrap();
        let lot = Decimal::parse("0.01").unwrap();
        let mut remaining = total.clone();
        let quantities = fractions
            .iter()
            .map(|f| {
                let q = total
                    .allocate(&Decimal::parse(f).unwrap(), &remaining, &lot)
                    .unwrap();
                remaining = remaining.sub(&q).unwrap();
                q.to_string()
            })
            .collect();
        (quantities, remaining.to_string())
    }
    #[test]
    fn allocation_matches_trading_station_floor_cap_and_remainder() {
        assert_eq!(
            simulated("0.06", &["0.33", "0.33", "0.34"]),
            (
                vec!["0.01".into(), "0.01".into(), "0.02".into()],
                "0.02".into()
            )
        );
        assert_eq!(
            simulated("0.03", &["0.5", "0.5"]),
            (vec!["0.01".into(), "0.01".into()], "0.01".into())
        );
        assert_eq!(
            simulated("0.05", &["0.8", "0.8"]),
            (vec!["0.04".into(), "0.01".into()], "0".into())
        );
        assert_eq!(simulated("0.07", &["1"]), (vec!["0.07".into()], "0".into()));
    }
    #[test]
    fn no_epsilon_promotes_below_minimum_and_diagnostics_are_bounded() {
        let e = plan(
            &entry("0.009999999999999999"),
            &prices(),
            &configured()[..1],
            &metadata(),
            false,
        )
        .unwrap_err();
        let AllocationError::Minimum(d) = e else {
            panic!("expected minimum")
        };
        assert_eq!(d.constraint, "minQty");
        assert_eq!(d.target_index, 0);
        assert_eq!(d.quantity, "0");
        assert_eq!(d.notional, "0");
        assert_eq!(d.minimum, "0.01");
        assert_eq!(d.maximum, "100");
    }
    #[test]
    fn arithmetic_overflow_and_omitted_invalid_inputs_are_internal() {
        let huge = vec![json!({"quantity":"340282366920938463463374607431768211455","price":"2"})];
        assert!(matches!(
            plan(&huge, &prices(), &configured(), &metadata(), false),
            Err(AllocationError::Invalid(_))
        ));
        let c = vec![json!({"fraction":1}), json!({"fraction":"bad"})];
        assert!(matches!(
            plan(&entry("0.1"), &prices()[..1], &c, &metadata(), false),
            Err(AllocationError::Invalid(_))
        ));
        let mut p = prices();
        p[3] = "bad".into();
        assert!(matches!(
            plan(&entry("0.1"), &p, &configured()[..1], &metadata(), false),
            Err(AllocationError::Invalid(_))
        ));
    }
    #[test]
    fn maximum_in_later_target_is_not_hidden_by_earlier_minimum() {
        let mut m = metadata();
        m["maxQty"] = json!("0.05");
        let c = vec![json!({"fraction":0.01}), json!({"fraction":0.99})];
        assert!(matches!(
            plan(&entry("0.1"), &prices(), &c, &m, false),
            Err(AllocationError::Maximum(_))
        ));
    }
}
