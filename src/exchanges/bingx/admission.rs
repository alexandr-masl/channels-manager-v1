//! Pure position-mode decisions and balance/leverage admission.
use super::client::AccountEvidence;
use crate::contracts::messages::ExecutionRoute;
use serde_json::Value;
use std::collections::HashSet;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionRejection {
    pub code: &'static str,
}
#[derive(Debug, Clone)]
pub struct AdmittedAccount {
    pub position_mode: &'static str,
    pub position_configuration: ExecutionRoute,
    pub available_balance: f64,
    pub available_balance_raw: Value,
}
impl AdmittedAccount {
    /// Carry admission's route into the eventual trade payload.
    pub fn apply_to_trade_object(&self, trade: &mut serde_json::Map<String, Value>) {
        trade.insert(
            "positionConfiguration".into(),
            Value::String(self.position_configuration.as_str().into()),
        );
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeDecision {
    Hedge,
    LegacyOneWay,
    MigrateToHedge,
}
struct ModeEvidence {
    decision: ModeDecision,
    owners: Vec<(String, bool, u64)>,
}
pub fn evaluate_position_mode(
    account: &AccountEvidence,
    managed: &[Value],
    exchange_client_id: &str,
    requested_symbol: &str,
) -> Result<ModeDecision, AdmissionRejection> {
    mode_evidence(account, managed, exchange_client_id, requested_symbol).map(|e| e.decision)
}
fn reject(code: &'static str) -> AdmissionRejection {
    AdmissionRejection { code }
}
pub(crate) fn valid_symbol(s: &str) -> bool {
    let mut p = s.split('-');
    matches!((p.next(),p.next(),p.next()),(Some(a),Some(b),None) if !a.is_empty() && !b.is_empty() && a.bytes().chain(b.bytes()).all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()))
}
fn symbol(v: &Value) -> Option<String> {
    let s = v.as_str()?.trim().to_ascii_uppercase();
    valid_symbol(&s).then_some(s)
}
fn number(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str()?.trim().parse().ok())
        .filter(|n| n.is_finite())
}
fn positive_integer(v: &Value) -> Option<u64> {
    if let Some(s) = v.as_str()
        && (s.trim().is_empty() || !s.trim().bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    let n = number(v)?;
    (n > 0.0 && n.fract() == 0.0 && n <= 9_007_199_254_740_991.0).then_some(n as u64)
}
fn id(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_owned()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(o) => id(o.get("$oid")?),
        _ => None,
    }
}
fn text(v: &Value) -> String {
    v.as_str().unwrap_or_default().trim().to_ascii_uppercase()
}
fn activity(value: &Value, orders: bool) -> Result<Vec<String>, AdmissionRejection> {
    let fail = || reject("incompleteExchangeEvidence");
    let rows = value
        .as_array()
        .or_else(|| orders.then(|| value["orders"].as_array()).flatten())
        .ok_or_else(fail)?;
    let mut ids = HashSet::new();
    let mut client_ids = HashSet::new();
    let mut symbols = Vec::new();
    for row in rows {
        let s = symbol(&row["symbol"]).ok_or_else(fail)?;
        if !matches!(
            text(&row["positionSide"]).as_str(),
            "LONG" | "SHORT" | "BOTH"
        ) {
            return Err(fail());
        }
        if orders {
            let order = id(&row["orderId"]);
            let client = id(&row["clientOrderId"]);
            if order.is_none() && client.is_none()
                || order.is_some_and(|i| !ids.insert(i))
                || client.is_some_and(|i| !client_ids.insert(i))
                || !matches!(text(&row["side"]).as_str(), "BUY" | "SELL")
                || text(&row["type"]).is_empty()
                || !number(&row["origQty"]).is_some_and(|n| n >= 0.0)
                || row
                    .get("executedQty")
                    .is_some_and(|v| !number(v).is_some_and(|n| n >= 0.0))
            {
                return Err(fail());
            }
        } else {
            let identity = id(&row["positionId"]).ok_or_else(fail)?;
            let amount = number(&row["positionAmt"]).ok_or_else(fail)?;
            if row.get("availableAmt").is_some_and(|v| number(v).is_none()) {
                return Err(fail());
            }
            if amount == 0.0 {
                continue;
            }
            if !ids.insert(identity) {
                return Err(fail());
            }
        }
        symbols.push(s);
    }
    Ok(symbols)
}
fn mode_evidence(
    account: &AccountEvidence,
    managed: &[Value],
    exchange_client_id: &str,
    requested_symbol: &str,
) -> Result<ModeEvidence, AdmissionRejection> {
    if exchange_client_id.is_empty() || !valid_symbol(requested_symbol) {
        return Err(reject("invalidAdmissionTarget"));
    }
    let hedge = match &account.mode["dualSidePosition"] {
        Value::Bool(v) => *v,
        Value::String(v) if v == "true" => true,
        Value::String(v) if v == "false" => false,
        _ => return Err(reject("incompleteExchangeEvidence")),
    };
    let positions = activity(&account.positions, false)?;
    let orders = activity(&account.orders, true)?;
    if !hedge
        && positions
            .iter()
            .chain(&orders)
            .any(|s| s == requested_symbol)
    {
        return Err(reject("migrationRequired"));
    }
    let mut ids = HashSet::new();
    let mut blocker = false;
    let mut same_blocker = false;
    let mut owners = Vec::new();
    for row in managed {
        let fail = || reject("incompleteManagedEvidence");
        let identity = id(&row["id"])
            .or_else(|| id(&row["_id"]))
            .ok_or_else(fail)?;
        let s = symbol(&row["symbol"]).ok_or_else(fail)?;
        if !ids.insert(identity)
            || row["exchangeClientId"].as_str() != Some(exchange_client_id)
            || row["exchange_client"].as_str() != Some("_binance_futures_")
        {
            return Err(fail());
        }
        let state = row["state"].as_str().unwrap_or_default();
        if state == "FINISHED" {
            continue;
        }
        // Match managed-trade-lifecycle.ts: historical, unknown, and contradictory
        // lifecycle rows are diagnostics only, not active ownership or mode blockers.
        let status = row
            .get("creationStatus")
            .map(|v| v.as_str().unwrap_or("INVALID"));
        let allowed = match state {
            "NEW" => {
                status.is_none() || matches!(status, Some("INITIALIZING" | "MARGIN_CONFIGURED"))
            }
            "OPENED" | "CLOSING" => {
                status.is_none()
                    || matches!(
                        status,
                        Some("MARGIN_CONFIGURED" | "TRADE_OPENED" | "COMPLETE")
                    )
            }
            _ => false,
        };
        if !allowed {
            continue;
        }
        if state != "CLOSING" {
            blocker = true;
            same_blocker |= s == requested_symbol;
        }
        let direction = row["is_long"].as_bool();
        let stored = if let Some(s) = row["tradeLeverage"].as_str() {
            let s = s.trim();
            let s = s
                .strip_suffix('x')
                .or_else(|| s.strip_suffix('X'))
                .unwrap_or(s);
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                None
            } else {
                positive_integer(&Value::String(s.to_owned()))
            }
        } else {
            positive_integer(&row["tradeLeverage"])
        };
        // The original snapshot keeps incomplete retained rows as mode blockers,
        // but records absent/invalid direction or leverage as diagnostics only.
        // Leverage admission receives only owners with both canonical fields.
        if let (Some(direction), Some(stored)) = (direction, stored) {
            owners.push((s, direction, stored));
        }
    }
    if !hedge && same_blocker {
        return Err(reject("migrationRequired"));
    }
    let decision = if hedge {
        ModeDecision::Hedge
    } else if positions.is_empty() && orders.is_empty() && !blocker {
        ModeDecision::MigrateToHedge
    } else {
        ModeDecision::LegacyOneWay
    };
    Ok(ModeEvidence { decision, owners })
}
pub fn evaluate_admission(
    account: &AccountEvidence,
    managed: &[Value],
    exchange_client_id: &str,
    requested_symbol: &str,
    is_long: bool,
    leverage: u32,
) -> Result<AdmittedAccount, AdmissionRejection> {
    if leverage == 0 {
        return Err(reject("invalidAdmissionTarget"));
    }
    let ModeEvidence { decision, owners } =
        mode_evidence(account, managed, exchange_client_id, requested_symbol)?;
    let hedge = match decision {
        ModeDecision::Hedge => true,
        ModeDecision::LegacyOneWay => false,
        ModeDecision::MigrateToHedge => return Err(reject("migrationRequired")),
    };
    let relevant: Vec<_> = owners
        .iter()
        .filter(|(s, direction, _)| s == requested_symbol && (!hedge || *direction == is_long))
        .collect();
    let owned = !relevant.is_empty();
    if relevant
        .iter()
        .any(|(_, _, stored)| *stored != leverage as u64)
    {
        return Err(reject("leverageConflict"));
    }
    let fields = [
        "longLeverage",
        "shortLeverage",
        "maxLongLeverage",
        "maxShortLeverage",
    ]
    .map(|f| {
        positive_integer(&account.leverage[f]).ok_or_else(|| reject("incompleteExchangeEvidence"))
    });
    let [long, short, max_long, max_short] = fields;
    let (long, short, max_long, max_short) = (long?, short?, max_long?, max_short?);
    let target = leverage as u64;
    if ((!hedge || is_long) && target > max_long) || ((!hedge || !is_long) && target > max_short) {
        return Err(reject("leverageLimitExceeded"));
    }
    if ((!hedge || is_long) && target != long) || ((!hedge || !is_long) && target != short) {
        return Err(reject(if owned {
            "ownedLeverageMismatch"
        } else {
            "leverageChangeRequired"
        }));
    }
    let balances = account
        .balance
        .as_array()
        .ok_or_else(|| reject("incompleteExchangeEvidence"))?;
    let usdt: Vec<_> = balances
        .iter()
        .filter(|b| b["asset"].as_str() == Some("USDT"))
        .collect();
    if usdt.len() != 1 {
        return Err(reject("incompleteExchangeEvidence"));
    }
    let raw = usdt[0]["availableMargin"].clone();
    let available = number(&raw).ok_or_else(|| reject("incompleteExchangeEvidence"))?;
    if available <= 0.0 {
        return Err(reject("insufficientBalance"));
    }
    Ok(AdmittedAccount {
        position_mode: if hedge { "HEDGE" } else { "ONEWAY" },
        position_configuration: if hedge {
            ExecutionRoute::Hedge
        } else {
            ExecutionRoute::OneWay
        },
        available_balance: available,
        available_balance_raw: raw,
    })
}
