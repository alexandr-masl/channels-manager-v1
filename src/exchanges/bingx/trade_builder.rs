//! Construct the original Trading Station envelope without network or storage IO.
use super::targets::{self, ExitContext, Precision, finite, positive};
use crate::{
    contracts::messages::{NewTradeMessage, trade_creation_id},
    signals::settings::{PositionSize, PositionSource, number},
    trading::{
        execution::PreparedAccount,
        job::{JobValidationError, validate_job},
    },
};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeBuildError {
    Expired,
    Rejected(&'static str),
}
fn display_number(n: f64) -> String {
    let text = format!("{n:.8}");
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}
pub fn build_trade(
    prepared: &PreparedAccount,
    fallback_ratio: f64,
    now_ms: u64,
) -> Result<NewTradeMessage, TradeBuildError> {
    let job = &prepared.job;
    let validated = validate_job(job, now_ms).map_err(|e| {
        if e == JobValidationError::Expired {
            TradeBuildError::Expired
        } else {
            TradeBuildError::Rejected("invalidTradeJob")
        }
    })?;
    let signal = &job.signal_data;
    let settings = &prepared.settings;
    let config = &settings.config;
    let snapshot = &job.market_data["bingXFutures"];
    let precision = Precision::new(&snapshot["symbolInfo"], &snapshot["currPrice"])?;
    let balance = finite(prepared.admission.available_balance)?;
    let (mode, source, requested, fraction) = match &settings.position_size {
        PositionSize::Dynamic {
            source,
            balance_fraction,
        } => {
            let fraction = positive(balance_fraction)?;
            (
                "dynamic",
                match source {
                    PositionSource::Signal => "signal",
                    PositionSource::Channel => "channel",
                },
                finite(balance * fraction)?,
                Some(fraction),
            )
        }
        PositionSize::Static { quote_amount } => {
            ("static", "channel", positive(quote_amount)?, None)
        }
    };
    if mode == "static"
        && (!fallback_ratio.is_finite() || fallback_ratio <= 0.0 || fallback_ratio > 1.0)
    {
        return Err(TradeBuildError::Rejected("invalidStaticFallback"));
    }
    if mode == "dynamic" && requested > balance {
        return Err(TradeBuildError::Rejected("positionExceedsBalance"));
    }
    let fallback = mode == "static" && requested > balance;
    let effective = finite(if fallback {
        balance * fallback_ratio
    } else {
        requested
    })?;
    let notional = finite(effective * validated.requested_leverage as f64)?;
    let raw_quantity = finite(notional / precision.current)?;
    let mut entries = targets::entries(
        signal,
        config,
        &precision,
        raw_quantity,
        &job.idempotency_key,
        &validated.normalized_symbol,
        validated.is_long,
    )?;
    let mut quantity = 0.0;
    let mut executable_notional = 0.0;
    for entry in &entries {
        let qty = positive(&entry["quantity"])?;
        quantity += qty;
        executable_notional += qty * positive(&entry["price"])?;
    }
    finite(quantity)?;
    finite(executable_notional)?;
    if mode == "static" && executable_notional > notional + (notional * 1e-8).max(1e-8) {
        return Err(TradeBuildError::Rejected(
            "executableNotionalExceedsStaticSize",
        ));
    }
    let display = if fallback {
        format!(
            "{} USDT effective (Static request: {} USDT)",
            display_number(effective),
            display_number(requested)
        )
    } else if let Some(f) = fraction {
        format!(
            "{}%{}",
            display_number(f * 100.0),
            if source == "signal" {
                " signal override"
            } else {
                ""
            }
        )
    } else {
        format!("{} USDT", display_number(requested))
    };
    let mut metadata = json!({"mode":mode,"source":source,"requestedQuoteAmount":requested,"effectiveQuoteAmount":effective,"balanceFallbackApplied":fallback,"fallbackUsageRatio":if fallback{Some(fallback_ratio)}else{None},"marginQuoteAmount":effective,"notionalQuoteAmount":notional,"rawTradeQuantity":raw_quantity,"executableTradeQuantity":quantity,"executableNotionalQuoteAmount":executable_notional,"display":display});
    if let Some(f) = fraction {
        metadata["balanceFraction"] = json!(f);
    }
    let _x_pro = config
        .get("xPro")
        .filter(|v| !v.is_null())
        .map(number)
        .unwrap_or(Some(0.0))
        .filter(|n| *n >= 0.0)
        .ok_or(TradeBuildError::Rejected("invalidStrategy"))?;
    let pro = config.get("xPro").is_some_and(|v| match v {
        Value::String(s) => !s.is_empty(),
        _ => number(v).is_some_and(|n| n != 0.0),
    });
    let (profits, stop) = targets::exits(ExitContext {
        signal,
        config,
        precision: &precision,
        quantity,
        notional: executable_notional,
        entries: &entries,
        pro,
        key: &job.idempotency_key,
        symbol: &validated.normalized_symbol,
        is_long: validated.is_long,
    })?;
    // The original stop-loss helper sorts entry targets ascending before publication.
    entries.sort_by(|a, b| {
        number(&a["price"])
            .unwrap()
            .total_cmp(&number(&b["price"]).unwrap())
    });
    let mut auto_trade = json!({"channel_id":job.channel_id,"message_id":job.message_id,"usingChannelConfig":!uses_own_settings(job)});
    if let Some(value) = config.get("name") {
        auto_trade["channelName"] = value.clone();
    }
    if let Some(value) = job.client.get("name") {
        auto_trade["exchangeClientName"] = value.clone();
    }
    let mut trade = json!({
        "id":trade_creation_id(&job.idempotency_key),"exch_client":"_binance_futures_","is_long":validated.is_long,"state":"NEW",
        "quantity":quantity,"symbol":validated.normalized_symbol,"chat_id":validated.chat_id,
        "strategy":{"name":config["strategy"],"is_pro":pro,"x_pro":config.get("xPro").filter(|_|pro).cloned().unwrap_or(json!(0)),"pro_sl_price":0},
        "used_coins":{"wished_quantity":quantity},"precisions":precision.wire,
        "buy_targets":entries,"sell_targets":profits,"stop_loss":stop,"positionSizing":metadata,
        "auto_Trade":auto_trade,"tradeLeverage":signal["leverage"],"futuresMarginMode":settings.margin_mode,
        "idempotencyKey":job.idempotency_key,
        "routing":{"jobType":"create_trade","provider":"BingX","market":"futures","symbol":job.symbol,"partitionKey":job.partition_key}
    });
    prepared
        .admission
        .apply_to_trade_object(trade.as_object_mut().unwrap());
    Ok(NewTradeMessage {
        expires_at: validated.expires_at,
        trade_object: trade,
        client_data: job.client.clone(),
    })
}
fn uses_own_settings(job: &crate::contracts::messages::ClientTradeJob) -> bool {
    job.user_config
        .as_ref()
        .and_then(|u| u["private_channels"].as_array())
        .and_then(|cs| {
            cs.iter()
                .find(|c| number(&c["id"]) == Some(job.channel_id as f64))
        })
        .is_some_and(|c| match &c["own_settings"] {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::String(s) => !s.is_empty(),
            Value::Number(n) => n.as_f64() != Some(0.0),
            _ => true,
        })
}
