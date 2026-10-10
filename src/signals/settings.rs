//! Pure TypeScript-compatible settings precedence; amounts keep their wire values.
use crate::contracts::messages::ClientTradeJob;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsError {
    MissingSettings,
    InvalidPosition,
    InvalidTargets,
    InvalidStrategy,
    InvalidXPro,
    InvalidUserConfig,
    InvalidMarginMode,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSource {
    Signal,
    Channel,
}
#[derive(Debug, Clone, PartialEq)]
pub enum PositionSize {
    Dynamic {
        source: PositionSource,
        balance_fraction: Value,
    },
    Static {
        quote_amount: Value,
    },
}
pub struct ResolvedSettings {
    pub config: Value,
    pub position_size: PositionSize,
    pub margin_mode: &'static str,
}

pub fn resolve_settings(job: &ClientTradeJob) -> Result<ResolvedSettings, SettingsError> {
    let mut config = job.channel_settings.clone();
    if !config.is_object() {
        return Err(SettingsError::MissingSettings);
    }
    let user = job.user_config.as_ref().filter(|v| !v.is_null());
    if let Some(user) = user {
        if !user.is_object() || numeric_id(&user["user"]) != numeric_id(&job.client["chatId"]) {
            return Err(SettingsError::InvalidUserConfig);
        }
        if let Some(channels) = user.get("private_channels") {
            let channels = channels
                .as_array()
                .ok_or(SettingsError::InvalidUserConfig)?;
            if let Some(settings) = channels
                .iter()
                .find(|ch| numeric_id(&ch["id"]) == Some(job.channel_id))
                && settings.get("own_settings").is_some_and(truthy)
            {
                let original = config.clone();
                for (key, value) in settings
                    .as_object()
                    .ok_or(SettingsError::InvalidUserConfig)?
                {
                    config[key] = value.clone();
                }
                for market in ["futures", "spot"] {
                    let mut merged = original[market].as_object().cloned().unwrap_or_default();
                    if let Some(values) = settings[market].as_object() {
                        merged.extend(values.clone());
                    }
                    config[market] = Value::Object(merged);
                }
            }
        }
    }
    let market = config["futures"].clone();
    if market["active"] == Value::Bool(true) {
        for field in [
            "default_quantity",
            "position_size_mode",
            "static_quote_amount",
            "default_buy_targets",
            "default_sell_targets",
            "strategy",
        ] {
            if let Some(value) = market.get(field) {
                config[field] = value.clone();
            }
        }
        if let Some(value) = market.get("xPro") {
            config["xPro"] = value.clone();
        } else if market["strategy"] == "basic" {
            config["xPro"] = Value::Null;
        }
    }
    let margin_mode = match config["futures"].get("margin") {
        None | Some(Value::Null) => "isolated",
        Some(Value::String(s)) if s.is_empty() || s.eq_ignore_ascii_case("isolated") => "isolated",
        Some(Value::String(s)) if s.eq_ignore_ascii_case("cross") => "cross",
        _ => return Err(SettingsError::InvalidMarginMode),
    };
    let position_size = resolve_position(&job.signal_data, &config)?;
    for field in ["default_buy_targets", "default_sell_targets"] {
        if !config[field].as_array().is_some_and(|ts| {
            !ts.is_empty() && ts.iter().all(|t| positive_number(&t["fraction"]).is_some())
        }) {
            return Err(SettingsError::InvalidTargets);
        }
    }
    if !config["strategy"]
        .as_str()
        .is_some_and(|s| !s.trim().is_empty())
    {
        return Err(SettingsError::InvalidStrategy);
    }
    if let Some(value) = config.get("xPro").filter(|v| !v.is_null())
        && !number(value).is_some_and(|n| n >= 0.0)
    {
        return Err(SettingsError::InvalidXPro);
    }
    Ok(ResolvedSettings {
        config,
        position_size,
        margin_mode,
    })
}
fn resolve_position(signal: &Value, config: &Value) -> Result<PositionSize, SettingsError> {
    if let Some(value) = signal.get("position") {
        positive_number(value).ok_or(SettingsError::InvalidPosition)?;
        return Ok(PositionSize::Dynamic {
            source: PositionSource::Signal,
            balance_fraction: value.clone(),
        });
    }
    let mode = match config.get("position_size_mode") {
        None | Some(Value::Null) => "dynamic".to_owned(),
        Some(Value::String(s)) if s.is_empty() => "dynamic".to_owned(),
        Some(Value::String(s)) => s.to_lowercase(),
        _ => return Err(SettingsError::InvalidPosition),
    };
    match mode.as_str() {
        "dynamic" => {
            let value = &config["default_quantity"];
            positive_number(value).ok_or(SettingsError::InvalidPosition)?;
            Ok(PositionSize::Dynamic {
                source: PositionSource::Channel,
                balance_fraction: value.clone(),
            })
        }
        "static" => {
            let value = &config["static_quote_amount"];
            positive_number(value).ok_or(SettingsError::InvalidPosition)?;
            Ok(PositionSize::Static {
                quote_amount: value.clone(),
            })
        }
        _ => Err(SettingsError::InvalidPosition),
    }
}
/// Only JSON numbers and numeric strings are accepted, never JS object/boolean coercions.
pub(crate) fn number(value: &Value) -> Option<f64> {
    let n = match value {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.trim().parse().ok()?,
        _ => return None,
    };
    n.is_finite().then_some(n)
}
pub(crate) fn positive_number(value: &Value) -> Option<f64> {
    number(value).filter(|n| *n > 0.0)
}
pub(crate) fn numeric_id(value: &Value) -> Option<i64> {
    number(value)
        .filter(|n| n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0)
        .map(|n| n as i64)
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
