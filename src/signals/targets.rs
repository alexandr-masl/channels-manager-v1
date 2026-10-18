//! Profit-target intent shared by Telegram parsing and the client-job boundary.
use super::{SignalError, validation::positive_decimal};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProfitTarget {
    Price(String),
    Percent(PercentageTarget),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PercentageTarget {
    #[serde(rename = "type")]
    kind: PercentageKind,
    value: f64,
    raw: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum PercentageKind {
    #[serde(rename = "percent")]
    Percent,
}

impl ProfitTarget {
    pub(crate) fn parse(raw: &str) -> Result<Self, SignalError> {
        match raw.strip_suffix('%') {
            Some(number) => Ok(Self::Percent(PercentageTarget {
                kind: PercentageKind::Percent,
                value: positive_decimal(number)?,
                raw: raw.to_owned(),
            })),
            None => {
                positive_decimal(raw)?;
                Ok(Self::Price(raw.to_owned()))
            }
        }
    }

    pub(crate) fn value(&self) -> Result<f64, SignalError> {
        match self {
            Self::Price(raw) => raw
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|n| n.is_finite() && *n > 0.0)
                .ok_or(SignalError::InvalidNumber),
            Self::Percent(target) => {
                let number = target
                    .raw
                    .strip_suffix('%')
                    .ok_or(SignalError::InvalidNumber)?;
                let value = positive_decimal(number)?;
                if value != target.value {
                    return Err(SignalError::InvalidNumber);
                }
                Ok(value)
            }
        }
    }

    pub(crate) fn is_percentage(&self) -> bool {
        matches!(self, Self::Percent(_))
    }
}

pub(crate) fn validate_targets(
    targets: &[ProfitTarget],
    is_long: bool,
    leverage: Option<f64>,
) -> Result<(), SignalError> {
    let percentage = targets.first().is_some_and(ProfitTarget::is_percentage);
    let mut previous = 0.0;
    for target in targets {
        if target.is_percentage() != percentage {
            return Err(SignalError::MixedTargetFormats);
        }
        let value = target.value()?;
        if percentage {
            if value <= previous {
                return Err(SignalError::InvalidPriceLevels);
            }
            if !is_long && leverage.is_some_and(|n| value >= n * 100.0) {
                return Err(SignalError::InvalidPriceLevels);
            }
            previous = value;
        }
    }
    Ok(())
}

pub(crate) fn validate_job_targets(
    value: &Value,
    is_long: bool,
    leverage: f64,
) -> Result<(), SignalError> {
    let targets = value.as_array().ok_or(SignalError::UnsupportedFormat)?;
    let targets = targets
        .iter()
        .map(|target| {
            // Existing client jobs also allow JSON numbers for absolute prices.
            let target = if target.is_number() {
                Value::String(target.to_string())
            } else {
                target.clone()
            };
            serde_json::from_value::<ProfitTarget>(target)
                .map_err(|_| SignalError::UnsupportedFormat)
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_targets(&targets, is_long, Some(leverage))
}
