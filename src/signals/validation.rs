use super::{SignalError, TradingSignal};

pub(super) fn positive_decimal(value: &str) -> Result<f64, SignalError> {
    let mut dots = 0;
    if value.is_empty()
        || !value.chars().all(|c| {
            if c == '.' {
                dots += 1;
                dots <= 1
            } else {
                c.is_ascii_digit()
            }
        })
    {
        return Err(SignalError::InvalidNumber);
    }
    let number: f64 = value.parse().map_err(|_| SignalError::InvalidNumber)?;
    if !number.is_finite() || number <= 0.0 {
        return Err(SignalError::InvalidNumber);
    }
    Ok(number)
}
pub(super) fn validate(signal: &TradingSignal) -> Result<(), SignalError> {
    let entries = signal
        .buy_targets
        .iter()
        .map(|n| positive_decimal(n))
        .collect::<Result<Vec<_>, _>>()?;
    let targets = signal
        .sell_targets
        .iter()
        .map(|n| positive_decimal(n))
        .collect::<Result<Vec<_>, _>>()?;
    let stop = signal
        .stop_loss
        .as_deref()
        .map(positive_decimal)
        .transpose()?;
    if let Some(leverage) = &signal.leverage {
        positive_decimal(leverage.trim_end_matches(['x', 'X']))?;
    }
    let low = entries.iter().copied().fold(f64::INFINITY, f64::min);
    let high = entries.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let valid = if signal.is_long {
        stop.is_none_or(|p| p < low)
            && targets.iter().all(|p| *p > high)
            && targets.windows(2).all(|p| p[0] < p[1])
    } else {
        stop.is_none_or(|p| p > high)
            && targets.iter().all(|p| *p < low)
            && targets.windows(2).all(|p| p[0] > p[1])
    };
    if valid {
        Ok(())
    } else {
        Err(SignalError::InvalidPriceLevels)
    }
}
