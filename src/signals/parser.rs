use super::{
    ParseOutcome, SignalError, TradingSignal,
    validation::{positive_decimal, validate},
};

pub fn parse_signal(text: &str) -> ParseOutcome {
    let normalized: String = text
        .chars()
        .map(|c| match c {
            '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}' | '\u{2212}'
            | '\u{ff0d}' => '-',
            _ => c,
        })
        .collect();
    let lines: Vec<Vec<&str>> = normalized
        .lines()
        .filter_map(|line| {
            let line = line
                .trim()
                .trim_start_matches(|c: char| !c.is_ascii_alphanumeric() && c != '#');
            let tokens: Vec<_> = line.split_whitespace().collect();
            (!tokens.is_empty()).then_some(tokens)
        })
        .collect();
    // Ordinary channel chatter is not a failed trading instruction.
    let has_entry = lines.iter().any(|line| {
        matches!(
            line[0].trim_end_matches(':').to_ascii_uppercase().as_str(),
            "ENTRY" | "BUY"
        )
    });
    let has_leverage = lines.iter().any(|line| {
        line[0]
            .trim_end_matches(':')
            .eq_ignore_ascii_case("LEVERAGE")
    });
    let has_target = lines
        .iter()
        .any(|line| target_label(&line[0].trim_end_matches(':').to_ascii_uppercase()));
    let has_symbol = lines.iter().flatten().any(|word| {
        word.trim_matches(['#', '|', ':'])
            .to_ascii_uppercase()
            .ends_with("USDT")
    });
    let candidate = (has_entry && (has_target || has_symbol)) || (has_leverage && has_symbol);
    if !candidate {
        return ParseOutcome::NotSignal;
    }
    match parse(&lines) {
        Ok(signal) => ParseOutcome::Parsed(signal),
        Err(error) => ParseOutcome::Rejected(error),
    }
}
fn parse(lines: &[Vec<&str>]) -> Result<TradingSignal, SignalError> {
    let mut symbol = None;
    let mut side = None;
    let mut entries = None;
    let mut targets = Vec::new();
    let mut indexed_targets = None;
    let mut stop = None;
    let mut leverage = None;
    let mut position = None;
    let mut breakout_entry = None;
    for line in lines {
        let first = line[0].trim_end_matches(':').to_ascii_uppercase();
        match first.as_str() {
            "ENTRY" | "BUY" => {
                let mut values = &line[1..];
                if values
                    .first()
                    .is_some_and(|s| s.trim_end_matches(':').eq_ignore_ascii_case("zone"))
                {
                    values = &values[1..];
                }
                if values.is_empty() {
                    return Err(SignalError::MissingField);
                }
                // A dash separates range endpoints, but never discard an empty
                // endpoint: doing so would turn a negative price into a positive one.
                let joined = values.join(" ");
                let mut values = Vec::new();
                for endpoint in joined.split('-') {
                    if endpoint.trim().is_empty() {
                        return Err(SignalError::InvalidNumber);
                    }
                    values.extend(
                        endpoint
                            .split_whitespace()
                            .map(|s| s.trim_end_matches(',').to_string()),
                    );
                }
                if entries.replace(values).is_some() {
                    return Err(SignalError::DuplicateField);
                }
            }
            "POSITION" => {
                if line.len() != 3 || !line[1].eq_ignore_ascii_case("size") {
                    return Err(SignalError::UnsupportedFormat);
                }
                let value = line[2].trim_end_matches([';', ',', '!', ':', '.']);
                let percentage = positive_decimal(value.strip_suffix('%').unwrap_or(value))?;
                let fraction = percentage / 100.0;
                if fraction <= 0.0 {
                    return Err(SignalError::InvalidNumber);
                }
                if position.replace(fraction).is_some() {
                    return Err(SignalError::DuplicateField);
                }
            }
            "LEVERAGE" => {
                if line.len() != 2 {
                    return Err(SignalError::UnsupportedFormat);
                }
                let value = line[1];
                let number = value.strip_suffix(['x', 'X']).unwrap_or(value);
                positive_decimal(number)?;
                if leverage.replace(value.to_string()).is_some() {
                    return Err(SignalError::DuplicateField);
                }
            }
            "SL" | "STOP" => {
                let mut values = &line[1..];
                if values
                    .first()
                    .is_some_and(|s| s.eq_ignore_ascii_case("loss"))
                {
                    values = &values[1..];
                }
                if values
                    .first()
                    .is_some_and(|s| s.eq_ignore_ascii_case("hard"))
                {
                    values = &values[1..];
                }
                if values.first().is_some_and(|s| s.eq_ignore_ascii_case("at")) {
                    values = &values[1..];
                }
                if values.len() != 1 {
                    return Err(SignalError::UnsupportedFormat);
                }
                if stop.replace(values[0].to_string()).is_some() {
                    return Err(SignalError::DuplicateField);
                }
            }
            _ if target_label(&first) => {
                if line.len() != 2 {
                    return Err(SignalError::UnsupportedFormat);
                }
                let suffix = &first[2..];
                let indexed = !suffix.is_empty();
                if indexed_targets.is_some_and(|previous| previous != indexed)
                    || (indexed && suffix != (targets.len() + 1).to_string())
                {
                    return Err(SignalError::UnsupportedFormat);
                }
                indexed_targets = Some(indexed);
                targets.push(line[1].to_string());
            }
            _ => {
                let is_header = line.iter().any(|word| {
                    let word = word.trim_matches(['#', '|', ':']).to_ascii_uppercase();
                    word.ends_with("USDT") || matches!(word.as_str(), "LONG" | "SHORT")
                });
                if !is_header {
                    return Err(SignalError::UnsupportedFormat);
                }
                for word in line {
                    let marked_symbol = word.starts_with('#');
                    let word = word.trim_matches(['#', '|', ':']).to_ascii_uppercase();
                    if matches!(word.as_str(), "LONG" | "SHORT") {
                        if side.replace(word == "LONG").is_some() {
                            return Err(SignalError::AmbiguousHeader);
                        }
                    } else if word == "BREAKOUT" {
                        breakout_entry = Some(true);
                    } else if !marked_symbol && !word.contains('/') && !word.ends_with("USDT") {
                        // Descriptive header text has no effect on the trade.
                        continue;
                    } else {
                        let canonical = word.replace('/', "");
                        let coin = canonical
                            .strip_suffix("USDT")
                            .ok_or(SignalError::UnsupportedFormat)?;
                        if coin.is_empty()
                            || !coin.chars().all(|c| c.is_ascii_alphanumeric())
                            || word.matches('/').count() > 1
                        {
                            return Err(SignalError::UnsupportedFormat);
                        }
                        if word.contains('/') && !word.ends_with("/USDT") {
                            return Err(SignalError::UnsupportedFormat);
                        }
                        if symbol.replace(canonical).is_some() {
                            return Err(SignalError::AmbiguousHeader);
                        }
                    }
                }
            }
        }
    }
    let symbol = symbol.ok_or(SignalError::MissingField)?;
    let signal = TradingSignal {
        exchange_client: "_futures",
        coin: symbol.strip_suffix("USDT").unwrap().to_string(),
        symbol,
        base_currency: "USDT",
        is_long: side.ok_or(SignalError::MissingField)?,
        buy_targets: entries.ok_or(SignalError::MissingField)?,
        sell_targets: targets,
        stop_loss: stop.ok_or(SignalError::MissingField)?,
        leverage: leverage.ok_or(SignalError::MissingField)?,
        position,
        breakout_entry,
    };
    if signal.sell_targets.is_empty() {
        return Err(SignalError::MissingField);
    }
    validate(&signal)?;
    Ok(signal)
}
fn target_label(label: &str) -> bool {
    ["TG", "TP"].iter().any(|prefix| {
        label
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.chars().all(|c| c.is_ascii_digit()))
    })
}
