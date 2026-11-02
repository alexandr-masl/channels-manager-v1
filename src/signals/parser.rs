use super::{
    ParseOutcome, ProfitTarget, SignalError, TradingSignal,
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
    let lines: Vec<(usize, Vec<&str>)> = normalized
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line
                .trim()
                .trim_start_matches(|c: char| !c.is_ascii_alphanumeric() && c != '#');
            let tokens: Vec<_> = line.split_whitespace().collect();
            (!tokens.is_empty()).then_some((index + 1, tokens))
        })
        .collect();
    // Ordinary channel chatter is not a failed trading instruction.
    let has_entry = lines.iter().any(|(_, line)| {
        matches!(
            line[0].trim_end_matches(':').to_ascii_uppercase().as_str(),
            "ENTRY" | "BUY"
        )
    });
    let candidate = has_entry
        && lines.iter().any(|(_, line)| {
            header(line)
                || matches!(
                    line[0].trim_end_matches(':').to_ascii_uppercase().as_str(),
                    "ENTRY" | "BUY"
                ) && line.get(1).is_none_or(|word| {
                    word.eq_ignore_ascii_case("ZONE")
                        || word.starts_with(|c: char| c.is_ascii_digit() || matches!(c, '.' | '-'))
                })
        });
    if !candidate {
        return ParseOutcome::NotSignal;
    }
    match parse(&lines) {
        Ok(signal) => ParseOutcome::Parsed(signal),
        Err(error) => ParseOutcome::Rejected(error),
    }
}
fn parse(lines: &[(usize, Vec<&str>)]) -> Result<TradingSignal, SignalError> {
    let mut symbol = None;
    let mut side = None;
    let mut entries = None;
    let mut targets = Vec::new();
    let mut indexed_targets = None;
    let mut stop = None;
    let mut leverage = None;
    let mut position = None;
    let mut breakout_entry = None;
    let mut ignored_lines = 0;
    for (source_line, line) in lines {
        let first = line[0].trim_end_matches(':').to_ascii_uppercase();
        (|| {
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
                    for value in &values {
                        positive_decimal(value)?;
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
                    positive_decimal(values[0])?;
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
                    targets.push(ProfitTarget::parse(line[1])?);
                }
                _ => {
                    if management(line) {
                        return Err(SignalError::AmbiguousInstruction);
                    }
                    if !header(line) {
                        if first.starts_with("TG") || first.starts_with("TP") {
                            return Err(SignalError::UnsupportedFormat);
                        }
                        ignored_lines += 1;
                        return Ok(());
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
            Ok(())
        })()
        .map_err(|reason| SignalError::AtLine {
            line: *source_line,
            field: if header(line) {
                "header".into()
            } else {
                first.clone()
            },
            reason: Box::new(reason),
        })?;
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
        stop_loss: stop,
        ignored_lines,
        leverage,
        position,
        breakout_entry,
    };
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

fn header(line: &[&str]) -> bool {
    let word = |s: &str| s.trim_matches(['#', '|', ':']).to_ascii_uppercase();
    let pair =
        |s: &str| s.starts_with('#') || s.ends_with("USDT") || s.contains('/') && !s.contains(':');
    let direction = |s: &str| matches!(s, "LONG" | "SHORT");
    let descriptor = |s: &str| {
        matches!(
            s,
            "DAY" | "SWING" | "SCALP" | "BREAKOUT" | "CUSTOM" | "STYLE"
        ) || s.starts_with(|c: char| c.is_ascii_digit())
    };
    let first = word(line[0]);
    if line.len() == 1 {
        return first.ends_with("USDT")
            || first.contains('/') && !first.contains(':')
            || direction(&first);
    }
    if !pair(line[0]) && !pair(&first) && !direction(&first) && !descriptor(&first) {
        return false;
    }
    let has_pair = line.iter().any(|s| pair(s) || pair(&word(s)));
    let has_direction = line.iter().any(|s| direction(&word(s)));
    if !has_pair {
        return has_direction
            && line
                .iter()
                .all(|s| direction(&word(s)) || descriptor(&word(s)));
    }
    line.iter().all(|raw| {
        let token = word(raw);
        pair(raw)
            || pair(&token)
            || direction(&token)
            || descriptor(&token)
            || has_direction && raw.chars().all(|c| !c.is_ascii_lowercase())
    })
}
fn management(line: &[&str]) -> bool {
    let first = line[0].trim_end_matches(':').to_ascii_uppercase();
    matches!(first.as_str(), "CLOSE" | "CANCEL")
        || matches!(first.as_str(), "MOVE" | "EDIT")
            && line.get(1).is_some_and(|word| {
                matches!(
                    word.to_ascii_uppercase().as_str(),
                    "SL" | "STOP" | "TARGET" | "ENTRY" | "TP" | "TG"
                )
            })
}
