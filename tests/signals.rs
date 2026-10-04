use channels_manager_v1::signals::{ParseOutcome, parse_signal};
use serde_json::json;
const ADA: &str = include_str!("../examples/fixtures/ada-signal.txt");

#[test]
fn breakout_ranges_and_position_size_compose_without_losing_fields() {
    // 144 complete signals, asserting the serialized contract used by the logger.
    for coin in ["BTC", "SOL", "1000PEPE"] {
        for is_long in [true, false] {
            let (side, targets, stop) = if is_long {
                ("LONG", ["110", "120"], "90")
            } else {
                ("SHORT", ["90", "80"], "110")
            };
            for entries in ["100-101.50", "100 – 101.50", "100 101.50"] {
                for label in [
                    "BREAKOUT",
                    "breakout",
                    "DAY BREAKOUT",
                    "CUSTOM STYLE BreakOut",
                ] {
                    for position in [false, true] {
                        let text = format!(
                            "⚜️#{coin}/USDT |{label} {side}\nENTRY ZONE {entries}\nTG1 {}\nTG2 {}\nLEVERAGE 5x\nSL Hard at {stop}{}",
                            targets[0],
                            targets[1],
                            if position { "\nPOSITION SIZE 0.5%" } else { "" }
                        );
                        let mut expected = json!({
                            "exchange_client":"_futures", "symbol":format!("{coin}USDT"),
                            "base_currency":"USDT", "coin":coin, "is_long":is_long,
                            "buy_targets":["100","101.50"], "sell_targets":targets,
                            "stop_loss":stop, "leverage":"5x", "breakOutEntry":true
                        });
                        if position {
                            expected["position"] = json!(0.005);
                        }
                        assert_eq!(
                            serde_json::to_value(expect_signal(&text)).unwrap(),
                            expected,
                            "{text}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn descriptive_headers_do_not_change_the_parsed_contract() {
    let expected = serde_json::to_value(expect_signal(ADA)).unwrap();
    for header in [
        "#ADA/USDT LONG",
        "#ADA/USDT |SCALP LONG",
        "#ADA/USDT |CUSTOM STYLE 4H LONG",
        "SWING #ADA/USDT LONG",
        "LONG #ADA/USDT CUSTOM STYLE",
        "#ADA/USDT |BREAKOUTISH LONG",
        "#ADA/USDT |PREBREAKOUT LONG",
    ] {
        let text = ADA.replace("#ADA/USDT |DAY LONG", header);
        assert_eq!(
            serde_json::to_value(expect_signal(&text)).unwrap(),
            expected,
            "{header}"
        );
    }
}

#[test]
fn breakout_and_ignored_labels_do_not_bypass_validation() {
    for label in ["BREAKOUT", "CUSTOM STYLE"] {
        let base = ADA.replace("DAY", label);
        for text in [
            base.replace("LONG", ""),
            base.replace("LONG", "LONG SHORT"),
            base.replace("#ADA/USDT", "#ADA/USDT #BTC/USDT"),
            base.replace("#ADA/USDT", "#ADA/USDC"),
            base.replace("0.2570", "-0.2570"),
            base.replace("0.18", "0.30"),
            base.replace("0.27", "0.26"),
            base.replace("Tg2", "Tg4"),
            base.replace("3x", "-3x"),
            format!("{base}\nPOSITION SIZE -0.5%"),
            format!("{base}\nUNKNOWN SETTING 10%"),
        ] {
            assert!(
                matches!(parse_signal(&text), ParseOutcome::Rejected(_)),
                "{text}"
            );
        }
    }
}
#[test]
fn breakout_short_preserves_original_wire_flag() {
    let text = "⚜️#BTC/USDT |BREAKOUT SHORT\n⚜️Entry Zone 84550\n⚜️Tg1 83595\n⚜️Tg2 81048\n⚜️Tg3 80719\n⚜️Tg4 79323\n⚜️Leverage 5x\n⚜️SL Hard at 86347";
    for label in ["BREAKOUT", "breakout", "BreakOut"] {
        assert_eq!(
            serde_json::to_value(expect_signal(&text.replace("BREAKOUT", label))).unwrap(),
            json!({
                "exchange_client":"_futures", "symbol":"BTCUSDT", "base_currency":"USDT",
                "coin":"BTC", "is_long":false, "buy_targets":["84550"],
                "sell_targets":["83595","81048","80719","79323"],
                "stop_loss":"86347", "leverage":"5x", "breakOutEntry":true
            })
        );
    }
}

#[test]
fn breakout_long_is_distinct_from_descriptive_labels() {
    let signal = expect_signal(&ADA.replace("DAY", "BREAKOUT"));
    assert_eq!(serde_json::to_value(signal).unwrap()["breakOutEntry"], true);
    for label in ["DAY", "SWING", "BREAKOUTISH"] {
        let signal = expect_signal(&ADA.replace("DAY", label));
        assert!(
            serde_json::to_value(signal)
                .unwrap()
                .get("breakOutEntry")
                .is_none()
        );
    }
    assert!(matches!(
        parse_signal(&ADA.replace("DAY", "BREAKOUT").replace("0.18", "0.30")),
        ParseOutcome::Rejected(_)
    ));
}
#[test]
fn swing_short_btc_matches_original_signal_fields() {
    let text = "⚜️#BTC/USDT |SWING SHORT\n⚜️Entry Zone 84550-84650\n⚜️Tg1 83595\n⚜️Tg2 81048\n⚜️Tg3 80719\n⚜️Tg4 79323\n⚜️Leverage 5x\n⚜️SL Hard at 86347";
    for label in ["SWING", "swing", "DAY", "SCALP", "CUSTOM STYLE", "", "4H"] {
        assert_eq!(
            serde_json::to_value(expect_signal(&text.replace("SWING", label))).unwrap(),
            json!({
                "exchange_client":"_futures", "symbol":"BTCUSDT", "base_currency":"USDT",
                "coin":"BTC", "is_long":false, "buy_targets":["84550","84650"],
                "sell_targets":["83595","81048","80719","79323"],
                "stop_loss":"86347", "leverage":"5x"
            })
        );
    }
}
#[test]
fn ada_matches_original_parser_output() {
    let ParseOutcome::Parsed(signal) = parse_signal(ADA) else {
        panic!("ADA must parse")
    };
    assert_eq!(
        serde_json::to_value(signal).unwrap(),
        json!({"exchange_client":"_futures","symbol":"ADAUSDT","base_currency":"USDT","coin":"ADA","is_long":true,"buy_targets":["0.2570"],"sell_targets":["0.26","0.27","0.28"],"stop_loss":"0.18","leverage":"3x"})
    );
}
#[test]
fn short_and_case_insensitive_labels() {
    let text =
        "#ada/usdt short\nentry zone 0.2570\ntg1 0.25\ntg2 0.24\nleverage 3x\nsl hard at 0.28";
    let ParseOutcome::Parsed(signal) = parse_signal(text) else {
        panic!("SHORT must parse")
    };
    assert!(!signal.is_long);
    assert_eq!(signal.symbol, "ADAUSDT");
}
#[test]
fn ordinary_messages_are_not_signals() {
    for text in [
        "",
        "Good morning",
        "Have a long weekend",
        "Keep the update short",
        "Leverage your skills",
        "ADA/USDT looks interesting",
        "Our next entry will come tomorrow",
    ] {
        assert!(
            matches!(parse_signal(text), ParseOutcome::NotSignal),
            "{text}"
        );
    }
}
#[test]
fn incomplete_and_ambiguous_signals_are_rejected() {
    for text in [
        ADA.replace("LONG", ""),
        ADA.replace("LONG", "").replace("LEVERAGE 3x", ""),
        ADA.replace("LONG", "LONG SHORT"),
        ADA.replace("LEVERAGE 3x", ""),
        ADA.replace("⚜️SL Hard at 0.18", ""),
        ADA.replace("#ADA/USDT", "#ADA/BTC"),
        ADA.replace("#ADA/USDT", "#ADA/USDT #BTC/USDT"),
        format!("{ADA}\nLEVERAGE 5x"),
    ] {
        assert!(
            matches!(parse_signal(&text), ParseOutcome::Rejected(_)),
            "{text}"
        );
    }
}
#[test]
fn malformed_and_unsupported_numbers_never_become_positive_prices() {
    for number in [
        "0",
        "-0.2570",
        "−0.2570",
        "NaN",
        "inf",
        "0.25junk",
        "0.25%",
        "0.25-0.26",
        "1e10",
    ] {
        assert!(
            matches!(
                parse_signal(&ADA.replace("0.2570", number)),
                ParseOutcome::Rejected(_)
            ),
            "{number}"
        );
    }
    for leverage in ["0x", "-3x", "−3x", "NaN", "3xx", "3x extra"] {
        assert!(
            matches!(
                parse_signal(&ADA.replace("3x", leverage)),
                ParseOutcome::Rejected(_)
            ),
            "{leverage}"
        );
    }
}
#[test]
fn validates_direction_and_rejects_unknown_instructions() {
    for text in [
        ADA.replace("0.18", "0.30"),
        ADA.replace("0.26", "0.20"),
        ADA.replace("0.27", "0.26"),
        format!("{ADA}\nUNKNOWN SETTING 10%"),
        ADA.replace("0.27", "0.29"),
    ] {
        assert!(
            matches!(parse_signal(&text), ParseOutcome::Rejected(_)),
            "{text}"
        );
    }
}

#[test]
fn target_numbering_cannot_change_the_meaning_silently() {
    for text in [
        ADA.replace("Tg2", "Tg1"),
        ADA.replace("Tg2", "Tg4"),
        ADA.replace("Tg1", "Tg2").replace("Tg3", "Tg1"),
    ] {
        assert!(matches!(parse_signal(&text), ParseOutcome::Rejected(_)));
    }
}

fn expect_signal(text: &str) -> channels_manager_v1::signals::TradingSignal {
    match parse_signal(text) {
        ParseOutcome::Parsed(signal) => signal,
        result => panic!("expected parsed signal, got {result:?}\n{text}"),
    }
}

#[test]
fn sol_range_and_position_match_original_parser_output() {
    let text = "⚜️#SOL/USDT |DAY LONG\n⚜️Entry Zone 71-72\n⚜️Tg1 88\n⚜️Tg2 90\n⚜️Tg3 92\n⚜️Tg4 94\nLEVERAGE 50x\n⚜️POSITION SIZE 0.5%\n⚜️SL Hard at 70";
    assert_eq!(
        serde_json::to_value(expect_signal(text)).unwrap(),
        json!({
            "exchange_client":"_futures", "symbol":"SOLUSDT", "base_currency":"USDT",
            "coin":"SOL", "is_long":true, "buy_targets":["71","72"],
            "sell_targets":["88","90","92","94"], "stop_loss":"70",
            "leverage":"50x", "position":0.005
        })
    );
}

#[test]
fn entry_ranges_support_spacing_decimals_unicode_and_both_sides() {
    for dash in [
        '-', '\u{2010}', '\u{2011}', '\u{2012}', '\u{2013}', '\u{2014}', '\u{2212}', '\u{ff0d}',
    ] {
        for spacing in ["", " "] {
            for (side, entries, target, stop) in [
                ("LONG", ["71.00", "72.50"], "88", "70"),
                ("SHORT", ["72.50", "71.00"], "60", "80"),
            ] {
                let text = format!(
                    "SOLUSDT {side}\nENTRY {}{spacing}{dash}{spacing}{}\nTG1 {target}\nLEVERAGE 50x\nSL {stop}",
                    entries[0], entries[1]
                );
                assert_eq!(expect_signal(&text).buy_targets, entries);
            }
        }
    }
}

#[test]
fn position_percentage_is_optional_and_preserved_as_fraction() {
    for value in ["0.5%", "0.5", "0.5%;", "0.5%."] {
        let signal = expect_signal(&format!("{ADA}\nposition size {value}"));
        assert_eq!(
            serde_json::to_value(signal).unwrap()["position"],
            json!(0.005)
        );
    }
}

#[test]
fn malformed_ranges_and_position_sizes_are_rejected() {
    for value in ["-71-72", "71--72", "71-", "71 - -72", "71-0", "71-NaN"] {
        assert!(
            matches!(
                parse_signal(&ADA.replace("0.2570", value)),
                ParseOutcome::Rejected(_)
            ),
            "{value}"
        );
    }
    for value in [
        "",
        "0%",
        "-0.5%",
        "−0.5%",
        "NaN%",
        "inf%",
        "0.5%%",
        "0.5% extra",
    ] {
        assert!(
            matches!(
                parse_signal(&format!("{ADA}\nPOSITION SIZE {value}")),
                ParseOutcome::Rejected(_)
            ),
            "{value}"
        );
    }
    assert!(matches!(
        parse_signal(&format!("{ADA}\nPOSITION SIZE 1%\nPOSITION SIZE 2%")),
        ParseOutcome::Rejected(_)
    ));
}

#[test]
fn symbol_price_side_and_leverage_matrix_preserves_all_fields() {
    // 5 symbols × 5 price scales × 2 sides × 4 leverage forms = 200 signals.
    for coin in ["BTC", "ETH", "SOL", "DOGE", "1000PEPE"] {
        for [low, middle, high, top] in [
            ["90", "100", "110", "120"],
            ["0.18", "0.2570", "0.26", "0.27"],
            ["0.00000110", "0.00000120", "0.00000130", "0.00000140"],
            ["2490.25", "2500.50", "2501.75", "2510.00"],
            ["900000000", "1000000000", "1100000000", "1200000000"],
        ] {
            for is_long in [true, false] {
                let (side, entry, targets, stop) = if is_long {
                    ("LONG", middle, [high, top], low)
                } else {
                    ("SHORT", high, [middle, low], top)
                };
                for leverage in ["1", "3x", "10X", "2.5x"] {
                    let text = format!(
                        "#{coin}/USDT {side}\nEntry Zone {entry}\nTG1 {}\nTG2 {}\nLEVERAGE {leverage}\nSL Hard at {stop}",
                        targets[0], targets[1]
                    );
                    assert_eq!(
                        serde_json::to_value(expect_signal(&text)).unwrap(),
                        json!({"exchange_client":"_futures","symbol":format!("{coin}USDT"),"base_currency":"USDT","coin":coin,"is_long":is_long,"buy_targets":[entry],"sell_targets":targets,"stop_loss":stop,"leverage":leverage}),
                        "{text}"
                    );
                }
            }
        }
    }
}

#[test]
fn supported_entry_target_and_stop_labels_are_interchangeable() {
    for entry_label in ["ENTRY", "Entry:", "ENTRY ZONE", "buy", "Buy Zone:"] {
        for targets in [
            "TG1: 110\nTG2: 120",
            "tp1 110\ntp2 120",
            "TG 110\nTG 120",
            "TP: 110\nTP: 120",
        ] {
            for stop_label in ["SL", "SL Hard at", "STOP LOSS", "Stop loss hard at"] {
                let text = format!(
                    "BTCUSDT LONG\n{entry_label} 100\n{targets}\nLeverage: 5x\n{stop_label} 90"
                );
                let signal = expect_signal(&text);
                assert_eq!(signal.buy_targets, ["100"]);
                assert_eq!(signal.sell_targets, ["110", "120"]);
                assert_eq!(signal.stop_loss, "90");
            }
        }
    }
}

#[test]
fn multiple_entries_and_single_target_work_for_both_sides() {
    for (side, entries, target, stop) in [
        ("LONG", "100, 101.50", "110", "90"),
        ("SHORT", "101.50 100", "90", "110"),
    ] {
        let signal = expect_signal(&format!(
            "SOLUSDT {side}\nENTRY {entries}\nTP1 {target}\nLEVERAGE 4\nSL {stop}"
        ));
        assert_eq!(signal.buy_targets.len(), 2);
        assert_eq!(signal.sell_targets, [target]);
        assert_eq!(signal.is_long, side == "LONG");
    }
}

#[test]
fn whitespace_emoji_case_and_line_endings_do_not_change_values() {
    let signal = expect_signal(
        "\r\n  ⚜️#eTh/usdt\u{00a0}|DAY\tShort  \r\n\r\n⚜️entry\tzone:\t2500.50\r\n🎯tp1:\u{00a0}2400.25\r\n🎯tp2 2300\r\nleverage:\t5X\r\n🛑sl hard at 2600\r\n",
    );
    assert_eq!(signal.symbol, "ETHUSDT");
    assert!(!signal.is_long);
    assert_eq!(signal.buy_targets, ["2500.50"]);
    assert_eq!(signal.sell_targets, ["2400.25", "2300"]);
    assert_eq!(signal.leverage, "5X");
}

#[test]
fn decimal_spelling_is_preserved_without_numeric_reserialization() {
    let signal = expect_signal("DOGEUSDT LONG\nENTRY .25 0.2500\nTP1 01.00\nLEVERAGE 3.0x\nSL .10");
    assert_eq!(signal.buy_targets, [".25", "0.2500"]);
    assert_eq!(signal.sell_targets, ["01.00"]);
    assert_eq!(signal.stop_loss, ".10");
    assert_eq!(signal.leverage, "3.0x");
}

#[test]
fn invalid_numbers_are_rejected_in_every_price_field() {
    for field in ["0.2570", "0.26", "0.18"] {
        for value in [
            "0", "-1", "+1", "NaN", "Infinity", "1e3", "1.2.3", ".", "1,000", "0,25", "10%",
            "12oops",
        ] {
            let text = ADA.replace(field, value);
            assert!(
                matches!(parse_signal(&text), ParseOutcome::Rejected(_)),
                "field={field} value={value}"
            );
        }
        // Every Unicode minus normalized by the parser must retain its sign.
        for minus in [
            '\u{2010}', '\u{2011}', '\u{2012}', '\u{2013}', '\u{2014}', '\u{2212}', '\u{ff0d}',
        ] {
            assert!(matches!(
                parse_signal(&ADA.replace(field, &format!("{minus}1"))),
                ParseOutcome::Rejected(_)
            ));
        }
    }
}

#[test]
fn short_price_ordering_and_entry_boundaries_are_checked() {
    for text in [
        "BTCUSDT SHORT\nENTRY 100\nTP1 110\nLEVERAGE 3x\nSL 120",
        "BTCUSDT SHORT\nENTRY 100\nTP1 90\nLEVERAGE 3x\nSL 80",
        "BTCUSDT SHORT\nENTRY 100\nTP1 80\nTP2 90\nLEVERAGE 3x\nSL 120",
        "BTCUSDT SHORT\nENTRY 100 110\nTP1 105\nLEVERAGE 3x\nSL 120",
        "BTCUSDT LONG\nENTRY 100 110\nTP1 105\nLEVERAGE 3x\nSL 90",
        "BTCUSDT LONG\nENTRY 100\nTP1 100\nLEVERAGE 3x\nSL 90",
        "BTCUSDT LONG\nENTRY 100\nTP1 110\nLEVERAGE 3x\nSL 100",
    ] {
        assert!(
            matches!(parse_signal(text), ParseOutcome::Rejected(_)),
            "{text}"
        );
    }
}

#[test]
fn missing_duplicate_and_unsupported_settings_cannot_be_guessed() {
    for text in [
        ADA.replace("⚜️Entry Zone 0.2570", ""),
        ADA.replace("⚜️Tg1 0.26\n⚜️Tg2 0.27\n⚜️Tg3 0.28\n", ""),
        ADA.replace("LONG", "BUY"),
        ADA.replace("#ADA/USDT", "#ADA/USDC"),
        ADA.replace("#ADA/USDT", "#ADA//USDT"),
        ADA.replace("#ADA/USDT", "#USDT"),
        ADA.replace("Tg2", "TP"),
        ADA.replace("Tg1", "TG0"),
        format!("{ADA}\nSL 0.17"),
        format!("{ADA}\nENTRY 0.25"),
        ADA.replace("SL Hard", "SL Soft"),
        ADA.replace("Entry Zone 0.2570", "ENTRY MARKET"),
    ] {
        assert!(
            matches!(parse_signal(&text), ParseOutcome::Rejected(_)),
            "{text}"
        );
    }
}
