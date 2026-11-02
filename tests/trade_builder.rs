use channels_manager_v1::{
    contracts::messages::{ClientTradeJob, ExecutionRoute},
    exchanges::bingx::{
        admission::AdmittedAccount,
        trade_builder::{TradeBuildError, build_trade},
    },
    signals::settings::resolve_settings,
    trading::execution::PreparedAccount,
};
use serde_json::{Value, json};
fn fixtures() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/bingx-trades.json")).unwrap()
}
fn prepared(case: &Value) -> PreparedAccount {
    let job: ClientTradeJob = serde_json::from_value(case["job"].clone()).unwrap();
    PreparedAccount {
        notification_context:
            channels_manager_v1::trading::notifications::NotificationContext::from_validated(
                &channels_manager_v1::trading::job::validate_job(&job, 1).unwrap(),
            ),
        settings: resolve_settings(&job).unwrap(),
        job,
        admission: AdmittedAccount {
            position_mode: if case["route"] == "ONEWAY_LEGACY" {
                "ONEWAY"
            } else {
                "HEDGE"
            },
            position_configuration: if case["route"] == "ONEWAY_LEGACY" {
                ExecutionRoute::OneWay
            } else {
                ExecutionRoute::Hedge
            },
            available_balance: case["balance"].as_f64().unwrap(),
            available_balance_raw: case["balance"].clone(),
        },
    }
}
fn compare(actual: &Value, expected: &Value, path: &str) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            assert!(
                (a - b).abs() <= 1e-12 * b.abs().max(1.),
                "{path}: {a} != {b}"
            );
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.len(), b.len(), "{path}: keys {a:?} vs {b:?}");
            for (k, v) in b {
                compare(&a[k], v, &format!("{path}.{k}"));
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{path}");
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                compare(a, b, &format!("{path}[{i}]"));
            }
        }
        _ => assert_eq!(actual, expected, "{path}"),
    }
}
#[test]
fn matches_original_typescript_fixtures() {
    for case in fixtures() {
        if let Some(reason) = case["expected_error"].as_str() {
            let Err(TradeBuildError::Rejected(actual)) = build_trade(&prepared(&case), 0.95, 1)
            else {
                panic!("expected documented policy rejection")
            };
            assert_eq!(actual, reason);
            continue;
        }
        let result = build_trade(&prepared(&case), 0.95, 1).unwrap();
        let mut actual = serde_json::to_value(result).unwrap();
        let trade = &mut actual["trade_object"];
        let mut ids = std::collections::HashSet::new();
        for field in ["buy_targets", "sell_targets"] {
            for t in trade[field].as_array_mut().unwrap() {
                let id = t.as_object_mut().unwrap().remove("clientOrderId").unwrap();
                assert!(ids.insert(id.as_str().unwrap().to_owned()));
            }
        }
        let id = trade["stop_loss"]
            .as_object_mut()
            .unwrap()
            .remove("clientOrderId")
            .unwrap();
        assert!(ids.insert(id.as_str().unwrap().to_owned()));
        compare(&actual, &case["expected"], case["name"].as_str().unwrap());
    }
}
#[test]
fn rejects_expiry_zero_quantity_bad_geometry_and_excessive_dynamic_size() {
    let case = &fixtures()[0];
    let mut p = prepared(case);
    assert!(matches!(
        build_trade(&p, 0.95, p.job.trade_expires_at.unwrap()),
        Err(TradeBuildError::Expired)
    ));
    for value in [0.00001, 2.0] {
        p.job.channel_settings["default_quantity"] = json!(value);
        p.settings = resolve_settings(&p.job).unwrap();
        assert!(build_trade(&p, 0.95, 1).is_err());
    }
    p = prepared(case);
    p.job.signal_data["stop_loss"] = json!("0.30");
    assert!(build_trade(&p, 0.95, 1).is_err());
    p = prepared(case);
    p.job.signal_data["sell_targets"] = json!(["0.20"]);
    assert!(build_trade(&p, 0.95, 1).is_err());
}
#[test]
fn static_orders_cannot_exceed_requested_notional_after_entry_prices() {
    let mut p = prepared(&fixtures()[0]);
    p.job.channel_settings["position_size_mode"] = json!("static");
    p.job.channel_settings["static_quote_amount"] = json!(10);
    p.settings = resolve_settings(&p.job).unwrap();
    p.job.signal_data["buy_targets"] = json!(["0.27"]);
    assert!(build_trade(&p, 0.95, 1).is_err());
}

#[test]
fn integer_quotes_use_tick_width_and_quantities_obey_exchange_steps() {
    let cases = fixtures();
    let case = cases.iter().find(|c| c["name"] == "btc_long").unwrap();
    let mut p = prepared(case);
    p.job.market_data["bingXFutures"]["currPrice"] = json!(62000);
    let trade = build_trade(&p, 0.95, 1).unwrap().trade_object;
    assert_eq!(trade["buy_targets"][0]["price"], "62000.00");
    let quantity = trade["quantity"].as_f64().unwrap();
    assert!((quantity / 0.001 - (quantity / 0.001).round()).abs() < 1e-10);
    p.job.market_data["bingXFutures"]["symbolInfo"]["maxQty"] = json!("0.001");
    assert_eq!(
        build_trade(&p, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("orderOutsideExchangeLimits"))
    );
}
#[test]
fn custom_static_fallback_and_user_settings_reach_payload() {
    let cases = fixtures();
    let mut p = prepared(
        cases
            .iter()
            .find(|c| c["name"] == "static_fallback")
            .unwrap(),
    );
    p.job.user_config = Some(
        json!({"user":42,"private_channels":[{"id":-100,"own_settings":true,"futures":{"margin":"cross"}}]}),
    );
    p.settings = resolve_settings(&p.job).unwrap();
    let result = build_trade(&p, 0.8, 1).unwrap();
    assert_eq!(
        result.trade_object["positionSizing"]["fallbackUsageRatio"],
        0.8
    );
    assert_eq!(
        result.trade_object["positionSizing"]["effectiveQuoteAmount"],
        16.0
    );
    assert_eq!(
        result.trade_object["auto_Trade"]["usingChannelConfig"],
        false
    );
    assert_eq!(result.trade_object["futuresMarginMode"], "cross");
    assert_eq!(result.client_data, p.job.client);
}
#[test]
fn rebuilding_preserves_identity_order_ids_and_expiry() {
    let p = prepared(&fixtures()[0]);
    let a = build_trade(&p, 0.95, 1).unwrap();
    let b = build_trade(&p, 0.95, 2).unwrap();
    assert_eq!(
        serde_json::to_value(a).unwrap(),
        serde_json::to_value(b).unwrap()
    );
}

#[test]
fn publishes_only_supplied_exits_for_long_and_short() {
    let cases = fixtures();
    for is_long in [true, false] {
        let case = cases
            .iter()
            .find(|case| case["job"]["signalData"]["is_long"] == is_long)
            .unwrap();
        let baseline = build_trade(&prepared(case), 0.95, 1).unwrap().trade_object;
        for targets in [true, false] {
            for stop in [true, false] {
                let mut p = prepared(case);
                if !targets {
                    p.job.signal_data["sell_targets"] = json!([]);
                    p.job
                        .channel_settings
                        .as_object_mut()
                        .unwrap()
                        .remove("default_sell_targets");
                    p.settings = resolve_settings(&p.job).unwrap();
                }
                if !stop {
                    p.job
                        .signal_data
                        .as_object_mut()
                        .unwrap()
                        .remove("stop_loss");
                }
                let trade = build_trade(&p, 0.95, 1).unwrap().trade_object;
                assert_eq!(trade["buy_targets"], baseline["buy_targets"]);
                assert_eq!(trade["quantity"], baseline["quantity"]);
                assert_eq!(
                    trade["sell_targets"],
                    if targets {
                        baseline["sell_targets"].clone()
                    } else {
                        json!([])
                    }
                );
                assert_eq!(
                    trade.get("stop_loss"),
                    if stop {
                        baseline.get("stop_loss")
                    } else {
                        None
                    }
                );
            }
        }
    }
}

#[test]
fn business_build_failure_uses_the_prepared_reporting_context() {
    let mut account = prepared(&fixtures()[0]);
    account.job.signal_data["position"] = json!(2);
    account.settings = resolve_settings(&account.job).unwrap();
    let Err(TradeBuildError::Rejected(reason)) = build_trade(&account, 0.95, 1) else {
        panic!("business build failure")
    };
    assert_eq!(reason, "positionExceedsBalance");
    let notice = account
        .user_failure(reason)
        .expect("allowlisted builder failure");
    assert_eq!(notice.context.chat_id, account.notification_context.chat_id);
    assert_eq!(notice.context.account_id, account.job.client["clientId"]);
    assert_eq!(notice.context.idempotency_key, account.job.idempotency_key);
    assert_eq!(notice.failure.reason(), reason);
}

fn percentage_account(
    is_long: bool,
    entry: &str,
    quote: &str,
    leverage: u32,
    percentages: &[f64],
) -> PreparedAccount {
    let mut account = prepared(&fixtures()[0]);
    let job = &mut account.job;
    job.symbol = "XLMUSDT".into();
    job.signal_data["symbol"] = json!("XLMUSDT");
    job.signal_data["is_long"] = json!(is_long);
    job.signal_data["buy_targets"] = json!([entry]);
    job.signal_data["leverage"] = json!(format!("{leverage}x"));
    job.signal_data["sell_targets"] = json!(
        percentages
            .iter()
            .map(|value| json!({"type":"percent","value":value,"raw":format!("{value}%")}))
            .collect::<Vec<_>>()
    );
    job.signal_data.as_object_mut().unwrap().remove("stop_loss");
    job.market_data["bingXFutures"]["currPrice"] = json!(quote);
    job.market_data["bingXFutures"]["symbolInfo"]["symbol"] = json!("XLM-USDT");
    let identity = channels_manager_v1::contracts::messages::TradeIdentity {
        channel_id: job.channel_id,
        message_id: job.message_id,
        chat_id: job.client["chatId"].as_i64().unwrap(),
        client_id: job.client["clientId"].as_str().unwrap(),
        symbol: &job.symbol,
    };
    job.idempotency_key = identity.idempotency_key();
    job.partition_key = identity.partition_key();
    job.channel_settings["default_sell_targets"] = json!(
        percentages
            .iter()
            .map(|_| json!({"fraction":1.0 / percentages.len() as f64}))
            .collect::<Vec<_>>()
    );
    account.settings = resolve_settings(job).unwrap();
    account
}
fn profit_prices(account: &PreparedAccount) -> Vec<String> {
    build_trade(account, 0.95, 1).unwrap().trade_object["sell_targets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|target| target["price"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn percentage_xlm_short_and_long_use_resolved_leverage() {
    for (is_long, expected) in [
        (false, ["0.165305", "0.141690", "0.118075", "0.094460"]),
        (true, ["0.212535", "0.236150", "0.259765", "0.283380"]),
    ] {
        let account =
            percentage_account(is_long, "0.18892", "0.188920", 8, &[100., 200., 300., 400.]);
        assert_eq!(profit_prices(&account), expected);
        assert!(
            build_trade(&account, 0.95, 1)
                .unwrap()
                .trade_object
                .get("stop_loss")
                .is_none()
        );
    }
    let account = percentage_account(false, "0.18892", "0.188920", 4, &[100.]);
    assert_eq!(profit_prices(&account), ["0.141690"]);
}

#[test]
fn percentage_reference_uses_selected_entries_and_their_fractions() {
    for (is_long, percentage, expected) in [(true, 80., "126.5000"), (false, 160., "92.0000")] {
        let mut account = percentage_account(is_long, "100", "110.0000", 8, &[percentage]);
        account.job.signal_data["buy_targets"] = json!(["100", "120"]);
        account.job.channel_settings["default_buy_targets"] =
            json!([{"fraction":"0.25"},{"fraction":"0.75"}]);
        account.settings = resolve_settings(&account.job).unwrap();
        assert_eq!(profit_prices(&account), [expected]);
        account.job.channel_settings["default_buy_targets"] = json!([{"fraction":1}]);
        account.settings = resolve_settings(&account.job).unwrap();
        assert_eq!(
            profit_prices(&account),
            [if is_long { "132.0000" } else { "80.0000" }]
        );
    }
}

#[test]
fn percentage_calculated_prices_do_not_use_integer_shorthand() {
    for (percentage, price) in [(300., "1.000000"), (700., "2.000000")] {
        let account = percentage_account(true, "0.25", "0.250000", 1, &[percentage]);
        assert_eq!(profit_prices(&account), [price]);
    }
    let account = percentage_account(false, "100", "100.0000", 8, &[12.5]);
    assert_eq!(profit_prices(&account), ["98.4375"]);
}

#[test]
fn percentage_rounding_and_entry_geometry_cannot_create_invalid_exits() {
    let account = percentage_account(true, "100", "100.00", 8, &[0.001]);
    assert_eq!(
        build_trade(&account, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("profitInsideEntryZone"))
    );
    let account = percentage_account(true, "100", "100.00", 8, &[10., 10.001]);
    assert_eq!(
        build_trade(&account, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("invalidProfitTargetOrder"))
    );
    let mut account = percentage_account(false, "100", "110.00", 8, &[1.]);
    account.job.signal_data["buy_targets"] = json!(["100", "120"]);
    account.job.channel_settings["default_buy_targets"] =
        json!([{"fraction":0.5},{"fraction":0.5}]);
    account.settings = resolve_settings(&account.job).unwrap();
    assert_eq!(
        build_trade(&account, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("profitInsideEntryZone"))
    );
}

#[test]
fn percentage_invalid_short_or_nonfinite_prices_never_build() {
    for values in [vec![800.], vec![900.], vec![100., 800.]] {
        let account = percentage_account(false, "100", "100.00", 8, &values);
        assert_eq!(
            build_trade(&account, 0.95, 1).err(),
            Some(TradeBuildError::Rejected("invalidTradeJob"))
        );
    }
    let account = percentage_account(false, "100", "100.00", 8, &[799.9999]);
    assert!(build_trade(&account, 0.95, 1).is_err()); // Floors to zero at exchange precision.
    let account = percentage_account(true, "1000", "1000.00", 1, &[f64::MAX]);
    assert!(build_trade(&account, 0.95, 1).is_err());
}

fn allocation_account(quantity: f64, is_long: bool) -> PreparedAccount {
    let mut account = prepared(&fixtures()[0]);
    let job = &mut account.job;
    job.symbol = "ETHUSDT".into();
    job.signal_data = json!({"symbol":"ETHUSDT","exchange_client":"_futures","is_long":is_long,"leverage":"1x","buy_targets":["1000"],"sell_targets":if is_long {json!(["1100","1200","1300","1400"])} else {json!(["900","800","700","600"])},"position":1});
    job.market_data = json!({"bingXFutures":{"currPrice":"1000.00","symbolInfo":{"symbol":"ETH-USDT","status":1,"tickSize":0.01,"lotSize":0.01,"minQty":0.01,"minNotional":10}}});
    job.channel_settings = json!({"default_quantity":1,"default_buy_targets":[{"fraction":1}],"default_sell_targets":[{"fraction":0.4},{"fraction":0.3},{"fraction":0.2},{"fraction":0.1}],"strategy":"basic","xPro":null});
    let identity = channels_manager_v1::contracts::messages::TradeIdentity {
        channel_id: job.channel_id,
        message_id: job.message_id,
        chat_id: job.client["chatId"].as_i64().unwrap(),
        client_id: job.client["clientId"].as_str().unwrap(),
        symbol: &job.symbol,
    };
    job.idempotency_key = identity.idempotency_key();
    job.partition_key = identity.partition_key();
    account.settings = resolve_settings(job).unwrap();
    account.admission.available_balance = quantity * 1000.;
    account.admission.available_balance_raw = json!(quantity * 1000.);
    account.notification_context =
        channels_manager_v1::trading::notifications::NotificationContext::from_validated(
            &channels_manager_v1::trading::job::validate_job(job, 1).unwrap(),
        );
    account
}

#[test]
fn reduction_checks_actual_lots_instead_of_target_count() {
    for (quantity, fractions) in [
        (0.01, json!([1.])),
        (0.02, json!([0.5, 0.5])),
        (0.03, json!([0.5, 0.5])),
        (0.04, json!([0.33, 0.33, 0.34])),
        (0.06, json!([0.33, 0.33, 0.34])),
        (0.09, json!([0.33, 0.33, 0.34])),
        (0.10, json!([0.4, 0.3, 0.2, 0.1])),
    ] {
        let account = allocation_account(quantity, true);
        let trade = build_trade(&account, 0.95, 1).unwrap().trade_object;
        let actual: Vec<_> = trade["sell_targets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|target| target["fraction"].clone())
            .collect();
        for (a, b) in actual.iter().zip(fractions.as_array().unwrap()) {
            assert_eq!(a.as_f64(), b.as_f64());
        }
        assert_eq!(
            actual.len(),
            fractions.as_array().unwrap().len(),
            "quantity={quantity}"
        );
        assert_eq!(trade["quantity"], json!(quantity));
    }
}

#[test]
fn reduction_uses_exact_serialized_entry_sum_without_changing_position_wire() {
    let mut account = allocation_account(0.07, true);
    account.job.signal_data["buy_targets"] = json!(["1000", "1000"]);
    account.job.channel_settings["default_buy_targets"] =
        json!([{"fraction":1./7.},{"fraction":6./7.}]);
    account.job.channel_settings["default_sell_targets"] = json!([{"fraction":"1"}]);
    account.job.market_data["bingXFutures"]["symbolInfo"]["minQty"] = json!(0.01);
    account.settings = resolve_settings(&account.job).unwrap();
    let trade = build_trade(&account, 0.95, 1).unwrap().trade_object;
    assert_eq!(trade["buy_targets"][0]["quantity"], 0.01);
    assert_eq!(trade["buy_targets"][1]["quantity"], 0.06);
    assert_eq!(trade["quantity"], json!(0.01_f64 + 0.06_f64));
    assert_eq!(
        trade["positionSizing"]["executableTradeQuantity"],
        json!(0.01_f64 + 0.06_f64)
    );
    assert_eq!(trade["sell_targets"].as_array().unwrap().len(), 1);
    assert_eq!(trade["sell_targets"][0]["fraction"].as_f64(), Some(1.));
    // 7 lots at this price meet 10; 6 lots fail. Float-total allocation must not lose a lot.
    account.job.signal_data["is_long"] = json!(false);
    account.job.signal_data["sell_targets"] = json!(["150"]);
    assert!(build_trade(&account, 0.95, 1).is_ok());
}

#[test]
fn reduction_uses_target_notional_and_retains_effective_quantity_minimum() {
    let mut short = allocation_account(0.01, false);
    short.job.channel_settings["default_sell_targets"] = json!([{"fraction":1}]);
    short.settings = resolve_settings(&short.job).unwrap();
    assert_eq!(
        build_trade(&short, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("insufficientTargetBalance"))
    );
    let mut long = allocation_account(0.01, true);
    long.job.channel_settings["default_sell_targets"] = json!([{"fraction":1}]);
    long.settings = resolve_settings(&long.job).unwrap();
    assert!(build_trade(&long, 0.95, 1).is_ok());
    let mut account = allocation_account(0.03, true);
    account.job.market_data["bingXFutures"]["currPrice"] = json!(500);
    account.job.signal_data["buy_targets"] = json!(["500"]);
    account.admission.available_balance = 15.; // q=.03; effective minQty=10/500=.02.
    account.settings = resolve_settings(&account.job).unwrap();
    let trade = build_trade(&account, 0.95, 1).unwrap().trade_object;
    assert_eq!(trade["sell_targets"].as_array().unwrap().len(), 1);
}

#[test]
fn reduction_validates_omitted_prices_and_preserves_internal_entry_failures() {
    let mut account = allocation_account(0.01, true);
    account.job.channel_settings["default_sell_targets"] = json!([{"fraction":1}]);
    account.job.signal_data["sell_targets"] = json!(["1100", "900"]);
    account.settings = resolve_settings(&account.job).unwrap();
    assert_eq!(
        build_trade(&account, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("profitInsideEntryZone"))
    );
    let account = allocation_account(0.001, true);
    let Err(TradeBuildError::Rejected(reason)) = build_trade(&account, 0.95, 1) else {
        panic!("invalid entry")
    };
    assert!(account.user_failure(reason).is_none());
}

#[test]
fn reduction_applies_pro_minimum_to_single_target_without_basic_multiplier() {
    let mut account = allocation_account(0.02, true);
    account.job.channel_settings["default_sell_targets"] = json!([{"fraction":1}]);
    account.settings = resolve_settings(&account.job).unwrap();
    assert!(build_trade(&account, 0.95, 1).is_ok());
    account.job.channel_settings["xPro"] = json!(1);
    account.settings = resolve_settings(&account.job).unwrap();
    assert_eq!(
        build_trade(&account, 0.95, 1).err(),
        Some(TradeBuildError::Rejected("insufficientTargetBalance"))
    );
    let reason = "insufficientTargetBalance";
    assert!(account.user_failure(reason).is_some());
}

#[test]
fn reported_eth_short_reduces_per_account_without_changing_entry_size() {
    for (balance, size, quantity, count) in [
        (132.1791, 0.1, 0.04, 3),
        (200., 0.1, 0.06, 3),
        (330., 0.1, 0.10, 4),
        (150., 0.3, 0.13, 1),
    ] {
        let mut account = allocation_account(quantity, false);
        account
            .job
            .signal_data
            .as_object_mut()
            .unwrap()
            .remove("position");
        account.job.signal_data["leverage"] = json!("7x");
        account.job.signal_data["buy_targets"] = json!(["2400"]);
        account.job.signal_data["sell_targets"] = json!(["2200", "2100", "2050", "1980"]);
        account.job.signal_data["stop_loss"] = json!("2650");
        account.job.market_data["bingXFutures"]["currPrice"] = json!("2269.99");
        account.job.market_data["bingXFutures"]["symbolInfo"]["minNotional"] = json!(2);
        account.job.channel_settings["default_quantity"] = json!(size);
        if count == 1 {
            account.job.channel_settings["default_sell_targets"] = json!([{"fraction":1}]);
        }
        account.admission.available_balance = balance;
        account.settings = resolve_settings(&account.job).unwrap();
        let trade = build_trade(&account, 0.95, 1).unwrap().trade_object;
        assert_eq!(trade["quantity"], json!(quantity));
        assert_eq!(trade["buy_targets"][0]["price"], "2400.00");
        assert_eq!(trade["sell_targets"].as_array().unwrap().len(), count);
        assert_eq!(trade["sell_targets"][0]["price"], "2200.00");
        assert_eq!(trade["stop_loss"]["price"], "2650.00");
    }
}
