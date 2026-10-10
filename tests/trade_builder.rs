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
