use channels_manager_v1::{
    contracts::messages::ClientTradeJob,
    signals::settings::{PositionSize, PositionSource, resolve_settings},
    trading::job::{JobValidationError, validate_job},
};
use serde_json::{Value, json};
fn job() -> ClientTradeJob {
    let mut v: Value = serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
    v["client"]["key"] = json!("test-key");
    v["client"]["keySecret"] = json!("test-secret");
    v["signalData"]["sell_targets"] = json!([63000]);
    v["signalData"]["stop_loss"] = json!(61000);
    v["marketData"]["bingXFutures"]["symbolInfo"] = json!({"symbol":"BTC-USDT","status":1,"tickSize":"0.01","lotSize":"0.001","minQty":"0.001","minNotional":0});
    v["channelSettings"] = json!({"id":-100,"default_quantity":"0.0500","default_buy_targets":[{"fraction":1}],"default_sell_targets":[{"fraction":"1"}],"strategy":"trailing","xPro":"2"});
    serde_json::from_value(v).unwrap()
}
#[test]
fn validates_identity_and_preserves_wire() {
    let j = job();
    let v = validate_job(&j, 1).unwrap();
    assert_eq!(v.chat_id, 42);
    assert_eq!(v.client_id, "account-1");
    assert_eq!(v.expires_at, 1791021660000);
    assert_eq!(v.job.signal_data, j.signal_data);
}
#[test]
fn expiry_is_required_and_never_rebuilt() {
    let mut j = job();
    for expiry in [None, Some(0), Some(100)] {
        j.trade_expires_at = expiry;
        assert!(validate_job(&j, 100).is_err());
    }
    j.trade_expires_at = Some(101);
    assert!(validate_job(&j, 100).is_ok());
}
#[test]
fn identity_and_credentials_must_match() {
    for field in [
        "idempotency",
        "partition",
        "provider",
        "symbol",
        "key",
        "chat",
    ] {
        let mut j = job();
        match field {
            "idempotency" => j.idempotency_key.push('x'),
            "partition" => j.partition_key.push('x'),
            "provider" => j.client["provider"] = json!("Binance"),
            "symbol" => j.signal_data["symbol"] = json!("ETHUSDT"),
            "key" => j.client["keySecret"] = json!(" "),
            _ => j.client["chatId"] = json!(43),
        }
        assert!(validate_job(&j, 1).is_err(), "{field}");
    }
}
#[test]
fn malformed_signal_is_rejected() {
    for (field, value) in [
        ("is_long", json!("true")),
        ("buy_targets", json!([])),
        ("sell_targets", json!([0])),
        ("stop_loss", json!("NaN")),
        ("leverage", json!(false)),
        ("position", Value::Null),
        ("breakOutEntry", json!("true")),
    ] {
        let mut j = job();
        j.signal_data[field] = value;
        assert_eq!(
            validate_job(&j, 1).err(),
            Some(JobValidationError::InvalidSignal),
            "{field}"
        );
    }
}
#[test]
fn settings_precedence_and_exact_amounts() {
    let mut j = job();
    j.channel_settings["futures"] =
        json!({"active":true,"default_quantity":"0.0700","strategy":"basic"});
    j.user_config = Some(
        json!({"user":42,"private_channels":[{"id":-100,"own_settings":true,"default_quantity":"0.09","futures":{"default_quantity":"0.12500"}}]}),
    );
    let s = resolve_settings(&j).unwrap();
    assert_eq!(s.config["xPro"], Value::Null);
    assert_eq!(
        s.position_size,
        PositionSize::Dynamic {
            source: PositionSource::Channel,
            balance_fraction: json!("0.12500")
        }
    );
}
#[test]
fn inactive_or_unowned_user_settings_do_not_override() {
    let mut j = job();
    j.user_config = Some(
        json!({"user":42,"private_channels":[{"id":-100,"own_settings":false,"default_quantity":null}]}),
    );
    assert_eq!(
        resolve_settings(&j).unwrap().config["default_quantity"],
        json!("0.0500")
    );
    j.channel_settings["futures"] = json!({"active":false,"default_quantity":null});
    assert!(resolve_settings(&j).is_ok());
}
#[test]
fn explicit_null_and_invalid_settings_reject() {
    for (key, value) in [
        ("default_quantity", Value::Null),
        ("default_buy_targets", json!([])),
        ("strategy", json!(" ")),
        ("xPro", json!(-1)),
        ("position_size_mode", json!("garbage")),
    ] {
        let mut j = job();
        j.channel_settings["futures"] = json!({"active":true,key:value});
        assert!(resolve_settings(&j).is_err(), "{key}");
    }
}
#[test]
fn signal_position_overrides_static_and_invalid_modes() {
    let mut j = job();
    j.channel_settings["position_size_mode"] = json!("static");
    j.channel_settings["static_quote_amount"] = json!("12.3400");
    assert_eq!(
        resolve_settings(&j).unwrap().position_size,
        PositionSize::Static {
            quote_amount: json!("12.3400")
        }
    );
    j.signal_data["position"] = json!("0.00100");
    j.channel_settings["position_size_mode"] = json!("invalid");
    assert_eq!(
        resolve_settings(&j).unwrap().position_size,
        PositionSize::Dynamic {
            source: PositionSource::Signal,
            balance_fraction: json!("0.00100")
        }
    );
    j.signal_data["position"] = Value::Null;
    assert!(resolve_settings(&j).is_err());
}

#[test]
fn snapshot_requires_valid_normalized_metadata() {
    for (field, value) in [
        ("currPrice", json!(0)),
        ("symbol", json!("ETH-USDT")),
        ("tickSize", json!(0)),
        ("lotSize", Value::Null),
        ("minNotional", json!(-1)),
    ] {
        let mut j = job();
        if field == "currPrice" {
            j.market_data["bingXFutures"][field] = value;
        } else {
            j.market_data["bingXFutures"]["symbolInfo"][field] = value;
        }
        assert_eq!(
            validate_job(&j, 1).err(),
            Some(JobValidationError::InvalidMarketData)
        );
    }
}

#[test]
fn margin_mode_defaults_and_uses_inactive_futures_settings() {
    let mut j = job();
    assert_eq!(resolve_settings(&j).unwrap().margin_mode, "isolated");
    for (value, expected) in [
        (Value::Null, "isolated"),
        (json!(""), "isolated"),
        (json!("CROSS"), "cross"),
        (json!("ISOLATED"), "isolated"),
    ] {
        j.channel_settings["futures"] = json!({"active":false,"margin":value});
        assert_eq!(resolve_settings(&j).unwrap().margin_mode, expected);
    }
    j.channel_settings["futures"]["margin"] = json!("bad");
    assert!(resolve_settings(&j).is_err());
}

#[test]
fn leverage_accepts_at_most_one_suffix() {
    let mut j = job();
    for value in [json!(10), json!("10"), json!("10x"), json!("10X")] {
        j.signal_data["leverage"] = value;
        assert_eq!(validate_job(&j, 1).unwrap().requested_leverage, 10);
    }
    for value in ["5xx", "5XX", "5xX"] {
        j.signal_data["leverage"] = json!(value);
        assert_eq!(
            validate_job(&j, 1).err(),
            Some(JobValidationError::InvalidSignal)
        );
    }
}

#[test]
fn error_snapshot_rejects_even_with_valid_metadata() {
    let mut j = job();
    j.market_data["bingXFutures"]["err"] = json!({"code":"unavailable"});
    assert_eq!(
        validate_job(&j, 1).err(),
        Some(JobValidationError::InvalidMarketData)
    );
}
