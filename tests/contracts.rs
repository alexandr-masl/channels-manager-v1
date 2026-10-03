use channels_manager_v1::{
    config::AppConfig,
    contracts::{dependencies::*, messages::*, rabbitmq::*, storage::*},
};
use serde_json::{Value, json};

fn config() -> AppConfig {
    AppConfig::from_lookup(|key| match key {
        "RABBIT_MQ" => Some("amqp://localhost".into()),
        "MONGO_PATH" | "TRADE_STATION_MONGO_PATH" | "ACCOUNT_VALIDATOR_MONGO_PATH" => {
            Some("mongodb://localhost/test".into())
        }
        "REDIS" => Some("localhost".into()),
        "RABBITMQ_QUEUE" => Some("custom-trades".into()),
        _ => None,
    })
    .unwrap()
}

#[test]
fn typescript_job_round_trips_without_losing_nested_fields() {
    let value: Value = serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
    let job: ClientTradeJob = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(job.channel_id, -100);
    assert_eq!(job.trade_expires_at, Some(1791021660000));
    assert_eq!(serde_json::to_value(job).unwrap(), value);
}

#[test]
fn optional_job_fields_remain_absent_when_absent() {
    let mut value: Value = serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
    for key in ["sourceCreatedAt", "tradeExpiresAt", "userConfig"] {
        value.as_object_mut().unwrap().remove(key);
    }
    let job: ClientTradeJob = serde_json::from_value(value.clone()).unwrap();
    assert!(job.trade_expires_at.is_none());
    assert_eq!(serde_json::to_value(job).unwrap(), value);
}

#[test]
fn unsupported_job_discriminants_are_rejected() {
    for (key, value) in [
        ("version", json!(2)),
        ("jobType", json!("close_trade")),
        ("provider", json!("Binance")),
        ("market", json!("spot")),
    ] {
        let mut job: Value = serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
        job[key] = value;
        assert!(
            serde_json::from_value::<ClientTradeJob>(job).is_err(),
            "{key}"
        );
    }
}

#[test]
fn outbound_envelope_preserves_expiry_and_exact_top_level_keys() {
    let value = json!({"expires_at":1791021660000_i64,"trade_object":{"id":"stable-id","unknown":{"v":1}},
        "client_data":{"provider":"BingX","api_secret":"fixture-secret"}});
    let message: NewTradeMessage = serde_json::from_value(value.clone()).unwrap();
    assert!(!message.is_expired(1791021659999));
    assert!(message.is_expired(1791021660000));
    assert_eq!(serde_json::to_value(&message).unwrap(), value);
    let mut invalid = value;
    invalid.as_object_mut().unwrap().remove("expires_at");
    assert!(serde_json::from_value::<NewTradeMessage>(invalid).is_err());
}

#[test]
fn stable_trade_identity_matches_node_sha256() {
    let identity = TradeIdentity {
        channel_id: -100,
        message_id: 7,
        chat_id: 42,
        client_id: "account-1",
        symbol: "BTCUSDT",
    };
    assert_eq!(
        identity.idempotency_key(),
        "auto-trade:-100:7:42:account-1:BingX:futures:BTCUSDT"
    );
    assert_eq!(
        identity.partition_key(),
        "client-symbol:account-1:futures:BTCUSDT"
    );
    assert_eq!(
        trade_creation_id(&identity.idempotency_key()),
        "b3e6c5f0d6048b687de5e4d462f0fbf1"
    );
}

#[test]
fn topology_keeps_ephemeral_queues_and_retry_return_route() {
    let queues = queue_contracts(&config().rabbitmq);
    assert_eq!(queues.len(), 5);
    assert!(
        queues
            .iter()
            .all(|q| !q.durable && !q.exclusive && !q.auto_delete)
    );
    assert!(queues.iter().any(|q| q.name == "custom-trades"));
    assert!(
        queues
            .iter()
            .any(|q| q.name == "tg_bot_bingx_position_mode_action_required")
    );
    assert!(
        queues
            .iter()
            .any(|q| q.name == "satoshi-channel-updates.dead-letter")
    );
    let retry = queues
        .iter()
        .find(|q| q.name.ends_with(".retry.delay"))
        .unwrap();
    assert_eq!(
        retry.arguments,
        json!({"x-message-ttl":1000,"x-dead-letter-exchange":"", "x-dead-letter-routing-key":"satoshi-channel-updates.client-trade.bingx.futures"})
    );
    let options = trade_publish_contract();
    assert_eq!(options.exchange, "");
    assert_eq!(options.delivery_mode, 1);
    assert!(options.mandatory && options.confirm);
    assert_eq!(options.content_type, "application/json");
}

#[test]
fn redis_keys_match_encode_uri_component_in_typescript() {
    assert_eq!(
        account_lease_key(DEFAULT_LOCK_PREFIX, "a/b:é !'()*"),
        "satoshi-channel-updates:locks:v1:bingx_futures_account_admission:BingX%3Afutures%3Aa%2Fb%3A%C3%A9%20!'()*"
    );
    assert_eq!(
        bingx_metadata_key(DEFAULT_CACHE_PREFIX, "BTC-USDT"),
        "satoshi-channel-updates:exchange-metadata:v1:bingx_symbol_metadata:BTC-USDT"
    );
    assert!(
        !dependency_contracts()
            .iter()
            .any(|d| d.name == "redis.notifications")
    );
    assert_eq!(ACCOUNT_LEASE_TTL.as_secs(), 30);
    assert_eq!(ACCOUNT_LEASE_RENEWAL.as_secs(), 10);
}

#[test]
fn mongo_contract_matches_mongoose_collection_names_and_claim_index() {
    assert_eq!(USER_CONFIGS_COLLECTION, "user_configs");
    assert_eq!(ACTIVE_TRADES_COLLECTION, "trade_station_active_trades");
    assert_eq!(CLAIMS_COLLECTION, "bingx_futures_execution_claims");
    let index = execution_claim_index();
    assert_eq!(index.name, "uq_bingx_futures_execution_claim_work_id");
    assert_eq!(index.keys, json!({"workId":1}));
    assert!(index.unique);
    assert_eq!(index.expire_after_seconds, None);
}

#[test]
fn required_dependencies_fail_closed_and_cache_fails_open() {
    let dependencies = dependency_contracts();
    for name in [
        "mongodb.bot",
        "mongodb.tradeStation",
        "mongodb.executionClaims",
        "rabbitmq.publisher",
        "rabbitmq.consumer.bingxFutures",
        "redis.locks",
    ] {
        assert_eq!(
            dependencies
                .iter()
                .find(|d| d.name == name)
                .unwrap()
                .requirement,
            Requirement::Required
        );
    }
    let cache = dependencies
        .iter()
        .find(|d| d.name == "redis.exchangeMetadataCache")
        .unwrap();
    assert_eq!(cache.requirement, Requirement::Optional);
    assert_eq!(cache.failure_policy, FailurePolicy::Fallback);
    assert!(
        dependencies
            .iter()
            .any(|d| d.name == "mongodb.accountValidator")
    );
}

#[test]
fn explicit_null_user_config_is_preserved() {
    let mut value: Value = serde_json::from_str(include_str!("fixtures/bingx-job.json")).unwrap();
    value["userConfig"] = Value::Null;
    let job: ClientTradeJob = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(job).unwrap(), value);
}

#[test]
fn additional_trade_envelope_fields_are_rejected() {
    let value =
        json!({"expires_at":1791021660000_i64,"trade_object":{},"client_data":{},"extra":true});
    assert!(serde_json::from_value::<NewTradeMessage>(value).is_err());
}

#[test]
fn admission_event_preserves_wire_field_names() {
    let value = json!({
        "eventType":"BINGX_FUTURES_POSITION_MODE_ACTION_REQUIRED",
        "eventId":"fixture-event", "chatId":42, "exchangeClientId":"account-1",
        "provider":"BingX", "market":"futures", "normalizedSymbol":"BTC-USDT",
        "channelId":-100, "signalMessageId":7, "signalSymbol":"BTCUSDT",
        "signalSide":"LONG", "reasonCode":"MIGRATION_REQUIRED",
        "createdAt":"2026-10-03T10:00:00.000Z"
    });
    let event: PositionModeActionRequired = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(event).unwrap(), value);
    assert_eq!(
        serde_json::to_value(ExecutionRoute::Hedge).unwrap(),
        "ORDER_LEDGER_V1"
    );
    assert_eq!(
        serde_json::to_value(ExecutionRoute::OneWay).unwrap(),
        "ONE_WAY_V1"
    );
}
