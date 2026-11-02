#[path = "../src/authorization/mod.rs"]
mod authorization;
#[path = "support/authorization_process.rs"]
mod authorization_process;
#[path = "../src/startup.rs"]
mod startup;

#[test]
fn authorization_runtime_child() {
    authorization_process::run_child();
}

#[path = "support/scripted_exchange.rs"]
mod scripted_exchange;
#[path = "support/services.rs"]
mod services;
#[path = "support/signal_workflow.rs"]
mod signal_workflow;
use serde_json::json;
use signal_workflow::{AllocationExpectation, Mode, PercentageExpectation, Scenario, Signal};

fn percentage_scenario(name: &'static str) -> Scenario {
    let mut scenario = Scenario::base(name, Signal::Xlm, 8);
    scenario.percentage = Some(PercentageExpectation {
        parsed: json!({"exchange_client":"_futures","symbol":"XLMUSDT","base_currency":"USDT","coin":"XLM","is_long":false,"buy_targets":["0.18892"],"sell_targets":[{"type":"percent","value":100.0,"raw":"100%"},{"type":"percent","value":200.0,"raw":"200%"},{"type":"percent","value":300.0,"raw":"300%"},{"type":"percent","value":400.0,"raw":"400%"}],"leverage":"8x","position":0.005}),
        prices: vec![vec!["0.165305", "0.141690", "0.118075", "0.094460"]],
        entries: vec![vec![("0.188920", 1.0, 211.7)]],
    });
    scenario
}

fn allocation_scenario(name: &'static str) -> Scenario {
    let mut scenario = Scenario::base(name, Signal::Eth, 1);
    scenario.overrides = vec![None, None, None];
    scenario.resolved = vec![1, 1, 1];
    scenario.allocation = Some(AllocationExpectation {
        balances: vec!["100", "900", "2000"],
        min_qty: 0.01,
        configured_fractions: vec![0.4, 0.3, 0.2, 0.1],
        quantities: vec![0.01, 0.09, 0.2],
        profits: vec![
            None,
            Some(vec![("900.00", 0.33), ("800.00", 0.33), ("700.00", 0.34)]),
            Some(vec![
                ("900.00", 0.4),
                ("800.00", 0.3),
                ("700.00", 0.2),
                ("600.00", 0.1),
            ]),
        ],
        failures: vec![Some("insufficientTargetBalance"), None, None],
    });
    scenario.rejection_reason = Some("insufficientTargetBalance");
    scenario
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB, Redis and signed HTTP; requires loopback access"]
async fn signal_workflow_matrix() {
    let broker = services::Broker::start().await;
    let mut scenarios = vec![
        Scenario::base("ada-matching-leverage", Signal::Ada, 3),
        Scenario::base("sol-range-position-override", Signal::Sol, 50),
        Scenario::base("ada-channel-fallback", Signal::Missing, 3),
    ];
    for (name, targets, stop) in [
        ("ada-targets-and-stop", true, true),
        ("ada-targets-without-stop", true, false),
        ("ada-stop-without-targets", false, true),
        ("ada-without-exits", false, false),
    ] {
        let mut scenario = Scenario::base(name, Signal::Ada, 3);
        scenario.targets = targets;
        scenario.stop = stop;
        scenarios.push(scenario);
    }
    let mut decorated = Scenario::base("ada-decorated-metadata", Signal::Ada, 3);
    decorated.decorated = true;
    scenarios.push(decorated);
    let mut commentary = Scenario::base("regular-channel-commentary", Signal::Ada, 3);
    commentary.raw_text = Some(
        "ADA/USDT is at 0.217. LONG interest is growing; BREAKOUT could follow.\nReference VQA-83827.1",
    );
    commentary.channel_terminal = Some("Channel update skipped:");
    scenarios.push(commentary);
    let mut management = Scenario::base("signal-mixed-close-instruction", Signal::Ada, 3);
    management.raw_text = Some(concat!(
        "#ADA/USDT LONG\nENTRY 0.217\nTP1 0.22\nSL 0.18\nLEVERAGE 3x\n",
        "CLOSE ADA/USDT"
    ));
    management.channel_terminal = Some("Channel update rejected:");
    scenarios.push(management);
    let mut changed = Scenario::base("ada-change-leverage", Signal::Ada, 3);
    changed.current_leverage = 5;
    scenarios.push(changed);
    let mut switched = Scenario::base("one-way-migrates-then-leverage", Signal::Ada, 3);
    switched.mode = Mode::Switch;
    switched.current_leverage = 5;
    scenarios.push(switched);
    let mut one_way = Scenario::base("one-way-permitted", Signal::Ada, 3);
    one_way.mode = Mode::OneWay;
    scenarios.push(one_way);
    let mut overrides = Scenario::base("two-user-leverage-overrides", Signal::Missing, 5);
    overrides.overrides = vec![Some(json!(5)), Some(json!(7))];
    overrides.resolved = vec![5, 7];
    overrides.current_leverage = 3;
    scenarios.push(overrides);
    let mut explicit = Scenario::base("explicit-leverage-wins", Signal::Ada, 3);
    explicit.overrides = vec![Some(json!(7))];
    explicit.channel_leverage = Some(json!(5));
    scenarios.push(explicit);
    let mut absent = Scenario::base("missing-fallback", Signal::Missing, 3);
    absent.channel_leverage = None;
    absent.resolved.clear();
    scenarios.push(absent);
    let mut invalid = Scenario::base("invalid-selected-fallback", Signal::Missing, 3);
    invalid.overrides = vec![Some(json!(0))];
    invalid.resolved.clear();
    scenarios.push(invalid);
    let mut blocked = Scenario::base("blocked-leverage-change", Signal::Ada, 3);
    blocked.current_leverage = 5;
    blocked.blocked = true;
    scenarios.push(blocked);
    let mut balance = Scenario::base("insufficient-balance-history", Signal::Ada, 3);
    balance.balance = "0";
    scenarios.push(balance);
    let mut limit = Scenario::base("leverage-limit-history", Signal::Ada, 3);
    limit.max_leverage = 2;
    scenarios.push(limit);
    let mut retry = Scenario::base("history-write-retries-only", Signal::Ada, 3);
    retry.balance = "0";
    retry.history_write_failures = true;
    scenarios.push(retry);
    let mut malformed = Scenario::base("malformed-client-work", Signal::Ada, 3);
    malformed.malformed = true;
    scenarios.push(malformed);
    let mut failed = Scenario::base("failed-leverage-post", Signal::Ada, 3);
    failed.current_leverage = 5;
    failed.post_fails = true;
    scenarios.push(failed);
    let mut expired = Scenario::base("expired-client-work", Signal::Ada, 3);
    expired.expired = true;
    scenarios.push(expired);
    let mut reply = Scenario::base("telegram-failure-still-publishes", Signal::Ada, 3);
    reply.telegram_fails = true;
    scenarios.push(reply);
    scenarios.push(percentage_scenario("xlm-short-percentages"));
    let mut long = percentage_scenario("xlm-long-percentages");
    long.raw_text = Some(include_str!("fixtures/signals/xlm-long-8x.txt"));
    let expected = long.percentage.as_mut().unwrap();
    expected.parsed["is_long"] = json!(true);
    expected.prices = vec![vec!["0.212535", "0.236150", "0.259765", "0.283380"]];
    scenarios.push(long);
    let mut range = percentage_scenario("xlm-weighted-entry-percentages");
    range.raw_text = Some(include_str!("fixtures/signals/xlm-range-8x.txt"));
    let expected = range.percentage.as_mut().unwrap();
    expected.parsed["buy_targets"] = json!(["0.18", "0.20"]);
    expected.entries = vec![vec![("0.180000", 0.25, 52.9), ("0.200000", 0.75, 158.7)]];
    expected.prices = vec![vec!["0.170625", "0.146250", "0.121875", "0.097500"]];
    scenarios.push(range);
    let mut users = percentage_scenario("xlm-per-user-percentage-leverage");
    users.raw_text = Some(include_str!("fixtures/signals/xlm-no-leverage.txt"));
    users.overrides = vec![Some(json!(5)), Some(json!(10))];
    users.resolved = vec![5, 10];
    users.current_leverage = 3;
    let expected = users.percentage.as_mut().unwrap();
    expected.parsed.as_object_mut().unwrap().remove("leverage");
    expected.entries = vec![
        vec![("0.188920", 1.0, 132.3)],
        vec![("0.188920", 1.0, 264.6)],
    ];
    expected.prices = vec![
        vec!["0.151136", "0.113352", "0.075568", "0.037784"],
        vec!["0.170028", "0.151136", "0.132244", "0.113352"],
    ];
    scenarios.push(users);
    let mut explicit = percentage_scenario("xlm-explicit-percentage-leverage");
    explicit.overrides = vec![Some(json!(5))];
    scenarios.push(explicit);
    for (name, text) in [
        (
            "xlm-percent-then-price",
            "XLMUSDT SHORT\nENTRY 0.18892\nTG1 100%\nTG2 0.14\nLEVERAGE 8x",
        ),
        (
            "xlm-price-then-percent",
            "XLMUSDT SHORT\nENTRY 0.18892\nTG1 0.17\nTG2 200%\nLEVERAGE 8x",
        ),
    ] {
        let mut mixed = Scenario::base(name, Signal::Xlm, 8);
        mixed.raw_text = Some(text);
        mixed.channel_terminal = Some("Channel update rejected:");
        mixed.rejection_reason = Some("MixedTargetFormats");
        scenarios.push(mixed);
    }
    let mut ceiling = percentage_scenario("xlm-short-percentage-ceiling");
    ceiling.raw_text =
        Some("XLMUSDT SHORT\nENTRY 0.18892\nTG1 800%\nLEVERAGE 8x\nPOSITION SIZE 0.5%");
    ceiling.resolved.clear();
    ceiling.rejection_reason = Some("InvalidTargets");
    ceiling.percentage.as_mut().unwrap().parsed["sell_targets"] =
        json!([{"type":"percent","value":800.0,"raw":"800%"}]);
    scenarios.push(ceiling);
    // Same signal, different account sizes: valid entry but no feasible exit,
    // reduction to three, and preservation of the configured four-target split.
    scenarios.push(allocation_scenario(
        "eth-account-specific-profit-allocation",
    ));
    let mut percent = allocation_scenario("eth-percentage-profit-allocation");
    percent.raw_text =
        Some("ETHUSDT SHORT\nENTRY 1000\nTG1 10%\nTG2 20%\nTG3 30%\nTG4 40%\nLEVERAGE 1x\nSL 1100");
    scenarios.push(percent);

    let mut single = allocation_scenario("eth-configured-single-profit-target");
    single.overrides = vec![None];
    single.resolved = vec![1];
    single.rejection_reason = None;
    let expected = single.allocation.as_mut().unwrap();
    expected.balances = vec!["200"];
    expected.quantities = vec![0.02];
    // One configured target must keep one target even with four source prices.
    expected.configured_fractions = vec![1.0];
    expected.profits = vec![Some(vec![("900.00", 1.0)])];
    expected.failures = vec![None];
    scenarios.push(single);

    for (name, targets, stop) in [
        ("eth-reduced-profits-without-stop", true, false),
        ("eth-stop-without-profits", false, true),
        ("eth-without-exits", false, false),
    ] {
        let mut optional = allocation_scenario(name);
        optional.targets = targets;
        optional.stop = stop;
        optional.overrides = vec![None];
        optional.resolved = vec![1];
        optional.rejection_reason = None;
        let expected = optional.allocation.as_mut().unwrap();
        expected.balances = vec!["900"];
        expected.quantities = vec![0.09];
        expected.profits = vec![Some(if targets {
            vec![("900.00", 0.33), ("800.00", 0.33), ("700.00", 0.34)]
        } else {
            vec![]
        })];
        expected.failures = vec![None];
        scenarios.push(optional);
    }

    let mut entry_minimum = allocation_scenario("eth-entry-minimum-is-internal");
    entry_minimum.overrides = vec![None];
    entry_minimum.resolved = vec![1];
    entry_minimum.rejection_reason = Some("orderOutsideExchangeLimits");
    let expected = entry_minimum.allocation.as_mut().unwrap();
    expected.balances = vec!["100"];
    expected.min_qty = 0.02;
    expected.quantities = vec![0.01];
    expected.profits = vec![None];
    expected.failures = vec![None];
    scenarios.push(entry_minimum);
    for scenario in scenarios {
        signal_workflow::run(&broker, scenario).await;
    }
}
