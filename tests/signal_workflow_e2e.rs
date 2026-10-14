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
use signal_workflow::{Mode, Scenario, Signal};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts isolated RabbitMQ, MongoDB, Redis and signed HTTP; requires loopback access"]
async fn signal_workflow_matrix() {
    let broker = services::Broker::start().await;
    let mut scenarios = vec![
        Scenario::base("ada-matching-leverage", Signal::Ada, 3),
        Scenario::base("sol-range-position-override", Signal::Sol, 50),
        Scenario::base("ada-channel-fallback", Signal::Missing, 3),
    ];
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
    for scenario in scenarios {
        signal_workflow::run(&broker, scenario).await;
    }
}
