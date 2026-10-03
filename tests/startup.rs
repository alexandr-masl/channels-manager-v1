use std::process::Command;

#[test]
fn invalid_configuration_exits_nonzero_without_exposing_the_uri() {
    let output = Command::new(env!("CARGO_BIN_EXE_channels-manager-v1"))
        .arg("--check-config")
        .env_clear()
        .env("RABBIT_MQ", "http://user:private-password@host")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("RABBIT_MQ"));
    assert!(!error.contains("private-password"));
}

#[test]
fn valid_configuration_is_checked_without_opening_connections() {
    let output = Command::new(env!("CARGO_BIN_EXE_channels-manager-v1"))
        .arg("--check-config")
        .env_clear()
        .env("RABBIT_MQ", "amqp://unreachable.invalid")
        .env("MONGO_PATH", "mongodb://unreachable.invalid/bot")
        .env(
            "TRADE_STATION_MONGO_PATH",
            "mongodb://unreachable.invalid/trading",
        )
        .env("REDIS", "unreachable.invalid")
        .env(
            "ACCOUNT_VALIDATOR_MONGO_PATH",
            "mongodb://unreachable.invalid/accounts",
        )
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Configuration valid"));
}

#[test]
fn unknown_startup_flags_fail_without_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_channels-manager-v1"))
        .arg("--consume")
        .env_clear()
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Usage:"));
}
