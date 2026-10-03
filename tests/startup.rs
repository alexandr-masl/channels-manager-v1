use std::{path::PathBuf, process::Command};

struct TestApp(PathBuf);
impl TestApp {
    fn new(contents: Option<&str>) -> Self {
        let path = std::env::temp_dir().join(format!("channels-env-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        if let Some(contents) = contents {
            std::fs::write(path.join(".env.local"), contents).unwrap();
        }
        Self(path)
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_channels-manager-v1"));
        command.current_dir(&self.0).env_clear();
        command
    }
}
impl Drop for TestApp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
const LOCAL: &str = "RABBIT_MQ='amqp://unreachable.invalid'\nMONGO_PATH=mongodb://unreachable.invalid/bot\nTRADE_STATION_MONGO_PATH=mongodb://unreachable.invalid/trading\nACCOUNT_VALIDATOR_MONGO_PATH=mongodb://unreachable.invalid/accounts\nREDIS=unreachable.invalid\n";

#[test]
fn invalid_configuration_exits_nonzero_without_exposing_the_uri() {
    let app = TestApp::new(None);
    let output = app
        .command()
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
    let app = TestApp::new(None);
    let output = app
        .command()
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
    let app = TestApp::new(None);
    let output = app.command().arg("--consume").env_clear().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Usage:"));
}

#[test]
fn local_environment_file_is_loaded_without_opening_connections() {
    let app = TestApp::new(Some(LOCAL));
    let output = app.command().arg("--check-config").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn exported_environment_overrides_local_file() {
    let app = TestApp::new(Some(
        &LOCAL.replace("amqp://unreachable.invalid", "invalid-url"),
    ));
    let output = app
        .command()
        .arg("--check-config")
        .env("RABBIT_MQ", "amqp://exported.invalid")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn malformed_environment_file_is_reported_without_secrets() {
    let app = TestApp::new(Some(r#"RABBIT_MQ="private-password"#));
    let output = app.command().arg("--check-config").output().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains(".env.local"));
    assert!(!error.contains("private-password"));
}
