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

fn bounded_output(command: &mut Command) -> std::process::Output {
    use std::{
        process::{Child, Stdio},
        time::{Duration, Instant},
    };
    struct Process(Option<Child>);
    impl Drop for Process {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let mut process = Process(Some(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if process.0.as_mut().unwrap().try_wait().unwrap().is_some() {
            return process.0.take().unwrap().wait_with_output().unwrap();
        }
        assert!(Instant::now() < deadline, "startup exceeded deadline");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn check_config_with_missing_authorization_never_connects_to_tripwires() {
    let app = TestApp::new(None);
    let listeners: Vec<_> = (0..5)
        .map(|_| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            listener
        })
        .collect();
    let mut command = app.command();
    command
        .arg("--check-config")
        .env("APP_AUTHORIZATION_FILE", app.0.join("missing.json"))
        .env(
            "RABBIT_MQ",
            format!("amqp://{}", listeners[0].local_addr().unwrap()),
        );
    for (key, listener) in [
        "MONGO_PATH",
        "TRADE_STATION_MONGO_PATH",
        "ACCOUNT_VALIDATOR_MONGO_PATH",
    ]
    .iter()
    .zip(&listeners[1..4])
    {
        command.env(
            key,
            format!("mongodb://{}/fixture", listener.local_addr().unwrap()),
        );
    }
    command.env("REDIS", "127.0.0.1").env(
        "REDIS_CLIENT_PORT",
        listeners[4].local_addr().unwrap().port().to_string(),
    );
    let output = bounded_output(&mut command);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"Configuration valid.\n");
    assert!(output.stderr.is_empty());
    for listener in listeners {
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[test]
fn unsupported_arguments_precede_authorization_and_configuration() {
    let app = TestApp::new(Some(r#"RABBIT_MQ="private-password"#));
    for args in [
        vec!["--help"],
        vec!["--check-config", "--consume"],
        vec!["--check-config", "--check-config"],
    ] {
        let output = bounded_output(
            app.command()
                .args(args)
                .env("APP_AUTHORIZATION_FILE", app.0.join("missing.json")),
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(
            output.stderr,
            b"Usage: channels-manager-v1 [--check-config]\n"
        );
    }
}

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
