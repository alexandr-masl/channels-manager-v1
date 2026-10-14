use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

const NOTICE: &str = "Authorization required. To run this application, obtain permission from the repository owner, Alexandr Maslov. Repository or container access alone does not authorize operation.";
struct App(PathBuf);
impl App {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("authorization-startup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_channels-manager-v1"));
        command
            .current_dir(&self.0)
            .env_clear()
            .env("APP_AUTHORIZATION_FILE", self.0.join("authorization.json"));
        command
    }
    fn write(&self, value: &serde_json::Value) {
        std::fs::write(
            self.0.join("authorization.json"),
            serde_json::to_vec(value).unwrap(),
        )
        .unwrap();
    }
}
impl Drop for App {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Process(Option<Child>);
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn output(command: &mut Command) -> Output {
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
fn rejected(output: Output, code: &str) {
    assert_eq!(
        output.status.code(),
        Some(78),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        format!("Authorization error: {code}\n{NOTICE}\n")
    );
}
fn fixture() -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(include_str!("fixtures/authorization/vectors.json"))
        .unwrap()["authorization_vectors"][0]["envelope"]
        .clone()
}
#[test]
fn missing_authorization_precedes_invalid_configuration_and_dotenv() {
    let app = App::new();
    std::fs::write(app.0.join(".env.local"), "RABBIT_MQ=\"private-secret").unwrap();
    rejected(
        output(app.command().env("RABBIT_MQ", "private-invalid-uri")),
        "AUTH_MISSING",
    );
}
#[test]
fn malformed_unknown_key_invalid_signature_and_public_fixture_fail_closed() {
    let app = App::new();
    for (value, code) in [
        (serde_json::json!({"private-secret":true}), "AUTH_MALFORMED"),
        (
            serde_json::json!({"key_id":"unknown-secret", "payload":"", "signature":""}),
            "AUTH_UNKNOWN_KEY",
        ),
        (
            serde_json::json!({"key_id":"owner-2026-01", "payload":"e30", "signature":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}),
            "AUTH_INVALID_SIGNATURE",
        ),
        (fixture(), "AUTH_UNKNOWN_KEY"),
    ] {
        app.write(&value);
        rejected(output(&mut app.command()), code);
    }
}
#[test]
fn production_ignores_test_runtime_and_disable_markers() {
    let app = App::new();
    app.write(&fixture());
    rejected(
        output(
            app.command()
                .env("CHANNELS_AUTHORIZATION_RUNTIME_CHILD", "1")
                .env("APP_AUTHORIZATION_DISABLED", "1")
                .env("APP_AUTHORIZATION_TRUST", "test-rfc8032-01"),
        ),
        "AUTH_UNKNOWN_KEY",
    );
}
#[test]
fn authorization_rejection_opens_no_infrastructure_connections() {
    let app = App::new();
    let listeners: Vec<_> = (0..5)
        .map(|_| {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.set_nonblocking(true).unwrap();
            l
        })
        .collect();
    let mut command = app.command();
    command.env(
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
    rejected(output(&mut command), "AUTH_MISSING");
    for listener in listeners {
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
