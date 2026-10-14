//! This module is compiled only into integration test executables. Production
//! always uses embedded owner trust; this harness verifies the public RFC fixture.
use std::{
    path::Path,
    process::{Command, ExitCode},
};

const MARKER: &str = "CHANNELS_AUTHORIZATION_RUNTIME_CHILD";

pub fn command(directory: &Path) -> Command {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/authorization/vectors.json")).unwrap();
    let path = directory.join("test-authorization.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&fixture["authorization_vectors"][0]["envelope"]).unwrap(),
    )
    .unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "authorization_runtime_child", "--nocapture"])
        .current_dir(directory)
        .env_clear()
        .env(MARKER, "1")
        .env("APP_AUTHORIZATION_FILE", path);
    command
}

pub fn run_child() {
    if std::env::var_os(MARKER).as_deref() != Some(std::ffi::OsStr::new("1")) {
        return;
    }
    let key_hex = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    let key = std::array::from_fn(|i| u8::from_str_radix(&key_hex[i * 2..i * 2 + 2], 16).unwrap());
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(crate::startup::run(vec![], || {
            crate::authorization::check_startup_with_test_trust(&[("test-rfc8032-01", key)])
        }));
    std::process::exit(if result == ExitCode::SUCCESS {
        0
    } else if result == ExitCode::from(78) {
        78
    } else {
        1
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        process::{Child, Output, Stdio},
        time::{Duration, Instant},
    };
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("authorization-harness-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
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
            assert!(
                Instant::now() < deadline,
                "test runtime exceeded startup deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    #[test]
    fn signed_fixture_reaches_shared_startup_configuration() {
        let directory = Directory::new();
        let result = output(command(&directory.0).env("RABBIT_MQ", "invalid-fixture-uri"));
        assert_eq!(result.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&result.stdout).contains("running 1 test"));
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stderr.contains("Configuration error: RABBIT_MQ"),
            "{stderr}"
        );
        assert!(!stderr.contains("AUTH_"));
    }
    #[test]
    fn tampered_fixture_is_rejected_by_shared_startup() {
        let directory = Directory::new();
        let mut command = command(&directory.0);
        let path = directory.0.join("test-authorization.json");
        let mut fixture: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        fixture["payload"] = "e30".into();
        std::fs::write(path, serde_json::to_vec(&fixture).unwrap()).unwrap();
        let result = output(&mut command);
        assert_eq!(result.status.code(), Some(78));
        assert!(String::from_utf8_lossy(&result.stdout).contains("running 1 test"));
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stderr.starts_with("Authorization error: AUTH_INVALID_SIGNATURE\n"),
            "{stderr}"
        );
        assert!(!stderr.contains("Configuration"));
    }
}
