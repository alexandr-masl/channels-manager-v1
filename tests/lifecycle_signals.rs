#![cfg(unix)]

use channels_manager_v1::{config::RuntimeConfig, runtime::*};
use std::{
    future::pending,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct SignalAdapter {
    during_startup: bool,
}

impl LifecycleAdapter for SignalAdapter {
    async fn initialize(&mut self, stage: StartupStage) -> Result<(), Failure> {
        if self.during_startup && stage == StartupStage::MongoConnections {
            println!("WAITING");
            std::io::stdout().flush().unwrap();
            pending::<()>().await;
        }
        if stage == StartupStage::Consumers {
            println!("WAITING");
            std::io::stdout().flush().unwrap();
        }
        Ok(())
    }
    fn quiesce(&mut self) {
        println!("QUIESCED");
    }
    fn abort_in_flight(&mut self) {
        panic!("this adapter has no in-flight work");
    }
    async fn shutdown(&mut self, step: ShutdownStep) -> Result<(), Failure> {
        println!("CLOSED:{step:?}");
        Ok(())
    }
    async fn wait_for_failure(&mut self) -> Failure {
        pending().await
    }
}

// The subprocess runs this test alone. No fake adapter is shipped in the app.
#[tokio::test]
async fn signal_process_child() {
    let Ok(mode) = std::env::var("LIFECYCLE_SIGNAL_TEST_CHILD") else {
        return;
    };
    let options = RuntimeConfig {
        startup_retry_delay: Duration::from_secs(1),
        startup_retry_max_delay: Duration::from_secs(2),
        startup_retry_jitter_ratio: 0.0,
        operation_timeout: Duration::from_secs(10),
        shutdown_drain_timeout: Duration::from_secs(1),
        shutdown_timeout: Duration::from_secs(3),
        service_revision: "signal-test".into(),
    };
    let lifecycle = Lifecycle::new(
        SignalAdapter {
            during_startup: mode == "startup",
        },
        options,
    )
    .unwrap();
    run_until_signal(lifecycle).await.unwrap();
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_signal_shutdown(signal: &str, mode: &str) {
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "signal_process_child", "--nocapture"])
            .env("LIFECYCLE_SIGNAL_TEST_CHILD", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let line = receive
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("child did not start");
        if line == "WAITING" {
            break;
        }
    }
    assert!(
        Command::new("kill")
            .args([signal, &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "child ignored shutdown signal");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "child failed: {status}");
    reader.join().unwrap();
    let output: Vec<_> = receive.try_iter().collect();
    let quiesced = output.iter().position(|line| line == "QUIESCED").unwrap();
    assert_eq!(
        &output[quiesced..quiesced + 8],
        [
            "QUIESCED",
            "CLOSED:Consumers",
            "CLOSED:Drain",
            "CLOSED:FlushPublisher",
            "CLOSED:BackgroundTasks",
            "CLOSED:RabbitMq",
            "CLOSED:Redis",
            "CLOSED:Mongo",
        ]
    );
}

#[test]
fn sigterm_drains_running_service() {
    assert_signal_shutdown("-TERM", "running");
}

#[test]
fn sigint_cancels_pending_startup() {
    assert_signal_shutdown("-INT", "startup");
}
