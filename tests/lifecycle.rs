use channels_manager_v1::{config::RuntimeConfig, runtime::*};
use std::{
    collections::VecDeque,
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
enum Behavior {
    Fail(Failure),
    Hang,
    Delay(Duration),
}

struct Adapter {
    trace: Arc<Mutex<Vec<String>>>,
    starts: VecDeque<(StartupStage, Behavior)>,
    stops: VecDeque<(ShutdownStep, Behavior)>,
    failures: mpsc::Receiver<Failure>,
}

impl Drop for Adapter {
    fn drop(&mut self) {
        self.trace.lock().unwrap().push("drop".into());
    }
}

impl LifecycleAdapter for Adapter {
    async fn initialize(&mut self, stage: StartupStage) -> Result<(), Failure> {
        self.trace.lock().unwrap().push(format!("start:{stage:?}"));
        if self
            .starts
            .front()
            .is_some_and(|(expected, _)| *expected == stage)
        {
            match self.starts.pop_front().unwrap().1 {
                Behavior::Fail(error) => return Err(error),
                Behavior::Hang => pending::<()>().await,
                Behavior::Delay(delay) => tokio::time::sleep(delay).await,
            }
        }
        Ok(())
    }
    fn quiesce(&mut self) {
        self.trace.lock().unwrap().push("quiesce".into());
    }
    fn abort_in_flight(&mut self) {
        self.trace.lock().unwrap().push("abort".into());
    }
    async fn shutdown(&mut self, step: ShutdownStep) -> Result<(), Failure> {
        self.trace.lock().unwrap().push(format!("stop:{step:?}"));
        if self
            .stops
            .front()
            .is_some_and(|(expected, _)| *expected == step)
        {
            match self.stops.pop_front().unwrap().1 {
                Behavior::Fail(error) => return Err(error),
                Behavior::Hang => pending::<()>().await,
                Behavior::Delay(delay) => tokio::time::sleep(delay).await,
            }
        }
        Ok(())
    }
    async fn wait_for_failure(&mut self) -> Failure {
        self.failures
            .recv()
            .await
            .unwrap_or(Failure::permanent("failure_stream_closed"))
    }
}

fn settings() -> RuntimeConfig {
    RuntimeConfig {
        startup_retry_delay: Duration::from_secs(1),
        startup_retry_max_delay: Duration::from_secs(8),
        startup_retry_jitter_ratio: 0.0,
        operation_timeout: Duration::from_secs(2),
        shutdown_drain_timeout: Duration::from_secs(3),
        shutdown_timeout: Duration::from_secs(10),
        service_revision: "test".into(),
    }
}

type Trace = Arc<Mutex<Vec<String>>>;
fn adapter() -> (Adapter, Trace, mpsc::Sender<Failure>) {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let (send, failures) = mpsc::channel(4);
    (
        Adapter {
            trace: trace.clone(),
            starts: VecDeque::new(),
            stops: VecDeque::new(),
            failures,
        },
        trace,
        send,
    )
}

async fn wait_for(state: &mut watch::Receiver<Phase>, expected: Phase) {
    state.wait_for(|phase| *phase == expected).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn startup_is_ordered_and_shutdown_flushes_before_closing_dependencies() {
    let (adapter, trace, _sender) = adapter();
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Running).await;
    cancellation.cancel();
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
    assert_eq!(*state.borrow(), Phase::Stopped);
    assert_eq!(
        *trace.lock().unwrap(),
        vec![
            "start:MongoConnections",
            "start:MongoIndexes",
            "start:RedisRequired",
            "start:RabbitDeclarations",
            "start:RabbitPublisher",
            "start:Consumers",
            "quiesce",
            "stop:Consumers",
            "stop:Drain",
            "stop:FlushPublisher",
            "stop:BackgroundTasks",
            "stop:RabbitMq",
            "stop:Redis",
            "stop:Mongo",
            "drop",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn startup_retries_only_the_failed_step_with_exponential_delays() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.starts.extend([
        (
            StartupStage::MongoIndexes,
            Behavior::Fail(Failure::retryable("unavailable")),
        ),
        (
            StartupStage::MongoIndexes,
            Behavior::Fail(Failure::retryable("unavailable")),
        ),
    ]);
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let start = Instant::now();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Running).await;
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    let events = trace.lock().unwrap().clone();
    assert_eq!(
        &events[..5],
        [
            "start:MongoConnections",
            "start:MongoIndexes",
            "start:MongoIndexes",
            "start:MongoIndexes",
            "start:RedisRequired"
        ]
    );
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_startup_and_cleans_partially_initialized_resources() {
    let (mut adapter, trace, _sender) = adapter();
    adapter
        .starts
        .push_back((StartupStage::MongoIndexes, Behavior::Hang));
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let start = Instant::now();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Starting(StartupStage::MongoIndexes)).await;
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
    assert_eq!(start.elapsed(), Duration::ZERO);
    let events = trace.lock().unwrap();
    assert!(!events.iter().any(|e| e == "start:RedisRequired"));
    assert!(events.iter().any(|e| e == "stop:Mongo"));
}

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_retry_sleep() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.starts.push_back((
        StartupStage::MongoConnections,
        Behavior::Fail(Failure::retryable("unavailable")),
    ));
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(
        &mut state,
        Phase::Retrying {
            stage: StartupStage::MongoConnections,
            attempt: 1,
        },
    )
    .await;
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "start:MongoConnections")
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn hung_startup_operations_time_out_and_retry() {
    let (mut adapter, _trace, _sender) = adapter();
    adapter
        .starts
        .push_back((StartupStage::RedisRequired, Behavior::Hang));
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let start = Instant::now();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Running).await;
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn permanent_failure_does_not_retry_or_start_consumers() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.starts.push_back((
        StartupStage::RabbitDeclarations,
        Behavior::Fail(Failure::permanent("queue_contract_mismatch")),
    ));
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let error = lifecycle.run(CancellationToken::new()).await.unwrap_err();
    assert_eq!(error.cause.unwrap().code, "queue_contract_mismatch");
    assert!(!trace.lock().unwrap().iter().any(|e| e == "start:Consumers"));
    assert_eq!(trace.lock().unwrap().last().unwrap(), "drop");
}

#[tokio::test(start_paused = true)]
async fn required_failure_stops_intake_before_recovery_and_restores_the_pipeline() {
    let (adapter, trace, sender) = adapter();
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Running).await;
    sender.send(Failure::retryable("redis_lost")).await.unwrap();
    wait_for(&mut state, Phase::Recovering).await;
    wait_for(&mut state, Phase::Running).await;
    let events = trace.lock().unwrap().clone();
    assert_eq!(events[6], "quiesce");
    assert_eq!(events.iter().filter(|e| *e == "start:Consumers").count(), 2);
    let last_close = events.iter().position(|e| e == "stop:Mongo").unwrap();
    assert_eq!(events[last_close + 1], "start:MongoConnections");
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn shutdown_errors_do_not_skip_later_cleanup() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.stops.push_back((
        ShutdownStep::FlushPublisher,
        Behavior::Fail(Failure::retryable("publish_failed")),
    ));
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = Lifecycle::new(adapter, settings())
        .unwrap()
        .run(cancellation)
        .await
        .unwrap_err();
    assert_eq!(error.shutdown.len(), 1);
    assert_eq!(error.shutdown[0].step, ShutdownStep::FlushPublisher);
    assert!(trace.lock().unwrap().iter().any(|e| e == "stop:Mongo"));
}

#[tokio::test(start_paused = true)]
async fn drain_timeout_aborts_work_before_publication_flush() {
    let (mut adapter, trace, _sender) = adapter();
    adapter
        .stops
        .push_back((ShutdownStep::Drain, Behavior::Hang));
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let start = Instant::now();
    let error = Lifecycle::new(adapter, settings())
        .unwrap()
        .run(cancellation)
        .await
        .unwrap_err();
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    assert_eq!(error.shutdown[0].step, ShutdownStep::Drain);
    let events = trace.lock().unwrap();
    let aborted = events.iter().position(|e| e == "abort").unwrap();
    assert_eq!(events[aborted + 1], "stop:FlushPublisher");
}

#[tokio::test(start_paused = true)]
async fn total_shutdown_deadline_bounds_multiple_hanging_steps() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.stops.extend(
        SHUTDOWN_STEPS
            .into_iter()
            .map(|step| (step, Behavior::Hang)),
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let start = Instant::now();
    let error = Lifecycle::new(adapter, settings())
        .unwrap()
        .run(cancellation)
        .await
        .unwrap_err();
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert_eq!(error.shutdown.len(), 7);
    assert_eq!(trace.lock().unwrap().last().unwrap(), "drop");
}

#[tokio::test(start_paused = true)]
async fn failed_recovery_cleanup_never_starts_a_second_consumer() {
    let (mut adapter, trace, sender) = adapter();
    adapter.stops.push_back((
        ShutdownStep::RabbitMq,
        Behavior::Fail(Failure::retryable("close_failed")),
    ));
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let run = tokio::spawn(lifecycle.run(CancellationToken::new()));
    wait_for(&mut state, Phase::Running).await;
    sender
        .send(Failure::retryable("connection_lost"))
        .await
        .unwrap();
    assert!(run.await.unwrap().is_err());
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "start:Consumers")
            .count(),
        1
    );
}

#[test]
fn jitter_backoff_is_capped_and_cannot_become_a_busy_loop() {
    let mut options = settings();
    options.startup_retry_jitter_ratio = 1.0;
    let backoff = Backoff::new(&options).unwrap();
    assert_eq!(backoff.delay(1, 0.5), Duration::from_secs(1));
    assert_eq!(backoff.delay(2, 0.5), Duration::from_secs(2));
    assert_eq!(backoff.delay(u32::MAX, 1.0), Duration::from_secs(8));
    assert_eq!(backoff.delay(1, 0.0), Duration::from_millis(1));
    assert_eq!(backoff.delay(1, f64::NAN), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn dependency_loss_before_consumers_restarts_earlier_startup_stages() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.starts.push_back((
        StartupStage::Consumers,
        Behavior::Fail(Failure::restart("publisher_lost_during_startup")),
    ));
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Running).await;
    let events = trace.lock().unwrap().clone();
    assert_eq!(
        events
            .iter()
            .filter(|e| *e == "start:RabbitPublisher")
            .count(),
        2
    );
    let gate = events.iter().position(|e| e == "quiesce").unwrap();
    assert_eq!(events[gate - 1], "start:Consumers");
    assert!(events[gate..].iter().any(|e| e == "start:MongoConnections"));
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn recovery_backoff_increases_and_resets_after_stable_operation() {
    let (adapter, _trace, sender) = adapter();
    let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
    let mut state = lifecycle.subscribe();
    let cancellation = CancellationToken::new();
    let run = tokio::spawn(lifecycle.run(cancellation.clone()));
    wait_for(&mut state, Phase::Running).await;
    for expected_delay in [1, 2, 1] {
        let start = Instant::now();
        sender.send(Failure::retryable("lost")).await.unwrap();
        wait_for(&mut state, Phase::Recovering).await;
        wait_for(&mut state, Phase::Running).await;
        assert_eq!(start.elapsed(), Duration::from_secs(expected_delay));
        if expected_delay == 2 {
            tokio::time::sleep(Duration::from_secs(8)).await;
        }
    }
    cancellation.cancel();
    assert!(run.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_recovery_cleanup_or_backoff_never_restarts() {
    for during_cleanup in [false, true] {
        let (mut adapter, trace, sender) = adapter();
        if during_cleanup {
            adapter
                .stops
                .push_back((ShutdownStep::Drain, Behavior::Delay(Duration::from_secs(1))));
        }
        let lifecycle = Lifecycle::new(adapter, settings()).unwrap();
        let mut state = lifecycle.subscribe();
        let cancellation = CancellationToken::new();
        let run = tokio::spawn(lifecycle.run(cancellation.clone()));
        wait_for(&mut state, Phase::Running).await;
        sender.send(Failure::retryable("lost")).await.unwrap();
        wait_for(&mut state, Phase::Recovering).await;
        cancellation.cancel();
        assert!(run.await.unwrap().is_ok());
        let events = trace.lock().unwrap();
        assert_eq!(events.iter().filter(|e| *e == "start:Consumers").count(), 1);
        assert_eq!(events.iter().filter(|e| *e == "quiesce").count(), 1);
        assert!(events.iter().any(|e| e == "stop:Mongo"));
    }
}

#[tokio::test(start_paused = true)]
async fn explicit_drain_failure_aborts_handlers_before_flush() {
    let (mut adapter, trace, _sender) = adapter();
    adapter.stops.push_back((
        ShutdownStep::Drain,
        Behavior::Fail(Failure::permanent("drain_failed")),
    ));
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = Lifecycle::new(adapter, settings())
        .unwrap()
        .run(cancellation)
        .await
        .unwrap_err();
    assert_eq!(error.shutdown[0].failure.code, "drain_failed");
    let events = trace.lock().unwrap();
    let abort = events.iter().position(|e| e == "abort").unwrap();
    assert_eq!(events[abort + 1], "stop:FlushPublisher");
}
