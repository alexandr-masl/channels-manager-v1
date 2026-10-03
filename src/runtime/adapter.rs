use std::{fmt, future::Future};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupStage {
    MongoConnections,
    MongoIndexes,
    RedisRequired,
    RabbitDeclarations,
    RabbitPublisher,
    Consumers,
}

pub const STARTUP_STAGES: [StartupStage; 6] = [
    StartupStage::MongoConnections,
    StartupStage::MongoIndexes,
    StartupStage::RedisRequired,
    StartupStage::RabbitDeclarations,
    StartupStage::RabbitPublisher,
    StartupStage::Consumers,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownStep {
    Consumers,
    Drain,
    FlushPublisher,
    BackgroundTasks,
    RabbitMq,
    Redis,
    Mongo,
}

pub const SHUTDOWN_STEPS: [ShutdownStep; 7] = [
    ShutdownStep::Consumers,
    ShutdownStep::Drain,
    ShutdownStep::FlushPublisher,
    ShutdownStep::BackgroundTasks,
    ShutdownStep::RabbitMq,
    ShutdownStep::Redis,
    ShutdownStep::Mongo,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Created,
    Starting(StartupStage),
    Retrying { stage: StartupStage, attempt: u32 },
    Running,
    Recovering,
    Stopping,
    Stopped,
}

/// Distinguishes a local initialization retry from loss of an earlier dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    RetryStep,
    Restart,
    Stop,
}

/// Only static, sanitized codes cross this boundary; driver errors may contain credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    pub code: &'static str,
    pub recovery: Recovery,
}

impl Failure {
    pub const fn retryable(code: &'static str) -> Self {
        Self {
            code,
            recovery: Recovery::RetryStep,
        }
    }
    /// A previously initialized dependency was lost; rebuild the whole pipeline.
    pub const fn restart(code: &'static str) -> Self {
        Self {
            code,
            recovery: Recovery::Restart,
        }
    }
    pub const fn permanent(code: &'static str) -> Self {
        Self {
            code,
            recovery: Recovery::Stop,
        }
    }
}

#[derive(Debug)]
pub struct ShutdownIssue {
    pub step: ShutdownStep,
    pub failure: Failure,
}

#[derive(Debug)]
pub struct LifecycleError {
    pub cause: Option<Failure>,
    pub shutdown: Vec<ShutdownIssue>,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lifecycle stopped: {}; {} cleanup failure(s)",
            self.cause.map_or("shutdown incomplete", |e| e.code),
            self.shutdown.len()
        )
    }
}
impl std::error::Error for LifecycleError {}

/// Implemented by the concrete infrastructure composition in stages 3–5.
///
/// All operations must be nonblocking, cancellation-safe, and idempotent. A
/// timed-out future is dropped, so partial resources must already be owned by
/// the adapter. Cleanup must accept stages that never initialized. Drop must
/// abort owned tasks and release remaining handles; tasks must not detach.
pub trait LifecycleAdapter: Send {
    /// Consumers may start only after all earlier stages are usable. Recheck
    /// latched dependency failures before enabling delivery after recovery.
    /// Return Failure::restart when an earlier dependency needs restoration;
    /// Failure::retryable retries only this initialization stage.
    fn initialize(
        &mut self,
        stage: StartupStage,
    ) -> impl Future<Output = Result<(), Failure>> + Send;

    /// Immediately close the local intake gate and suppress new reconnect work.
    /// This cannot wait for network I/O. Existing lease renewals must survive drain.
    fn quiesce(&mut self);

    /// Cancel/abort remaining handlers after failed or timed-out drain. Must be
    /// synchronous and must prevent their further application-side effects.
    fn abort_in_flight(&mut self);

    /// Drain includes handler completion/settlement; flush includes outstanding
    /// broker confirmations. Closing RabbitMQ also closes consumer channels.
    fn shutdown(&mut self, step: ShutdownStep) -> impl Future<Output = Result<(), Failure>> + Send;

    /// Wait for a required component failure, including failures latched during
    /// startup. Optional cache/on-use notification failures stay in their adapters.
    /// Must be cancellation-safe and must not consume a failure until returned.
    fn wait_for_failure(&mut self) -> impl Future<Output = Failure> + Send;
}
