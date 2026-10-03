use super::{
    Backoff, Failure, LifecycleAdapter, LifecycleError, Phase, Recovery, SHUTDOWN_STEPS,
    STARTUP_STAGES, ShutdownIssue, ShutdownStep,
};
use crate::config::{ConfigError, RuntimeConfig};
use tokio::{
    sync::watch,
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

/// Owns the adapter and runs once. Repeated shutdown requests are coalesced by
/// CancellationToken; completion drops the adapter and its remaining resources.
pub struct Lifecycle<D> {
    driver: D,
    settings: RuntimeConfig,
    backoff: Backoff,
    phase: watch::Sender<Phase>,
}

impl<D: LifecycleAdapter> Lifecycle<D> {
    pub fn new(driver: D, settings: RuntimeConfig) -> Result<Self, ConfigError> {
        let backoff = Backoff::new(&settings)?;
        let (phase, _) = watch::channel(Phase::Created);
        Ok(Self {
            driver,
            settings,
            backoff,
            phase,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<Phase> {
        self.phase.subscribe()
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<(), LifecycleError> {
        let mut recovery_attempt = 0_u32;
        loop {
            let failure = match self.start(&cancellation).await {
                Ok(false) => return self.stop(None).await,
                Err(error) if error.recovery == Recovery::Restart => error,
                Err(error) => return self.stop(Some(error)).await,
                Ok(true) => {
                    if cancellation.is_cancelled() {
                        return self.stop(None).await;
                    }
                    self.phase.send_replace(Phase::Running);
                    let running_since = Instant::now();
                    let failure = tokio::select! {
                        biased;
                        _=cancellation.cancelled() => return self.stop(None).await,
                        error=self.driver.wait_for_failure() => error,
                    };
                    if running_since.elapsed() >= self.settings.startup_retry_max_delay {
                        recovery_attempt = 0;
                    }
                    failure
                }
            };
            if failure.recovery == Recovery::Stop {
                return self.stop(Some(failure)).await;
            }
            self.driver.quiesce();
            self.phase.send_replace(Phase::Recovering);
            let issues = self.cleanup().await;
            if !issues.is_empty() {
                return self.complete(Some(failure), issues);
            }
            recovery_attempt = recovery_attempt.saturating_add(1);
            tokio::select! {
                biased;
                _=cancellation.cancelled() => return self.complete(None,Vec::new()),
                _=sleep(self.backoff.next(recovery_attempt)) => {},
            }
        }
    }

    async fn start(&mut self, cancellation: &CancellationToken) -> Result<bool, Failure> {
        for stage in STARTUP_STAGES {
            let mut attempt = 0_u32;
            loop {
                if cancellation.is_cancelled() {
                    return Ok(false);
                }
                self.phase.send_replace(Phase::Starting(stage));
                let result = tokio::select! {
                    biased;
                    _=cancellation.cancelled() => return Ok(false),
                    result=timeout(self.settings.operation_timeout,self.driver.initialize(stage)) =>
                        result.unwrap_or(Err(Failure::retryable("startup_operation_timeout"))),
                };
                match result {
                    Ok(()) => break,
                    Err(error) if error.recovery != Recovery::RetryStep => return Err(error),
                    Err(error) => {
                        attempt = attempt.saturating_add(1);
                        self.phase.send_replace(Phase::Retrying { stage, attempt });
                        eprintln!(
                            "lifecycle retry: stage={stage:?} attempt={attempt} code={}",
                            error.code
                        );
                        tokio::select! {
                            biased;
                            _=cancellation.cancelled() => return Ok(false),
                            _=sleep(self.backoff.next(attempt)) => {},
                        }
                    }
                }
            }
        }
        Ok(true)
    }

    async fn stop(&mut self, cause: Option<Failure>) -> Result<(), LifecycleError> {
        self.driver.quiesce();
        self.phase.send_replace(Phase::Stopping);
        let issues = self.cleanup().await;
        self.complete(cause, issues)
    }

    async fn cleanup(&mut self) -> Vec<ShutdownIssue> {
        let deadline = Instant::now() + self.settings.shutdown_timeout;
        let mut issues = Vec::new();
        for step in SHUTDOWN_STEPS {
            let limit = if step == ShutdownStep::Drain {
                self.settings.shutdown_drain_timeout
            } else {
                self.settings.operation_timeout
            };
            let result = if Instant::now() >= deadline {
                Err(Failure::permanent("shutdown_deadline_exceeded"))
            } else {
                timeout_at(
                    deadline.min(Instant::now() + limit),
                    self.driver.shutdown(step),
                )
                .await
                .unwrap_or(Err(Failure::permanent("shutdown_operation_timeout")))
            };
            if let Err(failure) = result {
                if step == ShutdownStep::Drain {
                    self.driver.abort_in_flight();
                }
                eprintln!(
                    "lifecycle cleanup failed: step={step:?} code={}",
                    failure.code
                );
                issues.push(ShutdownIssue { step, failure });
            }
        }
        issues
    }

    fn complete(
        &self,
        cause: Option<Failure>,
        shutdown: Vec<ShutdownIssue>,
    ) -> Result<(), LifecycleError> {
        self.phase.send_replace(Phase::Stopped);
        if cause.is_some() || !shutdown.is_empty() {
            Err(LifecycleError { cause, shutdown })
        } else {
            Ok(())
        }
    }
}
