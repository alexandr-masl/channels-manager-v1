use super::{Lifecycle, LifecycleAdapter, LifecycleError};
use std::{fmt, io};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum ServiceError {
    Signal(io::Error),
    Lifecycle(LifecycleError),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Signal(_) => f.write_str("could not receive shutdown signals"),
            Self::Lifecycle(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ServiceError {}

/// Register signals before startup; a signal cancels startup/recovery and then
/// awaits bounded cleanup. No detached signal-listener task is created.
pub async fn run_until_signal<D: LifecycleAdapter>(
    lifecycle: Lifecycle<D>,
) -> Result<(), ServiceError> {
    let mut signals = ShutdownSignals::install().map_err(ServiceError::Signal)?;
    let cancellation = CancellationToken::new();
    let run = lifecycle.run(cancellation.clone());
    tokio::pin!(run);
    tokio::select! {
        biased;
        received=signals.wait() => {
            cancellation.cancel();
            let result=run.await;
            received.map_err(ServiceError::Signal)?;
            result.map_err(ServiceError::Lifecycle)
        },
        result=&mut run => result.map_err(ServiceError::Lifecycle),
    }
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }
    async fn wait(&mut self) -> io::Result<()> {
        let received = tokio::select! {
            signal=self.interrupt.recv() => signal,
            signal=self.terminate.recv() => signal,
        };
        received.ok_or_else(|| io::Error::other("signal stream closed"))
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        Ok(Self)
    }
    async fn wait(&mut self) -> io::Result<()> {
        tokio::signal::ctrl_c().await
    }
}
