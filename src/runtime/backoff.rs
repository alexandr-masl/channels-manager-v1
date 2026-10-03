use crate::config::{ConfigError, RuntimeConfig};
use std::time::Duration;

pub struct Backoff {
    base_ms: u64,
    max_ms: u64,
    jitter: f64,
}

impl Backoff {
    pub fn new(config: &RuntimeConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            base_ms: config.startup_retry_delay.as_millis() as u64,
            max_ms: config.startup_retry_max_delay.as_millis() as u64,
            jitter: config.startup_retry_jitter_ratio,
        })
    }

    /// Attempts are one-based. A deterministic sample supports virtual-time tests.
    pub fn delay(&self, attempt: u32, sample: f64) -> Duration {
        let factor = 1_u64
            .checked_shl(attempt.saturating_sub(1))
            .unwrap_or(u64::MAX);
        let capped = self.base_ms.saturating_mul(factor).min(self.max_ms);
        let sample = if sample.is_finite() {
            sample.clamp(0.0, 1.0)
        } else {
            0.5
        };
        let jittered = (capped as f64 * (1.0 + (sample * 2.0 - 1.0) * self.jitter)).round() as u64;
        Duration::from_millis(jittered.clamp(1, self.max_ms))
    }

    pub(crate) fn next(&self, attempt: u32) -> Duration {
        self.delay(attempt, rand::random())
    }
}
