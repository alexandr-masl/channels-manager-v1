//! Cross-pod account leases and optional shared API metadata. No messaging.
mod cache;
mod connection;
mod lease;

pub use cache::MetadataCache;
pub use connection::RedisConnections;
pub use lease::{AccountLease, LeaseManager};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisError {
    InvalidConfiguration,
    InvalidIdentity,
    Unavailable,
    Timeout,
    Closed,
    Contended,
    LeaseLost,
}
impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Redis: {self:?}")
    }
}
impl std::error::Error for RedisError {}
impl From<RedisError> for crate::runtime::Failure {
    fn from(error: RedisError) -> Self {
        match error {
            RedisError::InvalidConfiguration | RedisError::InvalidIdentity => {
                Self::permanent("REDIS_INVALID_CONFIGURATION")
            }
            _ => Self::restart("REDIS_LOCKS_UNAVAILABLE"),
        }
    }
}
