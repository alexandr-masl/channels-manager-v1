use super::MongoRole;
use crate::runtime::Failure;
use mongodb::error::ErrorKind;
use std::{fmt, future::Future, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MongoErrorKind {
    NotConnected,
    InvalidConfiguration,
    Authentication,
    Unavailable,
    Timeout,
    IndexContract,
    Operation,
}

/// Raw driver errors can contain connection strings or documents; never retain them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoError {
    pub role: MongoRole,
    pub kind: MongoErrorKind,
}
impl MongoError {
    pub(super) fn new(role: MongoRole, kind: MongoErrorKind) -> Self {
        Self { role, kind }
    }
    pub(super) fn driver(role: MongoRole, error: mongodb::error::Error) -> Self {
        let kind = match error.kind.as_ref() {
            ErrorKind::InvalidArgument { .. } => MongoErrorKind::InvalidConfiguration,
            ErrorKind::Authentication { .. } => MongoErrorKind::Authentication,
            ErrorKind::Command(e) if e.code == 13 || e.code == 18 => MongoErrorKind::Authentication,
            ErrorKind::ServerSelection { .. } | ErrorKind::Shutdown | ErrorKind::Io(_) => {
                MongoErrorKind::Unavailable
            }
            ErrorKind::Command(e) if e.code == 85 || e.code == 86 => MongoErrorKind::IndexContract,
            _ => MongoErrorKind::Operation,
        };
        Self { role, kind }
    }
}
impl fmt::Display for MongoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MongoDB {:?}: {:?}", self.role, self.kind)
    }
}
impl std::error::Error for MongoError {}
impl From<MongoError> for Failure {
    fn from(error: MongoError) -> Self {
        match error.kind {
            MongoErrorKind::InvalidConfiguration => Self::permanent("MONGO_INVALID_CONFIGURATION"),
            MongoErrorKind::Authentication => Self::permanent("MONGO_AUTHENTICATION"),
            MongoErrorKind::IndexContract => Self::permanent("MONGO_INDEX_CONTRACT"),
            _ => Self::retryable("MONGO_UNAVAILABLE"),
        }
    }
}
pub(super) async fn bounded<T>(
    role: MongoRole,
    duration: Duration,
    future: impl Future<Output = mongodb::error::Result<T>>,
) -> Result<T, MongoError> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| MongoError::new(role, MongoErrorKind::Timeout))?
        .map_err(|e| MongoError::driver(role, e))
}
