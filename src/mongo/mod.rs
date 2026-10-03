//! MongoDB pools and repositories. Broker/worker composition follows in later slices.
mod claims;
mod connections;
mod error;
mod repositories;

pub use claims::{ClaimInput, ClaimOutcome, ClaimStore};
pub use connections::{MongoConnections, MongoRole};
pub use error::{MongoError, MongoErrorKind};
pub use repositories::{
    AccountRepository, MongoRepositories, NotificationRepository, TradeRepository,
};
