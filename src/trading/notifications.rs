//! User-failure mapping, trusted context and bounded history reporting.
use super::job::ValidatedJob;
use crate::{
    mongo::{FailureNotificationOutcome, MongoError, NotificationRepository},
    rabbitmq::DeliveryOutcome,
};
use futures_util::future::BoxFuture;
use mongodb::bson::DateTime;
use sha2::{Digest, Sha256};
use std::time::Duration;

pub trait FailureHistoryWriter: Send + Sync {
    fn append<'a>(
        &'a self,
        entry: &'a FailureHistoryEntry,
    ) -> BoxFuture<'a, Result<FailureNotificationOutcome, MongoError>>;
}

impl FailureHistoryWriter for NotificationRepository {
    fn append<'a>(
        &'a self,
        entry: &'a FailureHistoryEntry,
    ) -> BoxFuture<'a, Result<FailureNotificationOutcome, MongoError>> {
        Box::pin(self.append_failure(entry))
    }
}

/// Reporting retries only the history write; all outcomes settle the rejected job.
pub async fn report_rejection(
    notification: Option<&FailureNotification>,
    writer: &impl FailureHistoryWriter,
    budget: Duration,
) -> DeliveryOutcome {
    let Some(notification) = notification else {
        return DeliveryOutcome::Rejected;
    };
    let entry = notification.history_entry(DateTime::now());
    let result = tokio::time::timeout(budget, async {
        for attempt in 0..3 {
            match writer.append(&entry).await {
                Ok(outcome) => return Ok(outcome),
                Err(error) if attempt == 2 => return Err(error),
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        unreachable!("three attempts always return")
    })
    .await;
    let event = match result {
        Ok(Ok(
            FailureNotificationOutcome::Appended | FailureNotificationOutcome::AlreadyPresent,
        )) => "user_failure_notification_saved",
        Ok(Ok(FailureNotificationOutcome::UserMissing)) => "user_failure_notification_user_missing",
        _ => "user_failure_notification_save_failed",
    };
    println!(
        "{event}: channel_id={} message_id={} account_id={} reason={}",
        notification.context.channel_id,
        notification.context.message_id,
        notification.context.account_id,
        notification.failure.reason()
    );
    DeliveryOutcome::Rejected
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserFailure {
    reason: &'static str,
    explanation: &'static str,
}

impl UserFailure {
    /// Unknown, mixed and internal reasons deliberately produce no user message.
    pub fn from_reason(reason: &str) -> Option<Self> {
        let (reason, explanation) = match reason {
            "insufficientBalance" => (
                "insufficientBalance",
                "Insufficient available Futures balance to create the trade.",
            ),
            "positionExceedsBalance" => (
                "positionExceedsBalance",
                "The configured position size exceeds available balance.",
            ),
            "insufficientTargetBalance" => (
                "insufficientTargetBalance",
                "The position is too small for the configured target allocation.",
            ),
            "leverageLimitExceeded" => (
                "leverageLimitExceeded",
                "Requested leverage exceeds the symbol's supported limit.",
            ),
            "leverageChangeBlocked" => (
                "leverageChangeBlocked",
                "Existing positions or orders prevent using the requested leverage.",
            ),
            "migrationRequired" => (
                "migrationRequired",
                "Existing account activity prevents using the required position mode for this symbol.",
            ),
            _ => return None,
        };
        Some(Self {
            reason,
            explanation,
        })
    }

    /// Internal identity for later notification suppression; excluded from the text.
    pub fn reason(self) -> &'static str {
        self.reason
    }

    pub fn format(self, symbol: &str, account_name: &str) -> String {
        format!(
            "⚠️ #{} Auto-Trade was not created | {} | {}",
            escape_html(symbol),
            escape_html(account_name),
            self.explanation
        )
    }
}

// Dynamic values occupy HTML text nodes, never attributes or markup.
fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationContext {
    pub chat_id: i64,
    pub account_id: String,
    pub account_name: String,
    pub symbol: String,
    pub channel_id: i64,
    pub message_id: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureNotification {
    pub failure: UserFailure,
    pub context: NotificationContext,
}

/// Construct once and reuse across history-write retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureHistoryEntry {
    pub(crate) chat_id: i64,
    pub(crate) event_id: String,
    pub(crate) info: String,
    pub(crate) time: String,
}

impl FailureNotification {
    pub fn history_entry(&self, time: DateTime) -> FailureHistoryEntry {
        let identity = format!(
            "user-failure:{}:{}",
            self.context.idempotency_key,
            self.failure.reason()
        );
        FailureHistoryEntry {
            chat_id: self.context.chat_id,
            event_id: format!("{:x}", Sha256::digest(identity.as_bytes())),
            info: self
                .failure
                .format(&self.context.symbol, &self.context.account_name),
            time: time
                .try_to_rfc3339_string()
                .expect("notification date has an RFC3339 representation"),
        }
    }
}

impl NotificationContext {
    pub fn from_validated(job: &ValidatedJob<'_>) -> Self {
        Self {
            chat_id: job.chat_id,
            account_id: job.client_id.to_owned(),
            account_name: job.job.client["name"]
                .as_str()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or("BingX")
                .to_owned(),
            symbol: job.normalized_symbol.clone(),
            channel_id: job.job.channel_id,
            message_id: job.job.message_id,
            idempotency_key: job.job.idempotency_key.clone(),
        }
    }

    pub fn user_failure(&self, reason: &str) -> Option<FailureNotification> {
        Some(FailureNotification {
            failure: UserFailure::from_reason(reason)?,
            context: self.clone(),
        })
    }
}
