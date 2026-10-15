use channels_manager_v1::trading::notifications::UserFailure;

#[test]
fn business_failures_have_fixed_user_messages() {
    for (reason, explanation) in [
        (
            "insufficientBalance",
            "Insufficient available Futures balance to create the trade.",
        ),
        (
            "positionExceedsBalance",
            "The configured position size exceeds available balance.",
        ),
        (
            "insufficientTargetBalance",
            "The position is too small for the configured target allocation.",
        ),
        (
            "leverageLimitExceeded",
            "Requested leverage exceeds the symbol's supported limit.",
        ),
        (
            "leverageChangeBlocked",
            "Existing positions or orders prevent using the requested leverage.",
        ),
        (
            "migrationRequired",
            "Existing account activity prevents using the required position mode for this symbol.",
        ),
    ] {
        let failure = UserFailure::from_reason(reason).expect("allowlisted business failure");
        assert_eq!(failure.reason(), reason);
        assert_eq!(
            failure.format("ADA-USDT", "BingX-1"),
            format!("⚠️ #ADA-USDT Auto-Trade was not created | BingX-1 | {explanation}")
        );
        assert!(!failure.format("ADA-USDT", "BingX-1").contains(reason));
    }
}

#[test]
fn internal_mixed_and_unknown_failures_have_no_user_message() {
    for reason in [
        "",
        "unknown",
        "InsufficientBalance",
        "insufficientBalance: private-details",
        "InvalidPayload",
        "InvalidIdentity",
        "InvalidClient",
        "InvalidSignal",
        "InvalidMarketData",
        "MissingExpiry",
        "Expired",
        "MissingSettings",
        "InvalidPosition",
        "InvalidMarginMode",
        "InvalidLeverage",
        "InvalidTargets",
        "InvalidStrategy",
        "InvalidXPro",
        "InvalidUserConfig",
        "UnsupportedSymbol",
        "managedTradesUnavailable",
        "invalidManagedEvidence",
        "incompleteManagedEvidence",
        "incompleteExchangeEvidence",
        "invalidAdmissionTarget",
        "invalidExchangeResponse",
        "exchangeReadUnavailable",
        "exchangeReadRejected",
        "admissionTimeout",
        "modeSwitchFailed",
        "modeSwitchTimeout",
        "leverageChangeFailed",
        "leverageChangeTimeout",
        "admissionDeadlineExceeded",
        "modeSwitchAlreadyAttempted",
        "leverageChangeAlreadyAttempted",
        "leverageChangeRequired",
        "orderOutsideExchangeLimits",
        "invalidTradeNumber",
        "invalidCalculatedAmount",
        "invalidPrecision",
        "unsupportedPrecision",
        "invalidPrice",
        "invalidQuantity",
        "invalidStaticFallback",
        "executableNotionalExceedsStaticSize",
        "profitInsideEntryZone",
        "stopInsideEntryZone",
        "publicationRejected",
        "REDIS_UNAVAILABLE",
        "MongoError",
        "RateLimited",
        "user_failure_notification_save_failed",
    ] {
        assert_eq!(UserFailure::from_reason(reason), None, "{reason}");
    }
}

#[test]
fn dynamic_values_are_escaped_for_bot_html() {
    let failure = UserFailure::from_reason("insufficientBalance").unwrap();
    assert_eq!(
        failure.format("ADA<&>-USDT", "<b>BingX &amp; 1</b>"),
        "⚠️ #ADA&lt;&amp;&gt;-USDT Auto-Trade was not created | &lt;b&gt;BingX &amp;amp; 1&lt;/b&gt; | Insufficient available Futures balance to create the trade."
    );
    assert_eq!(
        failure.format("ADA-USDT", "BingX's \"main\" account 🟢"),
        "⚠️ #ADA-USDT Auto-Trade was not created | BingX's \"main\" account 🟢 | Insufficient available Futures balance to create the trade."
    );
}

use channels_manager_v1::{
    mongo::{FailureNotificationOutcome, MongoError, MongoErrorKind, MongoRole},
    rabbitmq::DeliveryOutcome,
    trading::notifications::{
        FailureHistoryEntry, FailureHistoryWriter, NotificationContext, report_rejection,
    },
};
use futures_util::future::BoxFuture;
use std::{collections::VecDeque, sync::Mutex, time::Duration};

struct HistoryWriter {
    results: Mutex<VecDeque<Result<FailureNotificationOutcome, MongoError>>>,
    entries: Mutex<Vec<FailureHistoryEntry>>,
    hang: bool,
}
impl FailureHistoryWriter for HistoryWriter {
    fn append<'a>(
        &'a self,
        entry: &'a FailureHistoryEntry,
    ) -> BoxFuture<'a, Result<FailureNotificationOutcome, MongoError>> {
        self.entries.lock().unwrap().push(entry.clone());
        Box::pin(async move {
            if self.hang {
                return std::future::pending().await;
            }
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected history retry")
        })
    }
}
fn notice() -> channels_manager_v1::trading::notifications::FailureNotification {
    NotificationContext {
        chat_id: 42,
        account_id: "account-1".into(),
        account_name: "BingX-1".into(),
        symbol: "ADA-USDT".into(),
        channel_id: -100,
        message_id: 7,
        idempotency_key: "validated-job-1".into(),
    }
    .user_failure("insufficientBalance")
    .unwrap()
}
fn writer(results: Vec<Result<FailureNotificationOutcome, MongoError>>) -> HistoryWriter {
    HistoryWriter {
        results: Mutex::new(results.into()),
        entries: Mutex::new(vec![]),
        hang: false,
    }
}
fn db_error() -> MongoError {
    MongoError {
        role: MongoRole::Bot,
        kind: MongoErrorKind::Unavailable,
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_business_rejection_finishes_reporting_before_settlement() {
    let writer = writer(vec![
        Err(db_error()),
        Err(db_error()),
        Ok(FailureNotificationOutcome::Appended),
    ]);
    let start = tokio::time::Instant::now();
    let outcome = report_rejection(Some(&notice()), &writer, Duration::from_secs(1)).await;
    assert!(matches!(outcome, DeliveryOutcome::Rejected));
    let entries = writer.entries.lock().unwrap();
    assert_eq!(entries.len(), 3);
    assert!(entries.iter().all(|entry| entry == &entries[0]));
    assert_eq!(start.elapsed(), Duration::from_millis(400));
}

#[tokio::test(start_paused = true)]
async fn duplicate_and_missing_user_settle_without_retry() {
    for result in [
        FailureNotificationOutcome::Appended,
        FailureNotificationOutcome::AlreadyPresent,
        FailureNotificationOutcome::UserMissing,
    ] {
        let writer = writer(vec![Ok(result)]);
        assert!(matches!(
            report_rejection(Some(&notice()), &writer, Duration::from_secs(1)).await,
            DeliveryOutcome::Rejected
        ));
        assert_eq!(writer.entries.lock().unwrap().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn internal_rejection_never_writes_history() {
    let writer = writer(vec![]);
    assert!(matches!(
        report_rejection(None, &writer, Duration::from_secs(1)).await,
        DeliveryOutcome::Rejected
    ));
    assert!(writer.entries.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn exhausted_history_retries_still_settle_the_rejected_job() {
    let writer = writer(vec![Err(db_error()); 3]);
    assert!(matches!(
        report_rejection(Some(&notice()), &writer, Duration::from_secs(1)).await,
        DeliveryOutcome::Rejected
    ));
    assert_eq!(writer.entries.lock().unwrap().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn total_budget_bounds_hung_writes_and_retry_delays() {
    let mut hung = writer(vec![]);
    hung.hang = true;
    let delayed = writer(vec![Err(db_error()); 3]);
    for writer in [&hung, &delayed] {
        let start = tokio::time::Instant::now();
        assert!(matches!(
            report_rejection(Some(&notice()), writer, Duration::from_millis(100)).await,
            DeliveryOutcome::Rejected
        ));
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        assert_eq!(writer.entries.lock().unwrap().len(), 1);
    }
}
