use channels_manager_v1::{
    contracts::messages::{ClientTradeJob, ExecutionRoute},
    exchanges::bingx::{admission::AdmittedAccount, migration::AdmissionAttempt},
    trading::{
        execution::{AdmissionDependencies, AdmissionFailure, ClientTradeWorker, WorkerOutcome},
        job::ValidatedJob,
    },
};
use futures_util::future::BoxFuture;
use serde_json::json;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
struct Dependencies {
    calls: AtomicUsize,
    failure: Option<AdmissionFailure>,
    stall: bool,
}
impl AdmissionDependencies for Dependencies {
    fn check<'a>(
        &'a self,
        _: &'a ValidatedJob<'a>,
        _: &'a AdmissionAttempt,
    ) -> BoxFuture<'a, Result<AdmittedAccount, AdmissionFailure>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.stall {
                std::future::pending::<()>().await;
            }
            if let Some(f) = self.failure {
                return Err(f);
            }
            Ok(AdmittedAccount {
                position_mode: "HEDGE",
                position_configuration: ExecutionRoute::Hedge,
                available_balance: 100.,
                available_balance_raw: json!("100.00"),
            })
        })
    }
}
fn body() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "jobType":"create_trade","version":1,"channelID":-100,"messageId":7,"tradeExpiresAt":1000,
        "signalData":{"symbol":"BTCUSDT","exchange_client":"_futures","is_long":true,"leverage":"10x","buy_targets":["62000"],"sell_targets":["63000"],"stop_loss":"61000"},
        "channelSettings":{"default_quantity":0.1,"default_buy_targets":[{"fraction":1}],"default_sell_targets":[{"fraction":1}],"strategy":"basic"},
        "client":{"clientId":"account-1","provider":"BingX","chatId":42,"key":"secret-key","keySecret":"secret-secret"},
        "marketData":{"bingXFutures":{"currPrice":"62000","symbolInfo":{"symbol":"BTC-USDT","status":1,"tickSize":"0.01","lotSize":"0.001","minQty":"0.001","minNotional":0}}},
        "openedTrades":[],"idempotencyKey":"auto-trade:-100:7:42:account-1:BingX:futures:BTCUSDT","partitionKey":"client-symbol:account-1:futures:BTCUSDT","provider":"BingX","market":"futures","symbol":"BTCUSDT"
    })).unwrap()
}
#[tokio::test]
async fn admission_retains_original_job_expiry_and_resolved_settings() {
    let deps = Dependencies {
        calls: AtomicUsize::new(0),
        failure: None,
        stall: false,
    };
    let WorkerOutcome::Admitted(prepared) = ClientTradeWorker::new(&deps, Duration::from_secs(1))
        .prepare(&body(), || 1)
        .await
    else {
        panic!("not admitted")
    };
    assert_eq!(prepared.job.trade_expires_at, Some(1000));
    assert_eq!(prepared.settings.config["strategy"], "basic");
    assert_eq!(
        prepared.admission.position_configuration,
        ExecutionRoute::Hedge
    );
    assert_eq!(deps.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn invalid_expired_or_invalid_settings_never_call_exchange() {
    let deps = Dependencies {
        calls: AtomicUsize::new(0),
        failure: None,
        stall: false,
    };
    let worker = ClientTradeWorker::new(&deps, Duration::from_secs(1));
    assert!(matches!(
        worker.prepare(b"bad-json", || 1).await,
        WorkerOutcome::Rejected(_)
    ));
    assert!(matches!(
        worker.prepare(&body(), || 1000).await,
        WorkerOutcome::Expired
    ));
    let mut job: ClientTradeJob = serde_json::from_slice(&body()).unwrap();
    job.channel_settings = json!({});
    assert!(matches!(
        worker
            .prepare(&serde_json::to_vec(&job).unwrap(), || 1)
            .await,
        WorkerOutcome::Rejected(_)
    ));
    assert_eq!(deps.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn dependency_rejections_and_retryable_reads_are_distinct() {
    for (failure, retry) in [
        (AdmissionFailure::Retryable("exchangeReadUnavailable"), true),
        (AdmissionFailure::Rejected("migrationRequired"), false),
    ] {
        let deps = Dependencies {
            calls: AtomicUsize::new(0),
            failure: Some(failure),
            stall: false,
        };
        let result = ClientTradeWorker::new(&deps, Duration::from_secs(1))
            .prepare(&body(), || 1)
            .await;
        assert_eq!(matches!(result, WorkerOutcome::Retryable(_)), retry);
        if !retry {
            assert!(matches!(result, WorkerOutcome::Rejected(_)));
        }
    }
}
#[tokio::test(start_paused = true)]
async fn hung_dependencies_are_bounded_by_operation_budget() {
    let deps = Dependencies {
        calls: AtomicUsize::new(0),
        failure: None,
        stall: true,
    };
    let start = tokio::time::Instant::now();
    let result = ClientTradeWorker::new(&deps, Duration::from_millis(50))
        .prepare(&body(), || 1)
        .await;
    assert!(matches!(
        result,
        WorkerOutcome::Retryable("admissionTimeout")
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(50));
}
#[tokio::test(start_paused = true)]
async fn expiry_bounds_admission_and_is_rechecked_after_dependency_work() {
    let deps = Dependencies {
        calls: AtomicUsize::new(0),
        failure: None,
        stall: true,
    };
    let start = tokio::time::Instant::now();
    let result = ClientTradeWorker::new(&deps, Duration::from_secs(10))
        .prepare(&body(), || 950 + start.elapsed().as_millis() as u64)
        .await;
    assert!(matches!(result, WorkerOutcome::Expired));
    assert_eq!(start.elapsed(), Duration::from_millis(50));
    let deps = Dependencies {
        calls: AtomicUsize::new(0),
        failure: None,
        stall: false,
    };
    let clock = AtomicUsize::new(0);
    let result = ClientTradeWorker::new(&deps, Duration::from_secs(1))
        .prepare(&body(), || {
            if clock.fetch_add(1, Ordering::SeqCst) == 0 {
                1
            } else {
                1000
            }
        })
        .await;
    assert!(matches!(result, WorkerOutcome::Expired));
}

#[tokio::test]
async fn opened_trade_count_does_not_reject_a_job() {
    let deps = Dependencies {
        calls: AtomicUsize::new(0),
        failure: None,
        stall: false,
    };
    let mut job: ClientTradeJob = serde_json::from_slice(&body()).unwrap();
    job.opened_trades = vec![json!({"state":"OPENED"}); 1000];
    assert!(matches!(
        ClientTradeWorker::new(&deps, Duration::from_secs(1))
            .prepare(&serde_json::to_vec(&job).unwrap(), || 1)
            .await,
        WorkerOutcome::Admitted(_)
    ));
}

struct SwitchingDependencies;
impl AdmissionDependencies for SwitchingDependencies {
    fn check<'a>(
        &'a self,
        _: &'a ValidatedJob<'a>,
        attempt: &'a AdmissionAttempt,
    ) -> BoxFuture<'a, Result<AdmittedAccount, AdmissionFailure>> {
        Box::pin(async move {
            attempt.begin_switch().unwrap();
            std::future::pending().await
        })
    }
}
#[tokio::test(start_paused = true)]
async fn worker_timeout_after_switch_started_rejects_without_retry() {
    let result = ClientTradeWorker::new(&SwitchingDependencies, Duration::from_millis(50))
        .prepare(&body(), || 1)
        .await;
    assert!(matches!(
        result,
        WorkerOutcome::Rejected(
            channels_manager_v1::trading::execution::WorkerRejection::Admission(
                "modeSwitchTimeout"
            )
        )
    ));
}
#[tokio::test(start_paused = true)]
async fn expiry_after_switch_started_stops_job_without_retry() {
    let start = tokio::time::Instant::now();
    let result = ClientTradeWorker::new(&SwitchingDependencies, Duration::from_secs(1))
        .prepare(&body(), || 950 + start.elapsed().as_millis() as u64)
        .await;
    assert!(matches!(result, WorkerOutcome::Expired));
}

struct LeverageDependencies;
impl AdmissionDependencies for LeverageDependencies {
    fn check<'a>(
        &'a self,
        _: &'a ValidatedJob<'a>,
        attempt: &'a AdmissionAttempt,
    ) -> BoxFuture<'a, Result<AdmittedAccount, AdmissionFailure>> {
        Box::pin(async move {
            attempt.begin_leverage().unwrap();
            std::future::pending().await
        })
    }
}
#[tokio::test(start_paused = true)]
async fn worker_timeout_after_leverage_started_rejects_without_retry() {
    let result = ClientTradeWorker::new(&LeverageDependencies, Duration::from_millis(50))
        .prepare(&body(), || 1)
        .await;
    assert!(matches!(
        result,
        WorkerOutcome::Rejected(
            channels_manager_v1::trading::execution::WorkerRejection::Admission(
                "leverageChangeTimeout"
            )
        )
    ));
}
