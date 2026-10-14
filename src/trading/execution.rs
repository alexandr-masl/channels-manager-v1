//! Admission coordinator with automatic eligible Hedge migration.
use super::job::{JobValidationError, ValidatedJob, validate_job};
use crate::{
    contracts::messages::ClientTradeJob,
    exchanges::bingx::{
        admission::AdmittedAccount,
        client::BingxReadClient,
        migration::{AdmissionAttempt, admit_with_migration},
    },
    mongo::TradeRepository,
    signals::settings::{ResolvedSettings, SettingsError, resolve_settings},
};
use futures_util::future::BoxFuture;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum AdmissionFailure {
    Retryable(&'static str),
    Rejected(&'static str),
}
#[derive(Debug)]
pub enum WorkerRejection {
    InvalidPayload,
    Job(JobValidationError),
    Settings(SettingsError),
    Admission(&'static str),
}
pub struct PreparedAccount {
    pub job: ClientTradeJob,
    pub settings: ResolvedSettings,
    pub admission: AdmittedAccount,
}
pub enum WorkerOutcome {
    Admitted(Box<PreparedAccount>),
    Rejected(WorkerRejection),
    Expired,
    Retryable(&'static str),
}
pub trait AdmissionDependencies: Send + Sync {
    fn check<'a>(
        &'a self,
        job: &'a ValidatedJob<'a>,
        attempt: &'a AdmissionAttempt,
    ) -> BoxFuture<'a, Result<AdmittedAccount, AdmissionFailure>>;
}
pub struct ProductionAdmission<'a> {
    pub trades: &'a TradeRepository,
    pub exchange: &'a BingxReadClient,
}
impl AdmissionDependencies for ProductionAdmission<'_> {
    fn check<'a>(
        &'a self,
        job: &'a ValidatedJob<'a>,
        attempt: &'a AdmissionAttempt,
    ) -> BoxFuture<'a, Result<AdmittedAccount, AdmissionFailure>> {
        Box::pin(async move {
            let (managed, account) = tokio::try_join!(
                async {
                    let documents = self
                        .trades
                        .get_active_managed_futures_trades(job.client_id)
                        .await
                        .map_err(|_| AdmissionFailure::Retryable("managedTradesUnavailable"))?;
                    documents
                        .into_iter()
                        .map(|doc| {
                            serde_json::to_value(doc)
                                .map_err(|_| AdmissionFailure::Rejected("invalidManagedEvidence"))
                        })
                        .collect::<Result<Vec<_>, _>>()
                },
                async {
                    self.exchange
                        .read_account(
                            job.job.client["key"].as_str().expect("validated key"),
                            job.job.client["keySecret"]
                                .as_str()
                                .expect("validated secret"),
                            &job.normalized_symbol,
                        )
                        .await
                        .map_err(|e| {
                            if e.retryable {
                                AdmissionFailure::Retryable(e.code)
                            } else {
                                AdmissionFailure::Rejected(e.code)
                            }
                        })
                }
            )?;
            admit_with_migration(self.exchange, job, account, &managed, attempt)
                .await
                .map_err(|e| AdmissionFailure::Rejected(e.code))
        })
    }
}
pub struct ClientTradeWorker<'a, D: ?Sized> {
    dependencies: &'a D,
    timeout: Duration,
}
impl<'a, D: AdmissionDependencies + ?Sized> ClientTradeWorker<'a, D> {
    pub fn new(dependencies: &'a D, timeout: Duration) -> Self {
        Self {
            dependencies,
            timeout,
        }
    }
    pub async fn prepare(&self, body: &[u8], clock: impl Fn() -> u64) -> WorkerOutcome {
        if body.len() > 1024 * 1024 {
            return WorkerOutcome::Rejected(WorkerRejection::InvalidPayload);
        }
        let job: ClientTradeJob = match serde_json::from_slice(body) {
            Ok(job) => job,
            Err(_) => return WorkerOutcome::Rejected(WorkerRejection::InvalidPayload),
        };
        let now = clock();
        let validated = match validate_job(&job, now) {
            Ok(v) => v,
            Err(JobValidationError::Expired) => return WorkerOutcome::Expired,
            Err(e) => return WorkerOutcome::Rejected(WorkerRejection::Job(e)),
        };
        let settings = match resolve_settings(&job) {
            Ok(s) => s,
            Err(e) => return WorkerOutcome::Rejected(WorkerRejection::Settings(e)),
        };
        let budget = self
            .timeout
            .min(Duration::from_millis(validated.expires_at - now));
        let attempt = AdmissionAttempt::new(budget);
        let result =
            tokio::time::timeout(budget, self.dependencies.check(&validated, &attempt)).await;
        if clock() >= validated.expires_at {
            return WorkerOutcome::Expired;
        }
        match result {
            Ok(Ok(admission)) => WorkerOutcome::Admitted(Box::new(PreparedAccount {
                job,
                settings,
                admission,
            })),
            Ok(Err(AdmissionFailure::Retryable(code))) => WorkerOutcome::Retryable(code),
            Ok(Err(AdmissionFailure::Rejected(code))) => {
                WorkerOutcome::Rejected(WorkerRejection::Admission(code))
            }
            Err(_) if attempt.leverage_started() => {
                WorkerOutcome::Rejected(WorkerRejection::Admission("leverageChangeTimeout"))
            }
            Err(_) if attempt.switch_started() => {
                WorkerOutcome::Rejected(WorkerRejection::Admission("modeSwitchTimeout"))
            }
            Err(_) => WorkerOutcome::Retryable("admissionTimeout"),
        }
    }
}
