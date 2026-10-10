//! Automatic eligible One-Way migration. A successful POST is authoritative here;
//! failures stop the job without readback or application-level mutation retries.
use super::{
    admission::{
        AdmissionRejection, AdmittedAccount, ModeDecision, evaluate_position_mode, plan_admission,
    },
    client::{AccountEvidence, BingxReadClient},
};
use crate::trading::job::ValidatedJob;
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::time::Instant;

/// Shared with the outer timeout so a started write cannot become a read retry.
pub struct AdmissionAttempt {
    deadline: Instant,
    switch_started: AtomicBool,
    leverage_started: AtomicBool,
}
impl AdmissionAttempt {
    pub fn new(budget: Duration) -> Self {
        Self {
            deadline: Instant::now() + budget,
            switch_started: AtomicBool::new(false),
            leverage_started: AtomicBool::new(false),
        }
    }
    pub fn begin_switch(&self) -> Result<(), AdmissionRejection> {
        if Instant::now() >= self.deadline {
            return Err(AdmissionRejection {
                code: "admissionDeadlineExceeded",
            });
        }
        if self.switch_started.swap(true, Ordering::SeqCst) {
            return Err(AdmissionRejection {
                code: "modeSwitchAlreadyAttempted",
            });
        }
        Ok(())
    }
    pub fn begin_leverage(&self) -> Result<(), AdmissionRejection> {
        if Instant::now() >= self.deadline {
            return Err(AdmissionRejection {
                code: "admissionDeadlineExceeded",
            });
        }
        if self.leverage_started.swap(true, Ordering::SeqCst) {
            return Err(AdmissionRejection {
                code: "leverageChangeAlreadyAttempted",
            });
        }
        Ok(())
    }
    pub fn leverage_started(&self) -> bool {
        self.leverage_started.load(Ordering::SeqCst)
    }
    pub fn switch_started(&self) -> bool {
        self.switch_started.load(Ordering::SeqCst)
    }
}

pub async fn admit_with_migration(
    client: &BingxReadClient,
    job: &ValidatedJob<'_>,
    mut account: AccountEvidence,
    managed: &[Value],
    attempt: &AdmissionAttempt,
) -> Result<AdmittedAccount, AdmissionRejection> {
    let decision =
        evaluate_position_mode(&account, managed, job.client_id, &job.normalized_symbol)?;
    if decision == ModeDecision::MigrateToHedge {
        attempt.begin_switch()?;
        client
            .switch_to_hedge(
                job.job.client["key"].as_str().expect("validated key"),
                job.job.client["keySecret"]
                    .as_str()
                    .expect("validated secret"),
            )
            .await
            .map_err(|_| AdmissionRejection {
                code: "modeSwitchFailed",
            })?;
        account.mode = json!({"dualSidePosition":true});
        println!(
            "BingX mode switched: channel_id={} message_id={} symbol={} positionConfiguration=ORDER_LEDGER_V1",
            job.job.channel_id, job.job.message_id, job.normalized_symbol
        );
    }
    let (admitted, change) = plan_admission(
        &account,
        managed,
        job.client_id,
        &job.normalized_symbol,
        job.is_long,
        job.requested_leverage,
    )?;
    let side = change.unwrap_or(if admitted.position_mode == "ONEWAY" {
        "BOTH"
    } else if job.is_long {
        "LONG"
    } else {
        "SHORT"
    });
    if let Some(side) = change {
        attempt.begin_leverage()?;
        client
            .set_leverage(
                job.job.client["key"].as_str().expect("validated key"),
                job.job.client["keySecret"]
                    .as_str()
                    .expect("validated secret"),
                &job.normalized_symbol,
                job.requested_leverage,
                side,
            )
            .await
            .map_err(|_| AdmissionRejection {
                code: "leverageChangeFailed",
            })?;
    }
    println!(
        "BingX leverage: channel_id={} message_id={} symbol={} side={} previous_long={} previous_short={} leverage={} outcome={}",
        job.job.channel_id,
        job.job.message_id,
        job.normalized_symbol,
        side,
        account.leverage["longLeverage"],
        account.leverage["shortLeverage"],
        job.requested_leverage,
        if change.is_some() {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(admitted)
}
