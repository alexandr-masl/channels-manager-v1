use super::{MongoError, MongoErrorKind, MongoRole, error::bounded};
use crate::contracts::{
    messages::{Market, Provider, SignalSide},
    storage::*,
};
use futures_util::TryStreamExt;
use mongodb::{
    Collection, Database, IndexModel,
    bson::{self, DateTime, Document, doc},
    error::{ErrorKind, WriteFailure},
    options::{
        Acknowledgment, CollectionOptions, IndexOptions, ReadConcern, ReadPreference,
        SelectionCriteria, WriteConcern,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

/// Immutable canonical projection supplied by the worker; identity hashing belongs there.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimInput {
    pub work_id: String,
    pub input_hash: String,
    #[serde(deserialize_with = "integer_identifier")]
    pub channel_id: i64,
    #[serde(deserialize_with = "integer_identifier")]
    pub signal_message_id: i64,
    pub source_created_at: String,
    #[serde(deserialize_with = "integer_identifier")]
    pub chat_id: i64,
    pub exchange_client_id: String,
    pub provider: Provider,
    pub market: Market,
    pub parser_canonical_symbol: String,
    pub normalized_symbol: String,
    pub signal_side: SignalSide,
    pub canonical_signal_data: Value,
}
#[derive(Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    Created { owner_token: String },
    Duplicate,
    Conflict,
    Unavailable { code: &'static str },
    Uncertain { code: &'static str },
}
#[derive(Clone)]
pub struct ClaimStore {
    collection: Collection<Document>,
    initialized: Arc<AtomicBool>,
}
impl ClaimStore {
    pub(super) fn new(database: &Database) -> Self {
        let options = CollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Majority)
                    .journal(CLAIM_JOURNALED)
                    .w_timeout(CLAIM_WRITE_TIMEOUT)
                    .build(),
            )
            .read_concern(ReadConcern::majority())
            .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
            .build();
        Self {
            collection: database.collection_with_options(CLAIMS_COLLECTION, options),
            initialized: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(super) fn invalidate(&self) {
        self.initialized.store(false, Ordering::Release);
    }
    pub(super) async fn initialize(&self, timeout: Duration) -> Result<(), MongoError> {
        self.invalidate();
        let indexes: Vec<IndexModel> = bounded(MongoRole::Bot, timeout, async {
            self.collection
                .create_index(
                    IndexModel::builder()
                        .keys(doc! {"workId":1})
                        .options(
                            IndexOptions::builder()
                                .name(CLAIMS_INDEX.to_owned())
                                .unique(true)
                                .build(),
                        )
                        .build(),
                )
                .await?;
            self.collection.list_indexes().await?.try_collect().await
        })
        .await?;
        let valid = indexes.iter().any(|index| {
            index.keys == doc! {"workId":1}
                && index.options.as_ref().is_some_and(|o| {
                    o.name.as_deref() == Some(CLAIMS_INDEX)
                        && o.unique == Some(true)
                        && o.expire_after.is_none()
                        && o.sparse != Some(true)
                        && o.partial_filter_expression.is_none()
                })
        });
        if !valid {
            return Err(MongoError::new(
                MongoRole::Bot,
                MongoErrorKind::IndexContract,
            ));
        }
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }
    pub async fn claim(&self, input: &ClaimInput, producer_revision: &str) -> ClaimOutcome {
        if !self.initialized.load(Ordering::Acquire) {
            return ClaimOutcome::Unavailable {
                code: "INDEX_NOT_READY",
            };
        }
        if producer_revision.trim().is_empty() {
            return ClaimOutcome::Unavailable {
                code: "INVALID_PRODUCER_REVISION",
            };
        }
        let Ok(mut document) = bson::to_document(input) else {
            return ClaimOutcome::Unavailable {
                code: "INVALID_CLAIM_INPUT",
            };
        };
        let id = Uuid::new_v4().to_string();
        let owner_token = Uuid::new_v4().to_string();
        document.extend(doc! {"_id":&id,"ownerToken":&owner_token,"claimedAt":DateTime::now(),"producerRevision":producer_revision});
        match tokio::time::timeout(CLAIM_OPERATION_DEADLINE, async {
            self.collection.insert_one(document).await
        })
        .await
        {
            Ok(Ok(result)) if result.inserted_id.as_str() == Some(&id) => {
                ClaimOutcome::Created { owner_token }
            }
            Ok(Err(error)) if duplicate_key(&error) => self.compare_duplicate(input).await,
            Ok(Err(error))
                if matches!(
                    error.kind.as_ref(),
                    ErrorKind::ServerSelection { .. } | ErrorKind::Shutdown
                ) =>
            {
                ClaimOutcome::Unavailable {
                    code: "CLAIM_UNAVAILABLE",
                }
            }
            _ => ClaimOutcome::Uncertain {
                code: "CLAIM_ACKNOWLEDGEMENT_UNCERTAIN",
            },
        }
    }
    async fn compare_duplicate(&self, input: &ClaimInput) -> ClaimOutcome {
        let result = bounded(MongoRole::Bot, CLAIM_OPERATION_DEADLINE, async {
            self.collection
                .find_one(doc! {"workId":&input.work_id})
                .max_time(CLAIM_WRITE_TIMEOUT)
                .await
        })
        .await;
        match result {
            Ok(Some(document)) => {
                let existing = bson::from_document::<ClaimInput>(document)
                    .ok()
                    .and_then(|v| serde_json::to_value(v).ok());
                let expected = serde_json::to_value(input).ok();
                if existing
                    .as_ref()
                    .zip(expected.as_ref())
                    .is_some_and(|(a, b)| json_equal(a, b))
                {
                    ClaimOutcome::Duplicate
                } else {
                    ClaimOutcome::Conflict
                }
            }
            Ok(None) => ClaimOutcome::Uncertain {
                code: "DUPLICATE_NOT_VISIBLE",
            },
            Err(_) => ClaimOutcome::Unavailable {
                code: "DUPLICATE_READ_UNAVAILABLE",
            },
        }
    }
    /// Best effort diagnostics only; claims remain durable regardless of terminal writes.
    pub async fn record_terminal(
        &self,
        work_id: &str,
        owner_token: &str,
        outcome: &str,
        code: &str,
    ) -> bool {
        bounded(MongoRole::Bot, CLAIM_TERMINAL_DEADLINE, async {
            self.collection
                .update_one(
                    doc! {"workId":work_id,"ownerToken":owner_token},
                    doc! {"$set":{"terminal.outcome":outcome,"terminal.code":code,"terminal.at":DateTime::now()}},
                )
                .await
        })
        .await
        .is_ok_and(|r| r.matched_count == 1)
    }
}
fn duplicate_key(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        ErrorKind::Write(WriteFailure::WriteError(e)) => e.code == 11000,
        ErrorKind::Command(e) => e.code == 11000,
        _ => false,
    }
}
// Node stores large Telegram identifiers as BSON doubles, even when integral.
fn integer_identifier<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let value = Value::deserialize(deserializer)?;
    if let Some(integer) = value.as_i64() {
        return Ok(integer);
    }
    if let Some(number) = value.as_f64()
        && number.is_finite()
        && number.fract() == 0.0
        && number.abs() <= 9_007_199_254_740_991.0
    {
        return Ok(number as i64);
    }
    Err(serde::de::Error::custom("expected an integral identifier"))
}
// Node represents JSON numbers as doubles, including integral BSON doubles.
fn json_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).is_some_and(|other| json_equal(v, other)))
        }
        _ => a == b,
    }
}
