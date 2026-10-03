use super::connection::Connection;
use crate::contracts::storage::bingx_metadata_key;
use serde_json::Value;
use std::{future::Future, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct MetadataCache {
    connection: Option<Arc<Connection>>,
    prefix: String,
    ttl: Duration,
}
impl MetadataCache {
    pub(super) fn new(connection: Option<Arc<Connection>>, prefix: String, ttl: Duration) -> Self {
        Self {
            connection,
            prefix,
            ttl,
        }
    }
    /// Redis failures/malformed JSON are misses. Loader errors propagate unchanged.
    pub async fn get_or_load<E, F>(
        &self,
        symbol: &str,
        loader: impl FnOnce() -> F,
    ) -> Result<Value, E>
    where
        F: Future<Output = Result<Value, E>>,
    {
        let key = bingx_metadata_key(&self.prefix, symbol);
        if let Some(conn) = &self.connection
            && let Ok(Some(raw)) = conn
                .query::<Option<String>>(redis::cmd("GET").arg(&key), conn.budget())
                .await
            && let Ok(value) = serde_json::from_str(&raw)
        {
            return Ok(value);
        }
        let value = loader().await?;
        let failed = value.get("err").is_some_and(json_truthy);
        if !failed && let Some(conn) = &self.connection {
            let _ = conn
                .query::<String>(
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(value.to_string())
                        .arg("PX")
                        .arg(self.ttl.as_millis() as u64),
                    conn.budget(),
                )
                .await;
        }
        Ok(value)
    }
}
fn json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}
