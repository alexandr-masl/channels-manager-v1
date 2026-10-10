//! Pure conversion to the existing TypeScript client-job wire contract.
use super::manager::PreparationError;
use crate::{
    contracts::messages::{ClientTradeJob, JobType, JobVersion, Market, Provider, TradeIdentity},
    telegram::ChannelContext,
};
use mongodb::bson::{Bson, DateTime, Document};
use serde_json::{Map, Value, json};

pub(super) fn build_jobs(
    context: &ChannelContext,
    opened: &[Document],
    snapshot: Value,
    expires_at: u64,
) -> Result<Vec<ClientTradeJob>, PreparationError> {
    let signal =
        serde_json::to_value(&context.signal).map_err(|_| PreparationError::InvalidContext)?;
    let channel_settings = document_json(&context.channel_settings)?;
    let source = iso_time(
        i64::try_from(context.message.source_created_at_ms())
            .map_err(|_| PreparationError::InvalidContext)?,
    )?;
    let mut trades = std::collections::BTreeMap::<i64, Vec<Value>>::new();
    for trade in opened {
        let user = numeric_id(trade.get("chat_id")).ok_or(PreparationError::InvalidContext)?;
        trades.entry(user).or_default().push(document_json(trade)?);
    }
    context.clients.iter().map(|client| {
        if client.client_id.trim().is_empty() || client.key.trim().is_empty() || client.key_secret.trim().is_empty() || client.chat_id<=0 {return Err(PreparationError::InvalidContext);}
        let identity=TradeIdentity {channel_id:context.message.channel_id(),message_id:context.message.message_id(),chat_id:client.chat_id,client_id:&client.client_id,symbol:&context.signal.symbol};
        let mut data=json!({"chatId":client.chat_id,"clientId":client.client_id,"provider":"BingX","key":client.key,"keySecret":client.key_secret});
        if let Some(name)=&client.name { data["name"]=json!(name); }
        Ok(ClientTradeJob {
            job_type:JobType::CreateTrade,version:JobVersion::V1,channel_id:context.message.channel_id(),message_id:context.message.message_id(),
            source_created_at:Some(source.clone()),trade_expires_at:Some(expires_at),signal_data:signal.clone(),channel_settings:channel_settings.clone(),
            client:data,user_config:client.user_config.as_ref().map(document_json).transpose()?,market_data:snapshot.clone(),
            opened_trades:trades.get(&client.chat_id).cloned().unwrap_or_default(),idempotency_key:identity.idempotency_key(),partition_key:identity.partition_key(),
            provider:Provider::BingX,market:Market::Futures,symbol:context.signal.symbol.clone(),
        })
    }).collect()
}
fn numeric_id(value: Option<&Bson>) -> Option<i64> {
    match value? {
        Bson::Int32(n) => Some(i64::from(*n)),
        Bson::Int64(n) => Some(*n),
        Bson::Double(n)
            if n.is_finite() && n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0 =>
        {
            Some(*n as i64)
        }
        _ => None,
    }
}
fn document_json(doc: &Document) -> Result<Value, PreparationError> {
    doc.iter()
        .map(|(k, v)| Ok((k.clone(), bson_json(v)?)))
        .collect::<Result<Map<_, _>, _>>()
        .map(Value::Object)
}
fn bson_json(value: &Bson) -> Result<Value, PreparationError> {
    Ok(match value {
        Bson::Document(doc) => document_json(doc)?,
        Bson::Array(values) => {
            Value::Array(values.iter().map(bson_json).collect::<Result<_, _>>()?)
        }
        Bson::ObjectId(id) => json!(id.to_hex()),
        Bson::DateTime(date) => json!(iso_time(date.timestamp_millis())?),
        Bson::String(s) => json!(s),
        Bson::Boolean(b) => json!(b),
        Bson::Null => Value::Null,
        Bson::Int32(n) => json!(n),
        Bson::Int64(n) => json!(n),
        Bson::Double(n) if n.is_finite() => {
            if n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0 {
                json!(*n as i64)
            } else {
                json!(n)
            }
        }
        // Do not silently emit Mongo Extended JSON where Mongoose sends plain JSON.
        _ => return Err(PreparationError::InvalidContext),
    })
}
fn iso_time(ms: i64) -> Result<String, PreparationError> {
    let mut text = DateTime::from_millis(ms)
        .try_to_rfc3339_string()
        .map_err(|_| PreparationError::InvalidContext)?;
    if !text.contains('.') {
        text = text.trim_end_matches('Z').to_owned() + ".000Z";
    } else {
        let (head, fraction) = text
            .split_once('.')
            .ok_or(PreparationError::InvalidContext)?;
        text = format!(
            "{head}.{fraction:0<3}Z",
            fraction = fraction.trim_end_matches('Z')
        );
    }
    Ok(text)
}
