//! Signal preparation only: no claims, locks, capacity limit or publication.
use crate::{
    contracts::messages::{ClientTradeJob, TRADE_EXPIRY_AFTER_ACCEPTANCE_MS},
    exchanges::bingx::market_data::{MarketDataError, MarketDataProvider},
    mongo::TradeRepository,
    telegram::ChannelContext,
};
use futures_util::future::BoxFuture;
use mongodb::bson::Document;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparationError {
    DatabaseUnavailable,
    MarketUnavailable,
    InvalidMarketResponse,
    InvalidContext,
}
#[derive(Debug, PartialEq, Eq)]
pub enum PreparationSkip {
    NoEligibleAccounts,
}
#[derive(Debug, PartialEq, Eq)]
pub enum PreparationReject {
    UnsupportedSymbol,
    InvalidContext,
}
pub enum PreparationOutcome {
    Prepared(PreparedSignal),
    Skipped(PreparationSkip),
    Rejected(PreparationReject),
}
// Jobs contain secrets: deliberately no Debug or Serialize on the enclosing batch.
pub struct PreparedSignal {
    pub jobs: Vec<ClientTradeJob>,
    pub summary: PreparationSummary,
}
#[derive(Debug, Serialize)]
pub struct PreparationSummary {
    pub channel_id: i64,
    pub message_id: i64,
    pub symbol: String,
    pub prepared_jobs: usize,
    pub eligible_users: usize,
    pub accepted_at_ms: u64,
    pub expires_at_ms: u64,
    pub published_jobs: usize,
}
pub trait OpenTradeRepository: Send + Sync {
    fn opened_trades(
        &self,
        users: Vec<i64>,
    ) -> BoxFuture<'_, Result<Vec<Document>, PreparationError>>;
}
impl OpenTradeRepository for TradeRepository {
    fn opened_trades(
        &self,
        users: Vec<i64>,
    ) -> BoxFuture<'_, Result<Vec<Document>, PreparationError>> {
        Box::pin(async move {
            self.get_opened_trades_by_users(&users)
                .await
                .map_err(|_| PreparationError::DatabaseUnavailable)
        })
    }
}
pub struct SignalManager<'a, R: ?Sized, M: ?Sized> {
    repository: &'a R,
    market: &'a M,
}
impl<'a, R: OpenTradeRepository + ?Sized, M: MarketDataProvider + ?Sized> SignalManager<'a, R, M> {
    pub fn new(repository: &'a R, market: &'a M) -> Self {
        Self { repository, market }
    }
    /// Read clock after dependency work so the job window starts at acceptance.
    pub async fn prepare(
        &self,
        context: ChannelContext,
        clock: impl FnOnce() -> u64,
    ) -> Result<PreparationOutcome, PreparationError> {
        if context.clients.is_empty() {
            return Ok(PreparationOutcome::Skipped(
                PreparationSkip::NoEligibleAccounts,
            ));
        }
        let users: BTreeSet<_> = context.clients.iter().map(|c| c.chat_id).collect();
        let opened = self
            .repository
            .opened_trades(users.iter().copied().collect())
            .await?;
        let snapshot = match self.market.snapshot(&context.signal.symbol).await {
            Ok(snapshot) => snapshot,
            Err(MarketDataError::UnsupportedSymbol) => {
                return Ok(PreparationOutcome::Rejected(
                    PreparationReject::UnsupportedSymbol,
                ));
            }
            Err(MarketDataError::Unavailable) => return Err(PreparationError::MarketUnavailable),
            Err(MarketDataError::InvalidResponse) => {
                return Err(PreparationError::InvalidMarketResponse);
            }
        };
        let accepted = clock();
        let Some(expiry) = accepted
            .checked_add(TRADE_EXPIRY_AFTER_ACCEPTANCE_MS)
            .filter(|_| accepted > 0)
        else {
            return Ok(PreparationOutcome::Rejected(
                PreparationReject::InvalidContext,
            ));
        };
        let jobs = match super::jobs::build_jobs(
            &context,
            &opened,
            json!({"bingXFutures":{"currPrice":snapshot.curr_price,"symbolInfo":snapshot.symbol_info}}),
            expiry,
        ) {
            Ok(jobs) => jobs,
            Err(_) => {
                return Ok(PreparationOutcome::Rejected(
                    PreparationReject::InvalidContext,
                ));
            }
        };
        Ok(PreparationOutcome::Prepared(PreparedSignal {
            summary: PreparationSummary {
                channel_id: context.message.channel_id(),
                message_id: context.message.message_id(),
                symbol: context.signal.symbol,
                prepared_jobs: jobs.len(),
                eligible_users: users.len(),
                accepted_at_ms: accepted,
                expires_at_ms: expiry,
                published_jobs: 0,
            },
            jobs,
        }))
    }
}
