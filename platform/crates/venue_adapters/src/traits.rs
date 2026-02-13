use crate::error::AdapterResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use domain::{BalanceSnapshot, MarketQuote, OrderRequest, Venue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub venue: Venue,
    pub ok: bool,
    pub latency_ms: u128,
    pub detail: String,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueOrderResult {
    pub venue: Venue,
    pub venue_order_id: Option<String>,
    pub accepted: bool,
    pub message: String,
}

#[async_trait]
pub trait VenueAdapter: Send + Sync {
    fn venue(&self) -> Venue;
    async fn health(&self) -> AdapterResult<HealthReport>;
    async fn fetch_quote(&self, symbol: &str) -> AdapterResult<MarketQuote>;
    async fn fetch_balances(&self) -> AdapterResult<Vec<BalanceSnapshot>>;
    async fn place_live_order(&self, order: &OrderRequest) -> AdapterResult<VenueOrderResult>;
}
