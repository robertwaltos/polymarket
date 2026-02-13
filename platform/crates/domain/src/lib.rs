use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Paper,
    Shadow,
    Live,
}

impl ExecutionMode {
    pub fn allows_live_execution(self) -> bool {
        matches!(self, Self::Live)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Kalshi,
    Ibkr,
    Coinbase,
    Polymarket,
    Other(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    Prediction,
    Crypto,
    Equity,
    Option,
    Future,
    Metal,
    Forex,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Instrument {
    pub venue: Venue,
    pub symbol: String,
    pub asset_class: AssetClass,
    pub quote_currency: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
    Gtc,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderRequest {
    pub request_id: Uuid,
    pub user_id: Uuid,
    pub strategy_id: Option<Uuid>,
    pub mode: ExecutionMode,
    pub instrument: Instrument,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub quantity: f64,
    pub limit_price: Option<f64>,
    pub submitted_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: IndexMap<String, String>,
}

impl OrderRequest {
    pub fn new(
        user_id: Uuid,
        mode: ExecutionMode,
        instrument: Instrument,
        side: Side,
        order_type: OrderType,
        time_in_force: TimeInForce,
        quantity: f64,
        limit_price: Option<f64>,
    ) -> Self {
        Self {
            request_id: Uuid::new_v4(),
            user_id,
            strategy_id: None,
            mode,
            instrument,
            side,
            order_type,
            time_in_force,
            quantity,
            limit_price,
            submitted_at: Utc::now(),
            metadata: IndexMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Accepted,
    Simulated,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderAck {
    pub request_id: Uuid,
    pub status: OrderStatus,
    pub venue_order_id: Option<String>,
    pub mode: ExecutionMode,
    pub message: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FillEvent {
    pub request_id: Uuid,
    pub fill_id: Uuid,
    pub instrument: Instrument,
    pub side: Side,
    pub quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub occurred_at: DateTime<Utc>,
    pub simulated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketQuote {
    pub instrument: Instrument,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub timestamp: DateTime<Utc>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BalanceSnapshot {
    pub venue: Venue,
    pub account_id: String,
    pub currency: String,
    pub total: f64,
    pub available: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PositionSnapshot {
    pub instrument: Instrument,
    pub quantity: f64,
    pub average_price: f64,
    pub market_price: Option<f64>,
    pub unrealized_pnl: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionContext {
    pub mode: ExecutionMode,
    pub live_enabled: bool,
    pub reason: Option<String>,
}

impl ExecutionContext {
    pub fn new(mode: ExecutionMode) -> Self {
        Self {
            mode,
            live_enabled: matches!(mode, ExecutionMode::Live),
            reason: None,
        }
    }
}
