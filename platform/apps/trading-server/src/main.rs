mod intel;
mod orchestrator;
mod slicing;

use anyhow::{anyhow, Context, Result};
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::extract::{Path, Query, State};
use axum::http::{header, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use domain::{
    AssetClass, ExecutionMode, Instrument, OrderRequest, OrderStatus, OrderType, Side, TimeInForce,
    Venue,
};
use dotenvy::dotenv;
use hmac::{Hmac, Mac};
use intel::{ManualClaimInput, SocialClaim, SocialScanRequest, SocialSource};
use oms::{JsonlAuditStore, OmsConfig, OmsEngine, OmsError, OrderAuditRecord};
use orchestrator::{
    CapitalAllocationInput, EnsemblePolicyInput, OrchestratorConfig, StrategyOrchestrator,
    StrategyPerformanceInput, UpsertStrategyAgentInput,
};
use portfolio::PortfolioState;
use risk::{RiskEngine, RiskLimits};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use venue_adapters::{
    AdapterRegistry, CoinbaseAdapter, CoinbaseConfig, IbkrAdapter, IbkrConfig, KalshiAdapter,
    KalshiConfig,
};

#[derive(Clone)]
struct AppState {
    oms: OmsManager,
    auth: AuthStore,
    default_mode: ExecutionMode,
    db: Option<Arc<PgStore>>,
    rate_limiter: OrgRateLimiter,
    trade_guard: HierarchicalTradeGuard,
    live_guard: LiveExecutionGuard,
    intel: intel::SocialIntelStore,
    orchestrator: StrategyOrchestrator,
    registry: InstrumentRegistryStore,
    research: ResearchRunStore,
    event_bus: EventBus,
}

#[derive(Clone)]
struct OmsManager {
    config: OmsConfig,
    adapters: AdapterRegistry,
    risk: RiskEngine,
    portfolio_template: PortfolioState,
    audit_dir: PathBuf,
    engines: Arc<Mutex<HashMap<uuid::Uuid, OmsEngine>>>,
}

impl OmsManager {
    fn new(
        config: OmsConfig,
        adapters: AdapterRegistry,
        risk: RiskEngine,
        portfolio_template: PortfolioState,
        audit_dir: PathBuf,
    ) -> Self {
        Self {
            config,
            adapters,
            risk,
            portfolio_template,
            audit_dir,
            engines: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn for_org(&self, org_id: uuid::Uuid) -> Result<OmsEngine> {
        let mut guard = self
            .engines
            .lock()
            .map_err(|_| anyhow!("oms manager lock poisoned"))?;
        if let Some(engine) = guard.get(&org_id) {
            return Ok(engine.clone());
        }

        let filename = format!("org_{}.jsonl", org_id.simple());
        let path = self.audit_dir.join(filename);
        let audit_store = Arc::new(
            JsonlAuditStore::new(path).context("failed to initialize org-specific audit store")?,
        );
        let engine = OmsEngine::with_audit_store(
            self.config.clone(),
            self.adapters.clone(),
            self.risk.clone(),
            self.portfolio_template.clone(),
            audit_store,
        );
        guard.insert(org_id, engine.clone());
        Ok(engine)
    }

    fn adapter_registry(&self) -> AdapterRegistry {
        self.adapters.clone()
    }

    fn configured_venues(&self) -> Vec<Venue> {
        self.adapters
            .all()
            .into_iter()
            .map(|adapter| adapter.venue())
            .collect()
    }
}

#[derive(Clone)]
struct InstrumentRegistryStore {
    entries: Arc<Mutex<HashMap<uuid::Uuid, HashMap<String, domain::CanonicalInstrument>>>>,
}

impl Default for InstrumentRegistryStore {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl InstrumentRegistryStore {
    fn upsert(
        &self,
        org_id: uuid::Uuid,
        record: domain::CanonicalInstrument,
    ) -> Result<domain::CanonicalInstrument> {
        let mut guard = self
            .entries
            .lock()
            .map_err(|_| anyhow!("instrument registry lock poisoned"))?;
        let org_bucket = guard.entry(org_id).or_default();
        org_bucket.insert(registry_key(&record.venue, &record.symbol), record.clone());
        Ok(record)
    }

    fn list(
        &self,
        org_id: uuid::Uuid,
        query: &InstrumentRegistryQuery,
    ) -> Result<Vec<domain::CanonicalInstrument>> {
        let guard = self
            .entries
            .lock()
            .map_err(|_| anyhow!("instrument registry lock poisoned"))?;
        let Some(records) = guard.get(&org_id) else {
            return Ok(Vec::new());
        };
        Ok(filter_registry_records(records.values().cloned().collect(), query))
    }

    fn lookup(
        &self,
        org_id: uuid::Uuid,
        venue: &Venue,
        symbol: &str,
    ) -> Result<Option<domain::CanonicalInstrument>> {
        let guard = self
            .entries
            .lock()
            .map_err(|_| anyhow!("instrument registry lock poisoned"))?;
        let Some(records) = guard.get(&org_id) else {
            return Ok(None);
        };
        Ok(records.get(&registry_key(venue, symbol)).cloned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResearchRunStatus {
    Pass,
    Fail,
    ManualReview,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResearchMetrics {
    sharpe: f64,
    max_drawdown_pct: f64,
    total_return_pct: f64,
    trades: u64,
    #[serde(default)]
    turnover_pct: Option<f64>,
    #[serde(default)]
    hit_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResearchAcceptanceThresholds {
    min_sharpe: f64,
    max_drawdown_pct: f64,
    min_trades: u64,
    min_return_pct: f64,
    min_sharpe_delta_vs_baseline: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResearchAcceptanceResult {
    pass: bool,
    status: ResearchRunStatus,
    reasons: Vec<String>,
    thresholds: ResearchAcceptanceThresholds,
    sharpe_delta_vs_baseline: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResearchRunRecord {
    id: uuid::Uuid,
    org_id: uuid::Uuid,
    strategy_id: Option<uuid::Uuid>,
    dataset_snapshot_id: String,
    feature_set_hash: String,
    code_version: String,
    config_hash: String,
    baseline_run_id: Option<uuid::Uuid>,
    metrics: ResearchMetrics,
    acceptance: ResearchAcceptanceResult,
    status: ResearchRunStatus,
    artifact_uri: Option<String>,
    notes: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ResearchAcceptanceConfig {
    min_sharpe: f64,
    max_drawdown_pct: f64,
    min_trades: u64,
    min_return_pct: f64,
    min_sharpe_delta_vs_baseline: f64,
}

impl ResearchAcceptanceConfig {
    fn from_env() -> Self {
        Self {
            min_sharpe: env_f64("RESEARCH_MIN_SHARPE", 0.8),
            max_drawdown_pct: env_f64("RESEARCH_MAX_DRAWDOWN_PCT", 18.0).max(1.0),
            min_trades: env_u64("RESEARCH_MIN_TRADES", 50),
            min_return_pct: env_f64("RESEARCH_MIN_RETURN_PCT", 2.0),
            min_sharpe_delta_vs_baseline: env_f64("RESEARCH_MIN_SHARPE_DELTA", 0.1),
        }
    }

    fn thresholds(&self) -> ResearchAcceptanceThresholds {
        ResearchAcceptanceThresholds {
            min_sharpe: self.min_sharpe,
            max_drawdown_pct: self.max_drawdown_pct,
            min_trades: self.min_trades,
            min_return_pct: self.min_return_pct,
            min_sharpe_delta_vs_baseline: self.min_sharpe_delta_vs_baseline,
        }
    }
}

#[derive(Clone, Default)]
struct ResearchRunStore {
    records: Arc<Mutex<HashMap<uuid::Uuid, Vec<ResearchRunRecord>>>>,
}

impl ResearchRunStore {
    fn insert(&self, record: ResearchRunRecord) -> Result<ResearchRunRecord> {
        let mut guard = self
            .records
            .lock()
            .map_err(|_| anyhow!("research run store lock poisoned"))?;
        let bucket = guard.entry(record.org_id).or_default();
        bucket.push(record.clone());
        bucket.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(record)
    }

    fn list(
        &self,
        org_id: uuid::Uuid,
        limit: usize,
        strategy_id: Option<uuid::Uuid>,
    ) -> Result<Vec<ResearchRunRecord>> {
        let guard = self
            .records
            .lock()
            .map_err(|_| anyhow!("research run store lock poisoned"))?;
        let mut records = guard.get(&org_id).cloned().unwrap_or_default();
        if let Some(strategy_id) = strategy_id {
            records.retain(|record| record.strategy_id == Some(strategy_id));
        }
        records.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let safe_limit = limit.clamp(1, 5_000);
        if records.len() > safe_limit {
            records.truncate(safe_limit);
        }
        Ok(records)
    }

    fn get(&self, org_id: uuid::Uuid, run_id: uuid::Uuid) -> Result<Option<ResearchRunRecord>> {
        let guard = self
            .records
            .lock()
            .map_err(|_| anyhow!("research run store lock poisoned"))?;
        Ok(guard
            .get(&org_id)
            .and_then(|records| records.iter().find(|record| record.id == run_id).cloned()))
    }
}

#[derive(Clone)]
struct OrgRateLimiter {
    max_per_minute: usize,
    events: Arc<Mutex<HashMap<uuid::Uuid, Vec<i64>>>>,
}

impl OrgRateLimiter {
    fn new(max_per_minute: usize) -> Self {
        Self {
            max_per_minute: max_per_minute.max(1),
            events: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn check(&self, org_id: uuid::Uuid, now_epoch_secs: i64) -> Result<(), anyhow::Error> {
        let window_start = now_epoch_secs - 60;
        let mut guard = self
            .events
            .lock()
            .map_err(|_| anyhow!("rate limiter lock poisoned"))?;
        let bucket = guard.entry(org_id).or_default();
        bucket.retain(|ts| *ts >= window_start);
        if bucket.len() >= self.max_per_minute {
            return Err(anyhow!("org rate limit exceeded"));
        }
        bucket.push(now_epoch_secs);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TradeGuardConfig {
    org_orders_per_min: usize,
    strategy_orders_per_min: usize,
    instrument_orders_per_min: usize,
    strategy_cooldown_secs: i64,
    max_spread_cross_bps: Option<f64>,
    max_adverse_selection_bps: Option<f64>,
}

impl TradeGuardConfig {
    fn from_env() -> Self {
        Self {
            org_orders_per_min: env_u32("TRADE_GUARD_ORG_ORDERS_PER_MIN", 240) as usize,
            strategy_orders_per_min: env_u32("TRADE_GUARD_STRATEGY_ORDERS_PER_MIN", 120) as usize,
            instrument_orders_per_min: env_u32("TRADE_GUARD_INSTRUMENT_ORDERS_PER_MIN", 80)
                as usize,
            strategy_cooldown_secs: env_i64("TRADE_GUARD_STRATEGY_COOLDOWN_SECS", 120).max(5),
            max_spread_cross_bps: env_optional_positive_f64("TRADE_GUARD_MAX_SPREAD_CROSS_BPS"),
            max_adverse_selection_bps: env_optional_positive_f64(
                "TRADE_GUARD_MAX_ADVERSE_SELECTION_BPS",
            ),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct StrategyBucketKey {
    org_id: uuid::Uuid,
    strategy_id: uuid::Uuid,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct InstrumentBucketKey {
    org_id: uuid::Uuid,
    venue: Venue,
    symbol: String,
}

#[derive(Debug, Clone)]
struct TradeGuardState {
    org_events: HashMap<uuid::Uuid, Vec<i64>>,
    strategy_events: HashMap<StrategyBucketKey, Vec<i64>>,
    instrument_events: HashMap<InstrumentBucketKey, Vec<i64>>,
    strategy_cooldowns: HashMap<StrategyBucketKey, DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct TradeGuardInput {
    org_id: uuid::Uuid,
    strategy_id: Option<uuid::Uuid>,
    venue: Venue,
    symbol: String,
    order_type: OrderType,
    metadata: indexmap::IndexMap<String, String>,
    now: DateTime<Utc>,
}

#[derive(Clone)]
struct HierarchicalTradeGuard {
    config: TradeGuardConfig,
    state: Arc<Mutex<TradeGuardState>>,
}

impl HierarchicalTradeGuard {
    fn new(config: TradeGuardConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(TradeGuardState {
                org_events: HashMap::new(),
                strategy_events: HashMap::new(),
                instrument_events: HashMap::new(),
                strategy_cooldowns: HashMap::new(),
            })),
        }
    }

    fn authorize_and_record(&self, input: &TradeGuardInput) -> Result<(), String> {
        let now = input.now;
        let now_epoch = now.timestamp();
        let window_start = now_epoch - 60;

        let mut guard = self
            .state
            .lock()
            .map_err(|_| "trade guard lock poisoned".to_string())?;

        if let Some(strategy_id) = input.strategy_id {
            let strategy_key = StrategyBucketKey {
                org_id: input.org_id,
                strategy_id,
            };
            if let Some(cooldown_until) = guard.strategy_cooldowns.get(&strategy_key) {
                if *cooldown_until > now {
                    return Err(format!(
                        "strategy cooldown active until {}",
                        cooldown_until.to_rfc3339()
                    ));
                }
            }
        }

        if let Some(limit_bps) = self.config.max_spread_cross_bps {
            if let Some(actual_bps) = metadata_map_f64(
                &input.metadata,
                &["spread_cross_bps", "spread_bps", "quoted_spread_bps"],
            ) {
                if actual_bps > limit_bps {
                    self.cooldown_strategy_locked(&mut guard, input.strategy_id, input.org_id, now);
                    return Err(format!(
                        "spread-cross guard tripped: {:.2} bps exceeds {:.2} bps limit",
                        actual_bps, limit_bps
                    ));
                }
            } else if matches!(input.order_type, OrderType::Market) {
                // Conservative fallback: market orders without spread context are restricted.
                self.cooldown_strategy_locked(&mut guard, input.strategy_id, input.org_id, now);
                return Err(
                    "spread-cross guard requires spread metadata for market orders".to_string()
                );
            }
        }

        if let Some(limit_bps) = self.config.max_adverse_selection_bps {
            if let Some(actual_bps) = metadata_map_f64(
                &input.metadata,
                &["adverse_selection_bps", "expected_adverse_selection_bps"],
            ) {
                if actual_bps > limit_bps {
                    self.cooldown_strategy_locked(&mut guard, input.strategy_id, input.org_id, now);
                    return Err(format!(
                        "adverse-selection guard tripped: {:.2} bps exceeds {:.2} bps limit",
                        actual_bps, limit_bps
                    ));
                }
            }
        }

        let org_limit = self.config.org_orders_per_min.max(1);
        {
            let org_bucket = guard.org_events.entry(input.org_id).or_default();
            org_bucket.retain(|ts| *ts >= window_start);
            if org_bucket.len() >= org_limit {
                return Err(format!(
                    "org order-rate limit exceeded: {} orders/min",
                    org_limit
                ));
            }
        }

        if let Some(strategy_id) = input.strategy_id {
            let strategy_key = StrategyBucketKey {
                org_id: input.org_id,
                strategy_id,
            };
            let strategy_limit = self.config.strategy_orders_per_min.max(1);
            {
                let strategy_bucket = guard
                    .strategy_events
                    .entry(strategy_key.clone())
                    .or_default();
                strategy_bucket.retain(|ts| *ts >= window_start);
                if strategy_bucket.len() >= strategy_limit {
                    self.cooldown_strategy_locked(&mut guard, Some(strategy_id), input.org_id, now);
                    return Err(format!(
                        "strategy order-rate limit exceeded: {} orders/min",
                        strategy_limit
                    ));
                }
            }
        }

        let instrument_key = InstrumentBucketKey {
            org_id: input.org_id,
            venue: input.venue.clone(),
            symbol: input.symbol.trim().to_ascii_uppercase(),
        };
        let instrument_limit = self.config.instrument_orders_per_min.max(1);
        {
            let instrument_bucket = guard
                .instrument_events
                .entry(instrument_key.clone())
                .or_default();
            instrument_bucket.retain(|ts| *ts >= window_start);
            if instrument_bucket.len() >= instrument_limit {
                return Err(format!(
                    "instrument order-rate limit exceeded: {} orders/min for {}:{}",
                    instrument_limit,
                    venue_code(&input.venue),
                    input.symbol.to_ascii_uppercase()
                ));
            }
        }

        guard.org_events.entry(input.org_id).or_default().push(now_epoch);
        if let Some(strategy_id) = input.strategy_id {
            let strategy_key = StrategyBucketKey {
                org_id: input.org_id,
                strategy_id,
            };
            guard
                .strategy_events
                .entry(strategy_key)
                .or_default()
                .push(now_epoch);
        }
        guard
            .instrument_events
            .entry(instrument_key)
            .or_default()
            .push(now_epoch);
        Ok(())
    }

    fn cooldown_strategy_locked(
        &self,
        state: &mut TradeGuardState,
        strategy_id: Option<uuid::Uuid>,
        org_id: uuid::Uuid,
        now: DateTime<Utc>,
    ) {
        let Some(strategy_id) = strategy_id else {
            return;
        };
        let until = now + Duration::seconds(self.config.strategy_cooldown_secs.max(5));
        state.strategy_cooldowns.insert(
            StrategyBucketKey {
                org_id,
                strategy_id,
            },
            until,
        );
    }
}

#[derive(Debug, Clone, Serialize)]
struct StreamEvent {
    kind: String,
    payload: serde_json::Value,
    at: DateTime<Utc>,
}

#[derive(Clone)]
struct EventBus {
    tx: broadcast::Sender<StreamEvent>,
}

impl EventBus {
    fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(32));
        Self { tx }
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.tx.subscribe()
    }

    fn publish(&self, kind: &str, payload: serde_json::Value) {
        let _ = self.tx.send(StreamEvent {
            kind: kind.to_string(),
            payload,
            at: Utc::now(),
        });
    }
}

#[derive(Debug, Clone)]
struct LiveGuardConfig {
    health_poll_interval_secs: u64,
    max_health_age_secs: i64,
    failure_threshold: u32,
    cooldown_secs: i64,
    require_healthy: bool,
    slo_window_secs: i64,
    slo_min_samples: usize,
    max_rejection_rate: Option<f64>,
    max_p95_latency_ms: Option<u128>,
}

impl LiveGuardConfig {
    fn from_env() -> Self {
        Self {
            health_poll_interval_secs: env_u64("LIVE_GUARD_HEALTH_POLL_SECS", 10),
            max_health_age_secs: env_i64("LIVE_GUARD_HEALTH_MAX_AGE_SECS", 45),
            failure_threshold: env_u32("LIVE_GUARD_FAILURE_THRESHOLD", 3),
            cooldown_secs: env_i64("LIVE_GUARD_COOLDOWN_SECS", 120),
            require_healthy: env_bool("LIVE_GUARD_REQUIRE_HEALTHY", true),
            slo_window_secs: env_i64("LIVE_GUARD_SLO_WINDOW_SECS", 900).max(30),
            slo_min_samples: env_u32("LIVE_GUARD_SLO_MIN_SAMPLES", 5) as usize,
            max_rejection_rate: env_optional_positive_f64("LIVE_GUARD_MAX_REJECTION_RATE")
                .map(|value| value.clamp(0.01, 1.0)),
            max_p95_latency_ms: env_optional_positive_f64("LIVE_GUARD_MAX_P95_LATENCY_MS")
                .map(|value| value.max(1.0).round() as u128),
        }
    }
}

#[derive(Debug, Clone)]
struct AutomationConfig {
    enabled: bool,
    interval_secs: u64,
    lookback_days: i64,
    max_per_query: usize,
    use_intel_signals: bool,
}

impl AutomationConfig {
    fn from_env() -> Self {
        Self {
            enabled: env_bool("AUTO_IMPROVE_ENABLED", false),
            interval_secs: env_u64("AUTO_IMPROVE_INTERVAL_SECS", 900).max(30),
            lookback_days: env_i64("AUTO_IMPROVE_LOOKBACK_DAYS", 4).clamp(1, 14),
            max_per_query: env_u64("AUTO_IMPROVE_MAX_PER_QUERY", 20) as usize,
            use_intel_signals: env_bool("AUTO_IMPROVE_USE_INTEL_SIGNALS", true),
        }
    }
}

#[derive(Debug, Clone)]
struct VenueHealthState {
    ok: bool,
    detail: String,
    latency_ms: Option<u128>,
    checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct VenueLiveGuardState {
    live_enabled: bool,
    consecutive_live_failures: u32,
    breaker_open_until: Option<DateTime<Utc>>,
    last_health: Option<VenueHealthState>,
    live_result_samples: Vec<(DateTime<Utc>, bool)>,
    health_latency_samples: Vec<(DateTime<Utc>, u128)>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct LiveGuardState {
    kill_switch_enabled: bool,
    kill_switch_reason: Option<String>,
    kill_switch_updated_at: DateTime<Utc>,
    venues: HashMap<Venue, VenueLiveGuardState>,
}

#[derive(Debug, Clone, Serialize)]
struct LiveGuardConfigView {
    require_healthy: bool,
    max_health_age_secs: i64,
    failure_threshold: u32,
    cooldown_secs: i64,
    health_poll_interval_secs: u64,
    slo_window_secs: i64,
    slo_min_samples: usize,
    max_rejection_rate: Option<f64>,
    max_p95_latency_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize)]
struct LiveGuardGlobalView {
    kill_switch_enabled: bool,
    kill_switch_reason: Option<String>,
    kill_switch_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct LiveGuardVenueView {
    venue: String,
    live_enabled: bool,
    live_allowed_now: bool,
    block_reason: Option<String>,
    consecutive_live_failures: u32,
    breaker_open: bool,
    breaker_open_until: Option<DateTime<Utc>>,
    last_health_ok: Option<bool>,
    last_health_detail: Option<String>,
    last_health_latency_ms: Option<u128>,
    last_health_at: Option<DateTime<Utc>>,
    rejection_rate: Option<f64>,
    health_latency_p95_ms: Option<u128>,
    slo_blocked: bool,
    slo_block_reason: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct LiveGuardSnapshot {
    config: LiveGuardConfigView,
    global: LiveGuardGlobalView,
    venues: Vec<LiveGuardVenueView>,
}

#[derive(Clone)]
struct LiveExecutionGuard {
    config: LiveGuardConfig,
    state: Arc<Mutex<LiveGuardState>>,
}

impl LiveExecutionGuard {
    fn new(configured_venues: Vec<Venue>, config: LiveGuardConfig) -> Self {
        let now = Utc::now();
        let disabled = env_csv_lower("LIVE_GUARD_DISABLED_VENUES");
        let mut venues = HashMap::new();
        for venue in configured_venues {
            let enabled = !disabled.contains(&venue_code(&venue).to_ascii_lowercase());
            venues.insert(
                venue,
                VenueLiveGuardState {
                    live_enabled: enabled,
                    consecutive_live_failures: 0,
                    breaker_open_until: None,
                    last_health: None,
                    live_result_samples: Vec::new(),
                    health_latency_samples: Vec::new(),
                    updated_at: now,
                },
            );
        }

        Self {
            config,
            state: Arc::new(Mutex::new(LiveGuardState {
                kill_switch_enabled: false,
                kill_switch_reason: None,
                kill_switch_updated_at: now,
                venues,
            })),
        }
    }

    fn poll_interval_secs(&self) -> u64 {
        self.config.health_poll_interval_secs.max(3)
    }

    fn authorize_live(&self, venue: &Venue, now: DateTime<Utc>) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "live guard lock poisoned".to_string())?;
        if guard.kill_switch_enabled {
            let reason = guard
                .kill_switch_reason
                .clone()
                .unwrap_or_else(|| "operator kill switch engaged".to_string());
            return Err(format!("live execution blocked: {reason}"));
        }

        let Some(venue_state) = guard.venues.get_mut(venue) else {
            return Err(format!(
                "live guard not configured for venue {}",
                venue_code(venue)
            ));
        };

        if !venue_state.live_enabled {
            return Err(format!(
                "live execution blocked: venue {} is disabled by operator policy",
                venue_code(venue)
            ));
        }

        if let Some(until) = venue_state.breaker_open_until {
            if until > now {
                return Err(format!(
                    "live execution blocked: venue {} circuit breaker open until {}",
                    venue_code(venue),
                    until.to_rfc3339()
                ));
            }
            venue_state.breaker_open_until = None;
            venue_state.consecutive_live_failures = 0;
            venue_state.updated_at = now;
        }

        if self.config.require_healthy {
            let Some(health) = venue_state.last_health.as_ref() else {
                return Err(format!(
                    "live execution blocked: venue {} has no health report yet",
                    venue_code(venue)
                ));
            };

            if !health.ok {
                return Err(format!(
                    "live execution blocked: venue {} health check failing ({})",
                    venue_code(venue),
                    health.detail
                ));
            }

            let age_secs = now.signed_duration_since(health.checked_at).num_seconds();
            if age_secs > self.config.max_health_age_secs {
                return Err(format!(
                    "live execution blocked: venue {} health stale ({}s old)",
                    venue_code(venue),
                    age_secs
                ));
            }
        }

        let (slo_blocked, slo_reason, _, _) = self.slo_status_locked(venue_state, now);
        if slo_blocked {
            return Err(format!(
                "live execution blocked: venue {} SLO guard ({})",
                venue_code(venue),
                slo_reason.unwrap_or_else(|| "threshold breached".to_string())
            ));
        }

        Ok(())
    }

    fn note_health(
        &self,
        venue: &Venue,
        ok: bool,
        detail: String,
        latency_ms: Option<u128>,
        checked_at: DateTime<Utc>,
    ) -> Result<bool> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("live guard lock poisoned"))?;
        let Some(venue_state) = guard.venues.get_mut(venue) else {
            return Err(anyhow!(
                "live guard not configured for venue {}",
                venue_code(venue)
            ));
        };
        let changed = match &venue_state.last_health {
            Some(existing) => {
                existing.ok != ok
                    || existing.detail != detail
                    || existing.latency_ms != latency_ms
            }
            None => true,
        };

        self.prune_slo_samples_locked(venue_state, Utc::now());
        if let Some(latency) = latency_ms {
            venue_state.health_latency_samples.push((checked_at, latency));
        }
        venue_state.last_health = Some(VenueHealthState {
            ok,
            detail,
            latency_ms,
            checked_at,
        });
        venue_state.updated_at = Utc::now();
        Ok(changed)
    }

    fn record_live_result(
        &self,
        venue: &Venue,
        success: bool,
        detail: &str,
        latency_ms: Option<u128>,
    ) -> Result<Option<String>> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("live guard lock poisoned"))?;
        let Some(venue_state) = guard.venues.get_mut(venue) else {
            return Err(anyhow!(
                "live guard not configured for venue {}",
                venue_code(venue)
            ));
        };

        let now = Utc::now();
        self.prune_slo_samples_locked(venue_state, now);
        venue_state.live_result_samples.push((now, success));
        if let Some(latency) = latency_ms {
            venue_state.health_latency_samples.push((now, latency));
        }
        if success {
            let had_failures = venue_state.consecutive_live_failures > 0
                || venue_state.breaker_open_until.is_some();
            venue_state.consecutive_live_failures = 0;
            venue_state.breaker_open_until = None;
            venue_state.updated_at = now;
            if had_failures {
                return Ok(Some(format!(
                    "{} guard reset after successful live order",
                    venue_code(venue)
                )));
            }
            return Ok(None);
        }

        let threshold = self.config.failure_threshold.max(1);
        venue_state.consecutive_live_failures =
            venue_state.consecutive_live_failures.saturating_add(1);
        venue_state.updated_at = now;

        if venue_state.consecutive_live_failures >= threshold {
            let cooldown = self.config.cooldown_secs.max(5);
            let open_until = now + Duration::seconds(cooldown);
            let newly_open = venue_state
                .breaker_open_until
                .map(|existing| existing <= now)
                .unwrap_or(true);
            venue_state.breaker_open_until = Some(open_until);
            if newly_open {
                return Ok(Some(format!(
                    "{} circuit breaker opened after {} failures (detail: {})",
                    venue_code(venue),
                    venue_state.consecutive_live_failures,
                    detail
                )));
            }
        }
        Ok(None)
    }

    fn set_kill_switch(&self, enabled: bool, reason: Option<String>) -> Result<LiveGuardSnapshot> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("live guard lock poisoned"))?;
        guard.kill_switch_enabled = enabled;
        guard.kill_switch_reason = reason.and_then(normalize_reason);
        guard.kill_switch_updated_at = Utc::now();
        Ok(self.build_snapshot_locked(&guard, Utc::now()))
    }

    fn set_venue_controls(
        &self,
        venue: &Venue,
        live_enabled: Option<bool>,
        reset_breaker: bool,
    ) -> Result<LiveGuardSnapshot> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("live guard lock poisoned"))?;
        let Some(venue_state) = guard.venues.get_mut(venue) else {
            return Err(anyhow!(
                "live guard not configured for venue {}",
                venue_code(venue)
            ));
        };
        let mut changed = false;
        if let Some(enabled) = live_enabled {
            if venue_state.live_enabled != enabled {
                venue_state.live_enabled = enabled;
                changed = true;
            }
        }
        if reset_breaker {
            if venue_state.breaker_open_until.is_some() || venue_state.consecutive_live_failures > 0
            {
                venue_state.breaker_open_until = None;
                venue_state.consecutive_live_failures = 0;
                changed = true;
            }
        }
        if changed {
            venue_state.updated_at = Utc::now();
        }
        Ok(self.build_snapshot_locked(&guard, Utc::now()))
    }

    fn snapshot(&self) -> Result<LiveGuardSnapshot> {
        let guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("live guard lock poisoned"))?;
        Ok(self.build_snapshot_locked(&guard, Utc::now()))
    }

    fn build_snapshot_locked(
        &self,
        state: &LiveGuardState,
        now: DateTime<Utc>,
    ) -> LiveGuardSnapshot {
        let mut venues: Vec<LiveGuardVenueView> = state
            .venues
            .iter()
            .map(|(venue, venue_state)| {
                let (live_allowed_now, block_reason) =
                    self.venue_access_status_locked(state, venue, venue_state, now);
                let (slo_blocked, slo_block_reason, rejection_rate, health_latency_p95_ms) =
                    self.slo_status_locked(venue_state, now);
                LiveGuardVenueView {
                    venue: venue_code(venue),
                    live_enabled: venue_state.live_enabled,
                    live_allowed_now,
                    block_reason,
                    consecutive_live_failures: venue_state.consecutive_live_failures,
                    breaker_open: venue_state
                        .breaker_open_until
                        .map(|until| until > now)
                        .unwrap_or(false),
                    breaker_open_until: venue_state.breaker_open_until,
                    last_health_ok: venue_state.last_health.as_ref().map(|v| v.ok),
                    last_health_detail: venue_state.last_health.as_ref().map(|v| v.detail.clone()),
                    last_health_latency_ms: venue_state
                        .last_health
                        .as_ref()
                        .and_then(|v| v.latency_ms),
                    last_health_at: venue_state.last_health.as_ref().map(|v| v.checked_at),
                    rejection_rate,
                    health_latency_p95_ms,
                    slo_blocked,
                    slo_block_reason,
                    updated_at: venue_state.updated_at,
                }
            })
            .collect();
        venues.sort_by(|a, b| a.venue.cmp(&b.venue));

        LiveGuardSnapshot {
            config: LiveGuardConfigView {
                require_healthy: self.config.require_healthy,
                max_health_age_secs: self.config.max_health_age_secs,
                failure_threshold: self.config.failure_threshold.max(1),
                cooldown_secs: self.config.cooldown_secs.max(5),
                health_poll_interval_secs: self.poll_interval_secs(),
                slo_window_secs: self.config.slo_window_secs.max(30),
                slo_min_samples: self.config.slo_min_samples.max(1),
                max_rejection_rate: self.config.max_rejection_rate,
                max_p95_latency_ms: self.config.max_p95_latency_ms,
            },
            global: LiveGuardGlobalView {
                kill_switch_enabled: state.kill_switch_enabled,
                kill_switch_reason: state.kill_switch_reason.clone(),
                kill_switch_updated_at: state.kill_switch_updated_at,
            },
            venues,
        }
    }

    fn venue_access_status_locked(
        &self,
        state: &LiveGuardState,
        venue: &Venue,
        venue_state: &VenueLiveGuardState,
        now: DateTime<Utc>,
    ) -> (bool, Option<String>) {
        if state.kill_switch_enabled {
            let reason = state
                .kill_switch_reason
                .clone()
                .unwrap_or_else(|| "operator kill switch engaged".to_string());
            return (false, Some(format!("global kill switch: {reason}")));
        }

        if !venue_state.live_enabled {
            return (false, Some("venue disabled by operator policy".to_string()));
        }

        if let Some(until) = venue_state.breaker_open_until {
            if until > now {
                return (
                    false,
                    Some(format!("circuit breaker open until {}", until.to_rfc3339())),
                );
            }
        }

        if self.config.require_healthy {
            match &venue_state.last_health {
                Some(health) => {
                    if !health.ok {
                        return (
                            false,
                            Some(format!("health check failing: {}", health.detail)),
                        );
                    }
                    let age_secs = now.signed_duration_since(health.checked_at).num_seconds();
                    if age_secs > self.config.max_health_age_secs {
                        return (
                            false,
                            Some(format!("health report stale ({}s old)", age_secs)),
                        );
                    }
                }
                None => {
                    return (false, Some("no health report yet".to_string()));
                }
            }
        }

        let (slo_blocked, slo_reason, _, _) = self.slo_status_locked(venue_state, now);
        if slo_blocked {
            return (
                false,
                Some(slo_reason.unwrap_or_else(|| "SLO guard tripped".to_string())),
            );
        }

        let _ = venue;
        (true, None)
    }

    fn prune_slo_samples_locked(&self, venue_state: &mut VenueLiveGuardState, now: DateTime<Utc>) {
        let cutoff = now - Duration::seconds(self.config.slo_window_secs.max(30));
        venue_state
            .live_result_samples
            .retain(|(at, _)| *at >= cutoff);
        venue_state
            .health_latency_samples
            .retain(|(at, _)| *at >= cutoff);
    }

    fn slo_status_locked(
        &self,
        venue_state: &VenueLiveGuardState,
        now: DateTime<Utc>,
    ) -> (bool, Option<String>, Option<f64>, Option<u128>) {
        let cutoff = now - Duration::seconds(self.config.slo_window_secs.max(30));
        let min_samples = self.config.slo_min_samples.max(1);

        let results: Vec<bool> = venue_state
            .live_result_samples
            .iter()
            .filter(|(at, _)| *at >= cutoff)
            .map(|(_, success)| *success)
            .collect();
        let latencies: Vec<u128> = venue_state
            .health_latency_samples
            .iter()
            .filter(|(at, _)| *at >= cutoff)
            .map(|(_, latency_ms)| *latency_ms)
            .collect();

        let rejection_rate = if results.len() >= min_samples {
            let rejects = results.iter().filter(|ok| !**ok).count() as f64;
            Some(rejects / results.len() as f64)
        } else {
            None
        };
        let p95_latency = if latencies.len() >= min_samples {
            percentile_u128(latencies, 0.95)
        } else {
            None
        };

        if let (Some(limit), Some(actual)) = (self.config.max_rejection_rate, rejection_rate) {
            if actual > limit {
                return (
                    true,
                    Some(format!(
                        "rejection rate {:.2}% exceeds {:.2}% limit",
                        actual * 100.0,
                        limit * 100.0
                    )),
                    rejection_rate,
                    p95_latency,
                );
            }
        }

        if let (Some(limit), Some(actual)) = (self.config.max_p95_latency_ms, p95_latency) {
            if actual > limit {
                return (
                    true,
                    Some(format!(
                        "p95 latency {}ms exceeds {}ms limit",
                        actual, limit
                    )),
                    rejection_rate,
                    p95_latency,
                );
            }
        }

        (false, None, rejection_rate, p95_latency)
    }
}

fn normalize_reason(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Role {
    Admin,
    Trader,
    Viewer,
}

impl Role {
    fn can_trade(&self) -> bool {
        matches!(self, Self::Admin | Self::Trader)
    }

    fn can_request_live(&self) -> bool {
        matches!(self, Self::Admin)
    }
}

#[derive(Debug, Clone)]
struct ApiIdentity {
    user_id: uuid::Uuid,
    org_id: uuid::Uuid,
    role: Role,
    label: String,
}

#[derive(Clone, Default)]
struct AuthStore {
    fallback_tokens: Arc<HashMap<String, ApiIdentity>>,
    jwt_secret: Option<String>,
    default_org_id: uuid::Uuid,
}

impl AuthStore {
    fn fallback_lookup(&self, token: &str) -> Option<ApiIdentity> {
        self.fallback_tokens.get(token).cloned()
    }

    fn jwt_secret(&self) -> Option<&str> {
        self.jwt_secret.as_deref()
    }

    fn is_configured(&self, has_db: bool) -> bool {
        has_db || !self.fallback_tokens.is_empty() || self.jwt_secret.is_some()
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    error: String,
}

#[derive(Debug, Deserialize)]
struct ApiOrderInput {
    venue: String,
    symbol: String,
    #[serde(default)]
    asset_class: Option<AssetClass>,
    #[serde(default)]
    quote_currency: Option<String>,
    side: Side,
    order_type: OrderType,
    #[serde(default)]
    time_in_force: Option<TimeInForce>,
    quantity: f64,
    #[serde(default)]
    limit_price: Option<f64>,
    #[serde(default)]
    mode: Option<ExecutionMode>,
    #[serde(default)]
    strategy_id: Option<uuid::Uuid>,
    #[serde(default)]
    metadata: Option<indexmap::IndexMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct SliceOrderInput {
    venue: String,
    symbol: String,
    #[serde(default)]
    asset_class: Option<AssetClass>,
    #[serde(default)]
    quote_currency: Option<String>,
    side: Side,
    quantity: f64,
    #[serde(default)]
    limit_price: Option<f64>,
    #[serde(default)]
    mode: Option<ExecutionMode>,
    #[serde(default)]
    strategy_id: Option<uuid::Uuid>,
    #[serde(default)]
    order_type: Option<OrderType>,
    #[serde(default)]
    time_in_force: Option<TimeInForce>,
    algorithm: slicing::SliceAlgorithm,
    #[serde(default)]
    duration_secs: Option<u64>,
    #[serde(default)]
    child_count: Option<usize>,
    #[serde(default)]
    participation_rate: Option<f64>,
    #[serde(default)]
    visible_fraction: Option<f64>,
    #[serde(default)]
    depth_usd: Option<f64>,
    #[serde(default)]
    spread_bps: Option<f64>,
    #[serde(default)]
    queue_ahead_qty: Option<f64>,
    #[serde(default)]
    target_child_notional_usd: Option<f64>,
    #[serde(default)]
    min_child_qty: Option<f64>,
    #[serde(default)]
    max_child_qty: Option<f64>,
}

#[derive(Debug, Serialize)]
struct SliceSubmitChildResult {
    child_index: usize,
    offset_ms: u64,
    quantity: f64,
    limit_price: f64,
    request_id: Option<uuid::Uuid>,
    status: Option<OrderStatus>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClosePositionInput {
    venue: String,
    symbol: String,
    #[serde(default)]
    mode: Option<ExecutionMode>,
}

#[derive(Debug, Deserialize)]
struct RecentOrdersQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct DecisionTraceQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    strategy_id: Option<uuid::Uuid>,
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    mode: Option<ExecutionMode>,
}

#[derive(Debug, Deserialize)]
struct TelemetrySummaryQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    lookback_hours: Option<i64>,
    #[serde(default)]
    strategy_id: Option<uuid::Uuid>,
}

#[derive(Debug, Deserialize)]
struct SignalOntologyQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    tradable_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SignalDislocationQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    min_spread_bps: Option<f64>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    include_stale: Option<bool>,
    #[serde(default)]
    max_quote_age_secs: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ResearchRunsQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    strategy_id: Option<uuid::Uuid>,
}

#[derive(Debug, Deserialize)]
struct CreateResearchRunInput {
    #[serde(default)]
    strategy_id: Option<uuid::Uuid>,
    dataset_snapshot_id: String,
    feature_set_hash: String,
    code_version: String,
    config_hash: String,
    #[serde(default)]
    baseline_run_id: Option<uuid::Uuid>,
    metrics: ResearchMetrics,
    #[serde(default)]
    artifact_uri: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Serialize)]
struct DecisionTraceView {
    request_id: uuid::Uuid,
    user_id: uuid::Uuid,
    strategy_id: Option<uuid::Uuid>,
    venue: String,
    symbol: String,
    recorded_at: DateTime<Utc>,
    trace: oms::DecisionTrace,
}

#[derive(Debug, Clone, Deserialize)]
struct UpsertCanonicalInstrumentInput {
    canonical_event_id: String,
    canonical_outcome_id: String,
    #[serde(default)]
    canonical_event_title: Option<String>,
    outcome_label: String,
    venue: String,
    symbol: String,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    lifecycle: Option<domain::InstrumentLifecycle>,
    #[serde(default)]
    manual_override: Option<bool>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct InstrumentRegistryQuery {
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    canonical_event_id: Option<String>,
    #[serde(default)]
    lifecycle: Option<domain::InstrumentLifecycle>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct LiveKillSwitchInput {
    enabled: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VenueGuardControlInput {
    #[serde(default)]
    live_enabled: Option<bool>,
    #[serde(default)]
    reset_breaker: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct IntelClaimsQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IntelTagsQuery {
    #[serde(default)]
    window_hours: Option<i64>,
    #[serde(default)]
    top_n: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct OrchestratorEvaluateInput {
    #[serde(default)]
    use_intel_signals: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OrchestratorStartRolloutInput {
    champion_id: uuid::Uuid,
    challenger_id: uuid::Uuid,
    #[serde(default)]
    shadow_allocation_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct OrchestratorCapitalAllocateInput {
    #[serde(default)]
    total_capital_override_usd: Option<f64>,
    #[serde(default)]
    regime: Option<orchestrator::MarketRegime>,
    #[serde(default)]
    min_allocation_floor_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct OrchestratorEnsemblePolicyInput {
    microstructure_score: f64,
    event_probability_score: f64,
    risk_allocator_score: f64,
    #[serde(default)]
    micro_weight: Option<f64>,
    #[serde(default)]
    event_weight: Option<f64>,
    #[serde(default)]
    risk_weight: Option<f64>,
    #[serde(default)]
    policy_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrchestratorSnapshotsQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct OrchestratorReplayInput {
    snapshot_id: i64,
}

#[derive(Debug, Serialize)]
struct OrchestratorSnapshotHistoryView {
    snapshot_id: i64,
    created_at: DateTime<Utc>,
    snapshot: orchestrator::OrchestratorSnapshot,
}

#[derive(Debug, Deserialize)]
struct CreateApiKeyInput {
    label: String,
    role: Role,
    #[serde(default)]
    user_id: Option<uuid::Uuid>,
}

#[derive(Debug, Deserialize)]
struct AuthLoginInput {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct AuthRefreshInput {
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct AuthChangePasswordInput {
    old_password: String,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct AuthResetPasswordInput {
    reset_token: String,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct AuthRevokeInput {
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    revoke_all: Option<bool>,
    #[serde(default)]
    user_id: Option<uuid::Uuid>,
}

#[derive(Debug, Serialize)]
struct AuthLoginResponse {
    access_token: String,
    refresh_token: String,
    token_type: &'static str,
    expires_in: usize,
}

#[derive(Debug, Deserialize)]
struct CreateOrgInput {
    name: String,
}

#[derive(Debug, Deserialize)]
struct CreateUserInput {
    email: String,
    role: Role,
    password: String,
    #[serde(default)]
    must_reset_password: Option<bool>,
}

#[derive(Debug, Serialize)]
struct UserResponse {
    id: uuid::Uuid,
    org_id: uuid::Uuid,
    email: String,
    role: Role,
    active: bool,
    must_reset_password: bool,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct PortfolioHistoryQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    token: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AccessClaims {
    sub: String,
    org_id: String,
    role: String,
    label: String,
    exp: usize,
}

#[derive(Debug, Serialize)]
struct PortfolioSnapshotView {
    cash_usd: f64,
    positions_json: serde_json::Value,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    exp: Option<usize>,
}

#[derive(Clone)]
struct PgStore {
    pool: PgPool,
}

#[derive(Debug)]
struct AuthUser {
    user_id: uuid::Uuid,
    org_id: uuid::Uuid,
    email: String,
    role: Role,
    active: bool,
    must_reset_password: bool,
    password_hash: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init_tracing();

    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "run-once".to_string());

    if command == "db-migrate" {
        return run_db_migrate_only().await;
    }

    let registry = build_registry_from_env()?;
    let has_adapters = !registry.all().is_empty();
    if !has_adapters {
        warn!("no venue adapters configured; serve mode will run with trading disabled");
    }

    let db = init_db_from_env().await?;
    let auth = build_auth_store_from_env()?;

    let allow_live = env_bool("LIVE_TRADING_ENABLED", false);
    let max_quote_age_secs = env_i64("PRETRADE_MAX_QUOTE_AGE_SECS", 30);
    let max_spread_bps = env_optional_f64("RISK_MAX_SPREAD_BPS")
        .filter(|value| value.is_finite() && *value > 0.0);
    let default_mode =
        parse_mode(std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "paper".to_string()))?;
    let starting_cash = env_f64("SIM_STARTING_CASH_USD", 50_000.0);
    let risk = RiskEngine::new(RiskLimits {
        allow_live,
        dynamic_limits_enabled: env_bool("RISK_DYNAMIC_LIMITS_ENABLED", true),
        target_volatility_bps: env_f64("RISK_TARGET_VOLATILITY_BPS", 80.0).max(1.0),
        target_top_of_book_liquidity_usd: env_f64("RISK_TARGET_TOP_BOOK_LIQ_USD", 10_000.0)
            .max(1.0),
        liquidity_take_fraction: env_f64("RISK_LIQUIDITY_TAKE_FRACTION", 0.20).clamp(0.01, 1.0),
        min_order_limit_factor: env_f64("RISK_MIN_ORDER_LIMIT_FACTOR", 0.10).clamp(0.01, 1.0),
        min_position_limit_factor: env_f64("RISK_MIN_POSITION_LIMIT_FACTOR", 0.25)
            .clamp(0.01, 1.0),
        max_spread_bps,
        ..RiskLimits::default()
    });
    let audit_dir = std::env::var("OMS_AUDIT_DIR")
        .map(PathBuf::from)
        .or_else(|_| {
            std::env::var("OMS_AUDIT_LOG_PATH")
                .map(PathBuf::from)
                .map(|p| {
                    p.parent()
                        .map(PathBuf::from)
                        .unwrap_or_else(|| PathBuf::from("var"))
                })
        })
        .unwrap_or_else(|_| PathBuf::from("var/oms_audit"));
    let oms = OmsManager::new(
        OmsConfig {
            live_trading_enabled: allow_live,
            shadow_reads_enabled: env_bool("SHADOW_READS_ENABLED", true),
            simulated_fee_bps: env_f64("SIM_FEE_BPS", 2.0),
            require_quote_for_market_data_modes: env_bool("PRETRADE_REQUIRE_QUOTE", true),
            max_quote_age_secs: if max_quote_age_secs > 0 {
                Some(max_quote_age_secs)
            } else {
                None
            },
        },
        registry.clone(),
        risk,
        PortfolioState::with_starting_cash("USD", starting_cash),
        audit_dir,
    );

    match command.as_str() {
        "health" => {
            if !has_adapters {
                return Err(anyhow!(
                    "no adapters configured. set Kalshi, IBKR, or Coinbase env variables first"
                ));
            }
            run_health_checks(&registry).await
        }
        "demo-order" => {
            if !has_adapters {
                return Err(anyhow!(
                    "no adapters configured. set Kalshi, IBKR, or Coinbase env variables first"
                ));
            }
            run_demo_order(&oms, default_mode).await
        }
        "run-once" => {
            if !has_adapters {
                return Err(anyhow!(
                    "no adapters configured. set Kalshi, IBKR, or Coinbase env variables first"
                ));
            }
            run_health_checks(&registry).await?;
            run_demo_order(&oms, default_mode).await
        }
        "serve" => serve_api(oms, auth, db, default_mode).await,
        other => Err(anyhow!(
            "unknown command: {other}. expected: health | demo-order | run-once | serve | db-migrate"
        )),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn build_registry_from_env() -> Result<AdapterRegistry> {
    let mut registry = AdapterRegistry::new();

    let kalshi_key_id = std::env::var("KALSHI_API_KEY_ID").ok();
    let kalshi_private_key = std::env::var("KALSHI_RSA_PRIVATE_KEY").ok();
    if let (Some(key_id), Some(private_key)) = (kalshi_key_id, kalshi_private_key) {
        let config = KalshiConfig {
            base_url: std::env::var("KALSHI_BASE_URL")
                .unwrap_or_else(|_| "https://trading-api.kalshi.com".to_string()),
            key_id,
            rsa_private_key_pem: private_key,
        };
        let adapter = KalshiAdapter::new(config).context("failed to initialize Kalshi adapter")?;
        registry.register(adapter);
    } else {
        warn!(
            "kalshi adapter not configured (missing KALSHI_API_KEY_ID or KALSHI_RSA_PRIVATE_KEY)"
        );
    }

    let ibkr_account_id = std::env::var("IBKR_ACCOUNT_ID").ok();
    if let Some(account_id) = ibkr_account_id {
        let config = IbkrConfig {
            base_url: std::env::var("IBKR_BASE_URL")
                .unwrap_or_else(|_| "https://localhost:5000".to_string()),
            account_id,
            session_token: std::env::var("IBKR_SESSION_TOKEN").ok(),
        };
        let adapter = IbkrAdapter::new(config).context("failed to initialize IBKR adapter")?;
        registry.register(adapter);
    } else {
        warn!("ibkr adapter not configured (missing IBKR_ACCOUNT_ID)");
    }

    let cb_key = std::env::var("COINBASE_API_KEY").ok();
    let cb_secret = std::env::var("COINBASE_API_SECRET").ok();
    let cb_passphrase = std::env::var("COINBASE_API_PASSPHRASE").ok();
    if let (Some(api_key), Some(api_secret), Some(passphrase)) = (cb_key, cb_secret, cb_passphrase)
    {
        let config = CoinbaseConfig {
            base_url: std::env::var("COINBASE_BASE_URL")
                .unwrap_or_else(|_| "https://api.coinbase.com".to_string()),
            api_key,
            api_secret,
            passphrase,
        };
        let adapter =
            CoinbaseAdapter::new(config).context("failed to initialize Coinbase adapter")?;
        registry.register(adapter);
    } else {
        warn!("coinbase adapter not configured (missing API key/secret/passphrase)");
    }

    Ok(registry)
}

fn build_auth_store_from_env() -> Result<AuthStore> {
    let default_org_id = std::env::var("API_ORG_ID")
        .ok()
        .and_then(|v| uuid::Uuid::parse_str(&v).ok())
        .unwrap_or_else(uuid::Uuid::nil);

    let mut map: HashMap<String, ApiIdentity> = HashMap::new();

    if let Ok(raw) = std::env::var("API_KEYS") {
        for spec in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let parts: Vec<&str> = spec.split(':').collect();
            if parts.len() < 3 {
                return Err(anyhow!(
                    "invalid API_KEYS entry `{spec}`. expected token:user_id:org_id:role[:label] or token:user_id:role[:label]"
                ));
            }

            let token = parts[0].to_string();
            let user_id = uuid::Uuid::parse_str(parts[1])
                .with_context(|| format!("invalid user_id in API_KEYS entry `{spec}`"))?;

            let (org_id, role_idx, label_idx) =
                if parts.len() >= 4 && uuid::Uuid::parse_str(parts[2]).is_ok() {
                    (
                        uuid::Uuid::parse_str(parts[2]).unwrap_or(default_org_id),
                        3usize,
                        4usize,
                    )
                } else {
                    (default_org_id, 2usize, 3usize)
                };
            let role = parse_role(parts[role_idx])?;
            let label = parts
                .get(label_idx)
                .copied()
                .unwrap_or("api-user")
                .to_string();

            map.insert(
                token,
                ApiIdentity {
                    user_id,
                    org_id,
                    role,
                    label,
                },
            );
        }
    }

    if map.is_empty() {
        if let Ok(token) = std::env::var("API_KEY") {
            let user_id = std::env::var("API_USER_ID")
                .ok()
                .and_then(|v| uuid::Uuid::parse_str(&v).ok())
                .unwrap_or_else(uuid::Uuid::new_v4);
            let role = std::env::var("API_ROLE")
                .ok()
                .map(|v| parse_role(&v))
                .transpose()?
                .unwrap_or(Role::Admin);
            map.insert(
                token,
                ApiIdentity {
                    user_id,
                    org_id: default_org_id,
                    role,
                    label: "default-user".to_string(),
                },
            );
        }
    }

    Ok(AuthStore {
        fallback_tokens: Arc::new(map),
        jwt_secret: std::env::var("JWT_SECRET").ok(),
        default_org_id,
    })
}

async fn init_db_from_env() -> Result<Option<Arc<PgStore>>> {
    let Some(url) = std::env::var("DATABASE_URL").ok() else {
        return Ok(None);
    };
    let max_connections = env_u32("DB_MAX_CONNECTIONS", 5);
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .context("failed to connect DATABASE_URL")?;

    let store = PgStore { pool };
    store.migrate().await?;
    store.bootstrap_from_env().await?;
    Ok(Some(Arc::new(store)))
}

async fn run_db_migrate_only() -> Result<()> {
    let Some(db) = init_db_from_env().await? else {
        return Err(anyhow!("DATABASE_URL is required for db-migrate"));
    };
    db.migrate().await?;
    info!("database migration complete");
    Ok(())
}

async fn run_health_checks(registry: &AdapterRegistry) -> Result<()> {
    for adapter in registry.all() {
        match adapter.health().await {
            Ok(report) => info!(
                venue = ?report.venue,
                ok = report.ok,
                latency_ms = report.latency_ms,
                detail = %report.detail,
                checked_at = %report.checked_at,
                "health check passed"
            ),
            Err(err) => warn!(
                venue = ?adapter.venue(),
                error = %err,
                "health check failed"
            ),
        }
    }
    Ok(())
}

async fn run_demo_order(oms_manager: &OmsManager, default_mode: ExecutionMode) -> Result<()> {
    let venue =
        parse_venue(std::env::var("DEMO_VENUE").unwrap_or_else(|_| "coinbase".to_string()))?;
    let mode = std::env::var("EXECUTION_MODE")
        .ok()
        .map(parse_mode)
        .transpose()?
        .unwrap_or(default_mode);
    let symbol =
        std::env::var("DEMO_SYMBOL").unwrap_or_else(|_| default_symbol(&venue).to_string());
    let quantity = env_f64("DEMO_QTY", 1.0);
    let limit_price = Some(env_f64("DEMO_LIMIT_PRICE", 100.0));

    let instrument = Instrument {
        venue: venue.clone(),
        symbol,
        asset_class: default_asset_class(&venue),
        quote_currency: "USD".to_string(),
    };

    let demo_user_id = uuid::Uuid::new_v4();

    let order = OrderRequest {
        request_id: uuid::Uuid::new_v4(),
        user_id: demo_user_id,
        strategy_id: None,
        mode,
        instrument,
        side: Side::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Day,
        quantity,
        limit_price,
        submitted_at: Utc::now(),
        metadata: Default::default(),
    };

    let oms = oms_manager.for_org(uuid::Uuid::nil())?;
    let (ack, _) = oms.submit_order_with_audit(order).await?;
    info!(
        request_id = %ack.request_id,
        mode = ?ack.mode,
        status = ?ack.status,
        venue_order_id = ?ack.venue_order_id,
        message = %ack.message,
        "demo order processed"
    );

    let snapshot = oms.portfolio_snapshot_for_user(demo_user_id)?;
    info!(cash_usd = snapshot.cash("USD"), "portfolio snapshot");
    Ok(())
}

fn spawn_live_guard_probe(
    registry: AdapterRegistry,
    live_guard: LiveExecutionGuard,
    event_bus: EventBus,
) {
    let poll_secs = live_guard.poll_interval_secs();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(poll_secs));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            for adapter in registry.all() {
                let venue = adapter.venue();
                let (ok, detail, latency_ms, checked_at) = match adapter.health().await {
                    Ok(report) => (
                        report.ok,
                        report.detail,
                        Some(report.latency_ms),
                        report.checked_at,
                    ),
                    Err(err) => (
                        false,
                        format!("health probe error: {err}"),
                        None,
                        Utc::now(),
                    ),
                };

                match live_guard.note_health(&venue, ok, detail.clone(), latency_ms, checked_at) {
                    Ok(changed) => {
                        if changed {
                            if let Ok(snapshot) = live_guard.snapshot() {
                                event_bus.publish(
                                    "execution_guard",
                                    serde_json::json!({
                                        "event": "health_update",
                                        "venue": venue_code(&venue),
                                        "ok": ok,
                                        "detail": detail,
                                        "latency_ms": latency_ms,
                                        "snapshot": snapshot
                                    }),
                                );
                            }
                        }
                    }
                    Err(err) => {
                        warn!(venue = %venue_code(&venue), error = %err, "failed to update live guard health");
                    }
                }
            }
        }
    });
}

async fn persist_claims_if_db(
    db: Option<&Arc<PgStore>>,
    org_id: uuid::Uuid,
    claims: &[SocialClaim],
) -> Result<()> {
    let Some(db) = db else {
        return Ok(());
    };
    if claims.is_empty() {
        return Ok(());
    }
    db.upsert_social_claims(org_id, claims).await
}

async fn persist_orchestrator_if_db(
    db: Option<&Arc<PgStore>>,
    org_id: uuid::Uuid,
    snapshot: &orchestrator::OrchestratorSnapshot,
) -> Result<()> {
    let Some(db) = db else {
        return Ok(());
    };
    db.upsert_orchestrator_snapshot(org_id, snapshot).await
}

async fn hydrate_automation_state(state: &AppState) -> Result<()> {
    let Some(db) = &state.db else {
        return Ok(());
    };
    let org_id = state.auth.default_org_id;
    if let Ok(claims) = db.recent_social_claims(org_id, 2_000).await {
        let _ = state.intel.replace_claims(claims);
    }
    if let Ok(Some(snapshot)) = db.latest_orchestrator_snapshot(org_id).await {
        let _ = state.orchestrator.load_snapshot(snapshot);
    }
    Ok(())
}

fn spawn_auto_improvement_loop(state: AppState, config: AutomationConfig) {
    if !config.enabled {
        return;
    }
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(config.interval_secs));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let scan_request = SocialScanRequest {
                sources: None,
                queries: None,
                lookback_days: Some(config.lookback_days),
                max_per_query: Some(config.max_per_query),
            };
            let outcome = match state.intel.run_scan(scan_request).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    warn!(error = %err, "auto-improve scan step failed");
                    continue;
                }
            };

            if let Err(err) = persist_claims_if_db(
                state.db.as_ref(),
                state.auth.default_org_id,
                &outcome.inserted_claims,
            )
            .await
            {
                warn!(error = %err, "failed persisting auto scan claims");
            }

            state.event_bus.publish(
                "social_intel",
                serde_json::json!({
                    "event": "auto_scan_completed",
                    "inserted": outcome.result.inserted,
                    "total_claims": outcome.result.total_claims,
                    "source_status": outcome.result.source_status,
                    "top_tags": outcome.result.top_tags
                }),
            );

            let tag_weights = if config.use_intel_signals {
                match state.intel.tag_weights(24) {
                    Ok(weights) => weights,
                    Err(err) => {
                        warn!(error = %err, "auto-improve could not compute tag weights");
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            };

            match state.orchestrator.evaluate(tag_weights.clone()) {
                Ok(snapshot) => {
                    if let Err(err) = persist_orchestrator_if_db(
                        state.db.as_ref(),
                        state.auth.default_org_id,
                        &snapshot,
                    )
                    .await
                    {
                        warn!(error = %err, "failed persisting auto orchestrator snapshot");
                    }
                    state.event_bus.publish(
                        "strategy_orchestrator",
                        serde_json::json!({
                            "event": "auto_evaluation_completed",
                            "use_intel_signals": config.use_intel_signals,
                            "tag_weight_count": tag_weights.len(),
                            "snapshot": snapshot
                        }),
                    );
                }
                Err(err) => {
                    warn!(error = %err, "auto-improve evaluation step failed");
                }
            }
        }
    });
}

async fn serve_api(
    oms: OmsManager,
    auth: AuthStore,
    db: Option<Arc<PgStore>>,
    default_mode: ExecutionMode,
) -> Result<()> {
    if !auth.is_configured(db.is_some()) {
        return Err(anyhow!(
            "no auth methods configured. set DATABASE_URL with seeded api_keys, API_KEY(S), or JWT_SECRET"
        ));
    }

    let live_guard = LiveExecutionGuard::new(oms.configured_venues(), LiveGuardConfig::from_env());
    let intel = intel::SocialIntelStore::new_from_env();
    let orchestrator = StrategyOrchestrator::new(OrchestratorConfig::from_env());
    let registry = InstrumentRegistryStore::default();
    let research = ResearchRunStore::default();
    let event_bus = EventBus::new(env_u32("STREAM_EVENT_BUFFER", 512) as usize);
    let state = AppState {
        oms,
        auth,
        default_mode,
        db,
        rate_limiter: OrgRateLimiter::new(env_u32("ORG_RATE_LIMIT_PER_MIN", 600) as usize),
        trade_guard: HierarchicalTradeGuard::new(TradeGuardConfig::from_env()),
        live_guard: live_guard.clone(),
        intel,
        orchestrator,
        registry,
        research,
        event_bus: event_bus.clone(),
    };
    if let Err(err) = hydrate_automation_state(&state).await {
        warn!(error = %err, "failed to hydrate social intel/orchestrator state");
    }
    spawn_live_guard_probe(state.oms.adapter_registry(), live_guard, event_bus);
    spawn_auto_improvement_loop(state.clone(), AutomationConfig::from_env());

    let protected = Router::new()
        .route("/v1/orders", post(api_place_order))
        .route("/v1/orders/:request_id/explain", get(api_explain_order))
        .route("/v1/positions/close", post(api_close_position))
        .route("/v1/portfolio", get(api_get_portfolio))
        .route("/v1/portfolio/history", get(api_portfolio_history))
        .route("/v1/orders/recent", get(api_recent_orders))
        .route("/v1/telemetry/summary", get(api_telemetry_summary))
        .route("/v1/signals/ontology", get(api_get_signal_ontology))
        .route("/v1/signals/dislocations", get(api_get_signal_dislocations))
        .route("/v1/decision-traces", get(api_recent_decision_traces))
        .route(
            "/v1/instruments/registry",
            get(api_list_instrument_registry).post(api_upsert_instrument_registry),
        )
        .route("/v1/execution/slice/plan", post(api_plan_slice_order))
        .route("/v1/execution/slice/submit", post(api_submit_slice_order))
        .route(
            "/v1/intel/claims",
            get(api_get_intel_claims).post(api_add_intel_claim),
        )
        .route("/v1/intel/scan", post(api_run_intel_scan))
        .route("/v1/intel/tags", get(api_get_intel_tags))
        .route(
            "/v1/research/runs",
            get(api_list_research_runs).post(api_create_research_run),
        )
        .route("/v1/research/runs/:run_id", get(api_get_research_run))
        .route("/v1/orchestrator", get(api_get_orchestrator))
        .route("/v1/orchestrator/agents", post(api_upsert_strategy_agent))
        .route(
            "/v1/orchestrator/agents/:agent_id/performance",
            post(api_set_strategy_performance),
        )
        .route(
            "/v1/orchestrator/agents/:agent_id/ensemble-policy",
            post(api_set_strategy_ensemble_policy),
        )
        .route("/v1/orchestrator/evaluate", post(api_evaluate_orchestrator))
        .route(
            "/v1/orchestrator/capital-allocate",
            post(api_allocate_orchestrator_capital),
        )
        .route("/v1/orchestrator/snapshots", get(api_list_orchestrator_snapshots))
        .route("/v1/orchestrator/replay", post(api_replay_orchestrator_snapshot))
        .route(
            "/v1/orchestrator/rollouts",
            get(api_get_orchestrator_rollouts).post(api_start_orchestrator_rollout),
        )
        .route(
            "/v1/orchestrator/rollouts/evaluate",
            post(api_evaluate_orchestrator_rollouts),
        )
        .route("/v1/execution/guards", get(api_get_live_guards))
        .route(
            "/v1/execution/guards/kill-switch",
            post(api_set_live_kill_switch),
        )
        .route(
            "/v1/execution/guards/venues/:venue",
            post(api_set_venue_guard),
        )
        .route("/v1/api-keys", post(api_create_api_key))
        .route("/v1/auth/change-password", post(api_auth_change_password))
        .route("/v1/auth/revoke", post(api_auth_revoke))
        .route("/v1/orgs", post(api_create_org))
        .route("/v1/users", get(api_list_users).post(api_create_user))
        .route(
            "/v1/users/:user_id/issue-reset",
            post(api_issue_password_reset),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let app = Router::new()
        .route("/v1/health", get(api_health))
        .route("/v1/stream", get(api_stream_events))
        .route("/v1/auth/login", post(api_auth_login))
        .route("/v1/auth/refresh", post(api_auth_refresh))
        .route("/v1/auth/reset-password", post(api_auth_reset_password))
        .merge(protected)
        .with_state(state);

    let bind = std::env::var("SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid SERVER_ADDR: {bind}"))?;
    info!(address = %addr, "trading server listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn auth_middleware(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?
        .trim();

    match authorize_token(&state, token).await {
        Ok(Some(identity)) => {
            if state
                .rate_limiter
                .check(identity.org_id, Utc::now().timestamp())
                .is_err()
            {
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
            request.extensions_mut().insert(identity);
            Ok(next.run(request).await)
        }
        Ok(None) => Err(StatusCode::UNAUTHORIZED),
        Err(err) => {
            warn!(error = %err, "auth middleware failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn authorize_token(state: &AppState, token: &str) -> Result<Option<ApiIdentity>> {
    if let Some(db) = &state.db {
        if let Some(identity) = db.resolve_api_key(token).await? {
            return Ok(Some(identity));
        }
    }

    if let Some(identity) = state.auth.fallback_lookup(token) {
        return Ok(Some(identity));
    }

    if let Some(secret) = state.auth.jwt_secret() {
        if let Ok(claims) = decode_hs256_jwt(secret, token) {
            let user_id = uuid::Uuid::parse_str(&claims.sub)
                .with_context(|| format!("invalid JWT sub uuid: {}", claims.sub))?;
            let org_id = claims
                .org_id
                .as_deref()
                .and_then(|v| uuid::Uuid::parse_str(v).ok())
                .unwrap_or(state.auth.default_org_id);
            let role = claims
                .role
                .as_deref()
                .map(parse_role)
                .transpose()?
                .unwrap_or(Role::Trader);
            let label = claims.label.unwrap_or_else(|| "jwt-user".to_string());
            return Ok(Some(ApiIdentity {
                user_id,
                org_id,
                role,
                label,
            }));
        }
    }

    Ok(None)
}

async fn api_health(State(state): State<AppState>) -> impl IntoResponse {
    let recent_count = state
        .oms
        .for_org(state.auth.default_org_id)
        .ok()
        .and_then(|oms| oms.recent_audit_records(1).ok())
        .map(|v| v.len())
        .unwrap_or(0usize);
    let intel_claims = state.intel.claim_count().unwrap_or(0usize);
    let orchestrator_agents = state
        .orchestrator
        .snapshot()
        .map(|snapshot| snapshot.agents.len())
        .unwrap_or(0usize);
    Json(serde_json::json!({
        "ok": true,
        "timestamp": Utc::now(),
        "execution_mode_default": state.default_mode,
        "audit_recent_count": recent_count,
        "db_enabled": state.db.is_some(),
        "intel_claims": intel_claims,
        "orchestrator_agents": orchestrator_agents
    }))
}

async fn api_stream_events(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>, StatusCode> {
    let token = query.token.trim();
    if token.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let identity = match authorize_token(&state, token).await {
        Ok(Some(identity)) => identity,
        Ok(None) => return Err(StatusCode::UNAUTHORIZED),
        Err(err) => {
            warn!(error = %err, "stream auth failed");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    if state
        .rate_limiter
        .check(identity.org_id, Utc::now().timestamp())
        .is_err()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let initial = serde_json::json!({
        "org_id": identity.org_id,
        "user_id": identity.user_id,
        "label": identity.label,
        "role": role_code(&identity.role),
        "message": "stream_connected"
    });

    let initial_event = StreamEvent {
        kind: "system".to_string(),
        payload: initial,
        at: Utc::now(),
    };
    let initial_event_json =
        serde_json::to_string(&initial_event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let initial_stream = tokio_stream::once(Ok(SseEvent::default()
        .event("system")
        .data(initial_event_json)));

    let event_stream =
        BroadcastStream::new(state.event_bus.subscribe()).filter_map(|message| match message {
            Ok(event) => match serde_json::to_string(&event) {
                Ok(data) => Some(Ok(SseEvent::default().event(event.kind).data(data))),
                Err(_) => None,
            },
            Err(_) => None,
        });

    let stream = initial_stream.chain(event_stream);
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(12))
            .text("keepalive"),
    ))
}

async fn api_place_order(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<ApiOrderInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow placing orders".to_string(),
        );
    }
    submit_order_from_input(&state, &identity, payload).await
}

async fn api_plan_slice_order(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<SliceOrderInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow planning sliced orders".to_string(),
        );
    }

    match build_slice_plan(&state, &identity, &payload).await {
        Ok((plan, _, _)) => (StatusCode::OK, Json(serde_json::json!({ "plan": plan }))).into_response(),
        Err(response) => response,
    }
}

async fn api_submit_slice_order(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<SliceOrderInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow placing orders".to_string(),
        );
    }

    let (plan, venue, requested_mode) = match build_slice_plan(&state, &identity, &payload).await {
        Ok(values) => values,
        Err(response) => return response,
    };
    if plan.children.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "slice plan produced zero child orders".to_string(),
        );
    }

    let oms = match state.oms.for_org(identity.org_id) {
        Ok(oms) => oms,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let parent_id = uuid::Uuid::new_v4();
    let order_type = payload.order_type.unwrap_or(OrderType::Limit);
    let time_in_force = payload.time_in_force.unwrap_or(TimeInForce::Day);

    let mut results: Vec<SliceSubmitChildResult> = Vec::with_capacity(plan.children.len());
    for child in &plan.children {
        let mut metadata = indexmap::IndexMap::new();
        metadata.insert("slice_parent_id".to_string(), parent_id.to_string());
        metadata.insert(
            "slice_algorithm".to_string(),
            format!("{:?}", payload.algorithm).to_ascii_lowercase(),
        );
        metadata.insert("slice_child_index".to_string(), child.child_index.to_string());
        metadata.insert("slice_offset_ms".to_string(), child.offset_ms.to_string());
        metadata.insert(
            "slice_expected_fill_prob".to_string(),
            format!("{:.4}", child.expected_fill_probability),
        );
        metadata.insert(
            "slice_rationale".to_string(),
            child.rationale.chars().take(240).collect(),
        );
        if let Some(spread_bps) = payload.spread_bps {
            metadata.insert("spread_bps".to_string(), format!("{spread_bps:.4}"));
        }
        if let Some(depth_usd) = payload.depth_usd {
            metadata.insert("top_of_book_liquidity_usd".to_string(), format!("{depth_usd:.4}"));
        }
        if let Err(reason) = state.trade_guard.authorize_and_record(&TradeGuardInput {
            org_id: identity.org_id,
            strategy_id: payload.strategy_id,
            venue: venue.clone(),
            symbol: payload.symbol.clone(),
            order_type,
            metadata: metadata.clone(),
            now: Utc::now(),
        }) {
            results.push(SliceSubmitChildResult {
                child_index: child.child_index,
                offset_ms: child.offset_ms,
                quantity: child.quantity,
                limit_price: child.limit_price,
                request_id: None,
                status: None,
                error: Some(reason),
            });
            if matches!(requested_mode, ExecutionMode::Live) {
                break;
            }
            continue;
        }

        let order = OrderRequest {
            request_id: uuid::Uuid::new_v4(),
            user_id: identity.user_id,
            strategy_id: payload.strategy_id,
            mode: requested_mode,
            instrument: Instrument {
                venue: venue.clone(),
                symbol: payload.symbol.clone(),
                asset_class: payload
                    .asset_class
                    .unwrap_or_else(|| default_asset_class(&venue)),
                quote_currency: payload
                    .quote_currency
                    .clone()
                    .unwrap_or_else(|| "USD".to_string())
                    .to_uppercase(),
            },
            side: payload.side,
            order_type,
            time_in_force,
            quantity: child.quantity,
            limit_price: Some(child.limit_price),
            submitted_at: Utc::now(),
            metadata,
        };

        match oms.submit_order_with_audit(order).await {
            Ok((ack, record)) => {
                if matches!(ack.mode, ExecutionMode::Live) {
                    let live_ok = matches!(ack.status, OrderStatus::Accepted);
                    let _ = state
                        .live_guard
                        .record_live_result(&venue, live_ok, &ack.message, None);
                }
                if let Some(db) = &state.db {
                    if let Err(err) = db
                        .insert_order_audit(identity.org_id, identity.user_id, &record)
                        .await
                    {
                        warn!(
                            request_id = %ack.request_id,
                            error = %err,
                            "failed to persist sliced order audit to postgres"
                        );
                    }
                }
                results.push(SliceSubmitChildResult {
                    child_index: child.child_index,
                    offset_ms: child.offset_ms,
                    quantity: child.quantity,
                    limit_price: child.limit_price,
                    request_id: Some(ack.request_id),
                    status: Some(ack.status),
                    error: None,
                });
            }
            Err(err) => {
                results.push(SliceSubmitChildResult {
                    child_index: child.child_index,
                    offset_ms: child.offset_ms,
                    quantity: child.quantity,
                    limit_price: child.limit_price,
                    request_id: None,
                    status: None,
                    error: Some(err.to_string()),
                });
                if matches!(requested_mode, ExecutionMode::Live) {
                    break;
                }
            }
        }
    }

    let succeeded = results.iter().filter(|entry| entry.error.is_none()).count();
    let failed = results.len().saturating_sub(succeeded);
    let attempted_quantity: f64 = results.iter().map(|entry| entry.quantity).sum();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "parent_id": parent_id,
            "plan": plan,
            "results": results,
            "summary": {
                "children_attempted": succeeded + failed,
                "children_succeeded": succeeded,
                "children_failed": failed,
                "attempted_quantity": attempted_quantity
            }
        })),
    )
        .into_response()
}

async fn build_slice_plan(
    state: &AppState,
    identity: &ApiIdentity,
    payload: &SliceOrderInput,
) -> Result<(slicing::SlicePlan, Venue, ExecutionMode), Response> {
    if payload.quantity <= 0.0 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "quantity must be positive for sliced orders".to_string(),
        ));
    }

    let venue = parse_venue(payload.venue.clone())
        .map_err(|err| api_error(StatusCode::BAD_REQUEST, err.to_string()))?;
    enforce_registry_lifecycle(state, identity.org_id, &venue, &payload.symbol).await?;
    let requested_mode = payload.mode.unwrap_or(state.default_mode);
    if matches!(requested_mode, ExecutionMode::Live) && !identity.role.can_request_live() {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "only admin role can request live mode".to_string(),
        ));
    }
    if matches!(requested_mode, ExecutionMode::Live) {
        state
            .live_guard
            .authorize_live(&venue, Utc::now())
            .map_err(|reason| api_error(StatusCode::FORBIDDEN, reason))?;
    }

    let adapter = state
        .oms
        .adapter_registry()
        .get(&venue)
        .map_err(|err| api_error(StatusCode::BAD_REQUEST, err.to_string()))?;

    let quote = adapter.fetch_quote(&payload.symbol).await.ok();
    let inferred_price = quote
        .as_ref()
        .and_then(|snapshot| snapshot.last.or(snapshot.bid).or(snapshot.ask));
    let reference_price = payload.limit_price.or(inferred_price).ok_or_else(|| {
        api_error(
            StatusCode::BAD_REQUEST,
            "limit_price is required when no quote is available".to_string(),
        )
    })?;
    let spread_bps = payload
        .spread_bps
        .or_else(|| quote.as_ref().and_then(compute_spread_bps));

    let plan = slicing::plan_slice(&slicing::SlicePlanConfig {
        algorithm: payload.algorithm,
        side: payload.side,
        total_quantity: payload.quantity,
        reference_price,
        duration_secs: payload.duration_secs.unwrap_or(60),
        child_count: payload.child_count,
        participation_rate: payload.participation_rate,
        visible_fraction: payload.visible_fraction,
        depth_usd: payload.depth_usd,
        spread_bps,
        queue_ahead_qty: payload.queue_ahead_qty,
        target_child_notional_usd: payload.target_child_notional_usd,
        min_child_qty: payload.min_child_qty,
        max_child_qty: payload.max_child_qty,
    });

    Ok((plan, venue, requested_mode))
}

fn compute_spread_bps(quote: &domain::MarketQuote) -> Option<f64> {
    let bid = quote.bid?;
    let ask = quote.ask?;
    if bid <= 0.0 || ask <= 0.0 || ask < bid {
        return None;
    }
    let mid = (bid + ask) / 2.0;
    if mid <= 0.0 {
        return None;
    }
    Some(((ask - bid) / mid) * 10_000.0)
}

async fn api_close_position(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<ClosePositionInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow closing positions".to_string(),
        );
    }

    let venue = match parse_venue(payload.venue.clone()) {
        Ok(v) => v,
        Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
    };

    let raw_symbol = payload.symbol.trim();
    if raw_symbol.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "symbol is required to close a position".to_string(),
        );
    }

    let oms = match state.oms.for_org(identity.org_id) {
        Ok(oms) => oms,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let portfolio_snapshot = match oms.portfolio_snapshot_for_user(identity.user_id) {
        Ok(snapshot) => snapshot,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let target = portfolio_snapshot.positions().into_iter().find(|position| {
        position.instrument.venue == venue
            && position.instrument.symbol.eq_ignore_ascii_case(raw_symbol)
            && position.quantity.abs() > f64::EPSILON
    });
    let Some(position) = target else {
        return api_error(
            StatusCode::NOT_FOUND,
            format!("no open position for {} on {}", raw_symbol, venue_code(&venue)),
        );
    };

    let close_side = if position.quantity > 0.0 {
        Side::Sell
    } else {
        Side::Buy
    };
    let close_payload = ApiOrderInput {
        venue: venue_code(&position.instrument.venue),
        symbol: position.instrument.symbol,
        asset_class: Some(position.instrument.asset_class),
        quote_currency: Some(position.instrument.quote_currency),
        side: close_side,
        order_type: OrderType::Market,
        time_in_force: Some(TimeInForce::Day),
        quantity: position.quantity.abs(),
        limit_price: position.market_price.or(Some(position.average_price)),
        mode: payload.mode,
        strategy_id: None,
        metadata: None,
    };
    submit_order_from_input(&state, &identity, close_payload).await
}

async fn submit_order_from_input(
    state: &AppState,
    identity: &ApiIdentity,
    payload: ApiOrderInput,
) -> Response {
    let requested_symbol = payload.symbol.clone();
    let requested_strategy_id = payload.strategy_id;
    let requested_order_type = payload.order_type;
    let requested_metadata = payload.metadata.unwrap_or_default();
    let venue = match parse_venue(payload.venue) {
        Ok(v) => v,
        Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
    };
    if let Err(response) =
        enforce_registry_lifecycle(state, identity.org_id, &venue, &requested_symbol).await
    {
        return response;
    }
    let requested_mode = payload.mode.unwrap_or(state.default_mode);
    if matches!(requested_mode, ExecutionMode::Live) && !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can request live mode".to_string(),
        );
    }
    if matches!(requested_mode, ExecutionMode::Live) {
        if let Err(reason) = state.live_guard.authorize_live(&venue, Utc::now()) {
            return api_error(StatusCode::FORBIDDEN, reason);
        }
    }
    if let Err(reason) = state.trade_guard.authorize_and_record(&TradeGuardInput {
        org_id: identity.org_id,
        strategy_id: requested_strategy_id,
        venue: venue.clone(),
        symbol: requested_symbol.clone(),
        order_type: requested_order_type,
        metadata: requested_metadata.clone(),
        now: Utc::now(),
    }) {
        let status = if reason.contains("limit")
            || reason.contains("cooldown")
            || reason.contains("rate")
        {
            StatusCode::TOO_MANY_REQUESTS
        } else {
            StatusCode::BAD_REQUEST
        };
        return api_error(status, reason);
    }

    let order = OrderRequest {
        request_id: uuid::Uuid::new_v4(),
        user_id: identity.user_id,
        strategy_id: requested_strategy_id,
        mode: requested_mode,
        instrument: Instrument {
            venue: venue.clone(),
            symbol: payload.symbol,
            asset_class: payload
                .asset_class
                .unwrap_or_else(|| default_asset_class(&venue)),
            quote_currency: payload
                .quote_currency
                .unwrap_or_else(|| "USD".to_string())
                .to_uppercase(),
        },
        side: payload.side,
        order_type: requested_order_type,
        time_in_force: payload.time_in_force.unwrap_or(TimeInForce::Day),
        quantity: payload.quantity,
        limit_price: payload.limit_price,
        submitted_at: Utc::now(),
        metadata: requested_metadata,
    };

    let oms = match state.oms.for_org(identity.org_id) {
        Ok(oms) => oms,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    match oms.submit_order_with_audit(order).await {
        Ok((ack, record)) => {
            if matches!(ack.mode, ExecutionMode::Live) {
                let live_ok = matches!(ack.status, OrderStatus::Accepted);
                match state
                    .live_guard
                    .record_live_result(&venue, live_ok, &ack.message, None)
                {
                    Ok(Some(guard_event)) => {
                        if let Ok(snapshot) = state.live_guard.snapshot() {
                            state.event_bus.publish(
                                "execution_guard",
                                serde_json::json!({
                                    "event": if live_ok { "live_recovered" } else { "live_failure_guarded" },
                                    "venue": venue_code(&venue),
                                    "detail": guard_event,
                                    "snapshot": snapshot
                                }),
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(err) => warn!(error = %err, "failed to update live guard after live ack"),
                }
            }

            let user_snapshot = state
                .oms
                .for_org(identity.org_id)
                .ok()
                .and_then(|engine| engine.portfolio_snapshot_for_user(identity.user_id).ok());

            if let Some(db) = &state.db {
                if let Err(err) = db
                    .insert_order_audit(identity.org_id, identity.user_id, &record)
                    .await
                {
                    warn!(
                        request_id = %ack.request_id,
                        error = %err,
                        "failed to persist order audit to postgres"
                    );
                }

                if let Some(snapshot) = &user_snapshot {
                    if let Err(err) = db
                        .insert_portfolio_snapshot(
                            identity.org_id,
                            identity.user_id,
                            snapshot.cash("USD"),
                            &snapshot.positions(),
                        )
                        .await
                    {
                        warn!(
                            request_id = %ack.request_id,
                            error = %err,
                            "failed to persist portfolio snapshot to postgres"
                        );
                    }
                }
            }

            state.event_bus.publish(
                "order_ack",
                serde_json::json!({
                    "org_id": identity.org_id,
                    "user_id": identity.user_id,
                    "request_id": ack.request_id,
                    "mode": &ack.mode,
                    "status": &ack.status,
                    "venue_order_id": &ack.venue_order_id,
                    "message": &ack.message
                }),
            );
            if let Some(snapshot) = user_snapshot {
                state.event_bus.publish(
                    "portfolio_snapshot",
                    serde_json::json!({
                        "org_id": identity.org_id,
                        "user_id": identity.user_id,
                        "cash_usd": snapshot.cash("USD"),
                        "positions": snapshot.positions()
                    }),
                );
            }

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "ack": ack,
                    "actor": {
                        "user_id": identity.user_id,
                        "org_id": identity.org_id,
                        "label": identity.label,
                        "role": identity.role
                    }
                })),
            )
                .into_response()
        }
        Err(err) => {
            if matches!(requested_mode, ExecutionMode::Live) && matches!(&err, OmsError::Adapter(_))
            {
                let err_detail = err.to_string();
                match state
                    .live_guard
                    .record_live_result(&venue, false, &err_detail, None)
                {
                    Ok(Some(guard_event)) => {
                        if let Ok(snapshot) = state.live_guard.snapshot() {
                            state.event_bus.publish(
                                "execution_guard",
                                serde_json::json!({
                                    "event": "live_failure_guarded",
                                    "venue": venue_code(&venue),
                                    "detail": guard_event,
                                    "snapshot": snapshot
                                }),
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(guard_err) => {
                        warn!(error = %guard_err, "failed to update live guard after adapter error")
                    }
                }
                return api_error(StatusCode::BAD_REQUEST, err_detail);
            }
            api_error(StatusCode::BAD_REQUEST, err.to_string())
        }
    }
}

async fn api_get_portfolio(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
) -> impl IntoResponse {
    if let Some(db) = &state.db {
        match db
            .latest_portfolio_snapshot(identity.org_id, identity.user_id)
            .await
        {
            Ok(Some(snapshot)) => {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "cash_usd": snapshot.cash_usd,
                        "positions": snapshot.positions_json,
                        "created_at": snapshot.created_at,
                        "source": "postgres"
                    })),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    }

    let oms = match state.oms.for_org(identity.org_id) {
        Ok(oms) => oms,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    match oms.portfolio_snapshot_for_user(identity.user_id) {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "cash_usd": snapshot.cash("USD"),
                "positions": snapshot.positions(),
                "source": "memory"
            })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_recent_orders(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<RecentOrdersQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).clamp(1, 500);

    if let Some(db) = &state.db {
        match db.recent_order_audits(identity.org_id, limit).await {
            Ok(records) => {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({ "records": records })),
                )
                    .into_response();
            }
            Err(err) => {
                return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
            }
        }
    }

    let oms = match state.oms.for_org(identity.org_id) {
        Ok(oms) => oms,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    match oms.recent_audit_records(limit) {
        Ok(records) => (
            StatusCode::OK,
            Json(serde_json::json!({ "records": records })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_explain_order(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Path(request_id): Path<uuid::Uuid>,
) -> impl IntoResponse {
    let record = if let Some(db) = &state.db {
        match db.order_audit_by_request_id(identity.org_id, request_id).await {
            Ok(record) => record,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        let oms = match state.oms.for_org(identity.org_id) {
            Ok(oms) => oms,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        match oms.recent_audit_records(2_000) {
            Ok(records) => records
                .into_iter()
                .find(|entry| entry.order.request_id == request_id),
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    };

    let Some(record) = record else {
        return api_error(
            StatusCode::NOT_FOUND,
            format!("order audit not found for request_id={request_id}"),
        );
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "request_id": request_id,
            "explanation": build_order_explanation(&record),
            "recorded_at": record.recorded_at
        })),
    )
        .into_response()
}

fn build_order_explanation(record: &OrderAuditRecord) -> serde_json::Value {
    let mut highlights: Vec<String> = Vec::new();
    highlights.push(format!(
        "requested_mode={} effective_mode={} status={}",
        mode_code(record.decision.requested),
        mode_code(record.decision.effective),
        order_status_code(record.ack.status)
    ));
    if let Some(reason) = &record.decision.reason {
        highlights.push(format!("mode_decision_reason={reason}"));
    }

    if let Some(trace) = &record.decision_trace {
        if let Some(confidence) = trace.signal.confidence {
            highlights.push(format!("signal_confidence={confidence:.3}"));
        }
        if let Some(age) = trace.risk.quote_age_secs {
            highlights.push(format!("quote_age_secs={age}"));
        }
        if let Some(spread) = trace.risk.checks.spread_bps {
            highlights.push(format!("spread_bps={spread:.2}"));
        }
        highlights.push(format!(
            "effective_order_limit={:.2} effective_position_limit={:.2}",
            trace.risk.checks.effective_max_order_notional,
            trace.risk.checks.effective_max_position_notional
        ));
        highlights.push(format!("execution_message={}", trace.execution.message));
    }

    serde_json::json!({
        "order": {
            "request_id": record.order.request_id,
            "user_id": record.order.user_id,
            "strategy_id": record.order.strategy_id,
            "venue": venue_code(&record.order.instrument.venue),
            "symbol": &record.order.instrument.symbol,
            "side": &record.order.side,
            "order_type": &record.order.order_type,
            "quantity": record.order.quantity,
            "limit_price": record.order.limit_price,
            "submitted_at": record.order.submitted_at,
            "metadata": &record.order.metadata
        },
        "mode_decision": &record.decision,
        "ack": &record.ack,
        "trace": &record.decision_trace,
        "highlights": highlights
    })
}

async fn api_telemetry_summary(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<TelemetrySummaryQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(500).clamp(10, 5_000);
    let lookback_hours = query.lookback_hours.unwrap_or(24).clamp(1, 24 * 30);
    let cutoff = Utc::now() - Duration::hours(lookback_hours);

    let records = if let Some(db) = &state.db {
        match db.recent_order_audits(identity.org_id, limit).await {
            Ok(records) => records,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        let oms = match state.oms.for_org(identity.org_id) {
            Ok(oms) => oms,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        match oms.recent_audit_records(limit) {
            Ok(records) => records,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    };

    let filtered: Vec<OrderAuditRecord> = records
        .into_iter()
        .filter(|record| record.recorded_at >= cutoff)
        .filter(|record| {
            query
                .strategy_id
                .map(|strategy_id| record.order.strategy_id == Some(strategy_id))
                .unwrap_or(true)
        })
        .collect();

    if filtered.is_empty() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "lookback_hours": lookback_hours,
                "record_count": 0,
                "metrics": {
                    "fill_efficiency": null,
                    "avg_signal_confidence": null,
                    "avg_slippage_bps": null,
                    "edge_decay_bps": null
                }
            })),
        )
            .into_response();
    }

    let total = filtered.len();
    let executed = filtered
        .iter()
        .filter(|record| matches!(record.ack.status, OrderStatus::Accepted | OrderStatus::Simulated))
        .count();
    let rejected = filtered
        .iter()
        .filter(|record| matches!(record.ack.status, OrderStatus::Rejected))
        .count();

    let confidences: Vec<f64> = filtered
        .iter()
        .filter_map(|record| {
            record
                .decision_trace
                .as_ref()
                .and_then(|trace| trace.signal.confidence)
        })
        .collect();

    let slippage_bps_samples: Vec<f64> = filtered
        .iter()
        .filter_map(|record| {
            let trace = record.decision_trace.as_ref()?;
            let reference_price = trace.risk.reference_price?;
            let execution_price = record
                .order
                .limit_price
                .or(trace.risk.reference_price)
                .filter(|value| *value > 0.0)?;
            if reference_price <= 0.0 {
                return None;
            }
            Some(((execution_price - reference_price) / reference_price) * 10_000.0)
        })
        .collect();

    let edge_decay_samples: Vec<f64> = filtered
        .iter()
        .filter_map(|record| {
            let expected = metadata_map_f64(&record.order.metadata, &["expected_edge_bps"])?;
            let realized = metadata_map_f64(&record.order.metadata, &["realized_edge_bps"])?;
            Some(expected - realized)
        })
        .collect();

    let fill_efficiency = executed as f64 / total as f64;
    let avg_signal_confidence = mean_f64(&confidences);
    let avg_slippage_bps = mean_f64(&slippage_bps_samples);
    let edge_decay_bps = mean_f64(&edge_decay_samples);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "lookback_hours": lookback_hours,
            "record_count": total,
            "executed_count": executed,
            "rejected_count": rejected,
            "metrics": {
                "fill_efficiency": fill_efficiency,
                "avg_signal_confidence": avg_signal_confidence,
                "avg_slippage_bps": avg_slippage_bps,
                "edge_decay_bps": edge_decay_bps
            }
        })),
    )
        .into_response()
}

async fn api_get_signal_ontology(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<SignalOntologyQuery>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow signal ontology access".to_string(),
        );
    }

    let limit = query.limit.unwrap_or(400).clamp(1, 5_000);
    let mut records = match list_registry_records_for_org(&state, identity.org_id, limit).await {
        Ok(records) => records,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    if query.tradable_only.unwrap_or(false) {
        records.retain(|record| record.lifecycle.is_tradable());
    }
    let domain_filter = query
        .domain
        .as_ref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());

    let mut grouped: HashMap<(String, String), Vec<domain::CanonicalInstrument>> = HashMap::new();
    for record in records {
        let domain = infer_event_domain(&record);
        if let Some(filter) = domain_filter.as_ref() {
            if &domain != filter {
                continue;
            }
        }
        grouped
            .entry((
                record.canonical_event_id.clone(),
                record.canonical_outcome_id.clone(),
            ))
            .or_default()
            .push(record);
    }

    let mut items = Vec::new();
    for ((event_id, outcome_id), mut group) in grouped {
        group.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        let Some(primary) = group.first().cloned() else {
            continue;
        };
        let domain = infer_event_domain(&primary);
        let settlement_at = group
            .iter()
            .find_map(|record| extract_settlement_at(&record.metadata));
        let venues = group
            .iter()
            .map(|record| {
                serde_json::json!({
                    "venue": venue_code(&record.venue),
                    "symbol": record.symbol,
                    "lifecycle": record.lifecycle,
                    "confidence": record.confidence,
                    "manual_override": record.manual_override,
                    "updated_at": record.updated_at
                })
            })
            .collect::<Vec<_>>();
        items.push(serde_json::json!({
            "canonical_event_id": event_id,
            "canonical_outcome_id": outcome_id,
            "canonical_event_title": primary.canonical_event_title,
            "outcome_label": primary.outcome_label,
            "domain": domain,
            "settlement_at": settlement_at,
            "settlement_window": settlement_window_label(settlement_at),
            "venues": venues
        }));
    }

    items.sort_by(|a, b| {
        let a_updated = a
            .get("venues")
            .and_then(|venues| venues.as_array())
            .and_then(|venues| venues.first())
            .and_then(|entry| entry.get("updated_at"))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let b_updated = b
            .get("venues")
            .and_then(|venues| venues.as_array())
            .and_then(|venues| venues.first())
            .and_then(|entry| entry.get("updated_at"))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        b_updated.cmp(&a_updated)
    });
    if items.len() > limit {
        items.truncate(limit);
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "count": items.len(),
            "ontology": items
        })),
    )
        .into_response()
}

#[derive(Debug, Clone)]
struct DislocationQuoteObservation {
    canonical_event_id: String,
    canonical_outcome_id: String,
    canonical_event_title: Option<String>,
    outcome_label: String,
    domain: String,
    venue: Venue,
    symbol: String,
    price: f64,
    quote_at: DateTime<Utc>,
    quote_age_secs: i64,
    spread_bps: Option<f64>,
    confidence: f64,
    settlement_at: Option<DateTime<Utc>>,
}

async fn api_get_signal_dislocations(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<SignalDislocationQuery>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow dislocation scans".to_string(),
        );
    }

    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let min_spread_bps = query.min_spread_bps.unwrap_or(25.0).max(0.0);
    let include_stale = query.include_stale.unwrap_or(false);
    let max_quote_age_secs = query.max_quote_age_secs.unwrap_or(120).max(5);
    let domain_filter = query
        .domain
        .as_ref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());

    let records = match list_registry_records_for_org(&state, identity.org_id, 3_000).await {
        Ok(records) => records,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let adapter_registry = state.oms.adapter_registry();
    let mut observations = Vec::new();
    let now = Utc::now();
    for record in records {
        if !record.lifecycle.is_tradable() {
            continue;
        }
        let domain = infer_event_domain(&record);
        if let Some(filter) = domain_filter.as_ref() {
            if &domain != filter {
                continue;
            }
        }

        let adapter = match adapter_registry.get(&record.venue) {
            Ok(adapter) => adapter,
            Err(_) => continue,
        };
        let quote = match adapter.fetch_quote(&record.symbol).await {
            Ok(quote) => quote,
            Err(_) => continue,
        };
        let price = quote
            .last
            .or_else(|| {
                match (quote.bid, quote.ask) {
                    (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => Some((bid + ask) / 2.0),
                    _ => None,
                }
            })
            .unwrap_or(0.0);
        if price <= 0.0 || !price.is_finite() {
            continue;
        }
        let age_secs = now
            .signed_duration_since(quote.timestamp)
            .num_seconds()
            .max(0);
        if !include_stale && age_secs > max_quote_age_secs {
            continue;
        }

        observations.push(DislocationQuoteObservation {
            canonical_event_id: record.canonical_event_id.clone(),
            canonical_outcome_id: record.canonical_outcome_id.clone(),
            canonical_event_title: record.canonical_event_title.clone(),
            outcome_label: record.outcome_label.clone(),
            domain,
            venue: record.venue.clone(),
            symbol: record.symbol.clone(),
            price,
            quote_at: quote.timestamp,
            quote_age_secs: age_secs,
            spread_bps: compute_spread_bps(&quote),
            confidence: record.confidence,
            settlement_at: extract_settlement_at(&record.metadata),
        });
    }

    let mut grouped: HashMap<(String, String), Vec<DislocationQuoteObservation>> = HashMap::new();
    for obs in observations {
        grouped
            .entry((obs.canonical_event_id.clone(), obs.canonical_outcome_id.clone()))
            .or_default()
            .push(obs);
    }

    let mut dislocations = Vec::new();
    for ((event_id, outcome_id), mut group) in grouped {
        if group.len() < 2 {
            continue;
        }
        group.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
        let min_obs = group.first().cloned();
        let max_obs = group.last().cloned();
        let (Some(min_obs), Some(max_obs)) = (min_obs, max_obs) else {
            continue;
        };
        let mid = ((min_obs.price + max_obs.price) / 2.0).max(0.0001);
        let spread_bps = ((max_obs.price - min_obs.price).abs() / mid) * 10_000.0;
        if spread_bps < min_spread_bps {
            continue;
        }
        let avg_confidence = group.iter().map(|entry| entry.confidence).sum::<f64>()
            / group.len() as f64;
        let venue_quotes = group
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "venue": venue_code(&entry.venue),
                    "symbol": entry.symbol,
                    "price": entry.price,
                    "quote_at": entry.quote_at,
                    "quote_age_secs": entry.quote_age_secs,
                    "spread_bps": entry.spread_bps
                })
            })
            .collect::<Vec<_>>();

        dislocations.push(serde_json::json!({
            "canonical_event_id": event_id,
            "canonical_outcome_id": outcome_id,
            "canonical_event_title": max_obs.canonical_event_title,
            "outcome_label": max_obs.outcome_label,
            "domain": max_obs.domain,
            "settlement_at": max_obs.settlement_at,
            "settlement_window": settlement_window_label(max_obs.settlement_at),
            "spread_bps": spread_bps,
            "implied_probability_spread": (max_obs.price - min_obs.price).abs(),
            "min_price": {
                "venue": venue_code(&min_obs.venue),
                "symbol": min_obs.symbol,
                "price": min_obs.price
            },
            "max_price": {
                "venue": venue_code(&max_obs.venue),
                "symbol": max_obs.symbol,
                "price": max_obs.price
            },
            "average_confidence": avg_confidence,
            "quotes": venue_quotes
        }));
    }

    dislocations.sort_by(|a, b| {
        let a_spread = a.get("spread_bps").and_then(|value| value.as_f64()).unwrap_or(0.0);
        let b_spread = b.get("spread_bps").and_then(|value| value.as_f64()).unwrap_or(0.0);
        b_spread.partial_cmp(&a_spread).unwrap_or(Ordering::Equal)
    });
    if dislocations.len() > limit {
        dislocations.truncate(limit);
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "count": dislocations.len(),
            "min_spread_bps": min_spread_bps,
            "dislocations": dislocations
        })),
    )
        .into_response()
}

async fn api_create_research_run(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<CreateResearchRunInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow recording research runs".to_string(),
        );
    }

    let dataset_snapshot_id = payload.dataset_snapshot_id.trim().to_string();
    let feature_set_hash = payload.feature_set_hash.trim().to_string();
    let code_version = payload.code_version.trim().to_string();
    let config_hash = payload.config_hash.trim().to_string();
    if dataset_snapshot_id.is_empty()
        || feature_set_hash.is_empty()
        || code_version.is_empty()
        || config_hash.is_empty()
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "dataset_snapshot_id, feature_set_hash, code_version and config_hash are required"
                .to_string(),
        );
    }

    let baseline = if let Some(baseline_run_id) = payload.baseline_run_id {
        match research_run_by_id(&state, identity.org_id, baseline_run_id).await {
            Ok(run) => run,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        None
    };
    let acceptance = evaluate_research_acceptance(
        &ResearchAcceptanceConfig::from_env(),
        &payload.metrics,
        baseline.as_ref(),
    );
    let status = acceptance.status.clone();
    let record = ResearchRunRecord {
        id: uuid::Uuid::new_v4(),
        org_id: identity.org_id,
        strategy_id: payload.strategy_id,
        dataset_snapshot_id,
        feature_set_hash,
        code_version,
        config_hash,
        baseline_run_id: payload.baseline_run_id,
        metrics: payload.metrics,
        acceptance,
        status,
        artifact_uri: payload.artifact_uri.and_then(normalize_reason),
        notes: payload.notes.and_then(normalize_reason),
        created_at: Utc::now(),
    };

    let persisted = match persist_research_run(&state, record.clone()).await {
        Ok(record) => record,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    state.event_bus.publish(
        "research_pipeline",
        serde_json::json!({
            "event": "research_run_recorded",
            "run_id": persisted.id,
            "status": persisted.status,
            "strategy_id": persisted.strategy_id
        }),
    );

    (StatusCode::OK, Json(serde_json::json!({ "run": persisted }))).into_response()
}

async fn api_list_research_runs(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<ResearchRunsQuery>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow research run listing".to_string(),
        );
    }

    let limit = query.limit.unwrap_or(200).clamp(1, 5_000);
    match list_research_runs(&state, identity.org_id, limit, query.strategy_id).await {
        Ok(runs) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "count": runs.len(),
                "runs": runs
            })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_get_research_run(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Path(run_id): Path<uuid::Uuid>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow research run access".to_string(),
        );
    }

    match research_run_by_id(&state, identity.org_id, run_id).await {
        Ok(Some(run)) => (StatusCode::OK, Json(serde_json::json!({ "run": run }))).into_response(),
        Ok(None) => api_error(StatusCode::NOT_FOUND, format!("research run {} not found", run_id)),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_recent_decision_traces(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<DecisionTraceQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let venue_filter = query.venue.as_ref().map(|v| v.to_ascii_lowercase());

    let records = if let Some(db) = &state.db {
        match db.recent_order_audits(identity.org_id, limit).await {
            Ok(records) => records,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        let oms = match state.oms.for_org(identity.org_id) {
            Ok(oms) => oms,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        match oms.recent_audit_records(limit) {
            Ok(records) => records,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    };

    let scanned_records = records.len();
    let traces: Vec<DecisionTraceView> = records
        .into_iter()
        .filter_map(|record| {
            if let Some(strategy_id) = query.strategy_id {
                if record.order.strategy_id != Some(strategy_id) {
                    return None;
                }
            }

            if let Some(mode) = query.mode {
                if record.decision.effective != mode {
                    return None;
                }
            }

            if let Some(venue) = venue_filter.as_ref() {
                if venue_code(&record.order.instrument.venue).to_ascii_lowercase() != *venue {
                    return None;
                }
            }

            let trace = record.decision_trace?;
            Some(DecisionTraceView {
                request_id: record.order.request_id,
                user_id: record.order.user_id,
                strategy_id: record.order.strategy_id,
                venue: venue_code(&record.order.instrument.venue),
                symbol: record.order.instrument.symbol,
                recorded_at: record.recorded_at,
                trace,
            })
        })
        .collect();
    let returned = traces.len();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "traces": traces,
            "returned": returned,
            "scanned_records": scanned_records
        })),
    )
        .into_response()
}

async fn api_upsert_instrument_registry(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<UpsertCanonicalInstrumentInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow registry updates".to_string(),
        );
    }

    let manual_override = payload.manual_override.unwrap_or(false);
    if manual_override && !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "manual_override requires admin role".to_string(),
        );
    }
    let venue = match parse_venue(payload.venue.clone()) {
        Ok(v) => v,
        Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let symbol = payload.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "symbol is required".to_string());
    }
    let canonical_event_id = payload.canonical_event_id.trim().to_string();
    if canonical_event_id.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "canonical_event_id is required".to_string(),
        );
    }
    let canonical_outcome_id = payload.canonical_outcome_id.trim().to_string();
    if canonical_outcome_id.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "canonical_outcome_id is required".to_string(),
        );
    }
    let outcome_label = payload.outcome_label.trim().to_string();
    if outcome_label.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "outcome_label is required".to_string(),
        );
    }

    let now = Utc::now();
    let record = domain::CanonicalInstrument {
        id: uuid::Uuid::new_v4(),
        canonical_event_id,
        canonical_outcome_id,
        canonical_event_title: payload
            .canonical_event_title
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        outcome_label,
        venue,
        symbol,
        confidence: payload.confidence.unwrap_or(0.75).clamp(0.0, 1.0),
        lifecycle: payload
            .lifecycle
            .unwrap_or(domain::InstrumentLifecycle::Tradable),
        manual_override,
        metadata: payload.metadata.unwrap_or_else(|| serde_json::json!({})),
        created_at: now,
        updated_at: now,
    };

    let stored = if let Some(db) = &state.db {
        match db.upsert_canonical_instrument(identity.org_id, &record).await {
            Ok(record) => record,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        match state.registry.upsert(identity.org_id, record) {
            Ok(record) => record,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    };

    state.event_bus.publish(
        "instrument_registry",
        serde_json::json!({
            "event": "registry_upsert",
            "org_id": identity.org_id,
            "actor_user_id": identity.user_id,
            "canonical_event_id": stored.canonical_event_id,
            "canonical_outcome_id": stored.canonical_outcome_id,
            "venue": venue_code(&stored.venue),
            "symbol": stored.symbol,
            "lifecycle": stored.lifecycle
        }),
    );

    (StatusCode::OK, Json(serde_json::json!({ "record": stored }))).into_response()
}

async fn api_list_instrument_registry(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<InstrumentRegistryQuery>,
) -> impl IntoResponse {
    let records = if let Some(db) = &state.db {
        match db.list_canonical_instruments(identity.org_id, &query).await {
            Ok(records) => records,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        match state.registry.list(identity.org_id, &query) {
            Ok(records) => records,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "records": records,
            "count": records.len()
        })),
    )
        .into_response()
}

async fn api_get_intel_claims(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<IntelClaimsQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(80).clamp(1, 500);
    let source = match query.source {
        Some(raw) => match parse_social_source(raw) {
            Ok(source) => Some(source),
            Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
        },
        None => None,
    };
    if let Some(db) = &state.db {
        match db
            .recent_social_claims(identity.org_id, limit)
            .await
            .map(|claims| match source {
                Some(ref target) => claims
                    .into_iter()
                    .filter(|claim| &claim.source == target)
                    .collect::<Vec<_>>(),
                None => claims,
            }) {
            Ok(claims) => {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({ "claims": claims })),
                )
                    .into_response()
            }
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    }
    match state.intel.list_claims(limit, source) {
        Ok(claims) => (
            StatusCode::OK,
            Json(serde_json::json!({ "claims": claims })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_add_intel_claim(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<ManualClaimInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow ingesting intelligence claims".to_string(),
        );
    }
    match state.intel.ingest_manual_claim(payload) {
        Ok(claim) => {
            if let Err(err) =
                persist_claims_if_db(state.db.as_ref(), identity.org_id, &[claim.clone()]).await
            {
                return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
            }
            state.event_bus.publish(
                "social_intel",
                serde_json::json!({
                    "event": "claim_ingested",
                    "source": claim.source.code(),
                    "claim_id": claim.id,
                    "tags": claim.strategy_tags,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(claim)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_run_intel_scan(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<SocialScanRequest>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can trigger social intel scan".to_string(),
        );
    }
    match state.intel.run_scan(payload).await {
        Ok(outcome) => {
            if let Err(err) =
                persist_claims_if_db(state.db.as_ref(), identity.org_id, &outcome.inserted_claims)
                    .await
            {
                return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
            }
            state.event_bus.publish(
                "social_intel",
                serde_json::json!({
                    "event": "scan_completed",
                    "inserted": outcome.result.inserted,
                    "total_claims": outcome.result.total_claims,
                    "source_status": outcome.result.source_status,
                    "top_tags": outcome.result.top_tags,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(outcome.result)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_get_intel_tags(
    State(state): State<AppState>,
    Query(query): Query<IntelTagsQuery>,
) -> impl IntoResponse {
    let window_hours = query.window_hours.unwrap_or(24).clamp(1, 24 * 30);
    let top_n = query.top_n.unwrap_or(20).clamp(1, 100);
    match state.intel.tag_signals(window_hours, top_n) {
        Ok(tags) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "window_hours": window_hours,
                "tags": tags
            })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_get_orchestrator(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
) -> impl IntoResponse {
    if let Some(db) = &state.db {
        match db.latest_orchestrator_snapshot(identity.org_id).await {
            Ok(Some(snapshot)) => return (StatusCode::OK, Json(snapshot)).into_response(),
            Ok(None) => {}
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    }
    match state.orchestrator.snapshot() {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_upsert_strategy_agent(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<UpsertStrategyAgentInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can modify strategy agents".to_string(),
        );
    }
    match state.orchestrator.upsert_agent(payload) {
        Ok(agent) => {
            if let Ok(snapshot) = state.orchestrator.snapshot() {
                if let Err(err) =
                    persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
                {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "agent_upserted",
                    "agent": agent,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(agent)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_set_strategy_performance(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Path(raw_agent_id): Path<String>,
    Json(payload): Json<StrategyPerformanceInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow updating strategy performance".to_string(),
        );
    }
    let agent_id = match uuid::Uuid::parse_str(raw_agent_id.trim()) {
        Ok(id) => id,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("invalid strategy agent id: {raw_agent_id}"),
            )
        }
    };
    match state.orchestrator.update_performance(agent_id, payload) {
        Ok(agent) => {
            if let Ok(snapshot) = state.orchestrator.snapshot() {
                if let Err(err) =
                    persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
                {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "performance_updated",
                    "agent": agent,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(agent)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_set_strategy_ensemble_policy(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Path(raw_agent_id): Path<String>,
    Json(payload): Json<OrchestratorEnsemblePolicyInput>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow updating strategy ensemble policy".to_string(),
        );
    }
    let agent_id = match uuid::Uuid::parse_str(raw_agent_id.trim()) {
        Ok(id) => id,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("invalid strategy agent id: {raw_agent_id}"),
            )
        }
    };
    match state.orchestrator.update_ensemble_policy(
        agent_id,
        EnsemblePolicyInput {
            microstructure_score: payload.microstructure_score,
            event_probability_score: payload.event_probability_score,
            risk_allocator_score: payload.risk_allocator_score,
            micro_weight: payload.micro_weight,
            event_weight: payload.event_weight,
            risk_weight: payload.risk_weight,
            policy_version: payload.policy_version,
        },
    ) {
        Ok(agent) => {
            if let Ok(snapshot) = state.orchestrator.snapshot() {
                if let Err(err) =
                    persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
                {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "ensemble_policy_updated",
                    "agent": agent,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(agent)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_evaluate_orchestrator(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<OrchestratorEvaluateInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can trigger orchestrator evaluation".to_string(),
        );
    }
    let use_intel = payload.use_intel_signals.unwrap_or(true);
    let tag_weights = if use_intel {
        match state.intel.tag_weights(24) {
            Ok(weights) => weights,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    } else {
        HashMap::new()
    };

    match state.orchestrator.evaluate(tag_weights.clone()) {
        Ok(snapshot) => {
            if let Err(err) =
                persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
            {
                return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "evaluation_completed",
                    "use_intel_signals": use_intel,
                    "tag_weight_count": tag_weights.len(),
                    "snapshot": snapshot,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(snapshot)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_allocate_orchestrator_capital(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<OrchestratorCapitalAllocateInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can run capital allocator".to_string(),
        );
    }

    match state.orchestrator.allocate_capital(CapitalAllocationInput {
        total_capital_override_usd: payload.total_capital_override_usd,
        regime: payload.regime,
        min_allocation_floor_usd: payload.min_allocation_floor_usd,
    }) {
        Ok(plan) => {
            if let Ok(snapshot) = state.orchestrator.snapshot() {
                if let Err(err) =
                    persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
                {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "capital_allocated",
                    "plan": plan,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(plan)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_list_orchestrator_snapshots(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<OrchestratorSnapshotsQuery>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow viewing orchestrator snapshots".to_string(),
        );
    }
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for orchestrator snapshot history".to_string(),
        );
    };
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    match db
        .orchestrator_snapshot_history(identity.org_id, limit)
        .await
    {
        Ok(snapshots) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "snapshots": snapshots,
                "count": snapshots.len()
            })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_replay_orchestrator_snapshot(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<OrchestratorReplayInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can replay orchestrator snapshots".to_string(),
        );
    }
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for orchestrator replay".to_string(),
        );
    };

    let view = match db
        .orchestrator_snapshot_by_id(identity.org_id, payload.snapshot_id)
        .await
    {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return api_error(
                StatusCode::NOT_FOUND,
                format!("snapshot_id {} not found", payload.snapshot_id),
            )
        }
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    if let Err(err) = state.orchestrator.load_snapshot(view.snapshot.clone()) {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    state.event_bus.publish(
        "strategy_orchestrator",
        serde_json::json!({
            "event": "snapshot_replayed",
            "snapshot_id": view.snapshot_id,
            "created_at": view.created_at,
            "actor": {
                "user_id": identity.user_id,
                "label": identity.label,
                "role": role_code(&identity.role)
            }
        }),
    );

    (StatusCode::OK, Json(view)).into_response()
}

async fn api_get_orchestrator_rollouts(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
) -> impl IntoResponse {
    if !identity.role.can_trade() {
        return api_error(
            StatusCode::FORBIDDEN,
            "role does not allow viewing rollouts".to_string(),
        );
    }
    match state.orchestrator.rollouts() {
        Ok(rollouts) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "rollouts": rollouts,
                "count": rollouts.len()
            })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_start_orchestrator_rollout(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<OrchestratorStartRolloutInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can start champion/challenger rollouts".to_string(),
        );
    }

    match state
        .orchestrator
        .start_challenger_rollout(orchestrator::StartChallengerInput {
            champion_id: payload.champion_id,
            challenger_id: payload.challenger_id,
            shadow_allocation_usd: payload.shadow_allocation_usd,
        }) {
        Ok(rollout) => {
            if let Ok(snapshot) = state.orchestrator.snapshot() {
                if let Err(err) =
                    persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
                {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "rollout_started",
                    "rollout": rollout,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (StatusCode::OK, Json(rollout)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_evaluate_orchestrator_rollouts(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can evaluate champion/challenger rollouts".to_string(),
        );
    }

    match state.orchestrator.evaluate_rollouts() {
        Ok(rollouts) => {
            if let Ok(snapshot) = state.orchestrator.snapshot() {
                if let Err(err) =
                    persist_orchestrator_if_db(state.db.as_ref(), identity.org_id, &snapshot).await
                {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            }
            state.event_bus.publish(
                "strategy_orchestrator",
                serde_json::json!({
                    "event": "rollouts_evaluated",
                    "rollouts": rollouts,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    }
                }),
            );
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "rollouts": rollouts,
                    "count": rollouts.len()
                })),
            )
                .into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_get_live_guards(State(state): State<AppState>) -> impl IntoResponse {
    match state.live_guard.snapshot() {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_set_live_kill_switch(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<LiveKillSwitchInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can modify live guards".to_string(),
        );
    }

    let reason = payload.reason.and_then(normalize_reason);
    match state
        .live_guard
        .set_kill_switch(payload.enabled, reason.clone())
    {
        Ok(snapshot) => {
            state.event_bus.publish(
                "execution_guard",
                serde_json::json!({
                    "event": if payload.enabled { "kill_switch_engaged" } else { "kill_switch_released" },
                    "reason": reason,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    },
                    "snapshot": snapshot
                }),
            );
            (StatusCode::OK, Json(snapshot)).into_response()
        }
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_set_venue_guard(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Path(raw_venue): Path<String>,
    Json(payload): Json<VenueGuardControlInput>,
) -> impl IntoResponse {
    if !identity.role.can_request_live() {
        return api_error(
            StatusCode::FORBIDDEN,
            "only admin role can modify live guards".to_string(),
        );
    }

    let venue = match parse_venue(raw_venue) {
        Ok(venue) => venue,
        Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let reset_breaker = payload.reset_breaker.unwrap_or(false);
    if payload.live_enabled.is_none() && !reset_breaker {
        return api_error(
            StatusCode::BAD_REQUEST,
            "set live_enabled or reset_breaker=true".to_string(),
        );
    }

    match state
        .live_guard
        .set_venue_controls(&venue, payload.live_enabled, reset_breaker)
    {
        Ok(snapshot) => {
            state.event_bus.publish(
                "execution_guard",
                serde_json::json!({
                    "event": "venue_guard_updated",
                    "venue": venue_code(&venue),
                    "live_enabled": payload.live_enabled,
                    "reset_breaker": reset_breaker,
                    "actor": {
                        "user_id": identity.user_id,
                        "label": identity.label,
                        "role": role_code(&identity.role)
                    },
                    "snapshot": snapshot
                }),
            );
            (StatusCode::OK, Json(snapshot)).into_response()
        }
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_portfolio_history(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Query(query): Query<PortfolioHistoryQuery>,
) -> impl IntoResponse {
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for portfolio history".to_string(),
        );
    };
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    match db
        .portfolio_snapshots(identity.org_id, identity.user_id, limit)
        .await
    {
        Ok(rows) => (
            StatusCode::OK,
            Json(serde_json::json!({ "snapshots": rows })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_auth_login(
    State(state): State<AppState>,
    Json(payload): Json<AuthLoginInput>,
) -> impl IntoResponse {
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for password login".to_string(),
        );
    };
    let Some(secret) = state.auth.jwt_secret() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "JWT_SECRET is required for auth login".to_string(),
        );
    };

    let user = match db.find_user_by_email(&payload.email).await {
        Ok(Some(u)) => u,
        Ok(None) => return api_error(StatusCode::UNAUTHORIZED, "invalid credentials".to_string()),
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    if !user.active {
        return api_error(StatusCode::FORBIDDEN, "user is inactive".to_string());
    }
    if user.must_reset_password {
        return api_error(
            StatusCode::FORBIDDEN,
            "password reset required before login".to_string(),
        );
    }
    let Some(stored_hash) = user.password_hash.as_deref() else {
        return api_error(StatusCode::UNAUTHORIZED, "invalid credentials".to_string());
    };
    if !verify_password_hash(stored_hash, &payload.password) {
        return api_error(StatusCode::UNAUTHORIZED, "invalid credentials".to_string());
    }

    let access_ttl = env_u64("JWT_ACCESS_TTL_SECS", 900);
    let refresh_ttl = env_i64("JWT_REFRESH_TTL_SECS", 2_592_000);
    let access_claims = AccessClaims {
        sub: user.user_id.to_string(),
        org_id: user.org_id.to_string(),
        role: role_code(&user.role).to_string(),
        label: user.email.clone(),
        exp: (Utc::now().timestamp().max(0) as u64 + access_ttl) as usize,
    };
    let access_token = match encode_hs256_jwt(secret, &access_claims) {
        Ok(token) => token,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let refresh_token = generate_refresh_token();
    let refresh_expires_at = Utc::now() + Duration::seconds(refresh_ttl.max(60));
    if let Err(err) = db
        .store_refresh_token(
            user.org_id,
            user.user_id,
            &refresh_token,
            refresh_expires_at,
        )
        .await
    {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }

    (
        StatusCode::OK,
        Json(AuthLoginResponse {
            access_token,
            refresh_token,
            token_type: "Bearer",
            expires_in: access_ttl as usize,
        }),
    )
        .into_response()
}

async fn api_auth_refresh(
    State(state): State<AppState>,
    Json(payload): Json<AuthRefreshInput>,
) -> impl IntoResponse {
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for refresh flow".to_string(),
        );
    };
    let Some(secret) = state.auth.jwt_secret() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "JWT_SECRET is required for token refresh".to_string(),
        );
    };

    let consumed = match db.consume_refresh_token(&payload.refresh_token).await {
        Ok(row) => row,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let Some((org_id, user_id)) = consumed else {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "invalid refresh token".to_string(),
        );
    };

    let user = match db.find_user_by_id(user_id).await {
        Ok(Some(u)) => u,
        Ok(None) => {
            return api_error(
                StatusCode::UNAUTHORIZED,
                "invalid refresh token".to_string(),
            )
        }
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    if !user.active {
        return api_error(StatusCode::FORBIDDEN, "user is inactive".to_string());
    }
    if user.must_reset_password {
        return api_error(
            StatusCode::FORBIDDEN,
            "password reset required before refresh".to_string(),
        );
    }

    let access_ttl = env_u64("JWT_ACCESS_TTL_SECS", 900);
    let refresh_ttl = env_i64("JWT_REFRESH_TTL_SECS", 2_592_000);
    let access_claims = AccessClaims {
        sub: user.user_id.to_string(),
        org_id: org_id.to_string(),
        role: role_code(&user.role).to_string(),
        label: user.email.clone(),
        exp: (Utc::now().timestamp().max(0) as u64 + access_ttl) as usize,
    };
    let access_token = match encode_hs256_jwt(secret, &access_claims) {
        Ok(token) => token,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    let new_refresh_token = generate_refresh_token();
    let refresh_expires_at = Utc::now() + Duration::seconds(refresh_ttl.max(60));
    if let Err(err) = db
        .store_refresh_token(org_id, user.user_id, &new_refresh_token, refresh_expires_at)
        .await
    {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }

    (
        StatusCode::OK,
        Json(AuthLoginResponse {
            access_token,
            refresh_token: new_refresh_token,
            token_type: "Bearer",
            expires_in: access_ttl as usize,
        }),
    )
        .into_response()
}

async fn api_auth_change_password(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<AuthChangePasswordInput>,
) -> impl IntoResponse {
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for password change".to_string(),
        );
    };
    if payload.new_password.trim().len() < 8 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "new password must be at least 8 characters".to_string(),
        );
    }

    let user = match db.find_user_by_id(identity.user_id).await {
        Ok(Some(user)) if user.org_id == identity.org_id => user,
        Ok(_) => return api_error(StatusCode::UNAUTHORIZED, "invalid user".to_string()),
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    if !user.active {
        return api_error(StatusCode::FORBIDDEN, "user is inactive".to_string());
    }
    let Some(stored_hash) = user.password_hash.as_deref() else {
        return api_error(StatusCode::UNAUTHORIZED, "invalid credentials".to_string());
    };
    if !verify_password_hash(stored_hash, &payload.old_password) {
        return api_error(StatusCode::UNAUTHORIZED, "invalid credentials".to_string());
    }

    if let Err(err) = db
        .set_user_password(
            identity.org_id,
            identity.user_id,
            &payload.new_password,
            false,
        )
        .await
    {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    let revoked_count = match db
        .revoke_all_refresh_tokens(identity.org_id, identity.user_id)
        .await
    {
        Ok(count) => count,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "revoked_refresh_tokens": revoked_count
        })),
    )
        .into_response()
}

async fn api_auth_reset_password(
    State(state): State<AppState>,
    Json(payload): Json<AuthResetPasswordInput>,
) -> impl IntoResponse {
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for password reset".to_string(),
        );
    };
    if payload.new_password.trim().len() < 8 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "new password must be at least 8 characters".to_string(),
        );
    }

    let consumed = match db.consume_password_reset_token(&payload.reset_token).await {
        Ok(row) => row,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let Some((org_id, user_id)) = consumed else {
        return api_error(StatusCode::UNAUTHORIZED, "invalid reset token".to_string());
    };

    if let Err(err) = db
        .set_user_password(org_id, user_id, &payload.new_password, false)
        .await
    {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    let revoked_count = match db.revoke_all_refresh_tokens(org_id, user_id).await {
        Ok(count) => count,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "revoked_refresh_tokens": revoked_count
        })),
    )
        .into_response()
}

async fn api_auth_revoke(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<AuthRevokeInput>,
) -> impl IntoResponse {
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for token revocation".to_string(),
        );
    };

    if payload.revoke_all.unwrap_or(false) {
        let target_user = payload.user_id.unwrap_or(identity.user_id);
        if target_user != identity.user_id && !matches!(identity.role, Role::Admin) {
            return api_error(
                StatusCode::FORBIDDEN,
                "admin role required to revoke tokens for another user".to_string(),
            );
        }
        let revoked_count = match db
            .revoke_all_refresh_tokens(identity.org_id, target_user)
            .await
        {
            Ok(count) => count,
            Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": true,
                "revoke_all": true,
                "target_user_id": target_user,
                "revoked_count": revoked_count
            })),
        )
            .into_response();
    }

    let token = payload
        .refresh_token
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let Some(token) = token else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "refresh_token is required unless revoke_all=true".to_string(),
        );
    };

    let target_user = payload.user_id.unwrap_or(identity.user_id);
    if target_user != identity.user_id && !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "admin role required to revoke tokens for another user".to_string(),
        );
    }

    let revoked = match db
        .revoke_refresh_token(identity.org_id, target_user, token)
        .await
    {
        Ok(value) => value,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "revoke_all": false,
            "target_user_id": target_user,
            "revoked": revoked
        })),
    )
        .into_response()
}

async fn api_create_org(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<CreateOrgInput>,
) -> impl IntoResponse {
    if !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "admin role required to create org".to_string(),
        );
    }
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for org management".to_string(),
        );
    };

    match db.create_org(payload.name.trim()).await {
        Ok(org_id) => (
            StatusCode::OK,
            Json(serde_json::json!({ "org_id": org_id })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_list_users(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
) -> impl IntoResponse {
    if !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "admin role required to list users".to_string(),
        );
    }
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for user management".to_string(),
        );
    };

    match db.list_users(identity.org_id).await {
        Ok(users) => (StatusCode::OK, Json(serde_json::json!({ "users": users }))).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn api_create_user(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<CreateUserInput>,
) -> impl IntoResponse {
    if !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "admin role required to create users".to_string(),
        );
    }
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for user creation".to_string(),
        );
    };
    if payload.password.trim().len() < 8 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "password must be at least 8 characters".to_string(),
        );
    }

    match db
        .create_user(
            identity.org_id,
            payload.email.trim(),
            &payload.role,
            &payload.password,
            payload.must_reset_password.unwrap_or(true),
        )
        .await
    {
        Ok(user) => (StatusCode::OK, Json(serde_json::json!({ "user": user }))).into_response(),
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn api_issue_password_reset(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Path(user_id): Path<uuid::Uuid>,
) -> impl IntoResponse {
    if !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "admin role required to issue password reset".to_string(),
        );
    }
    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for password reset flow".to_string(),
        );
    };

    let ttl_secs = env_i64("PASSWORD_RESET_TOKEN_TTL_SECS", 3600).max(60);
    let expires_at = Utc::now() + Duration::seconds(ttl_secs);
    let reset_token = match db
        .create_password_reset_token(identity.org_id, user_id, expires_at)
        .await
    {
        Ok(token) => token,
        Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let revoked_count = match db.revoke_all_refresh_tokens(identity.org_id, user_id).await {
        Ok(count) => count,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "user_id": user_id,
            "reset_token": reset_token,
            "expires_at": expires_at,
            "revoked_refresh_tokens": revoked_count
        })),
    )
        .into_response()
}

async fn api_create_api_key(
    State(state): State<AppState>,
    Extension(identity): Extension<ApiIdentity>,
    Json(payload): Json<CreateApiKeyInput>,
) -> impl IntoResponse {
    if !matches!(identity.role, Role::Admin) {
        return api_error(
            StatusCode::FORBIDDEN,
            "admin role required to create API keys".to_string(),
        );
    }

    let Some(db) = &state.db else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "database is required for API key management".to_string(),
        );
    };

    let user_id = payload.user_id.unwrap_or(identity.user_id);
    match db
        .create_api_key(identity.org_id, user_id, &payload.label, &payload.role)
        .await
    {
        Ok((api_key_id, token)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "api_key_id": api_key_id,
                "token": token,
                "label": payload.label,
                "role": payload.role
            })),
        )
            .into_response(),
        Err(err) => api_error(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

fn api_error(status: StatusCode, message: String) -> Response {
    (status, Json(ApiErrorBody { error: message })).into_response()
}

impl PgStore {
    async fn migrate(&self) -> Result<()> {
        let candidate_paths = [
            PathBuf::from("apps/trading-server/migrations"),
            PathBuf::from("migrations"),
        ];
        let migration_dir = candidate_paths
            .iter()
            .find(|p| p.exists())
            .cloned()
            .ok_or_else(|| anyhow!("could not locate migrations directory"))?;

        let mut files: Vec<PathBuf> = fs::read_dir(&migration_dir)
            .with_context(|| format!("failed to read migration dir {}", migration_dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("sql"))
            .collect();
        files.sort();

        for path in files {
            let sql = fs::read_to_string(&path)
                .with_context(|| format!("failed to read migration {}", path.display()))?;
            for statement in split_sql_statements(&sql) {
                sqlx::query(statement)
                    .execute(&self.pool)
                    .await
                    .with_context(|| format!("failed running migration {}", path.display()))?;
            }
        }

        Ok(())
    }

    async fn bootstrap_from_env(&self) -> Result<()> {
        let seed_key = std::env::var("DB_BOOTSTRAP_API_KEY").ok();
        let admin_password = std::env::var("DB_BOOTSTRAP_ADMIN_PASSWORD").ok();
        if seed_key.is_none() && admin_password.is_none() {
            return Ok(());
        }

        let org_name =
            std::env::var("DB_BOOTSTRAP_ORG_NAME").unwrap_or_else(|_| "default-org".to_string());
        let admin_email = std::env::var("DB_BOOTSTRAP_ADMIN_EMAIL")
            .unwrap_or_else(|_| "admin@localhost".to_string());
        let label = std::env::var("DB_BOOTSTRAP_API_KEY_LABEL")
            .unwrap_or_else(|_| "bootstrap-admin".to_string());

        let org_id = upsert_org(&self.pool, &org_name).await?;
        let user_id = upsert_user(&self.pool, org_id, &admin_email, "admin").await?;
        if let Some(password) = admin_password {
            let password_hash =
                make_password_hash(&password).context("failed to hash bootstrap admin password")?;
            sqlx::query(
                "UPDATE users SET password_hash = $1, active = TRUE, must_reset_password = FALSE WHERE id = $2",
            )
                .bind(password_hash)
                .bind(user_id)
                .execute(&self.pool)
                .await
                .context("failed to set bootstrap admin password hash")?;
        }
        if let Some(seed_key) = seed_key {
            let key_hash = sha256_hex(&seed_key);
            sqlx::query(
                r#"
                INSERT INTO api_keys (id, org_id, user_id, label, key_hash, role, active)
                VALUES ($1, $2, $3, $4, $5, $6, TRUE)
                ON CONFLICT (key_hash)
                DO UPDATE SET
                    org_id = EXCLUDED.org_id,
                    user_id = EXCLUDED.user_id,
                    label = EXCLUDED.label,
                    role = EXCLUDED.role,
                    active = TRUE
                "#,
            )
            .bind(uuid::Uuid::new_v4())
            .bind(org_id)
            .bind(user_id)
            .bind(label)
            .bind(key_hash)
            .bind("admin")
            .execute(&self.pool)
            .await
            .context("failed to upsert DB bootstrap API key")?;
        }

        info!(org_id = %org_id, user_id = %user_id, "database bootstrap complete");
        Ok(())
    }

    async fn resolve_api_key(&self, token: &str) -> Result<Option<ApiIdentity>> {
        let hash = sha256_hex(token);
        let row = sqlx::query(
            "SELECT org_id, user_id, role, label FROM api_keys WHERE key_hash = $1 AND active = TRUE LIMIT 1",
        )
        .bind(hash)
        .fetch_optional(&self.pool)
        .await
        .context("failed api key lookup query")?;

        match row {
            Some(found) => {
                let org_id: uuid::Uuid = found.try_get("org_id")?;
                let user_id: uuid::Uuid = found.try_get("user_id")?;
                let role: String = found.try_get("role")?;
                let label: String = found.try_get("label")?;
                Ok(Some(ApiIdentity {
                    org_id,
                    user_id,
                    role: parse_role(&role)?,
                    label,
                }))
            }
            None => Ok(None),
        }
    }

    async fn insert_order_audit(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        record: &OrderAuditRecord,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO order_audits (
                org_id,
                user_id,
                request_id,
                requested_mode,
                effective_mode,
                venue,
                status,
                record_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .bind(record.order.request_id)
        .bind(mode_code(record.decision.requested))
        .bind(mode_code(record.decision.effective))
        .bind(venue_code(&record.order.instrument.venue))
        .bind(order_status_code(record.ack.status))
        .bind(sqlx::types::Json(record))
        .execute(&self.pool)
        .await
        .context("failed to insert order audit row")?;
        Ok(())
    }

    async fn recent_order_audits(
        &self,
        org_id: uuid::Uuid,
        limit: usize,
    ) -> Result<Vec<OrderAuditRecord>> {
        let rows = sqlx::query(
            "SELECT record_json FROM order_audits WHERE org_id = $1 ORDER BY created_at DESC LIMIT $2",
        )
        .bind(org_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch recent order audits")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let value: serde_json::Value = row
                .try_get("record_json")
                .context("failed to parse record_json from row")?;
            let record: OrderAuditRecord =
                serde_json::from_value(value).context("invalid order_audit record_json payload")?;
            out.push(record);
        }
        out.reverse();
        Ok(out)
    }

    async fn order_audit_by_request_id(
        &self,
        org_id: uuid::Uuid,
        request_id: uuid::Uuid,
    ) -> Result<Option<OrderAuditRecord>> {
        let row = sqlx::query(
            "SELECT record_json FROM order_audits WHERE org_id = $1 AND request_id = $2 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(org_id)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch order audit by request id")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let value: serde_json::Value = row
            .try_get("record_json")
            .context("failed to parse record_json from row")?;
        let record: OrderAuditRecord =
            serde_json::from_value(value).context("invalid order_audit record_json payload")?;
        Ok(Some(record))
    }

    async fn insert_research_run(&self, org_id: uuid::Uuid, record: &ResearchRunRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO research_runs (
                id,
                org_id,
                strategy_id,
                status,
                dataset_snapshot_id,
                feature_set_hash,
                code_version,
                config_hash,
                baseline_run_id,
                metrics_json,
                acceptance_json,
                artifact_uri,
                notes,
                record_json,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#,
        )
        .bind(record.id)
        .bind(org_id)
        .bind(record.strategy_id)
        .bind(match record.status {
            ResearchRunStatus::Pass => "pass",
            ResearchRunStatus::Fail => "fail",
            ResearchRunStatus::ManualReview => "manual_review",
        })
        .bind(&record.dataset_snapshot_id)
        .bind(&record.feature_set_hash)
        .bind(&record.code_version)
        .bind(&record.config_hash)
        .bind(record.baseline_run_id)
        .bind(sqlx::types::Json(&record.metrics))
        .bind(sqlx::types::Json(&record.acceptance))
        .bind(&record.artifact_uri)
        .bind(&record.notes)
        .bind(sqlx::types::Json(record))
        .bind(record.created_at)
        .execute(&self.pool)
        .await
        .context("failed to insert research run")?;
        Ok(())
    }

    async fn recent_research_runs(
        &self,
        org_id: uuid::Uuid,
        limit: usize,
        strategy_id: Option<uuid::Uuid>,
    ) -> Result<Vec<ResearchRunRecord>> {
        let limit = limit.clamp(1, 5_000) as i64;
        let rows = if let Some(strategy_id) = strategy_id {
            sqlx::query(
                "SELECT record_json FROM research_runs WHERE org_id = $1 AND strategy_id = $2 ORDER BY created_at DESC LIMIT $3",
            )
            .bind(org_id)
            .bind(strategy_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .context("failed to fetch strategy research runs")?
        } else {
            sqlx::query(
                "SELECT record_json FROM research_runs WHERE org_id = $1 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(org_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .context("failed to fetch research runs")?
        };

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: serde_json::Value = row.try_get("record_json")?;
            let record: ResearchRunRecord = serde_json::from_value(payload)
                .context("invalid research run payload in record_json")?;
            out.push(record);
        }
        Ok(out)
    }

    async fn research_run_by_id(
        &self,
        org_id: uuid::Uuid,
        run_id: uuid::Uuid,
    ) -> Result<Option<ResearchRunRecord>> {
        let row = sqlx::query(
            "SELECT record_json FROM research_runs WHERE org_id = $1 AND id = $2 LIMIT 1",
        )
        .bind(org_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch research run by id")?;

        let Some(row) = row else {
            return Ok(None);
        };
        let payload: serde_json::Value = row.try_get("record_json")?;
        let record: ResearchRunRecord = serde_json::from_value(payload)
            .context("invalid research run payload in record_json")?;
        Ok(Some(record))
    }

    async fn upsert_canonical_instrument(
        &self,
        org_id: uuid::Uuid,
        record: &domain::CanonicalInstrument,
    ) -> Result<domain::CanonicalInstrument> {
        let row = sqlx::query(
            r#"
            INSERT INTO canonical_instrument_registry (
                id,
                org_id,
                canonical_event_id,
                canonical_outcome_id,
                canonical_event_title,
                outcome_label,
                venue,
                symbol,
                confidence,
                lifecycle,
                manual_override,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (org_id, venue, symbol)
            DO UPDATE SET
                canonical_event_id = EXCLUDED.canonical_event_id,
                canonical_outcome_id = EXCLUDED.canonical_outcome_id,
                canonical_event_title = EXCLUDED.canonical_event_title,
                outcome_label = EXCLUDED.outcome_label,
                confidence = EXCLUDED.confidence,
                lifecycle = EXCLUDED.lifecycle,
                manual_override = EXCLUDED.manual_override,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = EXCLUDED.updated_at
            RETURNING
                id,
                canonical_event_id,
                canonical_outcome_id,
                canonical_event_title,
                outcome_label,
                venue,
                symbol,
                confidence,
                lifecycle,
                manual_override,
                metadata_json,
                created_at,
                updated_at
            "#,
        )
        .bind(record.id)
        .bind(org_id)
        .bind(&record.canonical_event_id)
        .bind(&record.canonical_outcome_id)
        .bind(&record.canonical_event_title)
        .bind(&record.outcome_label)
        .bind(venue_code(&record.venue))
        .bind(record.symbol.to_ascii_uppercase())
        .bind(record.confidence.clamp(0.0, 1.0))
        .bind(lifecycle_code(record.lifecycle))
        .bind(record.manual_override)
        .bind(&record.metadata)
        .bind(record.created_at)
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await
        .context("failed to upsert canonical instrument registry row")?;

        map_canonical_instrument_row(row)
    }

    async fn list_canonical_instruments(
        &self,
        org_id: uuid::Uuid,
        query: &InstrumentRegistryQuery,
    ) -> Result<Vec<domain::CanonicalInstrument>> {
        let limit = query.limit.unwrap_or(500).clamp(1, 2_000);
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                canonical_event_id,
                canonical_outcome_id,
                canonical_event_title,
                outcome_label,
                venue,
                symbol,
                confidence,
                lifecycle,
                manual_override,
                metadata_json,
                created_at,
                updated_at
            FROM canonical_instrument_registry
            WHERE org_id = $1
            ORDER BY updated_at DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context("failed to list canonical instrument registry rows")?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            records.push(map_canonical_instrument_row(row)?);
        }
        Ok(filter_registry_records(records, query))
    }

    async fn lookup_canonical_instrument(
        &self,
        org_id: uuid::Uuid,
        venue: &Venue,
        symbol: &str,
    ) -> Result<Option<domain::CanonicalInstrument>> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                canonical_event_id,
                canonical_outcome_id,
                canonical_event_title,
                outcome_label,
                venue,
                symbol,
                confidence,
                lifecycle,
                manual_override,
                metadata_json,
                created_at,
                updated_at
            FROM canonical_instrument_registry
            WHERE org_id = $1
              AND venue = $2
              AND symbol = $3
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(venue_code(venue))
        .bind(symbol.trim().to_ascii_uppercase())
        .fetch_optional(&self.pool)
        .await
        .context("failed to lookup canonical instrument registry row")?;

        row.map(map_canonical_instrument_row).transpose()
    }

    async fn create_api_key(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        label: &str,
        role: &Role,
    ) -> Result<(uuid::Uuid, String)> {
        let user_exists = sqlx::query("SELECT 1 FROM users WHERE id = $1 AND org_id = $2 LIMIT 1")
            .bind(user_id)
            .bind(org_id)
            .fetch_optional(&self.pool)
            .await
            .context("failed to validate user for API key creation")?
            .is_some();

        if !user_exists {
            return Err(anyhow!(
                "target user does not exist in org for API key creation"
            ));
        }

        let key_id = uuid::Uuid::new_v4();
        let token = format!(
            "ak_{}_{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let hash = sha256_hex(&token);

        sqlx::query(
            r#"
            INSERT INTO api_keys (id, org_id, user_id, label, key_hash, role, active)
            VALUES ($1, $2, $3, $4, $5, $6, TRUE)
            "#,
        )
        .bind(key_id)
        .bind(org_id)
        .bind(user_id)
        .bind(label)
        .bind(hash)
        .bind(role_code(role))
        .execute(&self.pool)
        .await
        .context("failed to create API key row")?;

        Ok((key_id, token))
    }

    async fn create_org(&self, name: &str) -> Result<uuid::Uuid> {
        if name.is_empty() {
            return Err(anyhow!("org name cannot be empty"));
        }
        upsert_org(&self.pool, name).await
    }

    async fn create_user(
        &self,
        org_id: uuid::Uuid,
        email: &str,
        role: &Role,
        password: &str,
        must_reset_password: bool,
    ) -> Result<UserResponse> {
        let password_hash = make_password_hash(password).context("failed to hash user password")?;
        let row = sqlx::query(
            r#"
            INSERT INTO users (id, org_id, email, role, password_hash, active, must_reset_password)
            VALUES ($1, $2, $3, $4, $5, TRUE, $6)
            ON CONFLICT (email)
            DO UPDATE SET
                org_id = EXCLUDED.org_id,
                role = EXCLUDED.role,
                password_hash = EXCLUDED.password_hash,
                active = TRUE,
                must_reset_password = EXCLUDED.must_reset_password
            RETURNING id, org_id, email, role, active, must_reset_password, created_at
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(org_id)
        .bind(email)
        .bind(role_code(role))
        .bind(password_hash)
        .bind(must_reset_password)
        .fetch_one(&self.pool)
        .await
        .context("failed to create user")?;

        map_user_response(row)
    }

    async fn list_users(&self, org_id: uuid::Uuid) -> Result<Vec<UserResponse>> {
        let rows = sqlx::query(
            "SELECT id, org_id, email, role, active, must_reset_password, created_at FROM users WHERE org_id = $1 ORDER BY created_at ASC",
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list users")?;
        rows.into_iter().map(map_user_response).collect()
    }

    async fn find_user_by_email(&self, email: &str) -> Result<Option<AuthUser>> {
        let row = sqlx::query(
            "SELECT id, org_id, email, role, active, must_reset_password, password_hash FROM users WHERE lower(email) = lower($1) LIMIT 1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .context("failed to lookup user by email")?;
        row.map(map_auth_user).transpose()
    }

    async fn find_user_by_id(&self, user_id: uuid::Uuid) -> Result<Option<AuthUser>> {
        let row = sqlx::query(
            "SELECT id, org_id, email, role, active, must_reset_password, password_hash FROM users WHERE id = $1 LIMIT 1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to lookup user by id")?;
        row.map(map_auth_user).transpose()
    }

    async fn store_refresh_token(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        raw_token: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO refresh_tokens (id, org_id, user_id, token_hash, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(org_id)
        .bind(user_id)
        .bind(sha256_hex(raw_token))
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .context("failed to persist refresh token")?;
        Ok(())
    }

    async fn consume_refresh_token(
        &self,
        raw_token: &str,
    ) -> Result<Option<(uuid::Uuid, uuid::Uuid)>> {
        let row = sqlx::query(
            r#"
            UPDATE refresh_tokens
            SET revoked_at = NOW()
            WHERE token_hash = $1
              AND revoked_at IS NULL
              AND expires_at > NOW()
            RETURNING org_id, user_id
            "#,
        )
        .bind(sha256_hex(raw_token))
        .fetch_optional(&self.pool)
        .await
        .context("failed to consume refresh token")?;

        match row {
            Some(found) => {
                let org_id: uuid::Uuid = found.try_get("org_id")?;
                let user_id: uuid::Uuid = found.try_get("user_id")?;
                Ok(Some((org_id, user_id)))
            }
            None => Ok(None),
        }
    }

    async fn revoke_refresh_token(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        raw_token: &str,
    ) -> Result<bool> {
        let row = sqlx::query(
            r#"
            UPDATE refresh_tokens
            SET revoked_at = NOW()
            WHERE org_id = $1
              AND user_id = $2
              AND token_hash = $3
              AND revoked_at IS NULL
            RETURNING id
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .bind(sha256_hex(raw_token))
        .fetch_optional(&self.pool)
        .await
        .context("failed to revoke refresh token")?;
        Ok(row.is_some())
    }

    async fn revoke_all_refresh_tokens(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE refresh_tokens
            SET revoked_at = NOW()
            WHERE org_id = $1
              AND user_id = $2
              AND revoked_at IS NULL
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .context("failed to revoke all refresh tokens")?;
        Ok(result.rows_affected())
    }

    async fn create_password_reset_token(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        expires_at: DateTime<Utc>,
    ) -> Result<String> {
        let user_exists = sqlx::query("SELECT 1 FROM users WHERE id = $1 AND org_id = $2 LIMIT 1")
            .bind(user_id)
            .bind(org_id)
            .fetch_optional(&self.pool)
            .await
            .context("failed to validate user for password reset")?
            .is_some();
        if !user_exists {
            return Err(anyhow!(
                "target user does not exist in org for password reset"
            ));
        }

        let token = format!(
            "prt_{}_{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let token_hash = sha256_hex(&token);
        sqlx::query(
            r#"
            INSERT INTO password_reset_tokens (id, org_id, user_id, token_hash, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(org_id)
        .bind(user_id)
        .bind(token_hash)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .context("failed to create password reset token")?;

        sqlx::query("UPDATE users SET must_reset_password = TRUE WHERE id = $1 AND org_id = $2")
            .bind(user_id)
            .bind(org_id)
            .execute(&self.pool)
            .await
            .context("failed to mark user as requiring password reset")?;
        Ok(token)
    }

    async fn consume_password_reset_token(
        &self,
        raw_token: &str,
    ) -> Result<Option<(uuid::Uuid, uuid::Uuid)>> {
        let row = sqlx::query(
            r#"
            UPDATE password_reset_tokens
            SET consumed_at = NOW()
            WHERE token_hash = $1
              AND consumed_at IS NULL
              AND expires_at > NOW()
            RETURNING org_id, user_id
            "#,
        )
        .bind(sha256_hex(raw_token))
        .fetch_optional(&self.pool)
        .await
        .context("failed to consume password reset token")?;

        match row {
            Some(found) => {
                let org_id: uuid::Uuid = found.try_get("org_id")?;
                let user_id: uuid::Uuid = found.try_get("user_id")?;
                Ok(Some((org_id, user_id)))
            }
            None => Ok(None),
        }
    }

    async fn set_user_password(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        password: &str,
        must_reset_password: bool,
    ) -> Result<()> {
        let password_hash = make_password_hash(password).context("failed to hash password")?;
        let result = sqlx::query(
            r#"
            UPDATE users
            SET password_hash = $1,
                active = TRUE,
                must_reset_password = $2
            WHERE id = $3
              AND org_id = $4
            "#,
        )
        .bind(password_hash)
        .bind(must_reset_password)
        .bind(user_id)
        .bind(org_id)
        .execute(&self.pool)
        .await
        .context("failed to update user password")?;
        if result.rows_affected() == 0 {
            return Err(anyhow!("user not found for password update"));
        }
        Ok(())
    }

    async fn insert_portfolio_snapshot(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        cash_usd: f64,
        positions: &[domain::PositionSnapshot],
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO portfolio_snapshots (org_id, user_id, cash_usd, positions_json)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .bind(cash_usd)
        .bind(sqlx::types::Json(positions))
        .execute(&self.pool)
        .await
        .context("failed to insert portfolio snapshot")?;
        Ok(())
    }

    async fn latest_portfolio_snapshot(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Result<Option<PortfolioSnapshotView>> {
        let row = sqlx::query(
            r#"
            SELECT cash_usd, positions_json, created_at
            FROM portfolio_snapshots
            WHERE org_id = $1 AND user_id = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch latest portfolio snapshot")?;

        match row {
            Some(found) => {
                let cash_usd: f64 = found.try_get("cash_usd")?;
                let positions_json: serde_json::Value = found.try_get("positions_json")?;
                let created_at: DateTime<Utc> = found.try_get("created_at")?;
                Ok(Some(PortfolioSnapshotView {
                    cash_usd,
                    positions_json,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn portfolio_snapshots(
        &self,
        org_id: uuid::Uuid,
        user_id: uuid::Uuid,
        limit: usize,
    ) -> Result<Vec<PortfolioSnapshotView>> {
        let rows = sqlx::query(
            r#"
            SELECT cash_usd, positions_json, created_at
            FROM portfolio_snapshots
            WHERE org_id = $1 AND user_id = $2
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch portfolio snapshots")?;

        let mut out: Vec<PortfolioSnapshotView> = Vec::with_capacity(rows.len());
        for row in rows {
            let cash_usd: f64 = row.try_get("cash_usd")?;
            let positions_json: serde_json::Value = row.try_get("positions_json")?;
            let created_at: DateTime<Utc> = row.try_get("created_at")?;
            out.push(PortfolioSnapshotView {
                cash_usd,
                positions_json,
                created_at,
            });
        }
        out.reverse();
        Ok(out)
    }

    async fn upsert_social_claims(&self, org_id: uuid::Uuid, claims: &[SocialClaim]) -> Result<()> {
        for claim in claims {
            let dedup_key = social_claim_dedup_key(claim);
            sqlx::query(
                r#"
                INSERT INTO social_intel_claims (
                    id, org_id, source, query, author, community, posted_at, captured_at,
                    url, text_body, brag_score, hype_score, evidence_flags, strategy_tags, dedup_key
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, $14, $15
                )
                ON CONFLICT (org_id, dedup_key) DO NOTHING
                "#,
            )
            .bind(claim.id)
            .bind(org_id)
            .bind(claim.source.code())
            .bind(claim.query.as_deref())
            .bind(claim.author.as_deref())
            .bind(claim.community.as_deref())
            .bind(claim.posted_at)
            .bind(claim.captured_at)
            .bind(claim.url.as_deref())
            .bind(&claim.text)
            .bind(claim.brag_score)
            .bind(claim.hype_score)
            .bind(sqlx::types::Json(&claim.evidence_flags))
            .bind(sqlx::types::Json(&claim.strategy_tags))
            .bind(dedup_key)
            .execute(&self.pool)
            .await
            .context("failed to upsert social intel claim")?;
        }
        Ok(())
    }

    async fn recent_social_claims(
        &self,
        org_id: uuid::Uuid,
        limit: usize,
    ) -> Result<Vec<SocialClaim>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id, source, query, author, community, posted_at, captured_at, url, text_body,
                brag_score, hype_score, evidence_flags, strategy_tags
            FROM social_intel_claims
            WHERE org_id = $1
            ORDER BY captured_at DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch social intel claims")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let source_raw: String = row.try_get("source")?;
            let source = parse_social_source(source_raw)?;
            let evidence_flags: sqlx::types::Json<Vec<String>> = row.try_get("evidence_flags")?;
            let strategy_tags: sqlx::types::Json<Vec<String>> = row.try_get("strategy_tags")?;
            out.push(SocialClaim {
                id: row.try_get("id")?,
                source,
                query: row.try_get("query")?,
                author: row.try_get("author")?,
                community: row.try_get("community")?,
                posted_at: row.try_get("posted_at")?,
                captured_at: row.try_get("captured_at")?,
                url: row.try_get("url")?,
                text: row.try_get("text_body")?,
                brag_score: row.try_get("brag_score")?,
                hype_score: row.try_get("hype_score")?,
                evidence_flags: evidence_flags.0,
                strategy_tags: strategy_tags.0,
            });
        }
        Ok(out)
    }

    async fn upsert_orchestrator_snapshot(
        &self,
        org_id: uuid::Uuid,
        snapshot: &orchestrator::OrchestratorSnapshot,
    ) -> Result<()> {
        let payload =
            serde_json::to_value(snapshot).context("failed to encode orchestrator snapshot")?;
        sqlx::query(
            r#"
            INSERT INTO strategy_orchestrator_snapshots (org_id, snapshot_json, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (org_id)
            DO UPDATE SET snapshot_json = EXCLUDED.snapshot_json, updated_at = NOW()
            "#,
        )
        .bind(org_id)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("failed to upsert orchestrator snapshot")?;
        sqlx::query(
            r#"
            INSERT INTO strategy_orchestrator_snapshot_history (org_id, snapshot_json)
            VALUES ($1, $2)
            "#,
        )
        .bind(org_id)
        .bind(serde_json::to_value(snapshot).context("failed to encode orchestrator history snapshot")?)
        .execute(&self.pool)
        .await
        .context("failed to append orchestrator snapshot history")?;
        Ok(())
    }

    async fn latest_orchestrator_snapshot(
        &self,
        org_id: uuid::Uuid,
    ) -> Result<Option<orchestrator::OrchestratorSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT snapshot_json
            FROM strategy_orchestrator_snapshots
            WHERE org_id = $1
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch orchestrator snapshot")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: serde_json::Value = row.try_get("snapshot_json")?;
        let snapshot: orchestrator::OrchestratorSnapshot =
            serde_json::from_value(payload).context("invalid orchestrator snapshot JSON")?;
        Ok(Some(snapshot))
    }

    async fn orchestrator_snapshot_history(
        &self,
        org_id: uuid::Uuid,
        limit: usize,
    ) -> Result<Vec<OrchestratorSnapshotHistoryView>> {
        let rows = sqlx::query(
            r#"
            SELECT id, snapshot_json, created_at
            FROM strategy_orchestrator_snapshot_history
            WHERE org_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch orchestrator snapshot history")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: serde_json::Value = row.try_get("snapshot_json")?;
            let snapshot: orchestrator::OrchestratorSnapshot = serde_json::from_value(payload)
                .context("invalid orchestrator history snapshot JSON")?;
            out.push(OrchestratorSnapshotHistoryView {
                snapshot_id: row.try_get("id")?,
                created_at: row.try_get("created_at")?,
                snapshot,
            });
        }
        Ok(out)
    }

    async fn orchestrator_snapshot_by_id(
        &self,
        org_id: uuid::Uuid,
        snapshot_id: i64,
    ) -> Result<Option<OrchestratorSnapshotHistoryView>> {
        let row = sqlx::query(
            r#"
            SELECT id, snapshot_json, created_at
            FROM strategy_orchestrator_snapshot_history
            WHERE org_id = $1
              AND id = $2
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(snapshot_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch orchestrator snapshot by id")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: serde_json::Value = row.try_get("snapshot_json")?;
        let snapshot: orchestrator::OrchestratorSnapshot =
            serde_json::from_value(payload).context("invalid orchestrator snapshot payload")?;
        Ok(Some(OrchestratorSnapshotHistoryView {
            snapshot_id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            snapshot,
        }))
    }
}

async fn upsert_org(pool: &PgPool, name: &str) -> Result<uuid::Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO orgs (id, name)
        VALUES ($1, $2)
        ON CONFLICT (name)
        DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        "#,
    )
    .bind(uuid::Uuid::new_v4())
    .bind(name)
    .fetch_one(pool)
    .await
    .context("failed to upsert org")?;
    row.try_get("id").context("missing org id in upsert result")
}

async fn upsert_user(
    pool: &PgPool,
    org_id: uuid::Uuid,
    email: &str,
    role: &str,
) -> Result<uuid::Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO users (id, org_id, email, role)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (email)
        DO UPDATE SET org_id = EXCLUDED.org_id, role = EXCLUDED.role
        RETURNING id
        "#,
    )
    .bind(uuid::Uuid::new_v4())
    .bind(org_id)
    .bind(email)
    .bind(role)
    .fetch_one(pool)
    .await
    .context("failed to upsert user")?;
    row.try_get("id")
        .context("missing user id in upsert result")
}

fn map_auth_user(row: sqlx::postgres::PgRow) -> Result<AuthUser> {
    let user_id: uuid::Uuid = row.try_get("id")?;
    let org_id: uuid::Uuid = row.try_get("org_id")?;
    let email: String = row.try_get("email")?;
    let role: String = row.try_get("role")?;
    let active: bool = row.try_get("active")?;
    let must_reset_password: bool = row.try_get("must_reset_password")?;
    let password_hash: Option<String> = row.try_get("password_hash")?;
    Ok(AuthUser {
        user_id,
        org_id,
        email,
        role: parse_role(&role)?,
        active,
        must_reset_password,
        password_hash,
    })
}

fn map_user_response(row: sqlx::postgres::PgRow) -> Result<UserResponse> {
    let id: uuid::Uuid = row.try_get("id")?;
    let org_id: uuid::Uuid = row.try_get("org_id")?;
    let email: String = row.try_get("email")?;
    let role: String = row.try_get("role")?;
    let active: bool = row.try_get("active")?;
    let must_reset_password: bool = row.try_get("must_reset_password")?;
    let created_at: DateTime<Utc> = row.try_get("created_at")?;
    Ok(UserResponse {
        id,
        org_id,
        email,
        role: parse_role(&role)?,
        active,
        must_reset_password,
        created_at,
    })
}

fn map_canonical_instrument_row(row: sqlx::postgres::PgRow) -> Result<domain::CanonicalInstrument> {
    let venue_raw: String = row.try_get("venue")?;
    let lifecycle_raw: String = row.try_get("lifecycle")?;
    let metadata: serde_json::Value = row.try_get("metadata_json")?;
    Ok(domain::CanonicalInstrument {
        id: row.try_get("id")?,
        canonical_event_id: row.try_get("canonical_event_id")?,
        canonical_outcome_id: row.try_get("canonical_outcome_id")?,
        canonical_event_title: row.try_get("canonical_event_title")?,
        outcome_label: row.try_get("outcome_label")?,
        venue: parse_venue(venue_raw)?,
        symbol: row.try_get("symbol")?,
        confidence: row.try_get("confidence")?,
        lifecycle: parse_lifecycle(&lifecycle_raw)?,
        manual_override: row.try_get("manual_override")?,
        metadata,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn make_password_hash(password: &str) -> Result<String> {
    let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes())
        .map_err(|e| anyhow!("failed to create password salt: {e}"))?;
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| anyhow!("argon2 password hash failed: {e}"))?
        .to_string();
    Ok(hash)
}

fn verify_password_hash(stored: &str, password: &str) -> bool {
    if stored.starts_with("v1$") {
        // Backward-compat for earlier development hashes.
        let parts: Vec<&str> = stored.split('$').collect();
        if parts.len() != 3 {
            return false;
        }
        let salt = parts[1];
        let expected = parts[2];
        let mut hasher = Sha256::new();
        hasher.update(format!("{salt}:{password}").as_bytes());
        let digest = hasher.finalize();
        let calculated: String = digest.iter().map(|b| format!("{:02x}", b)).collect();
        return calculated == expected;
    }

    let parsed = match PasswordHash::new(stored) {
        Ok(v) => v,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

fn generate_refresh_token() -> String {
    format!(
        "rt_{}_{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn encode_hs256_jwt(secret: &str, claims: &AccessClaims) -> Result<String> {
    let header = serde_json::json!({ "alg": "HS256", "typ": "JWT" });
    let header_json = serde_json::to_vec(&header)?;
    let claims_json = serde_json::to_vec(claims)?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json);
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json);
    let signing_input = format!("{header_b64}.{claims_b64}");
    type HmacSha256 = Hmac<Sha256>;
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("invalid JWT secret length")?;
    mac.update(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{sig_b64}"))
}

fn decode_hs256_jwt(secret: &str, token: &str) -> Result<JwtClaims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(anyhow!("invalid JWT format"));
    }

    let header_bytes = URL_SAFE_NO_PAD
        .decode(parts[0].as_bytes())
        .context("invalid JWT header encoding")?;
    let header_value: serde_json::Value =
        serde_json::from_slice(&header_bytes).context("invalid JWT header json")?;
    let alg = header_value
        .get("alg")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if alg != "HS256" {
        return Err(anyhow!("unsupported JWT alg `{alg}`"));
    }

    type HmacSha256 = Hmac<Sha256>;
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("invalid JWT secret length")?;
    mac.update(signing_input.as_bytes());
    let expected_signature = mac.finalize().into_bytes();
    let actual_signature = URL_SAFE_NO_PAD
        .decode(parts[2].as_bytes())
        .context("invalid JWT signature encoding")?;
    if expected_signature.as_slice() != actual_signature.as_slice() {
        return Err(anyhow!("JWT signature mismatch"));
    }

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1].as_bytes())
        .context("invalid JWT payload encoding")?;
    let claims: JwtClaims =
        serde_json::from_slice(&payload_bytes).context("invalid JWT payload json")?;
    if let Some(exp) = claims.exp {
        let now = Utc::now().timestamp().max(0) as usize;
        if now > exp {
            return Err(anyhow!("JWT expired"));
        }
    }
    Ok(claims)
}

fn parse_role(raw: &str) -> Result<Role> {
    match raw.to_ascii_lowercase().as_str() {
        "admin" => Ok(Role::Admin),
        "trader" => Ok(Role::Trader),
        "viewer" => Ok(Role::Viewer),
        _ => Err(anyhow!(
            "invalid API role `{raw}` (expected admin|trader|viewer)"
        )),
    }
}

fn role_code(role: &Role) -> &'static str {
    match role {
        Role::Admin => "admin",
        Role::Trader => "trader",
        Role::Viewer => "viewer",
    }
}

fn lifecycle_code(lifecycle: domain::InstrumentLifecycle) -> &'static str {
    match lifecycle {
        domain::InstrumentLifecycle::New => "new",
        domain::InstrumentLifecycle::Tradable => "tradable",
        domain::InstrumentLifecycle::WatchlistOnly => "watchlist_only",
        domain::InstrumentLifecycle::Deprecated => "deprecated",
    }
}

fn parse_lifecycle(raw: &str) -> Result<domain::InstrumentLifecycle> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "new" => Ok(domain::InstrumentLifecycle::New),
        "tradable" => Ok(domain::InstrumentLifecycle::Tradable),
        "watchlist_only" => Ok(domain::InstrumentLifecycle::WatchlistOnly),
        "deprecated" => Ok(domain::InstrumentLifecycle::Deprecated),
        _ => Err(anyhow!(
            "invalid lifecycle `{raw}` (expected new|tradable|watchlist_only|deprecated)"
        )),
    }
}

fn parse_mode(raw: String) -> Result<ExecutionMode> {
    match raw.to_ascii_lowercase().as_str() {
        "paper" => Ok(ExecutionMode::Paper),
        "shadow" => Ok(ExecutionMode::Shadow),
        "live" => Ok(ExecutionMode::Live),
        _ => Err(anyhow!("invalid EXECUTION_MODE: {raw}")),
    }
}

fn parse_venue(raw: String) -> Result<Venue> {
    match raw.to_ascii_lowercase().as_str() {
        "kalshi" => Ok(Venue::Kalshi),
        "ibkr" => Ok(Venue::Ibkr),
        "coinbase" => Ok(Venue::Coinbase),
        "polymarket" => Ok(Venue::Polymarket),
        _ => Err(anyhow!("invalid venue: {raw}")),
    }
}

fn parse_social_source(raw: String) -> Result<SocialSource> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "x" | "twitter" => Ok(SocialSource::X),
        "reddit" => Ok(SocialSource::Reddit),
        "manual" => Ok(SocialSource::Manual),
        _ => Err(anyhow!("invalid source `{raw}` (expected x|reddit|manual)")),
    }
}

async fn enforce_registry_lifecycle(
    state: &AppState,
    org_id: uuid::Uuid,
    venue: &Venue,
    symbol: &str,
) -> Result<(), Response> {
    let lookup = if let Some(db) = &state.db {
        db.lookup_canonical_instrument(org_id, venue, symbol)
            .await
            .map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
    } else {
        state
            .registry
            .lookup(org_id, venue, symbol)
            .map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
    };
    let Some(record) = lookup else {
        return Ok(());
    };
    if record.lifecycle.is_tradable() {
        return Ok(());
    }
    Err(api_error(
        StatusCode::FORBIDDEN,
        format!(
            "instrument {} on {} is {:?}; only tradable instruments can be ordered",
            symbol,
            venue_code(venue),
            record.lifecycle
        ),
    ))
}

fn registry_key(venue: &Venue, symbol: &str) -> String {
    format!(
        "{}:{}",
        venue_code(venue).to_ascii_lowercase(),
        symbol.trim().to_ascii_uppercase()
    )
}

fn filter_registry_records(
    mut records: Vec<domain::CanonicalInstrument>,
    query: &InstrumentRegistryQuery,
) -> Vec<domain::CanonicalInstrument> {
    let venue_filter = query
        .venue
        .as_ref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    let symbol_filter = query
        .symbol
        .as_ref()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty());
    let event_filter = query
        .canonical_event_id
        .as_ref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    let lifecycle_filter = query.lifecycle;
    records.retain(|record| {
        if let Some(filter) = venue_filter.as_ref() {
            if venue_code(&record.venue).to_ascii_lowercase() != *filter {
                return false;
            }
        }
        if let Some(filter) = symbol_filter.as_ref() {
            if record.symbol.to_ascii_uppercase() != *filter {
                return false;
            }
        }
        if let Some(filter) = event_filter.as_ref() {
            if record.canonical_event_id.to_ascii_lowercase() != *filter {
                return false;
            }
        }
        if let Some(filter) = lifecycle_filter {
            if record.lifecycle != filter {
                return false;
            }
        }
        true
    });
    records.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    let limit = query.limit.unwrap_or(200).clamp(1, 2_000);
    if records.len() > limit {
        records.truncate(limit);
    }
    records
}

async fn list_registry_records_for_org(
    state: &AppState,
    org_id: uuid::Uuid,
    limit: usize,
) -> Result<Vec<domain::CanonicalInstrument>> {
    let query = InstrumentRegistryQuery {
        venue: None,
        symbol: None,
        canonical_event_id: None,
        lifecycle: None,
        limit: Some(limit),
    };
    if let Some(db) = &state.db {
        db.list_canonical_instruments(org_id, &query).await
    } else {
        state.registry.list(org_id, &query)
    }
}

fn infer_event_domain(record: &domain::CanonicalInstrument) -> String {
    if let Some(domain) = record
        .metadata
        .get("domain")
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
    {
        return domain;
    }
    let mut text = String::new();
    if let Some(title) = &record.canonical_event_title {
        text.push_str(title);
        text.push(' ');
    }
    text.push_str(&record.outcome_label);
    let lower = text.to_ascii_lowercase();

    if lower.contains("election")
        || lower.contains("senate")
        || lower.contains("president")
        || lower.contains("governor")
        || lower.contains("congress")
        || lower.contains("policy")
    {
        "politics".to_string()
    } else if lower.contains("weather")
        || lower.contains("rain")
        || lower.contains("hurricane")
        || lower.contains("temperature")
    {
        "weather".to_string()
    } else if lower.contains("nba")
        || lower.contains("nfl")
        || lower.contains("sports")
        || lower.contains("match")
        || lower.contains("tournament")
    {
        "sports".to_string()
    } else if lower.contains("btc")
        || lower.contains("bitcoin")
        || lower.contains("eth")
        || lower.contains("crypto")
        || lower.contains("sol")
    {
        "crypto".to_string()
    } else if lower.contains("cpi")
        || lower.contains("fed")
        || lower.contains("rates")
        || lower.contains("inflation")
        || lower.contains("gdp")
    {
        "macro".to_string()
    } else {
        "other".to_string()
    }
}

fn extract_settlement_at(metadata: &serde_json::Value) -> Option<DateTime<Utc>> {
    let metadata_obj = metadata.as_object()?;
    for key in ["settlement_at", "event_time", "close_time", "resolution_time"] {
        let Some(raw) = metadata_obj.get(key) else {
            continue;
        };
        if let Some(text) = raw.as_str() {
            if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
                return Some(dt.with_timezone(&Utc));
            }
        }
    }
    None
}

fn settlement_window_label(settlement_at: Option<DateTime<Utc>>) -> Option<String> {
    let settlement_at = settlement_at?;
    let now = Utc::now();
    let hours = settlement_at.signed_duration_since(now).num_hours();
    if hours <= 0 {
        Some("settled_or_expired".to_string())
    } else if hours <= 24 {
        Some("within_24h".to_string())
    } else if hours <= 72 {
        Some("within_72h".to_string())
    } else if hours <= 24 * 14 {
        Some("within_2w".to_string())
    } else {
        Some("long_dated".to_string())
    }
}

fn evaluate_research_acceptance(
    config: &ResearchAcceptanceConfig,
    metrics: &ResearchMetrics,
    baseline: Option<&ResearchRunRecord>,
) -> ResearchAcceptanceResult {
    let thresholds = config.thresholds();
    let mut reasons = Vec::new();
    if metrics.sharpe < config.min_sharpe {
        reasons.push(format!(
            "sharpe {:.3} below minimum {:.3}",
            metrics.sharpe, config.min_sharpe
        ));
    }
    if metrics.max_drawdown_pct > config.max_drawdown_pct {
        reasons.push(format!(
            "max_drawdown_pct {:.2}% above limit {:.2}%",
            metrics.max_drawdown_pct, config.max_drawdown_pct
        ));
    }
    if metrics.trades < config.min_trades {
        reasons.push(format!(
            "trades {} below minimum {}",
            metrics.trades, config.min_trades
        ));
    }
    if metrics.total_return_pct < config.min_return_pct {
        reasons.push(format!(
            "total_return_pct {:.2}% below minimum {:.2}%",
            metrics.total_return_pct, config.min_return_pct
        ));
    }

    let sharpe_delta_vs_baseline = baseline.map(|baseline| metrics.sharpe - baseline.metrics.sharpe);
    if let Some(delta) = sharpe_delta_vs_baseline {
        if delta < config.min_sharpe_delta_vs_baseline {
            reasons.push(format!(
                "sharpe delta {:.3} below baseline threshold {:.3}",
                delta, config.min_sharpe_delta_vs_baseline
            ));
        }
    }

    let pass = reasons.is_empty();
    let status = if pass {
        ResearchRunStatus::Pass
    } else if reasons.len() == 1 && metrics.sharpe >= config.min_sharpe {
        ResearchRunStatus::ManualReview
    } else {
        ResearchRunStatus::Fail
    };
    ResearchAcceptanceResult {
        pass,
        status,
        reasons,
        thresholds,
        sharpe_delta_vs_baseline,
    }
}

async fn persist_research_run(state: &AppState, record: ResearchRunRecord) -> Result<ResearchRunRecord> {
    if let Some(db) = &state.db {
        db.insert_research_run(record.org_id, &record).await?;
    } else {
        state.research.insert(record.clone())?;
    }
    Ok(record)
}

async fn list_research_runs(
    state: &AppState,
    org_id: uuid::Uuid,
    limit: usize,
    strategy_id: Option<uuid::Uuid>,
) -> Result<Vec<ResearchRunRecord>> {
    if let Some(db) = &state.db {
        db.recent_research_runs(org_id, limit, strategy_id).await
    } else {
        state.research.list(org_id, limit, strategy_id)
    }
}

async fn research_run_by_id(
    state: &AppState,
    org_id: uuid::Uuid,
    run_id: uuid::Uuid,
) -> Result<Option<ResearchRunRecord>> {
    if let Some(db) = &state.db {
        db.research_run_by_id(org_id, run_id).await
    } else {
        state.research.get(org_id, run_id)
    }
}

fn social_claim_dedup_key(claim: &SocialClaim) -> String {
    let source = claim.source.code();
    let url = claim.url.clone().unwrap_or_default().to_ascii_lowercase();
    let text = claim.text.trim().to_ascii_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(format!("{source}|{url}|{text}").as_bytes());
    format!("{:x}", hasher.finalize())
}

fn default_symbol(venue: &Venue) -> &'static str {
    match venue {
        Venue::Kalshi => "KXBTCUSD-26DEC31-T200000.00",
        Venue::Ibkr => "265598",
        Venue::Coinbase => "BTC-USD",
        Venue::Polymarket => "PRES-2028-DEM-WIN",
        _ => "UNKNOWN",
    }
}

fn default_asset_class(venue: &Venue) -> AssetClass {
    match venue {
        Venue::Kalshi => AssetClass::Prediction,
        Venue::Polymarket => AssetClass::Prediction,
        Venue::Coinbase => AssetClass::Crypto,
        Venue::Ibkr => AssetClass::Equity,
        _ => AssetClass::Other,
    }
}

fn mode_code(mode: ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::Paper => "paper",
        ExecutionMode::Shadow => "shadow",
        ExecutionMode::Live => "live",
    }
}

fn order_status_code(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Accepted => "accepted",
        OrderStatus::Simulated => "simulated",
        OrderStatus::Rejected => "rejected",
    }
}

fn venue_code(venue: &Venue) -> String {
    match venue {
        Venue::Kalshi => "kalshi".to_string(),
        Venue::Ibkr => "ibkr".to_string(),
        Venue::Coinbase => "coinbase".to_string(),
        Venue::Polymarket => "polymarket".to_string(),
        Venue::Other(v) => v.clone(),
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

fn split_sql_statements(sql: &str) -> Vec<&str> {
    sql.split(';')
        .map(str::trim)
        .filter(|stmt| !stmt.is_empty())
        .collect()
}

fn percentile_u128(mut values: Vec<u128>, percentile: f64) -> Option<u128> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let p = percentile.clamp(0.0, 1.0);
    let idx = ((values.len() - 1) as f64 * p).ceil() as usize;
    values.get(idx).copied()
}

fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|v| match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_csv_lower(key: &str) -> HashSet<String> {
    std::env::var(key)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(|part| part.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_optional_f64(key: &str) -> Option<f64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
}

fn env_optional_positive_f64(key: &str) -> Option<f64> {
    env_optional_f64(key).filter(|value| value.is_finite() && *value > 0.0)
}

fn metadata_map_f64(
    metadata: &indexmap::IndexMap<String, String>,
    keys: &[&str],
) -> Option<f64> {
    for key in keys {
        let Some(raw) = metadata.get(*key) else {
            continue;
        };
        let Ok(parsed) = raw.trim().parse::<f64>() else {
            continue;
        };
        if parsed.is_finite() {
            return Some(parsed);
        }
    }
    None
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}
