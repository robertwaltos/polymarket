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
use oms::{JsonlAuditStore, OmsConfig, OmsEngine, OmsError, OrderAuditRecord};
use portfolio::PortfolioState;
use risk::{RiskEngine, RiskLimits};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
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
    live_guard: LiveExecutionGuard,
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
}

impl LiveGuardConfig {
    fn from_env() -> Self {
        Self {
            health_poll_interval_secs: env_u64("LIVE_GUARD_HEALTH_POLL_SECS", 10),
            max_health_age_secs: env_i64("LIVE_GUARD_HEALTH_MAX_AGE_SECS", 45),
            failure_threshold: env_u32("LIVE_GUARD_FAILURE_THRESHOLD", 3),
            cooldown_secs: env_i64("LIVE_GUARD_COOLDOWN_SECS", 120),
            require_healthy: env_bool("LIVE_GUARD_REQUIRE_HEALTHY", true),
        }
    }
}

#[derive(Debug, Clone)]
struct VenueHealthState {
    ok: bool,
    detail: String,
    checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct VenueLiveGuardState {
    live_enabled: bool,
    consecutive_live_failures: u32,
    breaker_open_until: Option<DateTime<Utc>>,
    last_health: Option<VenueHealthState>,
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
    last_health_at: Option<DateTime<Utc>>,
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

        Ok(())
    }

    fn note_health(
        &self,
        venue: &Venue,
        ok: bool,
        detail: String,
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
            Some(existing) => existing.ok != ok || existing.detail != detail,
            None => true,
        };

        venue_state.last_health = Some(VenueHealthState {
            ok,
            detail,
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
                    last_health_at: venue_state.last_health.as_ref().map(|v| v.checked_at),
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

        let _ = venue;
        (true, None)
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
}

#[derive(Debug, Deserialize)]
struct RecentOrdersQuery {
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
    if registry.all().is_empty() {
        return Err(anyhow!(
            "no adapters configured. set Kalshi, IBKR, or Coinbase env variables first"
        ));
    }

    let db = init_db_from_env().await?;
    let auth = build_auth_store_from_env()?;

    let allow_live = env_bool("LIVE_TRADING_ENABLED", false);
    let default_mode =
        parse_mode(std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "paper".to_string()))?;
    let starting_cash = env_f64("SIM_STARTING_CASH_USD", 50_000.0);
    let risk = RiskEngine::new(RiskLimits {
        allow_live,
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
        },
        registry.clone(),
        risk,
        PortfolioState::with_starting_cash("USD", starting_cash),
        audit_dir,
    );

    match command.as_str() {
        "health" => run_health_checks(&registry).await,
        "demo-order" => run_demo_order(&oms, default_mode).await,
        "run-once" => {
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
                let (ok, detail, checked_at) = match adapter.health().await {
                    Ok(report) => (report.ok, report.detail, report.checked_at),
                    Err(err) => (false, format!("health probe error: {err}"), Utc::now()),
                };

                match live_guard.note_health(&venue, ok, detail.clone(), checked_at) {
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
    let event_bus = EventBus::new(env_u32("STREAM_EVENT_BUFFER", 512) as usize);
    let state = AppState {
        oms,
        auth,
        default_mode,
        db,
        rate_limiter: OrgRateLimiter::new(env_u32("ORG_RATE_LIMIT_PER_MIN", 600) as usize),
        live_guard: live_guard.clone(),
        event_bus: event_bus.clone(),
    };
    spawn_live_guard_probe(state.oms.adapter_registry(), live_guard, event_bus);

    let protected = Router::new()
        .route("/v1/orders", post(api_place_order))
        .route("/v1/portfolio", get(api_get_portfolio))
        .route("/v1/portfolio/history", get(api_portfolio_history))
        .route("/v1/orders/recent", get(api_recent_orders))
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
    Json(serde_json::json!({
        "ok": true,
        "timestamp": Utc::now(),
        "execution_mode_default": state.default_mode,
        "audit_recent_count": recent_count,
        "db_enabled": state.db.is_some()
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

    let venue = match parse_venue(payload.venue) {
        Ok(v) => v,
        Err(err) => return api_error(StatusCode::BAD_REQUEST, err.to_string()),
    };
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

    let order = OrderRequest {
        request_id: uuid::Uuid::new_v4(),
        user_id: identity.user_id,
        strategy_id: payload.strategy_id,
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
        order_type: payload.order_type,
        time_in_force: payload.time_in_force.unwrap_or(TimeInForce::Day),
        quantity: payload.quantity,
        limit_price: payload.limit_price,
        submitted_at: Utc::now(),
        metadata: Default::default(),
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
                    .record_live_result(&venue, live_ok, &ack.message)
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
                    .record_live_result(&venue, false, &err_detail)
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
        _ => Err(anyhow!("invalid venue: {raw}")),
    }
}

fn default_symbol(venue: &Venue) -> &'static str {
    match venue {
        Venue::Kalshi => "KXBTCUSD-26DEC31-T200000.00",
        Venue::Ibkr => "265598",
        Venue::Coinbase => "BTC-USD",
        _ => "UNKNOWN",
    }
}

fn default_asset_class(venue: &Venue) -> AssetClass {
    match venue {
        Venue::Kalshi => AssetClass::Prediction,
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
