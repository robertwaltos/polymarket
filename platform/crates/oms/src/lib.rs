pub mod audit;
pub use audit::{
    AuditError, AuditModeDecision, AuditStore, DecisionTrace, ExecutionDecisionTrace,
    InMemoryAuditStore, JsonlAuditStore, OrderAuditRecord, RiskDecisionTrace,
    SignalDecisionTrace,
};

use chrono::Utc;
use domain::{ExecutionMode, FillEvent, MarketQuote, OrderAck, OrderRequest, OrderStatus, Venue};
use portfolio::PortfolioState;
use risk::{RiskEngine, RiskViolation};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::{info, warn};
use venue_adapters::{AdapterError, AdapterRegistry};

#[derive(Debug, Clone)]
pub struct OmsConfig {
    pub live_trading_enabled: bool,
    pub shadow_reads_enabled: bool,
    pub simulated_fee_bps: f64,
    pub require_quote_for_market_data_modes: bool,
    pub max_quote_age_secs: Option<i64>,
}

impl Default for OmsConfig {
    fn default() -> Self {
        Self {
            live_trading_enabled: false,
            shadow_reads_enabled: true,
            simulated_fee_bps: 2.0,
            require_quote_for_market_data_modes: false,
            max_quote_age_secs: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModeDecision {
    pub requested: ExecutionMode,
    pub effective: ExecutionMode,
    pub reason: Option<String>,
}

#[derive(Debug, Error)]
pub enum OmsError {
    #[error("adapter error: {0}")]
    Adapter(#[from] AdapterError),
    #[error("risk check failed: {0}")]
    Risk(#[from] RiskViolation),
    #[error("pre-trade quote unavailable for {mode:?} order on {venue:?}:{symbol}")]
    QuoteUnavailable {
        mode: ExecutionMode,
        venue: Venue,
        symbol: String,
    },
    #[error("pre-trade quote stale for {venue:?}:{symbol} (age {age_secs}s > {max_age_secs}s)")]
    StaleQuote {
        venue: Venue,
        symbol: String,
        age_secs: i64,
        max_age_secs: i64,
    },
    #[error("missing reference price for simulation")]
    MissingReferencePrice,
    #[error("portfolio lock poisoned")]
    PortfolioPoisoned,
    #[error("audit error: {0}")]
    Audit(#[from] AuditError),
}

#[derive(Clone)]
pub struct OmsEngine {
    config: OmsConfig,
    adapters: AdapterRegistry,
    risk: RiskEngine,
    portfolio_template: PortfolioState,
    portfolios: Arc<Mutex<HashMap<uuid::Uuid, PortfolioState>>>,
    audit_store: Arc<dyn AuditStore>,
}

impl OmsEngine {
    pub fn new(
        config: OmsConfig,
        adapters: AdapterRegistry,
        risk: RiskEngine,
        portfolio: PortfolioState,
    ) -> Self {
        Self::with_audit_store(
            config,
            adapters,
            risk,
            portfolio,
            Arc::new(InMemoryAuditStore::new()),
        )
    }

    pub fn with_audit_store(
        config: OmsConfig,
        adapters: AdapterRegistry,
        risk: RiskEngine,
        portfolio: PortfolioState,
        audit_store: Arc<dyn AuditStore>,
    ) -> Self {
        Self {
            config,
            adapters,
            risk,
            portfolio_template: portfolio,
            portfolios: Arc::new(Mutex::new(HashMap::new())),
            audit_store,
        }
    }

    pub fn mode_decision(&self, requested: ExecutionMode) -> ModeDecision {
        match requested {
            ExecutionMode::Live if !self.config.live_trading_enabled => ModeDecision {
                requested,
                effective: ExecutionMode::Shadow,
                reason: Some("live disabled; downgraded to shadow".to_string()),
            },
            _ => ModeDecision {
                requested,
                effective: requested,
                reason: None,
            },
        }
    }

    pub async fn submit_order(&self, order: OrderRequest) -> Result<OrderAck, OmsError> {
        let (ack, _) = self.submit_order_with_audit(order).await?;
        Ok(ack)
    }

    pub async fn submit_order_with_audit(
        &self,
        mut order: OrderRequest,
    ) -> Result<(OrderAck, OrderAuditRecord), OmsError> {
        let decision = self.mode_decision(order.mode);
        order.mode = decision.effective;

        if let Some(reason) = &decision.reason {
            warn!(
                request_id = %order.request_id,
                requested_mode = ?decision.requested,
                effective_mode = ?decision.effective,
                "{reason}"
            );
        }

        let adapter = self.adapters.get(&order.instrument.venue)?;
        let reference_quote = self.resolve_reference_quote(&*adapter, &order).await;
        self.enforce_quote_freshness(&order, reference_quote.as_ref())?;
        let quote_age_secs = reference_quote.as_ref().map(|quote| {
            Utc::now()
                .signed_duration_since(quote.timestamp)
                .num_seconds()
                .max(0)
        });
        let reference_price = reference_quote
            .as_ref()
            .and_then(|quote| quote.last.or(quote.bid).or(quote.ask))
            .or(order.limit_price);
        if order.limit_price.is_none() {
            order.limit_price = reference_price;
        }

        let portfolio_for_risk = {
            let mut guard = self
                .portfolios
                .lock()
                .map_err(|_| OmsError::PortfolioPoisoned)?;
            guard
                .entry(order.user_id)
                .or_insert_with(|| self.portfolio_template.clone())
                .clone()
        };
        let risk_report = self.risk.evaluate_order(
            &order,
            &portfolio_for_risk,
            reference_price,
            reference_quote.as_ref(),
        )?;

        let ack = match decision.effective {
            ExecutionMode::Paper => self.simulate_order(&order, "paper").await,
            ExecutionMode::Shadow => {
                if self.config.shadow_reads_enabled {
                    match adapter.fetch_balances().await {
                        Ok(balances) => {
                            info!(
                                request_id = %order.request_id,
                                venue = ?order.instrument.venue,
                                balances = balances.len(),
                                "shadow mode fetched live balances"
                            );
                        }
                        Err(err) => {
                            warn!(
                                request_id = %order.request_id,
                                venue = ?order.instrument.venue,
                                error = %err,
                                "shadow mode balance fetch failed"
                            );
                        }
                    }
                }
                self.simulate_order(&order, "shadow").await
            }
            ExecutionMode::Live => {
                let result = adapter.place_live_order(&order).await?;
                let status = if result.accepted {
                    OrderStatus::Accepted
                } else {
                    OrderStatus::Rejected
                };
                Ok(OrderAck {
                    request_id: order.request_id,
                    status,
                    venue_order_id: result.venue_order_id,
                    mode: ExecutionMode::Live,
                    message: result.message,
                    received_at: Utc::now(),
                })
            }
        }?;

        let record = OrderAuditRecord {
            order: order.clone(),
            ack: ack.clone(),
            decision: AuditModeDecision {
                requested: decision.requested,
                effective: decision.effective,
                reason: decision.reason,
            },
            decision_trace: Some(DecisionTrace {
                signal: self.signal_trace_from_order(&order),
                risk: RiskDecisionTrace {
                    reference_price,
                    quote_age_secs,
                    checks: risk_report,
                },
                execution: ExecutionDecisionTrace {
                    venue: order.instrument.venue.clone(),
                    symbol: order.instrument.symbol.clone(),
                    requested_mode: decision.requested,
                    effective_mode: decision.effective,
                    status: ack.status,
                    simulated: matches!(ack.status, OrderStatus::Simulated),
                    venue_order_id: ack.venue_order_id.clone(),
                    message: ack.message.clone(),
                },
            }),
            recorded_at: Utc::now(),
        };
        self.audit_store.record(&record)?;
        Ok((ack, record))
    }

    pub fn portfolio_snapshot(&self) -> Result<PortfolioState, OmsError> {
        self.portfolio_snapshot_for_user(uuid::Uuid::nil())
    }

    pub fn portfolio_snapshot_for_user(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<PortfolioState, OmsError> {
        let mut guard = self
            .portfolios
            .lock()
            .map_err(|_| OmsError::PortfolioPoisoned)?;
        let portfolio = guard
            .entry(user_id)
            .or_insert_with(|| self.portfolio_template.clone())
            .clone();
        Ok(portfolio)
    }

    pub fn recent_audit_records(&self, limit: usize) -> Result<Vec<OrderAuditRecord>, OmsError> {
        let safe_limit = if limit == 0 { 1 } else { limit };
        Ok(self.audit_store.recent(safe_limit)?)
    }

    fn signal_trace_from_order(&self, order: &OrderRequest) -> SignalDecisionTrace {
        SignalDecisionTrace {
            strategy_id: order.strategy_id,
            agent_id: metadata_string(order, "agent_id"),
            policy_version: metadata_string(order, "policy_version"),
            feature_hash: metadata_string(order, "feature_hash"),
            confidence: metadata_f64(order, "confidence").map(|value| value.clamp(0.0, 1.0)),
        }
    }

    fn quote_guard_applies(&self, mode: ExecutionMode) -> bool {
        matches!(mode, ExecutionMode::Shadow | ExecutionMode::Live)
    }

    fn enforce_quote_freshness(
        &self,
        order: &OrderRequest,
        quote: Option<&MarketQuote>,
    ) -> Result<(), OmsError> {
        if !self.quote_guard_applies(order.mode) {
            return Ok(());
        }

        if self.config.require_quote_for_market_data_modes && quote.is_none() {
            return Err(OmsError::QuoteUnavailable {
                mode: order.mode,
                venue: order.instrument.venue.clone(),
                symbol: order.instrument.symbol.clone(),
            });
        }

        let Some(max_age_secs) = self.config.max_quote_age_secs else {
            return Ok(());
        };
        let Some(quote) = quote else {
            return Ok(());
        };

        let max_age_secs = max_age_secs.max(1);
        let age_secs = Utc::now()
            .signed_duration_since(quote.timestamp)
            .num_seconds()
            .max(0);
        if age_secs > max_age_secs {
            return Err(OmsError::StaleQuote {
                venue: order.instrument.venue.clone(),
                symbol: order.instrument.symbol.clone(),
                age_secs,
                max_age_secs,
            });
        }
        Ok(())
    }

    async fn resolve_reference_quote(
        &self,
        adapter: &dyn venue_adapters::VenueAdapter,
        order: &OrderRequest,
    ) -> Option<MarketQuote> {
        match adapter.fetch_quote(&order.instrument.symbol).await {
            Ok(quote) => Some(quote),
            Err(err) => {
                warn!(
                    request_id = %order.request_id,
                    venue = ?order.instrument.venue,
                    error = %err,
                    "quote lookup failed; using order limit as fallback when policy permits"
                );
                None
            }
        }
    }

    async fn simulate_order(
        &self,
        order: &OrderRequest,
        mode_label: &str,
    ) -> Result<OrderAck, OmsError> {
        let price = order.limit_price.ok_or(OmsError::MissingReferencePrice)?;
        let fee = (order.quantity.abs() * price) * (self.config.simulated_fee_bps / 10_000.0);
        let fill = FillEvent {
            request_id: order.request_id,
            fill_id: uuid::Uuid::new_v4(),
            instrument: order.instrument.clone(),
            side: order.side,
            quantity: order.quantity.abs(),
            price,
            fee,
            occurred_at: Utc::now(),
            simulated: true,
        };

        {
            let mut guard = self
                .portfolios
                .lock()
                .map_err(|_| OmsError::PortfolioPoisoned)?;
            guard
                .entry(order.user_id)
                .or_insert_with(|| self.portfolio_template.clone())
                .apply_fill(&fill)
                .map_err(|e| OmsError::Adapter(AdapterError::InvalidRequest(e.to_string())))?;
        }

        Ok(OrderAck {
            request_id: order.request_id,
            status: OrderStatus::Simulated,
            venue_order_id: None,
            mode: order.mode,
            message: format!("{mode_label} execution simulated"),
            received_at: Utc::now(),
        })
    }
}

pub fn map_venue_symbol(venue: &Venue, raw_symbol: &str) -> String {
    match venue {
        Venue::Coinbase => raw_symbol.replace('/', "-").to_uppercase(),
        _ => raw_symbol.to_uppercase(),
    }
}

fn metadata_string(order: &OrderRequest, key: &str) -> Option<String> {
    order
        .metadata
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn metadata_f64(order: &OrderRequest, key: &str) -> Option<f64> {
    let raw = metadata_string(order, key)?;
    let parsed = raw.parse::<f64>().ok()?;
    if parsed.is_finite() {
        Some(parsed)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use domain::{AssetClass, BalanceSnapshot, Instrument, OrderType, Side, TimeInForce};
    use risk::RiskLimits;
    use venue_adapters::{HealthReport, VenueAdapter, VenueOrderResult};

    #[derive(Clone)]
    struct TestAdapter {
        quote: Option<MarketQuote>,
    }

    #[async_trait::async_trait]
    impl VenueAdapter for TestAdapter {
        fn venue(&self) -> Venue {
            Venue::Coinbase
        }

        async fn health(&self) -> Result<HealthReport, AdapterError> {
            Ok(HealthReport {
                venue: Venue::Coinbase,
                ok: true,
                latency_ms: 1,
                detail: "ok".to_string(),
                checked_at: Utc::now(),
            })
        }

        async fn fetch_quote(&self, _symbol: &str) -> Result<MarketQuote, AdapterError> {
            self.quote.clone().ok_or_else(|| {
                AdapterError::InvalidRequest("quote unavailable in test adapter".to_string())
            })
        }

        async fn fetch_balances(&self) -> Result<Vec<BalanceSnapshot>, AdapterError> {
            Ok(Vec::new())
        }

        async fn place_live_order(
            &self,
            order: &OrderRequest,
        ) -> Result<VenueOrderResult, AdapterError> {
            Ok(VenueOrderResult {
                venue: order.instrument.venue.clone(),
                venue_order_id: Some("live-test-order".to_string()),
                accepted: true,
                message: "accepted".to_string(),
            })
        }
    }

    fn sample_instrument() -> Instrument {
        Instrument {
            venue: Venue::Coinbase,
            symbol: "BTC-USD".to_string(),
            asset_class: AssetClass::Crypto,
            quote_currency: "USD".to_string(),
        }
    }

    fn sample_quote(timestamp: chrono::DateTime<Utc>) -> MarketQuote {
        MarketQuote {
            instrument: sample_instrument(),
            bid: Some(100.0),
            ask: Some(100.5),
            last: Some(100.2),
            timestamp,
            source: "test".to_string(),
        }
    }

    fn sample_order(mode: ExecutionMode, limit_price: Option<f64>) -> OrderRequest {
        OrderRequest::new(
            uuid::Uuid::new_v4(),
            mode,
            sample_instrument(),
            Side::Buy,
            OrderType::Limit,
            TimeInForce::Day,
            1.0,
            limit_price,
        )
    }

    fn test_engine(config: OmsConfig, quote: Option<MarketQuote>) -> OmsEngine {
        let mut registry = AdapterRegistry::new();
        registry.register(TestAdapter { quote });
        let risk = RiskEngine::new(RiskLimits {
            max_order_notional: 1_000_000.0,
            max_position_notional: 1_000_000.0,
            max_daily_loss: 1_000_000.0,
            allow_live: true,
            ..RiskLimits::default()
        });
        OmsEngine::new(
            config,
            registry,
            risk,
            PortfolioState::with_starting_cash("USD", 100_000.0),
        )
    }

    #[tokio::test]
    async fn blocks_shadow_orders_when_quote_is_missing() {
        let engine = test_engine(
            OmsConfig {
                require_quote_for_market_data_modes: true,
                max_quote_age_secs: Some(30),
                ..OmsConfig::default()
            },
            None,
        );

        let err = engine
            .submit_order(sample_order(ExecutionMode::Shadow, Some(100.0)))
            .await
            .expect_err("shadow order should be blocked without quote");
        assert!(matches!(
            err,
            OmsError::QuoteUnavailable {
                mode: ExecutionMode::Shadow,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn blocks_shadow_orders_when_quote_is_stale() {
        let engine = test_engine(
            OmsConfig {
                require_quote_for_market_data_modes: true,
                max_quote_age_secs: Some(30),
                ..OmsConfig::default()
            },
            Some(sample_quote(Utc::now() - Duration::seconds(120))),
        );

        let err = engine
            .submit_order(sample_order(ExecutionMode::Shadow, None))
            .await
            .expect_err("shadow order should be blocked with stale quote");
        match err {
            OmsError::StaleQuote { max_age_secs, .. } => assert_eq!(max_age_secs, 30),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn allows_shadow_orders_with_fresh_quote() {
        let engine = test_engine(
            OmsConfig {
                require_quote_for_market_data_modes: true,
                max_quote_age_secs: Some(30),
                ..OmsConfig::default()
            },
            Some(sample_quote(Utc::now() - Duration::seconds(2))),
        );

        let ack = engine
            .submit_order(sample_order(ExecutionMode::Shadow, None))
            .await
            .expect("shadow order should pass with fresh quote");
        assert_eq!(ack.status, OrderStatus::Simulated);
    }

    #[tokio::test]
    async fn audit_record_includes_decision_trace() {
        let engine = test_engine(
            OmsConfig {
                require_quote_for_market_data_modes: true,
                max_quote_age_secs: Some(30),
                ..OmsConfig::default()
            },
            Some(sample_quote(Utc::now() - Duration::seconds(2))),
        );

        let mut order = sample_order(ExecutionMode::Shadow, None);
        order
            .metadata
            .insert("agent_id".to_string(), "alpha-scout".to_string());
        order
            .metadata
            .insert("policy_version".to_string(), "v1.2.3".to_string());
        order
            .metadata
            .insert("confidence".to_string(), "0.76".to_string());
        let (_, record) = engine
            .submit_order_with_audit(order)
            .await
            .expect("audit record should be created");
        let trace = record
            .decision_trace
            .expect("decision trace should be populated");
        assert_eq!(trace.signal.agent_id.as_deref(), Some("alpha-scout"));
        assert_eq!(trace.signal.policy_version.as_deref(), Some("v1.2.3"));
        assert_eq!(trace.execution.effective_mode, ExecutionMode::Shadow);
        assert!(trace.risk.reference_price.is_some());
    }

    #[tokio::test]
    async fn paper_mode_skips_quote_guard() {
        let engine = test_engine(
            OmsConfig {
                require_quote_for_market_data_modes: true,
                max_quote_age_secs: Some(30),
                ..OmsConfig::default()
            },
            None,
        );

        let ack = engine
            .submit_order(sample_order(ExecutionMode::Paper, Some(100.0)))
            .await
            .expect("paper order should not require live quote");
        assert_eq!(ack.status, OrderStatus::Simulated);
    }
}
