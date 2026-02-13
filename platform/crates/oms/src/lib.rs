pub mod audit;
pub use audit::{
    AuditError, AuditModeDecision, AuditStore, InMemoryAuditStore, JsonlAuditStore,
    OrderAuditRecord,
};

use chrono::Utc;
use domain::{ExecutionMode, FillEvent, OrderAck, OrderRequest, OrderStatus, Venue};
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
}

impl Default for OmsConfig {
    fn default() -> Self {
        Self {
            live_trading_enabled: false,
            shadow_reads_enabled: true,
            simulated_fee_bps: 2.0,
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
        let reference_price = self
            .resolve_reference_price(&*adapter, &order)
            .await
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
        self.risk
            .validate_order(&order, &portfolio_for_risk, reference_price)?;

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
            recorded_at: Utc::now(),
        };
        self.audit_store.record(&record)?;
        Ok((ack, record))
    }

    pub fn portfolio_snapshot(&self) -> Result<PortfolioState, OmsError> {
        self.portfolio_snapshot_for_user(uuid::Uuid::nil())
    }

    pub fn portfolio_snapshot_for_user(&self, user_id: uuid::Uuid) -> Result<PortfolioState, OmsError> {
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

    async fn resolve_reference_price(
        &self,
        adapter: &dyn venue_adapters::VenueAdapter,
        order: &OrderRequest,
    ) -> Option<f64> {
        match adapter.fetch_quote(&order.instrument.symbol).await {
            Ok(quote) => quote.last.or(quote.bid).or(quote.ask),
            Err(err) => {
                warn!(
                    request_id = %order.request_id,
                    venue = ?order.instrument.venue,
                    error = %err,
                    "quote lookup failed; using order limit as fallback"
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
