use domain::{ExecutionMode, OrderRequest, OrderType};
use portfolio::PortfolioState;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_order_notional: f64,
    pub max_position_notional: f64,
    pub max_daily_loss: f64,
    pub allow_live: bool,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_notional: 25_000.0,
            max_position_notional: 100_000.0,
            max_daily_loss: 5_000.0,
            allow_live: false,
        }
    }
}

#[derive(Debug, Error, Clone)]
pub enum RiskViolation {
    #[error("live mode disabled by policy")]
    LiveModeDisabled,
    #[error("price is required for notional checks")]
    MissingPrice,
    #[error("order notional {actual:.2} exceeds limit {limit:.2}")]
    MaxOrderNotionalExceeded { actual: f64, limit: f64 },
    #[error("position notional {actual:.2} exceeds limit {limit:.2}")]
    MaxPositionNotionalExceeded { actual: f64, limit: f64 },
}

#[derive(Debug, Clone)]
pub struct RiskEngine {
    limits: RiskLimits,
}

impl RiskEngine {
    pub fn new(limits: RiskLimits) -> Self {
        Self { limits }
    }

    pub fn limits(&self) -> &RiskLimits {
        &self.limits
    }

    pub fn validate_order(
        &self,
        order: &OrderRequest,
        portfolio: &PortfolioState,
        reference_price: Option<f64>,
    ) -> Result<(), RiskViolation> {
        if order.mode == ExecutionMode::Live && !self.limits.allow_live {
            return Err(RiskViolation::LiveModeDisabled);
        }

        let price = match order.order_type {
            OrderType::Limit | OrderType::StopLimit => order.limit_price.or(reference_price),
            _ => reference_price.or(order.limit_price),
        }
        .ok_or(RiskViolation::MissingPrice)?;

        let order_notional = price * order.quantity.abs();
        if order_notional > self.limits.max_order_notional {
            return Err(RiskViolation::MaxOrderNotionalExceeded {
                actual: order_notional,
                limit: self.limits.max_order_notional,
            });
        }

        let current_position_notional = portfolio
            .positions()
            .into_iter()
            .filter(|p| p.instrument.symbol == order.instrument.symbol && p.instrument.venue == order.instrument.venue)
            .map(|p| p.quantity.abs() * p.average_price.abs())
            .sum::<f64>();

        let projected = current_position_notional + order_notional;
        if projected > self.limits.max_position_notional {
            return Err(RiskViolation::MaxPositionNotionalExceeded {
                actual: projected,
                limit: self.limits.max_position_notional,
            });
        }

        Ok(())
    }
}
