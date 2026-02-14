use domain::{ExecutionMode, MarketQuote, OrderRequest, OrderType};
use portfolio::PortfolioState;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_order_notional: f64,
    pub max_position_notional: f64,
    pub max_daily_loss: f64,
    pub allow_live: bool,
    pub dynamic_limits_enabled: bool,
    pub target_volatility_bps: f64,
    pub target_top_of_book_liquidity_usd: f64,
    pub liquidity_take_fraction: f64,
    pub min_order_limit_factor: f64,
    pub min_position_limit_factor: f64,
    pub max_spread_bps: Option<f64>,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_notional: 25_000.0,
            max_position_notional: 100_000.0,
            max_daily_loss: 5_000.0,
            allow_live: false,
            dynamic_limits_enabled: false,
            target_volatility_bps: 80.0,
            target_top_of_book_liquidity_usd: 10_000.0,
            liquidity_take_fraction: 0.20,
            min_order_limit_factor: 0.10,
            min_position_limit_factor: 0.25,
            max_spread_bps: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCheckReport {
    pub mode: ExecutionMode,
    pub reference_price: f64,
    pub order_notional: f64,
    pub current_position_notional: f64,
    pub projected_position_notional: f64,
    pub base_max_order_notional: f64,
    pub base_max_position_notional: f64,
    pub effective_max_order_notional: f64,
    pub effective_max_position_notional: f64,
    pub dynamic_limits_applied: bool,
    pub volatility_bps: Option<f64>,
    pub spread_bps: Option<f64>,
    pub top_of_book_liquidity_usd: Option<f64>,
    pub volatility_scale: f64,
    pub liquidity_scale: f64,
    pub order_limit_scale: f64,
    pub position_limit_scale: f64,
    pub liquidity_cap_notional: Option<f64>,
}

#[derive(Debug, Error, Clone)]
pub enum RiskViolation {
    #[error("live mode disabled by policy")]
    LiveModeDisabled,
    #[error("price is required for notional checks")]
    MissingPrice,
    #[error("spread {actual_bps:.2} bps exceeds max allowed {limit_bps:.2} bps")]
    SpreadTooWide { actual_bps: f64, limit_bps: f64 },
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
        reference_quote: Option<&MarketQuote>,
    ) -> Result<(), RiskViolation> {
        self.evaluate_order(order, portfolio, reference_price, reference_quote)?;
        Ok(())
    }

    pub fn evaluate_order(
        &self,
        order: &OrderRequest,
        portfolio: &PortfolioState,
        reference_price: Option<f64>,
        reference_quote: Option<&MarketQuote>,
    ) -> Result<RiskCheckReport, RiskViolation> {
        if order.mode == ExecutionMode::Live && !self.limits.allow_live {
            return Err(RiskViolation::LiveModeDisabled);
        }

        let price = match order.order_type {
            OrderType::Limit | OrderType::StopLimit => order.limit_price.or(reference_price),
            _ => reference_price.or(order.limit_price),
        }
        .ok_or(RiskViolation::MissingPrice)?;

        let spread_bps = metadata_f64(order, &["spread_bps", "quoted_spread_bps"])
            .or_else(|| quote_spread_bps(reference_quote));
        if let (Some(actual_bps), Some(limit_bps)) = (
            spread_bps,
            self.limits
                .max_spread_bps
                .filter(|value| value.is_finite() && *value > 0.0),
        ) {
            if actual_bps > limit_bps {
                return Err(RiskViolation::SpreadTooWide {
                    actual_bps,
                    limit_bps,
                });
            }
        }

        let volatility_bps = metadata_f64(
            order,
            &["volatility_bps", "signal_volatility_bps", "sigma_bps"],
        )
        .or(spread_bps);
        let top_of_book_liquidity_usd = metadata_f64(
            order,
            &[
                "top_of_book_liquidity_usd",
                "liquidity_usd",
                "depth_usd",
                "book_liquidity_usd",
            ],
        );

        let mut volatility_scale = 1.0;
        let mut liquidity_scale = 1.0;
        let mut order_limit_scale = 1.0;
        let mut position_limit_scale = 1.0;
        let mut liquidity_cap_notional = None;

        if self.limits.dynamic_limits_enabled {
            let min_order_scale = clamp_scale(self.limits.min_order_limit_factor);
            let min_position_scale = clamp_scale(self.limits.min_position_limit_factor);

            if let Some(vol_bps) = volatility_bps.filter(|value| value.is_finite() && *value > 0.0)
            {
                let target = self.limits.target_volatility_bps.max(1.0);
                let raw = (target / vol_bps).min(1.0);
                volatility_scale = raw.clamp(min_order_scale, 1.0);
            }

            if let Some(liquidity_usd) =
                top_of_book_liquidity_usd.filter(|value| value.is_finite() && *value > 0.0)
            {
                let target = self.limits.target_top_of_book_liquidity_usd.max(1.0);
                let raw = (liquidity_usd / target).min(1.0);
                liquidity_scale = raw.clamp(min_order_scale, 1.0);
                let take_fraction = self.limits.liquidity_take_fraction.clamp(0.01, 1.0);
                liquidity_cap_notional = Some((liquidity_usd * take_fraction).max(1.0));
            }

            let combined = volatility_scale * liquidity_scale;
            order_limit_scale = combined.clamp(min_order_scale, 1.0);
            position_limit_scale = combined.clamp(min_position_scale, 1.0);
        }

        let base_order_limit = self.limits.max_order_notional.max(1.0);
        let base_position_limit = self.limits.max_position_notional.max(1.0);

        let mut effective_max_order_notional = (base_order_limit * order_limit_scale).max(1.0);
        if let Some(liquidity_cap) = liquidity_cap_notional {
            effective_max_order_notional = effective_max_order_notional.min(liquidity_cap);
        }
        let effective_max_position_notional = (base_position_limit * position_limit_scale).max(1.0);

        let order_notional = price * order.quantity.abs();
        if order_notional > effective_max_order_notional {
            return Err(RiskViolation::MaxOrderNotionalExceeded {
                actual: order_notional,
                limit: effective_max_order_notional,
            });
        }

        let current_position_notional = portfolio
            .positions()
            .into_iter()
            .filter(|p| {
                p.instrument.symbol == order.instrument.symbol
                    && p.instrument.venue == order.instrument.venue
            })
            .map(|p| p.quantity.abs() * p.average_price.abs())
            .sum::<f64>();

        let projected = current_position_notional + order_notional;
        if projected > effective_max_position_notional {
            return Err(RiskViolation::MaxPositionNotionalExceeded {
                actual: projected,
                limit: effective_max_position_notional,
            });
        }

        Ok(RiskCheckReport {
            mode: order.mode,
            reference_price: price,
            order_notional,
            current_position_notional,
            projected_position_notional: projected,
            base_max_order_notional: base_order_limit,
            base_max_position_notional: base_position_limit,
            effective_max_order_notional,
            effective_max_position_notional,
            dynamic_limits_applied: self.limits.dynamic_limits_enabled,
            volatility_bps,
            spread_bps,
            top_of_book_liquidity_usd,
            volatility_scale,
            liquidity_scale,
            order_limit_scale,
            position_limit_scale,
            liquidity_cap_notional,
        })
    }
}

fn clamp_scale(value: f64) -> f64 {
    if !value.is_finite() {
        return 0.1;
    }
    value.clamp(0.01, 1.0)
}

fn metadata_f64(order: &OrderRequest, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let Some(raw) = order.metadata.get(*key) else {
            continue;
        };
        let Ok(parsed) = raw.trim().parse::<f64>() else {
            continue;
        };
        if parsed.is_finite() && parsed > 0.0 {
            return Some(parsed);
        }
    }
    None
}

fn quote_spread_bps(quote: Option<&MarketQuote>) -> Option<f64> {
    let quote = quote?;
    let bid = quote.bid?;
    let ask = quote.ask?;
    if !(bid.is_finite() && ask.is_finite()) || bid <= 0.0 || ask <= 0.0 || ask < bid {
        return None;
    }
    let mid = (bid + ask) / 2.0;
    if mid <= 0.0 {
        return None;
    }
    Some(((ask - bid) / mid) * 10_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use domain::{AssetClass, Instrument, Side, TimeInForce, Venue};

    fn sample_order(quantity: f64, limit_price: Option<f64>) -> OrderRequest {
        OrderRequest::new(
            uuid::Uuid::new_v4(),
            ExecutionMode::Shadow,
            Instrument {
                venue: Venue::Coinbase,
                symbol: "BTC-USD".to_string(),
                asset_class: AssetClass::Crypto,
                quote_currency: "USD".to_string(),
            },
            Side::Buy,
            OrderType::Limit,
            TimeInForce::Day,
            quantity,
            limit_price,
        )
    }

    fn sample_quote(bid: f64, ask: f64, last: f64) -> MarketQuote {
        MarketQuote {
            instrument: Instrument {
                venue: Venue::Coinbase,
                symbol: "BTC-USD".to_string(),
                asset_class: AssetClass::Crypto,
                quote_currency: "USD".to_string(),
            },
            bid: Some(bid),
            ask: Some(ask),
            last: Some(last),
            timestamp: Utc::now(),
            source: "test".to_string(),
        }
    }

    #[test]
    fn dynamic_limit_scales_down_on_high_volatility() {
        let risk = RiskEngine::new(RiskLimits {
            max_order_notional: 10_000.0,
            dynamic_limits_enabled: true,
            target_volatility_bps: 50.0,
            min_order_limit_factor: 0.10,
            ..RiskLimits::default()
        });
        let mut order = sample_order(1.0, Some(2_000.0));
        order
            .metadata
            .insert("volatility_bps".to_string(), "500".to_string());

        let err = risk
            .evaluate_order(
                &order,
                &PortfolioState::with_starting_cash("USD", 100_000.0),
                Some(2_000.0),
                None,
            )
            .expect_err("order should fail after dynamic volatility scaling");

        match err {
            RiskViolation::MaxOrderNotionalExceeded { limit, .. } => {
                assert_eq!(limit, 1_000.0);
            }
            other => panic!("unexpected violation: {other:?}"),
        }
    }

    #[test]
    fn dynamic_liquidity_cap_enforced() {
        let risk = RiskEngine::new(RiskLimits {
            max_order_notional: 10_000.0,
            dynamic_limits_enabled: true,
            target_top_of_book_liquidity_usd: 10_000.0,
            liquidity_take_fraction: 0.10,
            min_order_limit_factor: 0.05,
            ..RiskLimits::default()
        });
        let mut order = sample_order(1.0, Some(600.0));
        order
            .metadata
            .insert("top_of_book_liquidity_usd".to_string(), "5000".to_string());

        let err = risk
            .evaluate_order(
                &order,
                &PortfolioState::with_starting_cash("USD", 100_000.0),
                Some(600.0),
                None,
            )
            .expect_err("order should fail liquidity cap");

        match err {
            RiskViolation::MaxOrderNotionalExceeded { limit, .. } => {
                assert_eq!(limit, 500.0);
            }
            other => panic!("unexpected violation: {other:?}"),
        }
    }

    #[test]
    fn spread_guard_blocks_wide_market() {
        let risk = RiskEngine::new(RiskLimits {
            max_spread_bps: Some(100.0),
            ..RiskLimits::default()
        });
        let order = sample_order(1.0, Some(100.0));
        let quote = sample_quote(99.0, 101.0, 100.0);

        let err = risk
            .evaluate_order(
                &order,
                &PortfolioState::with_starting_cash("USD", 100_000.0),
                Some(100.0),
                Some(&quote),
            )
            .expect_err("spread guard should reject wide spread");

        assert!(matches!(err, RiskViolation::SpreadTooWide { .. }));
    }
}
