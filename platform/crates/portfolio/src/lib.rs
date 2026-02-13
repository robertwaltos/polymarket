use anyhow::Result;
use domain::{FillEvent, PositionSnapshot, Side};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PortfolioState {
    cash_by_currency: IndexMap<String, f64>,
    positions: IndexMap<String, PositionBook>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PositionBook {
    instrument: domain::Instrument,
    quantity: f64,
    average_price: f64,
}

impl PortfolioState {
    pub fn with_starting_cash(currency: impl Into<String>, amount: f64) -> Self {
        let mut state = Self::default();
        state
            .cash_by_currency
            .insert(currency.into().to_uppercase(), amount.max(0.0));
        state
    }

    pub fn apply_fill(&mut self, fill: &FillEvent) -> Result<()> {
        let key = position_key(&fill.instrument);
        let quote_ccy = fill.instrument.quote_currency.to_uppercase();
        let signed_qty = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };
        let cash_delta = match fill.side {
            Side::Buy => -(fill.quantity * fill.price + fill.fee),
            Side::Sell => fill.quantity * fill.price - fill.fee,
        };
        *self.cash_by_currency.entry(quote_ccy).or_insert(0.0) += cash_delta;

        let position = self.positions.entry(key).or_insert_with(|| PositionBook {
            instrument: fill.instrument.clone(),
            quantity: 0.0,
            average_price: 0.0,
        });

        let new_qty = position.quantity + signed_qty;
        if new_qty.abs() < f64::EPSILON {
            position.quantity = 0.0;
            position.average_price = 0.0;
            return Ok(());
        }

        if (position.quantity > 0.0 && signed_qty > 0.0) || (position.quantity < 0.0 && signed_qty < 0.0) {
            let notional_before = position.average_price * position.quantity.abs();
            let notional_add = fill.price * signed_qty.abs();
            position.quantity = new_qty;
            position.average_price = (notional_before + notional_add) / position.quantity.abs();
        } else {
            position.quantity = new_qty;
            if position.quantity.abs() < f64::EPSILON {
                position.average_price = 0.0;
            } else if position.quantity.signum() == signed_qty.signum() {
                position.average_price = fill.price;
            }
        }
        Ok(())
    }

    pub fn cash(&self, currency: &str) -> f64 {
        self.cash_by_currency
            .get(&currency.to_uppercase())
            .copied()
            .unwrap_or(0.0)
    }

    pub fn positions(&self) -> Vec<PositionSnapshot> {
        self.positions
            .values()
            .map(|p| PositionSnapshot {
                instrument: p.instrument.clone(),
                quantity: p.quantity,
                average_price: p.average_price,
                market_price: None,
                unrealized_pnl: None,
            })
            .collect()
    }
}

fn position_key(instrument: &domain::Instrument) -> String {
    format!("{:?}:{}", instrument.venue, instrument.symbol)
}
