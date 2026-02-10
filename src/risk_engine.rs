use serde::{Deserialize, Serialize};

/// Constants for risk management
const MAX_POSITION_PERCENT: f64 = 0.06; // Hard cap at 6% of bankroll
const DEFAULT_MIN_EDGE_THRESHOLD: f64 = 0.08; // Default edge threshold

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignal {
    pub market_id: String,
    pub side: TradeSide,
    pub size_usdc: f64,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TradeSide {
    BuyYes,
    BuyNo,
    None,
}

/// Inputs required to calculate risk
pub struct RiskParams {
    pub bankroll: f64,      // Current wallet balance (e.g., $50.00)
    pub market_price: f64,  // Current price on Polymarket (0.0 to 1.0)
    pub ai_fair_value: f64, // Claude's estimated probability (0.0 to 1.0)
    pub market_id: String,
    pub edge_threshold: f64, // Minimum edge required to trade
}

/// Main entry point for the risk engine
pub fn analyze_opportunity(params: RiskParams) -> TradeSignal {
    // 1. Identify the Direction
    // If AI says 60%, Market says 40% -> Buy YES
    // If AI says 20%, Market says 40% -> Buy NO (equivalent to shorting Yes)

    let (side, edge) = if params.ai_fair_value > params.market_price {
        (TradeSide::BuyYes, params.ai_fair_value - params.market_price)
    } else {
        // "Buying No" is effectively buying the inverse outcome.
        // If Market Yes is 0.40, Market No is 0.60.
        // If AI Yes is 0.20, AI No is 0.80.
        // Edge = AI No (0.80) - Market No (0.60) = 0.20
        (TradeSide::BuyNo, params.market_price - params.ai_fair_value)
    };

    let edge_threshold = if params.edge_threshold > 0.0 {
        params.edge_threshold
    } else {
        DEFAULT_MIN_EDGE_THRESHOLD
    };

    // 2. Filter by Edge Threshold
    if edge.abs() < edge_threshold {
        return TradeSignal {
            market_id: params.market_id,
            side: TradeSide::None,
            size_usdc: 0.0,
            reasoning: format!(
                "Edge {:.2}% is below threshold {:.2}%",
                edge * 100.0,
                edge_threshold * 100.0
            ),
        };
    }

    // 3. Calculate Position Size (Kelly Criterion)
    // We need to normalize variables for the side we are trading.
    let (p, price) = match side {
        TradeSide::BuyYes => (params.ai_fair_value, params.market_price),
        TradeSide::BuyNo => (1.0 - params.ai_fair_value, 1.0 - params.market_price),
        TradeSide::None => (0.0, 0.0), // Should be unreachable
    };

    // Kelly Formula: f* = (bp - q) / b
    // b = Net Odds = (1 - price) / price
    // p = Probability of winning (AI estimate)
    // q = Probability of losing (1 - p)

    if price <= 0.0 || price >= 1.0 {
        // Avoid divide by zero on dead markets
        return TradeSignal {
            market_id: params.market_id,
            side: TradeSide::None,
            size_usdc: 0.0,
            reasoning: "Market price invalid".to_string(),
        };
    }

    let b = (1.0 - price) / price;
    let q = 1.0 - p;
    let raw_kelly_fraction = (b * p - q) / b;

    // 4. Sizing and Capping
    // If Kelly is negative (negative EV), don't trade.
    let kelly_size = if raw_kelly_fraction > 0.0 {
        raw_kelly_fraction
    } else {
        0.0
    };

    // Apply the 6% hard cap
    let final_fraction = kelly_size.min(MAX_POSITION_PERCENT);
    let position_size = params.bankroll * final_fraction;

    // Round to 2 decimal places (USDC standard)
    let position_size = (position_size * 100.0).floor() / 100.0;

    // Final Sanity Check for Minimum Bet Size (Polymarket dust limit is usually ~$1, we'll check > 0.01)
    if position_size < 1.0 {
        return TradeSignal {
            market_id: params.market_id,
            side: TradeSide::None,
            size_usdc: 0.0,
            reasoning: format!("Calculated size ${:.2} is below min bet ($1.00)", position_size),
        };
    }

    TradeSignal {
        market_id: params.market_id,
        side,
        size_usdc: position_size,
        reasoning: format!(
            "Edge detected: {:.1}%. Kelly: {:.1}%. Capped at {:.1}%.",
            edge * 100.0,
            raw_kelly_fraction * 100.0,
            final_fraction * 100.0
        ),
    }
}

// ==========================================
// UNIT TESTS (Run with `cargo test`)
// ==========================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buy_yes_opportunity() {
        let signal = analyze_opportunity(RiskParams {
            bankroll: 50.0,
            market_price: 0.30, // 30 cents
            ai_fair_value: 0.60, // AI thinks 60% chance
            market_id: "test_market_1".to_string(),
            edge_threshold: DEFAULT_MIN_EDGE_THRESHOLD,
        });

        // Edge is 30% (> 8%).
        // b (odds) = 0.70 / 0.30 = 2.333
        // f* = (2.33 * 0.60 - 0.40) / 2.33 = ~0.428 (42% of bankroll)
        // Cap is 6%.
        // Expect size: $50 * 0.06 = $3.00.

        assert_eq!(signal.side, TradeSide::BuyYes);
        assert_eq!(signal.size_usdc, 3.00);
    }

    #[test]
    fn test_buy_no_opportunity() {
        let signal = analyze_opportunity(RiskParams {
            bankroll: 50.0,
            market_price: 0.80, // Market thinks 80% Yes
            ai_fair_value: 0.60, // AI thinks 60% Yes
            market_id: "test_market_2".to_string(),
            edge_threshold: DEFAULT_MIN_EDGE_THRESHOLD,
        });

        // Buying NO.
        // Market No Price = 0.20. AI No Prob = 0.40.
        // Edge = 0.20 (> 0.08).
        // b = 0.80 / 0.20 = 4.0.
        // p = 0.40. q = 0.60.
        // f* = (4 * 0.4 - 0.6) / 4 = (1.6 - 0.6) / 4 = 0.25 (25%)
        // Cap is 6%.
        // Expect size: $50 * 0.06 = $3.00.

        assert_eq!(signal.side, TradeSide::BuyNo);
        assert_eq!(signal.size_usdc, 3.00);
    }

    #[test]
    fn test_ignore_small_edge() {
        let signal = analyze_opportunity(RiskParams {
            bankroll: 50.0,
            market_price: 0.50,
            ai_fair_value: 0.55, // 5% edge
            market_id: "test_market_3".to_string(),
            edge_threshold: DEFAULT_MIN_EDGE_THRESHOLD,
        });

        assert_eq!(signal.side, TradeSide::None);
        assert_eq!(signal.size_usdc, 0.0);
    }
}
