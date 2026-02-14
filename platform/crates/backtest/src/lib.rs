use domain::{OrderRequest, Side};
use oms::{OmsEngine, OmsError};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestInput {
    #[serde(default)]
    pub config: BacktestExecutionConfig,
    pub orders: Vec<OrderRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestExecutionConfig {
    pub seed: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub base_slippage_bps: f64,
    pub slippage_bps_per_10k_notional: f64,
    pub slippage_jitter_bps: f64,
    pub rejection_probability: f64,
    pub partial_fill_probability: f64,
    pub min_partial_fill_fraction: f64,
    pub max_partial_fill_fraction: f64,
    pub simulate_wall_clock: bool,
}

impl Default for BacktestExecutionConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            min_latency_ms: 10,
            max_latency_ms: 120,
            base_slippage_bps: 4.0,
            slippage_bps_per_10k_notional: 1.5,
            slippage_jitter_bps: 1.0,
            rejection_probability: 0.01,
            partial_fill_probability: 0.30,
            min_partial_fill_fraction: 0.25,
            max_partial_fill_fraction: 0.90,
            simulate_wall_clock: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub total_orders: usize,
    pub submitted_orders: usize,
    pub simulated_orders: usize,
    pub rejected_orders: usize,
    pub partial_fill_orders: usize,
    pub average_latency_ms: f64,
    pub average_slippage_bps: f64,
    pub total_slippage_notional: f64,
    pub executed_notional: f64,
    pub ending_cash_usd: f64,
    pub ending_position_count: usize,
}

pub async fn run_backtest(engine: &OmsEngine, input: BacktestInput) -> Result<BacktestReport, OmsError> {
    let config = input.config;
    let total_orders = input.orders.len();
    let mut submitted_orders = 0usize;
    let mut simulated_orders = 0usize;
    let mut rejected_orders = 0usize;
    let mut partial_fill_orders = 0usize;
    let mut latency_total_ms = 0u64;
    let mut slippage_total_bps = 0.0f64;
    let mut slippage_samples = 0usize;
    let mut total_slippage_notional = 0.0f64;
    let mut executed_notional = 0.0f64;
    let mut rng = StdRng::seed_from_u64(config.seed);

    for mut order in input.orders {
        if rng.gen::<f64>() < config.rejection_probability.clamp(0.0, 1.0) {
            rejected_orders += 1;
            continue;
        }

        let min_latency = config.min_latency_ms.min(config.max_latency_ms);
        let max_latency = config.max_latency_ms.max(min_latency);
        let latency_ms = if min_latency == max_latency {
            min_latency
        } else {
            rng.gen_range(min_latency..=max_latency)
        };
        latency_total_ms = latency_total_ms.saturating_add(latency_ms);
        if config.simulate_wall_clock {
            sleep(Duration::from_millis(latency_ms)).await;
        }

        let original_qty = order.quantity.abs();
        let mut fill_fraction = 1.0;
        if rng.gen::<f64>() < config.partial_fill_probability.clamp(0.0, 1.0) {
            let low = config.min_partial_fill_fraction.clamp(0.01, 1.0);
            let high = config.max_partial_fill_fraction.clamp(low, 1.0);
            fill_fraction = rng.gen_range(low..=high);
            partial_fill_orders += 1;
        }
        let executed_qty = (original_qty * fill_fraction).max(0.0);
        if executed_qty <= f64::EPSILON {
            rejected_orders += 1;
            continue;
        }
        order.quantity = executed_qty;

        let mut sampled_slippage_bps = None;
        if let Some(price) = order.limit_price {
            let notional = price.abs() * executed_qty;
            let dynamic_bps = (notional / 10_000.0) * config.slippage_bps_per_10k_notional.max(0.0);
            let jitter = rng.gen_range(-config.slippage_jitter_bps..=config.slippage_jitter_bps);
            let slippage_bps = (config.base_slippage_bps.max(0.0) + dynamic_bps + jitter).max(0.0);
            let slippage_factor = slippage_bps / 10_000.0;
            let adjusted_price = match order.side {
                Side::Buy => price * (1.0 + slippage_factor),
                Side::Sell => price * (1.0 - slippage_factor),
            }
            .max(0.000_001);
            order.limit_price = Some(adjusted_price);
            sampled_slippage_bps = Some(slippage_bps);
            slippage_total_bps += slippage_bps;
            slippage_samples += 1;
            total_slippage_notional += (adjusted_price - price).abs() * executed_qty;
            executed_notional += adjusted_price * executed_qty;
        }
        if let Some(slip) = sampled_slippage_bps {
            order
                .metadata
                .insert("backtest_slippage_bps".to_string(), format!("{slip:.4}"));
        }
        order
            .metadata
            .insert("backtest_latency_ms".to_string(), latency_ms.to_string());
        order.metadata.insert(
            "backtest_fill_fraction".to_string(),
            format!("{fill_fraction:.4}"),
        );

        let ack = engine.submit_order(order).await?;
        submitted_orders += 1;
        match ack.status {
            domain::OrderStatus::Simulated => simulated_orders += 1,
            domain::OrderStatus::Rejected => rejected_orders += 1,
            domain::OrderStatus::Accepted => {}
        }
    }

    let average_latency_ms = if submitted_orders == 0 {
        0.0
    } else {
        latency_total_ms as f64 / submitted_orders as f64
    };
    let average_slippage_bps = if slippage_samples == 0 {
        0.0
    } else {
        slippage_total_bps / slippage_samples as f64
    };
    let snapshot = engine.portfolio_snapshot()?;
    let ending_cash_usd = snapshot.cash("USD");
    let ending_position_count = snapshot.positions().len();

    Ok(BacktestReport {
        total_orders,
        submitted_orders,
        simulated_orders,
        rejected_orders,
        partial_fill_orders,
        average_latency_ms,
        average_slippage_bps,
        total_slippage_notional,
        executed_notional,
        ending_cash_usd,
        ending_position_count,
    })
}
