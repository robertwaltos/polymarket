use domain::OrderRequest;
use oms::{OmsEngine, OmsError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestInput {
    pub orders: Vec<OrderRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub total_orders: usize,
    pub simulated_orders: usize,
    pub rejected_orders: usize,
}

pub async fn run_backtest(engine: &OmsEngine, input: BacktestInput) -> Result<BacktestReport, OmsError> {
    let total_orders = input.orders.len();
    let mut simulated_orders = 0usize;
    let mut rejected_orders = 0usize;

    for order in input.orders {
        let ack = engine.submit_order(order).await?;
        match ack.status {
            domain::OrderStatus::Simulated => simulated_orders += 1,
            domain::OrderStatus::Rejected => rejected_orders += 1,
            domain::OrderStatus::Accepted => {}
        }
    }

    Ok(BacktestReport {
        total_orders,
        simulated_orders,
        rejected_orders,
    })
}
