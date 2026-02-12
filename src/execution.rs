use crate::clob_client::ClobClient;
use crate::error::AgentError;
use crate::kalshi_client::KalshiClient;
use crate::risk_engine::TradeSignal;
use crate::strategy::KalshiMatch;
use crate::types::Market;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionVenue {
    Polymarket,
    Kalshi,
}

impl ExecutionVenue {
    pub fn from_env(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "kalshi" => ExecutionVenue::Kalshi,
            _ => ExecutionVenue::Polymarket,
        }
    }
}

pub enum ExecutionClient {
    Polymarket(ClobClient),
    Kalshi(KalshiClient),
}

impl ExecutionClient {
    pub async fn get_balance_usd(&self) -> Result<f64, AgentError> {
        match self {
            ExecutionClient::Polymarket(client) => client.get_usdc_balance().await,
            ExecutionClient::Kalshi(client) => client.get_cash_balance().await,
        }
    }

    pub async fn place_limit_order(
        &self,
        market: &Market,
        signal: &TradeSignal,
        price: f64,
        kalshi_match: Option<&KalshiMatch>,
    ) -> Result<(), AgentError> {
        match self {
            ExecutionClient::Polymarket(client) => {
                client.place_limit_order(market, signal, price).await
            }
            ExecutionClient::Kalshi(client) => {
                let Some(kalshi) = kalshi_match else {
                    return Err(AgentError::Kalshi(
                        "No Kalshi ticker match for market".to_string(),
                    ));
                };
                client
                    .place_limit_order(&kalshi.ticker, signal, price)
                    .await
            }
        }
    }
}
