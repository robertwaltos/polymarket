use crate::error::AgentError;
use crate::types::Market;
use reqwest::Client;
use serde_json::Value;

pub struct GammaClient {
    client: Client,
    base_url: String,
}

impl GammaClient {
    pub fn new(client: Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }

    pub async fn fetch_active_markets(&self, limit: usize) -> Result<Vec<Market>, AgentError> {
        let url = format!("{}/markets?active=true&limit={}", self.base_url, limit);
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?;
        let payload: Value = resp.json().await?;

        let markets_value = if payload.is_array() {
            payload
        } else if payload.get("markets").is_some() {
            payload.get("markets").cloned().unwrap_or(Value::Null)
        } else if payload.get("data").is_some() {
            payload.get("data").cloned().unwrap_or(Value::Null)
        } else {
            return Err(AgentError::BadResponse(
                "Unexpected Gamma API response shape".to_string(),
            ));
        };

        let markets: Vec<Market> = serde_json::from_value(markets_value)?;
        Ok(markets)
    }
}
