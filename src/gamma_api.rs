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
        let url = format!(
            "{}/markets?closed=false&order=volume24hr&ascending=false&limit={}",
            self.base_url, limit
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?;
        let payload: Value = resp.json().await?;
        parse_markets(payload, true)
    }

    pub async fn fetch_settled_markets(&self, limit: usize) -> Result<Vec<Market>, AgentError> {
        let url = format!(
            "{}/markets?closed=true&order=updatedAt&ascending=false&limit={}",
            self.base_url, limit
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?;
        let payload: Value = resp.json().await?;
        parse_markets(payload, false)
    }
}

fn parse_markets(payload: Value, require_open: bool) -> Result<Vec<Market>, AgentError> {
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

    let items = markets_value.as_array().ok_or_else(|| {
        AgentError::BadResponse("Gamma markets response is not an array".to_string())
    })?;

    let mut markets = Vec::with_capacity(items.len());
    for item in items {
        if require_open && item["closed"].as_bool().unwrap_or(false) {
            continue;
        }

        if let Some(market) = parse_market(item) {
            markets.push(market);
        }
    }

    Ok(markets)
}

fn parse_market(item: &Value) -> Option<Market> {
    let id = value_to_string(item.get("id")?)?;
    let question = item
        .get("question")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| item.get("title").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_default();
    if question.trim().is_empty() {
        return None;
    }

    let description = item
        .get("description")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let (yes_price, no_price) = extract_prices(item);
    Some(Market {
        id,
        question,
        description,
        yes_price,
        no_price,
    })
}

fn extract_prices(item: &Value) -> (Option<f64>, Option<f64>) {
    let mut yes = item
        .get("yes_price")
        .and_then(value_to_f64)
        .filter(|v| (0.0..=1.0).contains(v));
    let mut no = item
        .get("no_price")
        .and_then(value_to_f64)
        .filter(|v| (0.0..=1.0).contains(v));

    if yes.is_none() || no.is_none() {
        if let Some(prices) = parse_outcome_prices(item.get("outcomePrices")) {
            if yes.is_none() {
                yes = prices.first().copied();
            }
            if no.is_none() {
                no = prices.get(1).copied();
            }
        }
    }

    if yes.is_none() || no.is_none() {
        if let Some(last) = item
            .get("lastTradePrice")
            .and_then(value_to_f64)
            .filter(|v| (0.0..=1.0).contains(v))
        {
            yes.get_or_insert(last);
            no.get_or_insert((1.0 - last).clamp(0.0, 1.0));
        }
    }

    if yes.is_none() || no.is_none() {
        let bid = item
            .get("bestBid")
            .and_then(value_to_f64)
            .filter(|v| (0.0..=1.0).contains(v));
        let ask = item
            .get("bestAsk")
            .and_then(value_to_f64)
            .filter(|v| (0.0..=1.0).contains(v));
        if let (Some(bid), Some(ask)) = (bid, ask) {
            let midpoint = ((bid + ask) / 2.0).clamp(0.0, 1.0);
            yes.get_or_insert(midpoint);
            no.get_or_insert((1.0 - midpoint).clamp(0.0, 1.0));
        }
    }

    (yes, no)
}

fn parse_outcome_prices(raw: Option<&Value>) -> Option<Vec<f64>> {
    let raw = raw?;
    let as_array = if let Some(arr) = raw.as_array() {
        arr.clone()
    } else if let Some(s) = raw.as_str() {
        serde_json::from_str::<Vec<Value>>(s).ok()?
    } else {
        return None;
    };

    let prices = as_array
        .iter()
        .filter_map(value_to_f64)
        .filter(|v| (0.0..=1.0).contains(v))
        .collect::<Vec<_>>();
    if prices.is_empty() {
        None
    } else {
        Some(prices)
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }
    if let Some(n) = value.as_i64() {
        return Some(n.to_string());
    }
    if let Some(n) = value.as_u64() {
        return Some(n.to_string());
    }
    None
}

fn value_to_f64(value: &Value) -> Option<f64> {
    if let Some(n) = value.as_f64() {
        return Some(n);
    }
    if let Some(s) = value.as_str() {
        return s.parse::<f64>().ok();
    }
    None
}
