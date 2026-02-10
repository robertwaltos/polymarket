use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Market {
    pub id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub yes_price: Option<f64>,
    #[serde(default)]
    pub no_price: Option<f64>,
}

impl Market {
    pub fn market_text(&self) -> String {
        match &self.description {
            Some(desc) if !desc.is_empty() => format!("{}\n{}", self.question, desc),
            _ => self.question.clone(),
        }
    }

    pub fn yes_price_or(&self, fallback: f64) -> f64 {
        self.yes_price.unwrap_or(fallback)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketNiche {
    Weather,
    Sports,
    Crypto,
    Politics,
    Betting,
}

impl MarketNiche {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketNiche::Weather => "weather",
            MarketNiche::Sports => "sports",
            MarketNiche::Crypto => "crypto",
            MarketNiche::Politics => "politics",
            MarketNiche::Betting => "betting",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiEstimate {
    pub yes_probability: f64,
    pub no_probability: f64,
    pub reasoning: String,
    pub token_cost_usd: f64,
}

pub type ExternalData = serde_json::Value;
