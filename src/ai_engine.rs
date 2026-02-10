use crate::config::Config;
use crate::error::AgentError;
use crate::types::{AiEstimate, ExternalData, Market, MarketNiche};
use crate::utils::{clamp_probability, extract_json_block};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub struct AiEngine {
    client: Client,
    config: Config,
}

impl AiEngine {
    pub fn new(client: Client, config: Config) -> Self {
        Self { client, config }
    }

    pub async fn infer_probabilities(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: Option<MarketNiche>,
    ) -> Result<AiEstimate, AgentError> {
        let prompt = build_prompt(market, external_data, niche);

        let payload = ClaudeRequest {
            model: self.config.anthropic_model.clone(),
            max_tokens: self.config.anthropic_max_tokens,
            temperature: 0.1,
            messages: vec![ClaudeMessage {
                role: "user".to_string(),
                content: prompt,
            }],
        };

        let resp = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.config.anthropic_key)
            .header("anthropic-version", &self.config.anthropic_version)
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;

        let body: ClaudeResponse = resp.json().await?;
        let text = body
            .content
            .iter()
            .filter(|c| c.kind == "text")
            .map(|c| c.text.clone())
            .collect::<Vec<_>>()
            .join("\n");

        let json_block = extract_json_block(&text).ok_or_else(|| {
            AgentError::Anthropic("Claude output did not include JSON".to_string())
        })?;

        let mut estimate: ClaudeEstimate = serde_json::from_str(&json_block).map_err(|e| {
            AgentError::Anthropic(format!("Claude JSON parse failed: {}", e))
        })?;

        estimate.yes_probability = clamp_probability(estimate.yes_probability);
        estimate.no_probability = clamp_probability(estimate.no_probability);

        let total = estimate.yes_probability + estimate.no_probability;
        if total > 0.0 {
            estimate.yes_probability /= total;
            estimate.no_probability /= total;
        } else {
            estimate.yes_probability = 0.5;
            estimate.no_probability = 0.5;
        }

        let token_cost_usd = estimate_usage_cost(&body.usage, &self.config);

        Ok(AiEstimate {
            yes_probability: estimate.yes_probability,
            no_probability: estimate.no_probability,
            reasoning: estimate.reasoning,
            token_cost_usd,
        })
    }
}

#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    temperature: f32,
    messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Serialize)]
struct ClaudeMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeContent>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

#[derive(Debug, Deserialize)]
struct ClaudeContent {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct ClaudeEstimate {
    yes_probability: f64,
    no_probability: f64,
    reasoning: String,
}

fn build_prompt(market: &Market, external_data: &ExternalData, niche: Option<MarketNiche>) -> String {
    let focus_hint = match niche {
        Some(MarketNiche::Politics) => {
            "Use fivethirtyeight_polls, realclearpolling averages, predictit_markets, manifold_markets, and local_polls for political markets."
        }
        Some(MarketNiche::Weather) => "Use NOAA weather data for forecasts and timing.",
        Some(MarketNiche::Sports) => {
            "Use sports injuries plus sportsbook odds (betfair/sports_odds/scraped_betting) and news."
        }
        Some(MarketNiche::Crypto) => {
            "Use on-chain metrics, CFTC positioning, and sentiment for crypto markets."
        }
        Some(MarketNiche::Betting) => {
            "Use sportsbook odds, betting exchanges, and cross-market prices for arbitrage signals."
        }
        None => "Use the most relevant data available.",
    };

    json!({
        "instruction": "Estimate fair yes/no probabilities for the market. Output ONLY strict JSON: {\"yes_probability\":0.00, \"no_probability\":0.00, \"reasoning\":\"...\"}.",
        "niche": niche.map(|n| n.as_str()),
        "focus_hint": focus_hint,
        "market": {
            "id": market.id,
            "question": market.question,
            "description": market.description,
            "yes_price": market.yes_price,
            "no_price": market.no_price
        },
        "external_data": external_data
    })
    .to_string()
}

fn estimate_usage_cost(usage: &Option<ClaudeUsage>, config: &Config) -> f64 {
    let Some(usage) = usage else {
        return 0.0;
    };

    let input_cost = (usage.input_tokens as f64 / 1_000_000.0)
        * config.claude_cost_per_million_input;
    let output_cost = (usage.output_tokens as f64 / 1_000_000.0)
        * config.claude_cost_per_million_output;
    input_cost + output_cost
}
