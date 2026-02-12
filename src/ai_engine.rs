use crate::config::Config;
use crate::error::AgentError;
use crate::types::{AiEstimate, ExternalData, Market, MarketNiche};
use crate::utils::{clamp_probability, extract_json_block, truncate_string};
use log::warn;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;

const STRICT_JSON_SYSTEM_PROMPT: &str = "You are a probabilistic forecasting engine for trading. Return ONLY one valid JSON object with keys: yes_probability (number 0..1), no_probability (number 0..1), reasoning (string). No markdown, no prose, no code fences, no extra keys.";

pub struct AiEngine {
    client: Client,
    config: Config,
    resolved_model: RwLock<Option<String>>,
}

impl AiEngine {
    pub fn new(client: Client, config: Config) -> Self {
        Self {
            client,
            config,
            resolved_model: RwLock::new(None),
        }
    }

    pub async fn infer_probabilities(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: Option<MarketNiche>,
    ) -> Result<AiEstimate, AgentError> {
        let prompt = self.build_prompt(market, external_data, niche);
        let model = self.resolve_model().await?;
        let body = match self.request_messages(&model, &prompt).await {
            Ok(body) => body,
            Err(err) if is_model_not_found_error(&err) => {
                let fallback = self.refresh_model(Some(&model)).await?;
                self.request_messages(&fallback, &prompt).await?
            }
            Err(err) => return Err(err),
        };
        let text = body
            .content
            .iter()
            .filter(|c| c.kind == "text")
            .map(|c| c.text.clone())
            .collect::<Vec<_>>()
            .join("\n");

        let mut estimate = parse_estimate_from_text(&text).ok_or_else(|| {
            AgentError::Anthropic("Claude output did not include JSON".to_string())
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
        let input_tokens = body.usage.as_ref().map(|u| u.input_tokens).unwrap_or(0);
        let output_tokens = body.usage.as_ref().map(|u| u.output_tokens).unwrap_or(0);

        Ok(AiEstimate {
            yes_probability: estimate.yes_probability,
            no_probability: estimate.no_probability,
            reasoning: estimate.reasoning,
            token_cost_usd,
            input_tokens,
            output_tokens,
        })
    }

    async fn resolve_model(&self) -> Result<String, AgentError> {
        if let Some(model) = self.resolved_model.read().await.clone() {
            return Ok(model);
        }
        self.refresh_model(None).await
    }

    async fn refresh_model(&self, attempted: Option<&str>) -> Result<String, AgentError> {
        let configured = self.config.anthropic_model.trim();
        let models = self.fetch_models().await.unwrap_or_else(|err| {
            warn!(
                "Failed to list Anthropic models ({}). Using configured model '{}'.",
                err, configured
            );
            Vec::new()
        });

        let selected = select_model(configured, &models);
        if selected != configured {
            if let Some(old) = attempted {
                warn!(
                    "Anthropic model '{}' not available, switching to '{}'.",
                    old, selected
                );
            } else {
                warn!(
                    "Configured ANTHROPIC_MODEL '{}' not found; using '{}'.",
                    configured, selected
                );
            }
        }

        *self.resolved_model.write().await = Some(selected.clone());
        Ok(selected)
    }

    async fn fetch_models(&self) -> Result<Vec<String>, AgentError> {
        let base = self.config.anthropic_base_url.trim_end_matches('/');
        let resp = self
            .client
            .get(format!("{}/v1/models", base))
            .header("x-api-key", &self.config.anthropic_key)
            .header("anthropic-version", &self.config.anthropic_version)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentError::Anthropic(format!(
                "models {}: {}",
                status.as_u16(),
                compact_error_message(&body)
            )));
        }

        let model_list: ClaudeModelList = resp.json().await?;
        Ok(model_list
            .data
            .into_iter()
            .map(|m| m.id)
            .filter(|id| !id.trim().is_empty())
            .collect())
    }

    async fn request_messages(
        &self,
        model: &str,
        prompt: &str,
    ) -> Result<ClaudeResponse, AgentError> {
        let payload = ClaudeRequest {
            model: model.to_string(),
            system: STRICT_JSON_SYSTEM_PROMPT.to_string(),
            max_tokens: self.config.anthropic_max_tokens,
            temperature: 0.0,
            messages: vec![ClaudeMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
        };

        let base = self.config.anthropic_base_url.trim_end_matches('/');
        let resp = self
            .client
            .post(format!("{}/v1/messages", base))
            .header("x-api-key", &self.config.anthropic_key)
            .header("anthropic-version", &self.config.anthropic_version)
            .json(&payload)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let retry_after = parse_retry_after_secs(resp.headers().get("retry-after"));
            let body = resp.text().await.unwrap_or_default();
            let mut message = format!(
                "messages {}: {}",
                status.as_u16(),
                compact_error_message(&body)
            );
            if let Some(seconds) = retry_after {
                message.push_str(&format!(" (retry_after={}s)", seconds));
            }
            return Err(AgentError::Anthropic(message));
        }

        Ok(resp.json::<ClaudeResponse>().await?)
    }

    fn build_prompt(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: Option<MarketNiche>,
    ) -> String {
        let limit = self.config.anthropic_prompt_max_chars.max(1200);
        let focused_data = focus_external_data_for_prompt(external_data, niche, limit);
        let prompt = build_prompt_base(market, &focused_data, niche);

        if prompt.len() <= limit {
            return prompt;
        }

        let serialized = serde_json::to_string(&focused_data).unwrap_or_default();
        let compact = json!({
            "summary": truncate_string(&serialized, limit / 3),
            "hint": "Use summary only. Keep output strict JSON."
        });
        let fallback = build_prompt_base(market, &compact, niche);

        if fallback.len() <= limit {
            fallback
        } else {
            truncate_string(&fallback, limit)
        }
    }

    pub fn estimate_input_tokens(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: Option<MarketNiche>,
    ) -> u64 {
        let prompt = self.build_prompt(market, external_data, niche);
        // Practical approximation used for local throttling.
        ((prompt.chars().count() as f64) / 4.0).ceil() as u64
    }
}

#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    system: String,
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

#[derive(Debug, Deserialize)]
struct ClaudeModelList {
    #[serde(default)]
    data: Vec<ClaudeModelInfo>,
}

#[derive(Debug, Deserialize)]
struct ClaudeModelInfo {
    id: String,
}

fn build_prompt_base(
    market: &Market,
    external_data: &ExternalData,
    niche: Option<MarketNiche>,
) -> String {
    let focus_hint = match niche {
        Some(MarketNiche::Politics) => {
            "Use market_context (matched PredictIt/Kalshi), fivethirtyeight_polls, realclearpolling averages, predictit_markets, kalshi_markets, manifold_markets, and local_polls for political markets."
        }
        Some(MarketNiche::Weather) => "Use NOAA weather data for forecasts and timing.",
        Some(MarketNiche::Sports) => {
            "Use sports injuries plus sportsbook odds (betfair/sports_odds/scraped_betting) and news."
        }
        Some(MarketNiche::Crypto) => {
            "Use on-chain metrics, CFTC positioning, and sentiment for crypto markets."
        }
        Some(MarketNiche::Betting) => {
            "Use market_context (matched PredictIt/Kalshi), sportsbook odds, betting exchanges, Kalshi/Manifold/Augur prices, and cross-market prices for arbitrage signals."
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

fn select_model(configured: &str, available: &[String]) -> String {
    let configured = configured.trim();
    if configured.is_empty() {
        return fallback_model(available).unwrap_or_else(|| "claude-sonnet-4-20250514".to_string());
    }

    if available.is_empty() {
        return configured.to_string();
    }

    if available.iter().any(|m| m == configured) {
        return configured.to_string();
    }

    let family_hint = model_family_hint(configured);
    if let Some(family) = family_hint {
        if let Some(candidate) = newest_model_for_family(available, family) {
            return candidate;
        }
    }

    fallback_model(available).unwrap_or_else(|| configured.to_string())
}

fn model_family_hint(model: &str) -> Option<&'static str> {
    let normalized = model.to_ascii_lowercase();
    if normalized.contains("sonnet") {
        Some("sonnet")
    } else if normalized.contains("haiku") {
        Some("haiku")
    } else if normalized.contains("opus") {
        Some("opus")
    } else {
        None
    }
}

fn newest_model_for_family(models: &[String], family: &str) -> Option<String> {
    models
        .iter()
        .filter(|m| m.to_ascii_lowercase().contains(family))
        .max()
        .cloned()
}

fn fallback_model(models: &[String]) -> Option<String> {
    newest_model_for_family(models, "sonnet")
        .or_else(|| newest_model_for_family(models, "haiku"))
        .or_else(|| newest_model_for_family(models, "opus"))
        .or_else(|| models.first().cloned())
}

fn compact_error_message(body: &str) -> String {
    let parsed = serde_json::from_str::<Value>(body).ok();
    if let Some(val) = parsed {
        if let Some(message) = val
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
        {
            let err_type = val
                .get("error")
                .and_then(|e| e.get("type"))
                .and_then(|t| t.as_str())
                .unwrap_or("unknown_error");
            return format!("{} ({})", message, err_type);
        }
    }

    body.chars().take(180).collect()
}

fn is_model_not_found_error(err: &AgentError) -> bool {
    match err {
        AgentError::Anthropic(message) => {
            message.contains("not_found_error") && message.contains("model:")
        }
        _ => false,
    }
}

fn parse_retry_after_secs(header: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    let raw = header?.to_str().ok()?;
    raw.trim().parse::<u64>().ok()
}

fn parse_estimate_from_text(text: &str) -> Option<ClaudeEstimate> {
    if let Some(json_block) = extract_json_block(text) {
        if let Ok(estimate) = serde_json::from_str::<ClaudeEstimate>(&json_block) {
            return Some(estimate);
        }
    }

    let lower = text.to_ascii_lowercase();
    let yes = extract_probability_after_keywords(
        &lower,
        &["yes_probability", "yes probability", "\"yes\"", "yes:"],
    );
    let no = extract_probability_after_keywords(
        &lower,
        &["no_probability", "no probability", "\"no\"", "no:"],
    );

    match (yes, no) {
        (Some(yes), Some(no)) => Some(ClaudeEstimate {
            yes_probability: yes,
            no_probability: no,
            reasoning: truncate_string(text.trim(), 500),
        }),
        (Some(yes), None) => Some(ClaudeEstimate {
            yes_probability: yes,
            no_probability: (1.0 - yes).clamp(0.0, 1.0),
            reasoning: truncate_string(text.trim(), 500),
        }),
        (None, Some(no)) => Some(ClaudeEstimate {
            yes_probability: (1.0 - no).clamp(0.0, 1.0),
            no_probability: no,
            reasoning: truncate_string(text.trim(), 500),
        }),
        _ => None,
    }
}

fn extract_probability_after_keywords(text: &str, keywords: &[&str]) -> Option<f64> {
    for keyword in keywords {
        let mut search_from = 0usize;
        while let Some(found) = text[search_from..].find(keyword) {
            let idx = search_from + found + keyword.len();
            let tail = &text[idx..];
            if let Some(prob) = extract_first_probability(tail) {
                return Some(prob);
            }
            search_from = idx;
            if search_from >= text.len() {
                break;
            }
        }
    }
    None
}

fn extract_first_probability(input: &str) -> Option<f64> {
    let mut token = String::new();
    let mut seen_digit = false;

    for ch in input.chars() {
        if ch.is_ascii_whitespace() || ch == ':' || ch == '=' || ch == ',' || ch == '"' {
            if token.is_empty() {
                continue;
            }
            break;
        }

        if ch.is_ascii_digit() || ch == '.' {
            token.push(ch);
            seen_digit = true;
            continue;
        }

        if ch == '%' && seen_digit {
            let value = token.parse::<f64>().ok()?;
            return Some((value / 100.0).clamp(0.0, 1.0));
        }

        if token.is_empty() {
            continue;
        }
        break;
    }

    if !seen_digit {
        return None;
    }

    let value = token.parse::<f64>().ok()?;
    if (0.0..=1.0).contains(&value) {
        Some(value)
    } else if (1.0..=100.0).contains(&value) {
        Some((value / 100.0).clamp(0.0, 1.0))
    } else {
        None
    }
}

fn focus_external_data_for_prompt(
    external_data: &ExternalData,
    niche: Option<MarketNiche>,
    total_limit: usize,
) -> Value {
    let Some(map) = external_data.as_object() else {
        let serialized = serde_json::to_string(external_data).unwrap_or_default();
        return Value::String(truncate_string(&serialized, total_limit / 2));
    };

    let mut keys = vec!["market_context"];
    match niche {
        Some(MarketNiche::Politics) => keys.extend([
            "fivethirtyeight_polls",
            "realclearpolling",
            "predictit_markets",
            "kalshi_markets",
            "manifold_markets",
            "local_polls",
            "news_headlines",
        ]),
        Some(MarketNiche::Sports) => keys.extend([
            "sports_odds",
            "rotowire_injuries",
            "espn_injuries",
            "betfair_odds",
            "oddsportal",
            "betexplorer",
            "news_headlines",
        ]),
        Some(MarketNiche::Crypto) => keys.extend([
            "dune_crypto",
            "moralis_onchain",
            "cftc_reports",
            "x_sentiment",
            "news_headlines",
            "kalshi_markets",
            "manifold_markets",
        ]),
        Some(MarketNiche::Weather) => keys.extend([
            "noaa_weather",
            "noaa_station_observations",
            "news_headlines",
            "kalshi_markets",
            "manifold_markets",
        ]),
        Some(MarketNiche::Betting) => keys.extend([
            "sports_odds",
            "betfair_odds",
            "smarkets_markets",
            "oddsportal",
            "betexplorer",
            "kalshi_markets",
            "manifold_markets",
        ]),
        None => keys.extend(["news_headlines", "kalshi_markets", "manifold_markets"]),
    }

    let mut seen = std::collections::HashSet::new();
    let unique_keys = keys
        .into_iter()
        .filter(|k| seen.insert(*k))
        .collect::<Vec<_>>();

    let selected_count = unique_keys
        .iter()
        .filter(|key| map.contains_key(**key))
        .count()
        .max(1);
    let per_key_limit = (total_limit / selected_count).clamp(300, 1200);

    let mut out = Map::new();
    for key in unique_keys {
        let Some(value) = map.get(key) else {
            continue;
        };
        let serialized = serde_json::to_string(value).unwrap_or_default();
        if serialized.len() <= per_key_limit {
            out.insert(key.to_string(), value.clone());
        } else {
            out.insert(
                key.to_string(),
                Value::String(truncate_string(&serialized, per_key_limit)),
            );
        }
    }

    if out.is_empty() {
        let serialized = serde_json::to_string(external_data).unwrap_or_default();
        return Value::String(truncate_string(&serialized, total_limit / 2));
    }

    Value::Object(out)
}
