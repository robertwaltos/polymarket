use crate::ai_engine::AiEngine;
use crate::config::Config;
use crate::error::AgentError;
use crate::risk_engine::{RiskParams, TradeSignal, TradeSide};
use crate::types::{AiEstimate, ExternalData, Market, MarketNiche};
use crate::utils::bayesian_aggregate;
use rand::Rng;
use reqwest::Client;
use serde_json::Value;

const DEFAULT_EDGE_THRESHOLD: f64 = 0.08;
const POLITICS_EDGE_THRESHOLD: f64 = 0.05;
const BETTING_EDGE_THRESHOLD: f64 = 0.05;

pub struct Strategy {
    ai_engine: AiEngine,
    config: Config,
}

impl Strategy {
    pub fn new(client: Client, config: Config) -> Self {
        Self {
            ai_engine: AiEngine::new(client, config.clone()),
            config,
        }
    }

    pub fn classify_niche(&self, market: &Market) -> Option<MarketNiche> {
        let text = market.market_text().to_lowercase();

        let has_any = |terms: &[&str]| terms.iter().any(|term| text.contains(term));

        if has_any(&[
            "weather", "rain", "snow", "temperature", "hurricane", "storm",
        ]) {
            Some(MarketNiche::Weather)
        } else if has_any(&[
            "sports", "nfl", "nba", "mlb", "nhl", "injury", "touchdown", "playoffs",
        ]) {
            Some(MarketNiche::Sports)
        } else if has_any(&[
            "crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "defi",
        ]) {
            Some(MarketNiche::Crypto)
        } else if has_any(&[
            "election", "president", "senate", "house", "congress", "primary",
            "nomination", "politics", "governor", "poll", "texas", "tx",
        ]) {
            Some(MarketNiche::Politics)
        } else if has_any(&["betting", "odds", "futures", "spread", "line"]) {
            Some(MarketNiche::Betting)
        } else {
            None
        }
    }

    pub fn edge_threshold_for_niche(&self, niche: MarketNiche) -> f64 {
        match niche {
            MarketNiche::Politics => POLITICS_EDGE_THRESHOLD,
            MarketNiche::Betting => BETTING_EDGE_THRESHOLD,
            _ => DEFAULT_EDGE_THRESHOLD,
        }
    }

    pub async fn infer_probabilities(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: MarketNiche,
    ) -> Result<AiEstimate, AgentError> {
        self.ai_engine
            .infer_probabilities(market, external_data, Some(niche))
            .await
    }

    pub fn adjusted_ai_probability(
        &self,
        ai: &AiEstimate,
        external_data: &ExternalData,
        niche: MarketNiche,
    ) -> f64 {
        let prior = ai.yes_probability;
        let sources = extract_external_sources(external_data, niche);
        if sources.is_empty() {
            return prior;
        }
        bayesian_aggregate(prior, &sources)
    }

    pub fn build_risk_params(
        &self,
        market: &Market,
        ai_fair_value: f64,
        bankroll: f64,
        niche: MarketNiche,
        edge_override: Option<f64>,
    ) -> Option<RiskParams> {
        let yes_price = market.yes_price?;
        let edge_threshold = edge_override.unwrap_or_else(|| self.edge_threshold_for_niche(niche));
        Some(RiskParams {
            bankroll,
            market_price: yes_price,
            ai_fair_value,
            market_id: market.id.clone(),
            edge_threshold,
        })
    }

    pub fn limit_price_for_signal(
        &self,
        market: &Market,
        signal: &TradeSignal,
    ) -> Option<f64> {
        let yes_price = market.yes_price?;
        let no_price = market.no_price.unwrap_or(1.0 - yes_price);
        let offset = (self.config.limit_price_offset_bps as f64) / 10_000.0;
        match signal.side {
            TradeSide::BuyYes => Some((yes_price - offset).max(0.01)),
            TradeSide::BuyNo => Some((no_price - offset).max(0.01)),
            TradeSide::None => None,
        }
    }

    pub fn maybe_exploration_trade(
        &self,
        market: &Market,
        ai: &AiEstimate,
        bankroll: f64,
        epsilon: f64,
        fraction: f64,
    ) -> Option<TradeSignal> {
        if epsilon <= 0.0 {
            return None;
        }

        let mut rng = rand::thread_rng();
        let roll: f64 = rng.gen();
        if roll > epsilon {
            return None;
        }

        let side = if ai.yes_probability >= 0.5 {
            TradeSide::BuyYes
        } else {
            TradeSide::BuyNo
        };

        let size = (bankroll * fraction).min(bankroll * 0.06);
        let size = (size * 100.0).floor() / 100.0;
        if size < 1.0 {
            return None;
        }

        Some(TradeSignal {
            market_id: market.id.clone(),
            side,
            size_usdc: size,
            reasoning: format!(
                "Exploration trade (epsilon {:.2}).",
                epsilon
            ),
        })
    }
}

fn extract_external_sources(external_data: &Value, niche: MarketNiche) -> Vec<(f64, f64)> {
    let keys = match niche {
        MarketNiche::Politics => vec![
            "predictit_markets",
            "kalshi_markets",
            "realclearpolling",
            "fivethirtyeight_polls",
            "robinhood_predicts",
            "manifold_markets",
            "local_polls",
        ],
        MarketNiche::Sports | MarketNiche::Betting => vec![
            "sports_odds",
            "betfair_odds",
            "smarkets_markets",
            "oddsportal",
            "betexplorer",
            "oddsshark",
            "manifold_markets",
        ],
        MarketNiche::Crypto => vec![
            "betfair_odds",
            "smarkets_markets",
            "cftc_reports",
            "ibkr_event_contracts",
            "manifold_markets",
        ],
        MarketNiche::Weather => vec![],
    };

    let mut sources = Vec::new();
    for key in keys {
        if let Some(value) = external_data.get(key) {
            collect_sources(value, key, &mut sources);
        }
    }

    sources
}

fn collect_sources(value: &Value, key_hint: &str, out: &mut Vec<(f64, f64)>) {
    match value {
        Value::Number(num) => {
            if let Some(val) = num.as_f64() {
                if (0.0..=1.0).contains(&val) {
                    out.push((val, variance_for_key(key_hint)));
                } else if (key_hint.contains("odds") || key_hint.contains("price"))
                    && val > 1.0
                    && val < 100.0
                {
                    let implied = (1.0 / val).clamp(0.0, 1.0);
                    out.push((implied, variance_for_key(key_hint)));
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_sources(item, key_hint, out);
            }
        }
        Value::Object(map) => {
            for (key, value) in map {
                collect_sources(value, key.as_str(), out);
            }
        }
        _ => {}
    }
}

fn variance_for_key(key: &str) -> f64 {
    let key = key.to_lowercase();
    if key.contains("predictit") {
        0.08
    } else if key.contains("kalshi") {
        0.05
    } else if key.contains("manifold") {
        0.07
    } else if key.contains("betfair") {
        0.04
    } else if key.contains("sports_odds") || key.contains("odds") {
        0.06
    } else if key.contains("poll") {
        0.10
    } else {
        0.09
    }
}
