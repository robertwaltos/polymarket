use crate::ai_engine::AiEngine;
use crate::config::Config;
use crate::error::AgentError;
use crate::risk_engine::{RiskParams, TradeSignal, TradeSide};
use crate::types::{AiEstimate, ExternalData, Market, MarketNiche};
use crate::utils::{bayesian_aggregate, clamp_probability};
use rand::Rng;
use reqwest::Client;
use serde_json::{json, Value};
use strsim::jaro_winkler;

const DEFAULT_EDGE_THRESHOLD: f64 = 0.08;
const POLITICS_EDGE_THRESHOLD: f64 = 0.05;
const BETTING_EDGE_THRESHOLD: f64 = 0.05;
const WEATHER_EDGE_THRESHOLD: f64 = 0.06;

pub struct Strategy {
    ai_engine: AiEngine,
    config: Config,
}

#[derive(Debug, Clone)]
pub struct KalshiMatch {
    pub ticker: String,
    pub title: String,
    pub subtitle: String,
    pub probability: f64,
    pub volume: f64,
    pub variance: f64,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct PredictItMatch {
    pub market_name: String,
    pub contract_name: String,
    pub yes_price: f64,
    pub no_price: f64,
    pub score: f64,
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
            MarketNiche::Weather => WEATHER_EDGE_THRESHOLD,
            _ => DEFAULT_EDGE_THRESHOLD,
        }
    }

    pub fn weather_fast_estimate(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: MarketNiche,
    ) -> Option<AiEstimate> {
        if niche != MarketNiche::Weather || !self.config.weather_fast_path {
            return None;
        }

        let signal = infer_weather_signal(
            market,
            external_data,
            self.config.weather_station_scale_f,
        )?;
        Some(AiEstimate {
            yes_probability: signal.yes_probability,
            no_probability: 1.0 - signal.yes_probability,
            reasoning: format!(
                "Weather station fast-path: {} {:.1}F vs threshold {:.1}F ({}).",
                signal.city,
                signal.station_temp_f,
                signal.threshold_f,
                if signal.yes_if_above { "above" } else { "below" }
            ),
            token_cost_usd: 0.0,
            input_tokens: 0,
            output_tokens: 0,
        })
    }

    pub async fn infer_probabilities(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: MarketNiche,
        kalshi_match: Option<&KalshiMatch>,
        predictit_match: Option<&PredictItMatch>,
    ) -> Result<AiEstimate, AgentError> {
        let enriched =
            enrich_external_data(external_data, kalshi_match, predictit_match);
        self.ai_engine
            .infer_probabilities(market, &enriched, Some(niche))
            .await
    }

    pub fn estimate_input_tokens(
        &self,
        market: &Market,
        external_data: &ExternalData,
        niche: MarketNiche,
        kalshi_match: Option<&KalshiMatch>,
        predictit_match: Option<&PredictItMatch>,
    ) -> u64 {
        let enriched =
            enrich_external_data(external_data, kalshi_match, predictit_match);
        self.ai_engine
            .estimate_input_tokens(market, &enriched, Some(niche))
    }

    pub fn adjusted_ai_probability(
        &self,
        market: &Market,
        ai: &AiEstimate,
        external_data: &ExternalData,
        niche: MarketNiche,
        kalshi_match: Option<&KalshiMatch>,
        predictit_match: Option<&PredictItMatch>,
    ) -> f64 {
        let prior = ai.yes_probability;
        if niche == MarketNiche::Weather {
            if let Some(signal) =
                infer_weather_signal(market, external_data, self.config.weather_station_scale_f)
            {
                let weight = self.config.weather_station_weight.clamp(0.0, 1.0);
                let blended = prior * (1.0 - weight) + signal.yes_probability * weight;
                return clamp_probability(blended);
            }
        }

        let sources = extract_external_sources(external_data, niche);
        let mut sources = sources;
        let kalshi = kalshi_match.cloned().or_else(|| find_kalshi_match(market, external_data));
        if let Some(kalshi) = kalshi {
            sources.push((kalshi.probability, kalshi.variance));
        }
        let predictit =
            predictit_match.cloned().or_else(|| find_predictit_match(market, external_data));
        if let Some(predictit) = predictit {
            if predictit.yes_price > 0.0 {
                sources.push((predictit.yes_price, variance_for_key("predictit")));
            }
        }
        if sources.is_empty() {
            return prior;
        }
        bayesian_aggregate(prior, &sources)
    }

    pub fn kalshi_match(
        &self,
        market: &Market,
        external_data: &ExternalData,
    ) -> Option<KalshiMatch> {
        find_kalshi_match(market, external_data)
    }

    pub fn predictit_match(
        &self,
        market: &Market,
        external_data: &ExternalData,
    ) -> Option<PredictItMatch> {
        find_predictit_match(market, external_data)
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
            "realclearpolling",
            "fivethirtyeight_polls",
            "robinhood_predicts",
            "manifold_markets",
            "local_polls",
            "augur_markets",
        ],
        MarketNiche::Sports | MarketNiche::Betting => vec![
            "sports_odds",
            "betfair_odds",
            "smarkets_markets",
            "oddsportal",
            "betexplorer",
            "oddsshark",
            "manifold_markets",
            "augur_markets",
        ],
        MarketNiche::Crypto => vec![
            "betfair_odds",
            "smarkets_markets",
            "cftc_reports",
            "ibkr_event_contracts",
            "manifold_markets",
            "augur_markets",
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
    } else if key.contains("augur") {
        0.06
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

fn find_kalshi_match(market: &Market, external_data: &Value) -> Option<KalshiMatch> {
    let kalshi = external_data.get("kalshi_markets")?;
    let markets = kalshi.get("markets")?.as_array()?;

    let text = market.market_text().to_lowercase();
    let mut best: Option<(f64, String, String, String, f64, f64, f64)> = None;

    for entry in markets {
        let title = entry["title"].as_str().unwrap_or("");
        let subtitle = entry["subtitle"].as_str().unwrap_or("");
        let ticker = entry["ticker"].as_str().unwrap_or("");

        let combined = format!("{} {} {}", title, subtitle, ticker).to_lowercase();
        if combined.trim().is_empty() {
            continue;
        }

        let score = jaro_winkler(&combined, &text);
        if score < 0.82 {
            continue;
        }

        let yes_prob = entry["yes_prob"].as_f64()?;
        let volume = entry["volume_24h"]
            .as_f64()
            .or_else(|| entry["volume"].as_f64())
            .or_else(|| entry["liquidity"].as_f64())
            .unwrap_or(0.0);
        let var = kalshi_variance(volume);

        if best
            .as_ref()
            .map_or(true, |(best_score, _, _, _, _, _, _)| score > *best_score)
        {
            best = Some((
                score,
                ticker.to_string(),
                title.to_string(),
                subtitle.to_string(),
                yes_prob,
                volume,
                var,
            ));
        }
    }

    best.map(
        |(score, ticker, title, subtitle, prob, volume, var)| KalshiMatch {
            ticker,
            title,
            subtitle,
            probability: prob,
            volume,
            variance: var,
            score,
        },
    )
}

fn kalshi_variance(volume: f64) -> f64 {
    if volume > 10_000.0 {
        0.02
    } else if volume > 1_000.0 {
        0.04
    } else if volume > 100.0 {
        0.06
    } else {
        0.09
    }
}

fn find_predictit_match(market: &Market, external_data: &Value) -> Option<PredictItMatch> {
    let predictit = external_data.get("predictit_markets")?;
    let markets = predictit.get("markets")?.as_array()?;
    let text = market.market_text().to_lowercase();

    let mut best: Option<PredictItMatch> = None;

    for entry in markets {
        let market_name = entry["name"].as_str().unwrap_or("").to_string();
        let contracts = entry["contracts"].as_array().cloned().unwrap_or_default();

        for contract in contracts {
            let contract_name = contract["name"].as_str().unwrap_or("").to_string();
            let combined =
                format!("{} {}", market_name, contract_name).to_lowercase();
            if combined.trim().is_empty() {
                continue;
            }
            let score = jaro_winkler(&combined, &text);
            if score < 0.82 {
                continue;
            }

            let yes_price = contract["yes_price"].as_f64().unwrap_or(0.0);
            let no_price = contract["no_price"].as_f64().unwrap_or(0.0);

            let candidate = PredictItMatch {
                market_name: market_name.clone(),
                contract_name: contract_name.clone(),
                yes_price,
                no_price,
                score,
            };
            if best.as_ref().map_or(true, |b| score > b.score) {
                best = Some(candidate);
            }
        }
    }

    best
}

fn enrich_external_data(
    external_data: &ExternalData,
    kalshi_match: Option<&KalshiMatch>,
    predictit_match: Option<&PredictItMatch>,
) -> ExternalData {
    let mut enriched = external_data.clone();
    let context = json!({
        "kalshi_match": kalshi_match.map(|k| json!({
            "ticker": k.ticker,
            "title": k.title,
            "subtitle": k.subtitle,
            "probability": k.probability,
            "volume": k.volume,
            "variance": k.variance,
            "score": k.score
        })),
        "predictit_match": predictit_match.map(|p| json!({
            "market": p.market_name,
            "contract": p.contract_name,
            "yes_price": p.yes_price,
            "no_price": p.no_price,
            "score": p.score
        }))
    });

    match &mut enriched {
        Value::Object(map) => {
            map.insert("market_context".to_string(), context);
        }
        _ => {
            let mut map = serde_json::Map::new();
            map.insert("market_context".to_string(), context);
            map.insert("external".to_string(), enriched);
            enriched = Value::Object(map);
        }
    }

    enriched
}

#[derive(Debug, Clone)]
struct WeatherSignal {
    yes_probability: f64,
    city: String,
    station_temp_f: f64,
    threshold_f: f64,
    yes_if_above: bool,
}

fn infer_weather_signal(
    market: &Market,
    external_data: &ExternalData,
    base_scale_f: f64,
) -> Option<WeatherSignal> {
    let text = market.market_text().to_lowercase();
    let (yes_if_above, threshold_f) = parse_weather_threshold(&text)?;
    let (city, station_temp_f) = match_station_temperature_f(&text, external_data)?;

    let mut scale = base_scale_f.max(0.5);
    if text.contains("tomorrow")
        || text.contains("monday")
        || text.contains("tuesday")
        || text.contains("wednesday")
        || text.contains("thursday")
        || text.contains("friday")
        || text.contains("saturday")
        || text.contains("sunday")
    {
        scale *= 1.6;
    } else if text.contains("today") || text.contains("tonight") {
        scale *= 1.2;
    }

    let delta = if yes_if_above {
        station_temp_f - threshold_f
    } else {
        threshold_f - station_temp_f
    };
    let yes_probability = sigmoid(delta / scale);

    Some(WeatherSignal {
        yes_probability: clamp_probability(yes_probability),
        city,
        station_temp_f,
        threshold_f,
        yes_if_above,
    })
}

fn match_station_temperature_f(market_text: &str, external_data: &ExternalData) -> Option<(String, f64)> {
    let stations = external_data
        .get("noaa_station_observations")?
        .get("stations")?
        .as_array()?;

    let mut best: Option<(usize, String, f64)> = None;
    for station in stations {
        let city = station["city"].as_str()?.trim();
        if city.is_empty() {
            continue;
        }
        let city_lower = city.to_lowercase();
        if !contains_city_token(market_text, &city_lower) {
            continue;
        }
        let temp_f = station["temperature_f"]
            .as_f64()
            .or_else(|| station["temp_f"].as_f64())?;
        let score = city_lower.len();
        if best.as_ref().map_or(true, |(best_score, _, _)| score > *best_score) {
            best = Some((score, city.to_string(), temp_f));
        }
    }

    best.map(|(_, city, temp_f)| (city, temp_f))
}

fn contains_city_token(text: &str, city: &str) -> bool {
    if text.contains(city) {
        return true;
    }
    if city == "washington dc" && text.contains("washington d.c.") {
        return true;
    }
    false
}

fn parse_weather_threshold(text: &str) -> Option<(bool, f64)> {
    let above_keywords = ["above", "over", "at least", "greater than", ">"];
    let below_keywords = ["below", "under", "at most", "less than", "<"];

    let yes_if_above = above_keywords.iter().any(|k| text.contains(k));
    let yes_if_below = below_keywords.iter().any(|k| text.contains(k));

    if yes_if_above == yes_if_below {
        return None;
    }

    let threshold = if yes_if_above {
        extract_number_after_keywords(text, &above_keywords)
    } else {
        extract_number_after_keywords(text, &below_keywords)
    }?;

    if !(-80.0..=160.0).contains(&threshold) {
        return None;
    }

    Some((yes_if_above, threshold))
}

fn extract_number_after_keywords(text: &str, keywords: &[&str]) -> Option<f64> {
    for keyword in keywords {
        let mut from = 0usize;
        while let Some(found) = text[from..].find(keyword) {
            let start = from + found + keyword.len();
            let tail = &text[start..];
            if let Some(value) = extract_first_number(tail) {
                return Some(value);
            }
            from = start;
            if from >= text.len() {
                break;
            }
        }
    }
    None
}

fn extract_first_number(text: &str) -> Option<f64> {
    let mut buf = String::new();
    let mut started = false;
    for ch in text.chars() {
        if ch.is_ascii_digit() || ch == '.' || (ch == '-' && !started) {
            buf.push(ch);
            started = true;
            continue;
        }
        if started {
            break;
        }
    }

    if buf.is_empty() || buf == "-" || buf == "." || buf == "-." {
        return None;
    }
    buf.parse::<f64>().ok()
}

fn sigmoid(x: f64) -> f64 {
    let clipped = x.clamp(-20.0, 20.0);
    1.0 / (1.0 + (-clipped).exp())
}
