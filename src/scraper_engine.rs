use crate::config::Config;
use crate::error::AgentError;
use crate::utils::{retry, truncate_string};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Utc};
use csv::StringRecord;
use futures::future::join_all;
use log::warn;
use reqwest::Client;
use scraper::{Html, Selector};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const MAX_BLOB_CHARS: usize = 6000;
const MAX_RECENT_POLLS: usize = 20;
const POLL_LOOKBACK_DAYS: i64 = 30;

#[async_trait]
pub trait ExternalDataSource: Send + Sync {
    fn name(&self) -> &'static str;
    async fn fetch(&self, client: &Client) -> Result<Value, AgentError>;
}

pub struct ScraperEngine {
    client: Client,
    base_sources: Vec<Arc<dyn ExternalDataSource>>,
    politics_sources: Vec<Arc<dyn ExternalDataSource>>,
    betting_sources: Vec<Arc<dyn ExternalDataSource>>,
    local_sources: Vec<Arc<dyn ExternalDataSource>>,
}

impl ScraperEngine {
    pub fn new(client: Client, config: Config) -> Self {
        let mut base_sources: Vec<Arc<dyn ExternalDataSource>> = Vec::new();
        base_sources.push(Arc::new(NoaaSource::new(config.noaa_endpoint)));
        base_sources.push(Arc::new(RotowireSource::new(config.rotowire_rss)));
        base_sources.push(Arc::new(EspnSource::new(config.espn_injuries_url)));
        base_sources.push(Arc::new(DuneSource::new(
            config.dune_api_key,
            config.dune_query_ids,
        )));
        base_sources.push(Arc::new(MoralisSource::new(config.moralis_api_key)));
        base_sources.push(Arc::new(XSource::new(config.x_bearer_token)));
        base_sources.push(Arc::new(NewsSource::new(config.news_api_key)));

        let mut politics_sources: Vec<Arc<dyn ExternalDataSource>> = Vec::new();
        politics_sources.push(Arc::new(FiveThirtyEightSource::new()));
        politics_sources.push(Arc::new(RealClearPoliticsSource::new()));
        politics_sources.push(Arc::new(PredictItSource::new()));
        politics_sources.push(Arc::new(ManifoldSource::new()));

        let mut betting_sources: Vec<Arc<dyn ExternalDataSource>> = Vec::new();
        betting_sources.push(Arc::new(BetfairSource::new(
            config.betfair_app_key.clone(),
            config.betfair_session.clone(),
            config.betfair_event_type_ids.clone(),
        )));
        betting_sources.push(Arc::new(CftcSource::new(config.cftc_url.clone())));
        betting_sources.push(Arc::new(KalshiSource::new(config.kalshi_api_key.clone())));
        betting_sources.push(Arc::new(SmarketsSource::new(config.smarkets_session.clone())));
        betting_sources.push(Arc::new(IbkrSource::new(config.ibkr_api_key.clone())));
        betting_sources.push(Arc::new(OddsApiSource::new(
            config.odds_api_key.clone(),
            config.odds_api_sport.clone(),
        )));
        betting_sources.push(Arc::new(OddsPortalSource::new()));
        betting_sources.push(Arc::new(BetExplorerSource::new()));
        betting_sources.push(Arc::new(OddsSharkSource::new()));
        betting_sources.push(Arc::new(RobinhoodPredictsSource::new(
            config.robinhood_predicts_url.clone(),
        )));
        betting_sources.push(Arc::new(ManifoldSource::new()));

        let mut local_sources: Vec<Arc<dyn ExternalDataSource>> = Vec::new();
        local_sources.push(Arc::new(LocalPollsSource::new(config.local_polls_url.clone())));

        Self {
            client,
            base_sources,
            politics_sources,
            betting_sources,
            local_sources,
        }
    }

    pub async fn fetch_all(
        &self,
        include_politics: bool,
        include_betting: bool,
        include_local: bool,
    ) -> Result<Value, AgentError> {
        let mut sources: HashMap<String, Arc<dyn ExternalDataSource>> =
            HashMap::new();

        for source in &self.base_sources {
            sources.entry(source.name().to_string()).or_insert_with(|| Arc::clone(source));
        }
        if include_politics {
            for source in &self.politics_sources {
                sources.entry(source.name().to_string()).or_insert_with(|| Arc::clone(source));
            }
        }
        if include_betting {
            for source in &self.betting_sources {
                sources.entry(source.name().to_string()).or_insert_with(|| Arc::clone(source));
            }
        }
        if include_local {
            for source in &self.local_sources {
                sources.entry(source.name().to_string()).or_insert_with(|| Arc::clone(source));
            }
        }

        let futures = sources.values().map(|source| {
            let client = self.client.clone();
            let source = Arc::clone(source);
            async move {
                let name = source.name();
                let result = source.fetch(&client).await;
                (name, result)
            }
        });

        let results = join_all(futures).await;
        let mut map = serde_json::Map::new();
        for (name, result) in results {
            match result {
                Ok(value) => {
                    map.insert(name.to_string(), value);
                }
                Err(err) => {
                    warn!("Data source {} failed: {}", name, err);
                    map.insert(name.to_string(), Value::Null);
                }
            }
        }

        Ok(Value::Object(map))
    }
}

struct NoaaSource {
    endpoint: Option<String>,
}

impl NoaaSource {
    fn new(endpoint: Option<String>) -> Self {
        Self { endpoint }
    }
}

#[async_trait]
impl ExternalDataSource for NoaaSource {
    fn name(&self) -> &'static str {
        "noaa_weather"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = self.endpoint.clone().unwrap_or_else(|| {
            "https://api.weather.gov/gridpoints/TOP/31,49/forecast".to_string()
        });
        let text = retry(
            || async {
                let resp = client
                    .get(&url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;
        let value: Value = serde_json::from_str(&text).unwrap_or(Value::String(truncate_string(
            &text,
            MAX_BLOB_CHARS,
        )));
        Ok(value)
    }
}

struct RotowireSource {
    rss_url: Option<String>,
}

impl RotowireSource {
    fn new(rss_url: Option<String>) -> Self {
        Self { rss_url }
    }
}

#[async_trait]
impl ExternalDataSource for RotowireSource {
    fn name(&self) -> &'static str {
        "rotowire_injuries"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = self
            .rss_url
            .clone()
            .unwrap_or_else(|| "https://www.rotowire.com/basketball/injury-rss.php".to_string());
        let text = retry(
            || async {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;
        Ok(Value::String(truncate_string(&text, MAX_BLOB_CHARS)))
    }
}

struct EspnSource {
    injuries_url: Option<String>,
}

impl EspnSource {
    fn new(injuries_url: Option<String>) -> Self {
        Self { injuries_url }
    }
}

#[async_trait]
impl ExternalDataSource for EspnSource {
    fn name(&self) -> &'static str {
        "espn_injuries"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = self
            .injuries_url
            .clone()
            .unwrap_or_else(|| "https://www.espn.com/nfl/injuries".to_string());
        let html = retry(
            || async {
                let resp = client
                    .get(&url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;

        let doc = Html::parse_document(&html);
        let row_selector = Selector::parse("table tbody tr").unwrap();
        let cell_selector = Selector::parse("td").unwrap();
        let mut rows = Vec::new();
        for row in doc.select(&row_selector).take(50) {
            let cells: Vec<String> = row
                .select(&cell_selector)
                .map(|cell| cell.text().collect::<Vec<_>>().join(" "))
                .collect();
            if !cells.is_empty() {
                rows.push(cells.join(" | "));
            }
        }

        if rows.is_empty() {
            return Ok(Value::String(truncate_string(&html, MAX_BLOB_CHARS)));
        }

        Ok(json!(rows))
    }
}

struct DuneSource {
    api_key: Option<String>,
    query_ids: Vec<u64>,
}

impl DuneSource {
    fn new(api_key: Option<String>, query_ids: Vec<u64>) -> Self {
        Self { api_key, query_ids }
    }
}

#[async_trait]
impl ExternalDataSource for DuneSource {
    fn name(&self) -> &'static str {
        "dune_crypto"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(api_key) = &self.api_key else {
            return Ok(Value::Null);
        };

        if self.query_ids.is_empty() {
            return Ok(Value::Null);
        }

        let mut outputs = Vec::new();
        for id in &self.query_ids {
            let url = format!(
                "https://api.dune.com/api/v1/query/{}/results?api_key={}",
                id, api_key
            );
            let result = retry(
                || async {
                    let resp = client.get(&url).send().await?.error_for_status()?;
                    Ok(resp.json::<Value>().await?)
                },
                3,
            )
            .await?;
            outputs.push(result);
        }

        Ok(json!(outputs))
    }
}

struct MoralisSource {
    api_key: Option<String>,
}

impl MoralisSource {
    fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }
}

#[async_trait]
impl ExternalDataSource for MoralisSource {
    fn name(&self) -> &'static str {
        "moralis_onchain"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(api_key) = &self.api_key else {
            return Ok(Value::Null);
        };

        let url = "https://deep-index.moralis.io/api/v2.2/market-data/global/market-cap?chain=eth";
        let result = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("X-API-Key", api_key)
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;
        Ok(result)
    }
}

struct XSource {
    bearer_token: Option<String>,
}

impl XSource {
    fn new(bearer_token: Option<String>) -> Self {
        Self { bearer_token }
    }
}

#[async_trait]
impl ExternalDataSource for XSource {
    fn name(&self) -> &'static str {
        "x_sentiment"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(bearer_token) = &self.bearer_token else {
            return Ok(Value::Null);
        };

        let query = "crypto OR weather OR sports injury lang:en -is:retweet";
        let url = format!(
            "https://api.twitter.com/2/tweets/search/recent?query={}&max_results=50&tweet.fields=public_metrics",
            urlencoding::encode(query)
        );

        let response = retry(
            || async {
                let resp = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {}", bearer_token))
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(response)
    }
}

struct NewsSource {
    api_key: Option<String>,
}

impl NewsSource {
    fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }
}

#[async_trait]
impl ExternalDataSource for NewsSource {
    fn name(&self) -> &'static str {
        "news_headlines"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(api_key) = &self.api_key else {
            return Ok(Value::Null);
        };

        let query = "crypto OR sports OR weather";
        let url = format!(
            "https://newsapi.org/v2/everything?q={}&apiKey={}&sortBy=publishedAt&pageSize=20",
            urlencoding::encode(query),
            api_key
        );

        let response = retry(
            || async {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(response)
    }
}

struct FiveThirtyEightSource {
    urls: Vec<&'static str>,
}

impl FiveThirtyEightSource {
    fn new() -> Self {
        Self {
            urls: vec![
                "https://raw.githubusercontent.com/fivethirtyeight/data/master/polls/president_polls.csv",
                "https://raw.githubusercontent.com/fivethirtyeight/data/master/polls/senate_polls.csv",
                "https://raw.githubusercontent.com/fivethirtyeight/data/master/polls/house_polls.csv",
                "https://raw.githubusercontent.com/fivethirtyeight/data/master/polls/generic_ballot_polls.csv",
            ],
        }
    }
}

#[async_trait]
impl ExternalDataSource for FiveThirtyEightSource {
    fn name(&self) -> &'static str {
        "fivethirtyeight_polls"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let now = Utc::now();
        let cutoff = now - ChronoDuration::days(POLL_LOOKBACK_DAYS);
        let mut sources = Vec::new();

        for url in &self.urls {
            let csv_text = retry(
                || async {
                    let resp = client.get(*url).send().await?.error_for_status()?;
                    Ok(resp.text().await?)
                },
                3,
            )
            .await?;

            let mut rdr = csv::ReaderBuilder::new()
                .flexible(true)
                .from_reader(csv_text.as_bytes());
            let headers = rdr.headers()?.clone();

            let end_idx = header_index(&headers, "end_date");
            let candidate_idx = header_index(&headers, "candidate_name");
            let pct_idx = header_index(&headers, "pct");
            let sample_idx = header_index(&headers, "sample_size");

            let mut all_rows: Vec<Value> = Vec::new();
            let mut dated_rows: Vec<(DateTime<Utc>, Value)> = Vec::new();
            let mut candidate_sums: HashMap<String, (f64, u64)> = HashMap::new();

            for result in rdr.records() {
                let record = result?;
                let row_map = record_to_map(&headers, &record);
                let row_value = Value::Object(row_map);
                all_rows.push(row_value.clone());

                let end_date = end_idx
                    .and_then(|idx| record.get(idx))
                    .and_then(parse_date_any);

                if let Some(date) = end_date {
                    if date >= cutoff {
                        dated_rows.push((date, row_value.clone()));

                        if let (Some(cand_idx), Some(pct_idx), Some(sample_idx)) =
                            (candidate_idx, pct_idx, sample_idx)
                        {
                            let candidate = record.get(cand_idx).unwrap_or("").trim();
                            let pct_raw = record.get(pct_idx).unwrap_or("");
                            let sample_raw = record.get(sample_idx).unwrap_or("");
                            if candidate.is_empty() {
                                continue;
                            }

                            let pct = match parse_percent(pct_raw) {
                                Some(val) => val,
                                None => continue,
                            };
                            let sample = parse_sample_size(sample_raw);
                            if sample == 0 {
                                continue;
                            }

                            let entry = candidate_sums
                                .entry(candidate.to_string())
                                .or_insert((0.0, 0));
                            entry.0 += pct * sample as f64;
                            entry.1 += sample;
                        }
                    }
                }
            }

            let recent_rows = if !dated_rows.is_empty() {
                let mut sorted = dated_rows;
                sorted.sort_by_key(|(dt, _)| *dt);
                sorted
                    .into_iter()
                    .rev()
                    .take(MAX_RECENT_POLLS)
                    .map(|(_, value)| value)
                    .collect::<Vec<_>>()
            } else {
                all_rows
                    .into_iter()
                    .rev()
                    .take(MAX_RECENT_POLLS)
                    .collect::<Vec<_>>()
            };

            let mut averages = Map::new();
            for (candidate, (weighted_sum, total_sample)) in candidate_sums {
                if total_sample > 0 {
                    averages.insert(candidate, json!(weighted_sum / total_sample as f64));
                }
            }

            let source_name = url.split('/').last().unwrap_or("unknown");
            sources.push(json!({
                "source": source_name,
                "recent_polls": recent_rows,
                "averages": averages
            }));
        }

        Ok(json!({ "sources": sources }))
    }
}

struct RealClearPoliticsSource {
}

impl RealClearPoliticsSource {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExternalDataSource for RealClearPoliticsSource {
    fn name(&self) -> &'static str {
        "realclearpolling"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let now = Utc::now();
        let cutoff = now - ChronoDuration::days(POLL_LOOKBACK_DAYS);
        let mut sources = Vec::new();

        let urls = discover_rcp_urls(client).await?;
        for url in urls {
            let html = retry(
                || async {
                    let resp = client
                        .get(&url)
                        .header("User-Agent", "polymarket-agent/1.0")
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(resp.text().await?)
                },
                3,
            )
            .await?;

            let doc = Html::parse_document(&html);
            let table_selectors = [
                Selector::parse("#polling-data-full").unwrap(),
                Selector::parse("table.data.large").unwrap(),
                Selector::parse("table").unwrap(),
            ];

            let mut table = None;
            for selector in &table_selectors {
                if let Some(found) = doc.select(selector).next() {
                    table = Some(found);
                    break;
                }
            }

            let Some(table) = table else {
                sources.push(json!({
                    "source": url.split('/').last().unwrap_or("unknown"),
                    "polls": [],
                    "averages": {}
                }));
                continue;
            };

            let header_selector = Selector::parse("thead th").unwrap();
            let row_selector = Selector::parse("tbody tr").unwrap();
            let cell_selector = Selector::parse("td").unwrap();

            let headers: Vec<String> = table
                .select(&header_selector)
                .map(|th| clean_text(&th.text().collect::<Vec<_>>().join(" ")))
                .collect();

            let mut polls: Vec<Map<String, Value>> = Vec::new();
            for row in table.select(&row_selector).take(50) {
                let cells: Vec<String> = row
                    .select(&cell_selector)
                    .map(|td| clean_text(&td.text().collect::<Vec<_>>().join(" ")))
                    .collect();
                if cells.len() != headers.len() {
                    continue;
                }

                let mut row_map = Map::new();
                for (header, cell) in headers.iter().zip(cells.iter()) {
                    row_map.insert(header.clone(), json!(cell));
                }
                polls.push(row_map);
            }

            let mut candidate_sums: HashMap<String, (f64, u64)> = HashMap::new();
            for poll in &polls {
                let date_str = poll
                    .get("Date")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let (_start_date, end_date) = match parse_rcp_date_range(date_str, now) {
                    Some(range) => range,
                    None => continue,
                };
                let poll_date = end_date;
                if poll_date < cutoff {
                    continue;
                }

                let sample_raw = poll
                    .get("Sample")
                    .and_then(|v| v.as_str())
                    .unwrap_or("0");
                let sample = parse_sample_size(sample_raw);
                if sample == 0 {
                    continue;
                }

                for (key, value) in poll.iter() {
                    if is_rcp_meta_field(key) {
                        continue;
                    }
                    let pct_raw = value.as_str().unwrap_or("");
                    let pct = match parse_percent(pct_raw) {
                        Some(val) => val,
                        None => continue,
                    };

                    let entry = candidate_sums.entry(key.clone()).or_insert((0.0, 0));
                    entry.0 += pct * sample as f64;
                    entry.1 += sample;
                }
            }

            let mut averages = Map::new();
            for (candidate, (weighted_sum, total_sample)) in candidate_sums {
                if total_sample > 0 {
                    averages.insert(candidate, json!(weighted_sum / total_sample as f64));
                }
            }

            let polls_recent: Vec<Value> = polls
                .into_iter()
                .take(MAX_RECENT_POLLS)
                .map(Value::Object)
                .collect();

            sources.push(json!({
                "source": url.split('/').last().unwrap_or("unknown"),
                "polls": polls_recent,
                "averages": averages
            }));
        }

        Ok(json!({ "sources": sources }))
    }
}

struct PredictItSource;

impl PredictItSource {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ExternalDataSource for PredictItSource {
    fn name(&self) -> &'static str {
        "predictit_markets"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = "https://www.predictit.org/api/marketdata/all/";
        let payload = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        let markets = payload["markets"].as_array().cloned().unwrap_or_default();
        let mut filtered = Vec::new();

        for market in markets {
            let name = market["name"].as_str().unwrap_or("").to_string();
            let name_lower = name.to_lowercase();
            if !matches_politics_keywords(&name_lower) {
                continue;
            }

            let id = market["id"].as_i64().unwrap_or(0);
            let contracts_array = market["contracts"].as_array().cloned().unwrap_or_default();
            let contracts = contracts_array
                .iter()
                .map(|contract| {
                    json!({
                        "name": contract["name"].as_str().unwrap_or(""),
                        "yes_price": contract["bestBuyYesCost"].as_f64().unwrap_or(0.0),
                        "no_price": contract["bestBuyNoCost"].as_f64().unwrap_or(0.0)
                    })
                })
                .collect::<Vec<_>>();

            filtered.push(json!({
                "id": id,
                "name": name,
                "contracts": contracts
            }));
        }

        Ok(json!({ "markets": filtered }))
    }
}

struct ManifoldSource;

impl ManifoldSource {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ExternalDataSource for ManifoldSource {
    fn name(&self) -> &'static str {
        "manifold_markets"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = "https://manifold.markets/api/v0/markets?limit=1000";
        let payload = retry(
            || async {
                let resp = client.get(url).send().await?.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        let markets = payload.as_array().cloned().unwrap_or_default();
        let mut filtered = Vec::new();
        for market in markets {
            let question = market["question"].as_str().unwrap_or("").to_string();
            let question_lower = question.to_lowercase();
            if !(question_lower.contains("politics")
                || question_lower.contains("election")
                || question_lower.contains("crypto")
                || question_lower.contains("bitcoin")
                || question_lower.contains("nba")
                || question_lower.contains("nfl")
                || question_lower.contains("sports"))
            {
                continue;
            }

            filtered.push(json!({
                "id": market["id"],
                "question": question,
                "probability": market["probability"],
                "liquidity": market["totalLiquidity"]
            }));
        }

        Ok(json!({ "markets": filtered }))
    }
}

struct BetfairSource {
    app_key: Option<String>,
    session_token: Option<String>,
    event_type_ids: Vec<String>,
}

impl BetfairSource {
    fn new(app_key: Option<String>, session_token: Option<String>, event_type_ids: Vec<String>) -> Self {
        Self {
            app_key,
            session_token,
            event_type_ids,
        }
    }
}

#[async_trait]
impl ExternalDataSource for BetfairSource {
    fn name(&self) -> &'static str {
        "betfair_odds"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(app_key) = &self.app_key else {
            return Ok(Value::Null);
        };
        let Some(session_token) = &self.session_token else {
            return Ok(Value::Null);
        };

        let payload = json!({
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": { "eventTypeIds": self.event_type_ids.clone() },
                "maxResults": "50",
                "marketProjection": ["COMPETITION", "EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION"]
            },
            "id": 1
        });

        let resp = retry(
            || async {
                let resp = client
                    .post("https://api.betfair.com/exchange/betting/json-rpc/v1")
                    .header("X-Application", app_key)
                    .header("X-Authentication", session_token)
                    .json(&payload)
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(resp)
    }
}

struct CftcSource {
    url: Option<String>,
}

impl CftcSource {
    fn new(url: Option<String>) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ExternalDataSource for CftcSource {
    fn name(&self) -> &'static str {
        "cftc_reports"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let default_url = format!(
            "https://publicreporting.cftc.gov/api/v1/cot/report?reportType=legacy_futures&reportYear={}",
            Utc::now().year()
        );
        let url = self.url.clone().unwrap_or(default_url);
        let text = retry(
            || async {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;

        if let Ok(json_val) = serde_json::from_str::<Value>(&text) {
            return Ok(json_val);
        }

        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(text.as_bytes());
        let headers = rdr.headers()?.clone();
        let mut rows = Vec::new();
        for record in rdr.records().take(200) {
            let record = record?;
            let row = record_to_map(&headers, &record);
            rows.push(Value::Object(row));
        }

        Ok(json!({ "rows": rows }))
    }
}

struct KalshiSource {
    api_key: Option<String>,
}

impl KalshiSource {
    fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }
}

#[async_trait]
impl ExternalDataSource for KalshiSource {
    fn name(&self) -> &'static str {
        "kalshi_markets"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(api_key) = &self.api_key else {
            return Ok(Value::Null);
        };

        let url = "https://trading-api.kalshi.com/trade-api/v2/markets?status=open&limit=100";
        let payload = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("Authorization", format!("Bearer {}", api_key))
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(payload)
    }
}

struct SmarketsSource {
    session_token: Option<String>,
}

impl SmarketsSource {
    fn new(session_token: Option<String>) -> Self {
        Self { session_token }
    }
}

#[async_trait]
impl ExternalDataSource for SmarketsSource {
    fn name(&self) -> &'static str {
        "smarkets_markets"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(token) = &self.session_token else {
            return Ok(Value::Null);
        };

        let url = "https://api.smarkets.com/v3/events?states=upcoming&types=football_match";
        let payload = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("Authorization", format!("Session-Token {}", token))
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(payload)
    }
}

struct IbkrSource {
    api_key: Option<String>,
}

impl IbkrSource {
    fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }
}

#[async_trait]
impl ExternalDataSource for IbkrSource {
    fn name(&self) -> &'static str {
        "ibkr_event_contracts"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url =
            "https://api.ibkr.com/v1/api/trsrv/contracts?symbol=SPX&secType=OPT&exp=20261231";
        let payload = retry(
            || async {
                let mut req = client.get(url);
                if let Some(key) = &self.api_key {
                    req = req.header("Authorization", key);
                }
                let resp = req.send().await?.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(payload)
    }
}

struct OddsApiSource {
    api_key: Option<String>,
    sport: String,
}

impl OddsApiSource {
    fn new(api_key: Option<String>, sport: String) -> Self {
        Self { api_key, sport }
    }
}

#[async_trait]
impl ExternalDataSource for OddsApiSource {
    fn name(&self) -> &'static str {
        "sports_odds"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(api_key) = &self.api_key else {
            return Ok(Value::Null);
        };

        let url = format!(
            "https://api.the-odds-api.com/v4/sports/{}/odds/?apiKey={}&regions=us&markets=h2h,spreads",
            self.sport, api_key
        );
        let payload = retry(
            || async {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(payload)
    }
}

struct OddsPortalSource;

impl OddsPortalSource {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ExternalDataSource for OddsPortalSource {
    fn name(&self) -> &'static str {
        "oddsportal"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = "https://www.oddsportal.com/soccer/england/premier-league/";
        let html = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;

        let doc = Html::parse_document(&html);
        let row_selector = Selector::parse("table tbody tr").unwrap();
        let cell_selector = Selector::parse("td").unwrap();
        let mut rows = Vec::new();
        for row in doc.select(&row_selector).take(30) {
            let cells: Vec<String> = row
                .select(&cell_selector)
                .map(|cell| clean_text(&cell.text().collect::<Vec<_>>().join(" ")))
                .collect();
            if !cells.is_empty() {
                rows.push(cells);
            }
        }

        if rows.is_empty() {
            return Ok(Value::String(truncate_string(&html, MAX_BLOB_CHARS)));
        }

        Ok(json!(rows))
    }
}

struct BetExplorerSource;

impl BetExplorerSource {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ExternalDataSource for BetExplorerSource {
    fn name(&self) -> &'static str {
        "betexplorer"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = "https://www.betexplorer.com/odds/";
        let html = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;

        let doc = Html::parse_document(&html);
        let row_selector = Selector::parse("table tbody tr").unwrap();
        let cell_selector = Selector::parse("td").unwrap();
        let mut rows = Vec::new();
        for row in doc.select(&row_selector).take(30) {
            let cells: Vec<String> = row
                .select(&cell_selector)
                .map(|cell| clean_text(&cell.text().collect::<Vec<_>>().join(" ")))
                .collect();
            if !cells.is_empty() {
                rows.push(cells);
            }
        }

        if rows.is_empty() {
            return Ok(Value::String(truncate_string(&html, MAX_BLOB_CHARS)));
        }

        Ok(json!(rows))
    }
}

struct OddsSharkSource;

impl OddsSharkSource {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ExternalDataSource for OddsSharkSource {
    fn name(&self) -> &'static str {
        "oddsshark"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let url = "https://www.oddsshark.com/mlb/database";
        let html = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.text().await?)
            },
            3,
        )
        .await?;

        let doc = Html::parse_document(&html);
        let row_selector = Selector::parse("table tbody tr").unwrap();
        let cell_selector = Selector::parse("td").unwrap();
        let mut rows = Vec::new();
        for row in doc.select(&row_selector).take(30) {
            let cells: Vec<String> = row
                .select(&cell_selector)
                .map(|cell| clean_text(&cell.text().collect::<Vec<_>>().join(" ")))
                .collect();
            if !cells.is_empty() {
                rows.push(cells);
            }
        }

        if rows.is_empty() {
            return Ok(Value::String(truncate_string(&html, MAX_BLOB_CHARS)));
        }

        Ok(json!(rows))
    }
}

struct LocalPollsSource {
    url: Option<String>,
}

impl LocalPollsSource {
    fn new(url: Option<String>) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ExternalDataSource for LocalPollsSource {
    fn name(&self) -> &'static str {
        "local_polls"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(url) = &self.url else {
            return Ok(Value::Null);
        };

        let payload = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(payload)
    }
}

struct RobinhoodPredictsSource {
    url: Option<String>,
}

impl RobinhoodPredictsSource {
    fn new(url: Option<String>) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ExternalDataSource for RobinhoodPredictsSource {
    fn name(&self) -> &'static str {
        "robinhood_predicts"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let Some(url) = &self.url else {
            return Ok(Value::Null);
        };

        let payload = retry(
            || async {
                let resp = client
                    .get(url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(payload)
    }
}

fn header_index(headers: &StringRecord, name: &str) -> Option<usize> {
    headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case(name))
}

fn record_to_map(headers: &StringRecord, record: &StringRecord) -> Map<String, Value> {
    headers
        .iter()
        .zip(record.iter())
        .map(|(header, value)| (header.to_string(), json!(value)))
        .collect()
}

fn parse_date_any(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let formats = ["%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"]; 
    for fmt in &formats {
        if let Ok(date) = NaiveDate::parse_from_str(trimmed, fmt) {
            let dt = DateTime::<Utc>::from_utc(date.and_hms_opt(0, 0, 0)?, Utc);
            return Some(dt);
        }
    }

    None
}

fn parse_rcp_date_range(value: &str, now: DateTime<Utc>) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let cleaned = value.replace('–', "-").replace('—', "-");
    let (start_raw, end_raw) = cleaned
        .split_once('-')
        .map(|(a, b)| (a.trim(), b.trim()))
        .unwrap_or((cleaned.trim(), cleaned.trim()));

    let current_year = now.year();
    let end_date = parse_month_day_with_year(end_raw, current_year)
        .or_else(|| parse_month_day_with_year(end_raw, current_year - 1))?;

    let mut end_date = end_date;
    if end_date > now + ChronoDuration::days(30) {
        end_date = parse_month_day_with_year(end_raw, current_year - 1)?;
    }

    let start_year = if is_wraparound(start_raw, end_raw) {
        end_date.year() - 1
    } else {
        end_date.year()
    };

    let start_date = parse_month_day_with_year(start_raw, start_year).unwrap_or(end_date);
    Some((start_date, end_date))
}

fn parse_month_day_with_year(value: &str, year: i32) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let formats_with_year = ["%m/%d/%Y", "%m/%d/%y"];
    for fmt in &formats_with_year {
        if let Ok(date) = NaiveDate::parse_from_str(trimmed, fmt) {
            return Some(DateTime::<Utc>::from_utc(
                date.and_hms_opt(0, 0, 0)?,
                Utc,
            ));
        }
    }

    let full = format!("{} {}", trimmed, year);
    if let Ok(date) = NaiveDate::parse_from_str(&full, "%m/%d %Y") {
        return Some(DateTime::<Utc>::from_utc(
            date.and_hms_opt(0, 0, 0)?,
            Utc,
        ));
    }

    let full_alt = format!("{}/{}", trimmed, year);
    if let Ok(date) = NaiveDate::parse_from_str(&full_alt, "%m/%d/%Y") {
        return Some(DateTime::<Utc>::from_utc(
            date.and_hms_opt(0, 0, 0)?,
            Utc,
        ));
    }

    None
}

fn is_wraparound(start_raw: &str, end_raw: &str) -> bool {
    let Some((start_m, start_d)) = parse_month_day(start_raw) else {
        return false;
    };
    let Some((end_m, end_d)) = parse_month_day(end_raw) else {
        return false;
    };

    if end_m < start_m {
        return true;
    }
    if end_m == start_m && end_d < start_d {
        return true;
    }
    false
}

fn parse_sample_size(value: &str) -> u64 {
    let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.parse::<u64>().unwrap_or(0)
}

fn parse_percent(value: &str) -> Option<f64> {
    let cleaned = value.trim().trim_end_matches('%').trim();
    if cleaned.is_empty() {
        return None;
    }

    let numeric = cleaned.parse::<f64>().ok()?;
    let pct = if numeric > 1.0 { numeric / 100.0 } else { numeric };
    Some(pct)
}

fn is_rcp_meta_field(key: &str) -> bool {
    matches!(key, "Date" | "Poll" | "Sample" | "MoE" | "Spread")
}

fn clean_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn parse_month_day(value: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = value.trim().split('/').collect();
    if parts.len() < 2 {
        return None;
    }
    let month = parts[0].trim().parse::<u32>().ok()?;
    let day = parts[1].trim().parse::<u32>().ok()?;
    Some((month, day))
}

async fn discover_rcp_urls(client: &Client) -> Result<Vec<String>, AgentError> {
    let main_url = "https://www.realclearpolling.com/polls";
    let html = retry(
        || async {
            let resp = client
                .get(main_url)
                .header("User-Agent", "polymarket-agent/1.0")
                .send()
                .await?
                .error_for_status()?;
            Ok(resp.text().await?)
        },
        3,
    )
    .await?;

    let doc = Html::parse_document(&html);
    let selector = Selector::parse("a[href^='/polls/']").unwrap();
    let mut urls = HashSet::new();
    for link in doc.select(&selector) {
        let Some(href) = link.value().attr("href") else {
            continue;
        };
        let href_lower = href.to_lowercase();
        if !(href_lower.contains("2026")
            || href_lower.contains("2028")
            || href_lower.contains("president")
            || href_lower.contains("senate")
            || href_lower.contains("house")
            || href_lower.contains("generic"))
        {
            continue;
        }
        let full = if href.starts_with("http") {
            href.to_string()
        } else {
            format!("https://www.realclearpolling.com{}", href)
        };
        urls.insert(full);
    }

    let mut urls: Vec<String> = urls.into_iter().collect();
    urls.sort();
    if urls.len() > 10 {
        urls.truncate(10);
    }
    Ok(urls)
}

fn matches_politics_keywords(text: &str) -> bool {
    let keywords = [
        "president",
        "senate",
        "house",
        "governor",
        "election",
        "primary",
        "nomination",
        "2026",
        "2028",
    ];
    keywords.iter().any(|k| text.contains(k))
}
