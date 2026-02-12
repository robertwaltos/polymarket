use crate::config::Config;
use crate::error::AgentError;
use crate::utils::{retry, truncate_string};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Utc};
use csv::StringRecord;
use futures::future::join_all;
use log::warn;
use reqwest::Client;
use reqwest::StatusCode;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::RsaPrivateKey;
use sha2::{Digest, Sha256};
use base64::engine::general_purpose;
use base64::Engine as _;
use scraper::{Html, Selector};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use ethers_contract::Contract;
use ethers_core::abi::AbiParser;
use ethers_core::types::Address;
use ethers_providers::{Http, Provider};

const MAX_BLOB_CHARS: usize = 6000;
const MAX_RECENT_POLLS: usize = 20;
const POLL_LOOKBACK_DAYS: i64 = 30;
const MAX_SOURCE_CHARS: usize = 2500;
const MAX_ARRAY_ITEMS: usize = 40;
const MAX_OBJECT_FIELDS: usize = 40;
const MAX_STRING_CHARS: usize = 600;
const MAX_PREDICTIT_MARKETS: usize = 50;
const MAX_PREDICTIT_CONTRACTS: usize = 12;
const MAX_MANIFOLD_MARKETS: usize = 200;
const MAX_KALSHI_MARKETS: usize = 200;
const MAX_NEWS_ARTICLES: usize = 15;
const MAX_TWEETS: usize = 40;

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
        base_sources.push(Arc::new(WeatherStationsSource::new()));
        base_sources.push(Arc::new(NoaaSource::new(config.noaa_endpoint)));
        base_sources.push(Arc::new(RotowireSource::new(config.rotowire_rss)));
        base_sources.push(Arc::new(EspnSource::new(config.espn_injuries_url)));
        if config.dune_api_key.is_some() && !config.dune_query_ids.is_empty() {
            base_sources.push(Arc::new(DuneSource::new(
                config.dune_api_key.clone(),
                config.dune_query_ids.clone(),
            )));
        }
        if config.moralis_api_key.is_some() {
            base_sources.push(Arc::new(MoralisSource::new(config.moralis_api_key.clone())));
        }
        if config.x_bearer_token.is_some() {
            base_sources.push(Arc::new(XSource::new(config.x_bearer_token.clone())));
        }
        if config.news_api_key.is_some() {
            base_sources.push(Arc::new(NewsSource::new(config.news_api_key.clone())));
        }

        let kalshi_source: Arc<dyn ExternalDataSource> = Arc::new(KalshiSource::new(
            config.kalshi_api_key.clone(),
            config.kalshi_key_id.clone(),
            config.kalshi_private_key_b64.clone(),
            config.kalshi_base_url.clone(),
        ));

        let mut politics_sources: Vec<Arc<dyn ExternalDataSource>> = Vec::new();
        politics_sources.push(Arc::new(FiveThirtyEightSource::new()));
        politics_sources.push(Arc::new(RealClearPoliticsSource::new()));
        politics_sources.push(Arc::new(PredictItSource::new()));
        politics_sources.push(Arc::new(ManifoldSource::new()));
        politics_sources.push(Arc::clone(&kalshi_source));

        let mut betting_sources: Vec<Arc<dyn ExternalDataSource>> = Vec::new();
        if config.betfair_app_key.is_some() && config.betfair_session.is_some() {
            betting_sources.push(Arc::new(BetfairSource::new(
                config.betfair_app_key.clone(),
                config.betfair_session.clone(),
                config.betfair_event_type_ids.clone(),
            )));
        }
        betting_sources.push(Arc::new(CftcSource::new(config.cftc_url.clone())));
        betting_sources.push(Arc::clone(&kalshi_source));
        if config.smarkets_session.is_some() {
            betting_sources.push(Arc::new(SmarketsSource::new(config.smarkets_session.clone())));
        }
        if config.ibkr_api_key.is_some() {
            betting_sources.push(Arc::new(IbkrSource::new(config.ibkr_api_key.clone())));
        }
        if config.odds_api_key.is_some() {
            betting_sources.push(Arc::new(OddsApiSource::new(
                config.odds_api_key.clone(),
                config.odds_api_sport.clone(),
            )));
        }
        betting_sources.push(Arc::new(OddsPortalSource::new()));
        betting_sources.push(Arc::new(BetExplorerSource::new()));
        betting_sources.push(Arc::new(OddsSharkSource::new()));
        betting_sources.push(Arc::new(RobinhoodPredictsSource::new(
            config.robinhood_predicts_url.clone(),
        )));
        betting_sources.push(Arc::new(ManifoldSource::new()));
        betting_sources.push(Arc::new(AugurSource::new(
            config.augur_rpc_url.clone(),
            config.augur_contract.clone(),
            config.augur_markets_url.clone(),
        )));

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
        politics_hints: &[String],
    ) -> Result<Value, AgentError> {
        let mut sources: HashMap<String, Arc<dyn ExternalDataSource>> =
            HashMap::new();

        for source in &self.base_sources {
            sources.entry(source.name().to_string()).or_insert_with(|| Arc::clone(source));
        }
        if include_politics {
            let rcp_source = Arc::new(RealClearPoliticsSource::with_hints(
                politics_hints.to_vec(),
            ));
            sources.insert(rcp_source.name().to_string(), rcp_source);
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
                    let compacted = compact_value(&value);
                    map.insert(name.to_string(), compacted);
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

#[derive(Clone, Copy)]
struct WeatherStationSpec {
    city: &'static str,
    station_id: &'static str,
    latitude: f64,
    longitude: f64,
}

const DEFAULT_WEATHER_STATIONS: [WeatherStationSpec; 12] = [
    WeatherStationSpec { city: "Dallas", station_id: "KDAL", latitude: 32.8471, longitude: -96.8518 },
    WeatherStationSpec { city: "New York", station_id: "KJFK", latitude: 40.6413, longitude: -73.7781 },
    WeatherStationSpec { city: "Chicago", station_id: "KORD", latitude: 41.9742, longitude: -87.9073 },
    WeatherStationSpec { city: "Los Angeles", station_id: "KLAX", latitude: 33.9416, longitude: -118.4085 },
    WeatherStationSpec { city: "Miami", station_id: "KMIA", latitude: 25.7959, longitude: -80.2871 },
    WeatherStationSpec { city: "London", station_id: "EGLL", latitude: 51.4700, longitude: -0.4543 },
    WeatherStationSpec { city: "Paris", station_id: "LFPG", latitude: 49.0097, longitude: 2.5479 },
    WeatherStationSpec { city: "Tokyo", station_id: "RJTT", latitude: 35.5494, longitude: 139.7798 },
    WeatherStationSpec { city: "Sydney", station_id: "YSSY", latitude: -33.9399, longitude: 151.1753 },
    WeatherStationSpec { city: "Buenos Aires", station_id: "SABE", latitude: -34.5592, longitude: -58.4156 },
    WeatherStationSpec { city: "Toronto", station_id: "CYYZ", latitude: 43.6777, longitude: -79.6248 },
    WeatherStationSpec { city: "Berlin", station_id: "EDDB", latitude: 52.3667, longitude: 13.5033 },
];

struct WeatherStationsSource {
    stations: Vec<WeatherStationSpec>,
}

impl WeatherStationsSource {
    fn new() -> Self {
        Self {
            stations: DEFAULT_WEATHER_STATIONS.to_vec(),
        }
    }
}

#[async_trait]
impl ExternalDataSource for WeatherStationsSource {
    fn name(&self) -> &'static str {
        "noaa_station_observations"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let futures = self.stations.iter().copied().map(|spec| {
            let client = client.clone();
            async move {
                let url = format!(
                    "https://api.weather.gov/stations/{}/observations/latest",
                    spec.station_id
                );
                let resp = client
                    .get(&url)
                    .header("User-Agent", "polymarket-agent/1.0")
                    .send()
                    .await?;
                if resp.status().is_success() {
                    let payload: Value = resp.json().await?;
                    return Ok(weather_station_observation_from_noaa(spec, &payload));
                }

                let fallback_url = format!(
                    "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&current=temperature_2m,dew_point_2m,surface_pressure,wind_speed_10m,cloud_cover&temperature_unit=fahrenheit&windspeed_unit=mph",
                    spec.latitude, spec.longitude
                );
                let fallback = client.get(&fallback_url).send().await?;
                if !fallback.status().is_success() {
                    return Err(AgentError::BadResponse(format!(
                        "{} fallback status {}",
                        spec.station_id,
                        fallback.status()
                    )));
                }
                let payload: Value = fallback.json().await?;
                Ok(weather_station_observation_from_open_meteo(spec, &payload))
            }
        });

        let mut stations = Vec::new();
        for result in join_all(futures).await {
            if let Ok(row) = result {
                stations.push(row);
            }
        }

        if stations.is_empty() {
            return Ok(Value::Null);
        }

        Ok(json!({
            "updated_at": Utc::now().to_rfc3339(),
            "count": stations.len(),
            "stations": stations
        }))
    }
}

fn weather_station_observation_from_noaa(spec: WeatherStationSpec, payload: &Value) -> Value {
    let temp_c = payload["properties"]["temperature"]["value"].as_f64();
    let temp_f = temp_c.map(|c| (c * 9.0 / 5.0) + 32.0);
    let dewpoint_c = payload["properties"]["dewpoint"]["value"].as_f64();
    let dewpoint_f = dewpoint_c.map(|c| (c * 9.0 / 5.0) + 32.0);
    let wind_m_s = payload["properties"]["windSpeed"]["value"].as_f64();
    let wind_mph = wind_m_s.map(|mps| mps * 2.23694);
    let pressure_pa = payload["properties"]["barometricPressure"]["value"].as_f64();
    let pressure_hpa = pressure_pa.map(|pa| pa / 100.0);
    let cloud_layers = payload["properties"]["cloudLayers"]
        .as_array()
        .map(|layers| layers.len())
        .unwrap_or(0);

    json!({
        "city": spec.city,
        "station_id": spec.station_id,
        "source": "weather.gov",
        "latitude": spec.latitude,
        "longitude": spec.longitude,
        "observed_at": payload["properties"]["timestamp"],
        "temperature_c": temp_c,
        "temperature_f": temp_f,
        "dewpoint_f": dewpoint_f,
        "wind_mph": wind_mph,
        "pressure_hpa": pressure_hpa,
        "cloud_layers": cloud_layers,
        "text_description": payload["properties"]["textDescription"],
    })
}

fn weather_station_observation_from_open_meteo(
    spec: WeatherStationSpec,
    payload: &Value,
) -> Value {
    let temp_f = payload["current"]["temperature_2m"].as_f64();
    let dewpoint_f = payload["current"]["dew_point_2m"].as_f64();
    let wind_mph = payload["current"]["wind_speed_10m"].as_f64();
    let pressure_hpa = payload["current"]["surface_pressure"].as_f64();
    let cloud_cover = payload["current"]["cloud_cover"].as_f64();
    json!({
        "city": spec.city,
        "station_id": spec.station_id,
        "source": "open-meteo",
        "latitude": spec.latitude,
        "longitude": spec.longitude,
        "observed_at": payload["current"]["time"],
        "temperature_c": temp_f.map(|f| (f - 32.0) * 5.0 / 9.0),
        "temperature_f": temp_f,
        "dewpoint_f": dewpoint_f,
        "wind_mph": wind_mph,
        "pressure_hpa": pressure_hpa,
        "cloud_layers": cloud_cover,
        "text_description": Value::Null,
    })
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
        let mut value: Value =
            serde_json::from_str(&text).unwrap_or(Value::String(truncate_string(
                &text,
                MAX_BLOB_CHARS,
            )));

        if let Some(periods) = value["properties"]["periods"].as_array() {
            let trimmed = periods
                .iter()
                .take(6)
                .map(|period| {
                    json!({
                        "name": period["name"],
                        "startTime": period["startTime"],
                        "temperature": period["temperature"],
                        "shortForecast": period["shortForecast"],
                        "windSpeed": period["windSpeed"]
                    })
                })
                .collect::<Vec<_>>();
            value = json!({ "periods": trimmed });
        }

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
            .unwrap_or_else(|| "https://www.rotowire.com/rss/news.php?sport=NBA".to_string());
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
            return Ok(json!({
                "parse_failed": true,
                "rows": []
            }));
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
                    let resp = client.get(&url).send().await?;
                    if is_optional_http_status(resp.status()) {
                        return Ok(Value::Null);
                    }
                    let resp = resp.error_for_status()?;
                    Ok(resp.json::<Value>().await?)
                },
                3,
            )
            .await?;
            if result.is_null() {
                return Ok(Value::Null);
            }
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
                    .await?;
                if is_optional_http_status(resp.status()) {
                    return Ok(Value::Null);
                }
                let resp = resp.error_for_status()?;
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
                    .await?;
                if is_optional_http_status(resp.status()) {
                    return Ok(Value::Null);
                }
                let resp = resp.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;
        if response.is_null() {
            return Ok(Value::Null);
        }

        let tweets = response["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .take(MAX_TWEETS)
            .map(|tweet| {
                json!({
                    "text": truncate_string(tweet["text"].as_str().unwrap_or(""), MAX_STRING_CHARS),
                    "metrics": tweet["public_metrics"]
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({ "tweets": tweets }))
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
                let resp = client.get(&url).send().await?;
                if is_optional_http_status(resp.status()) {
                    return Ok(Value::Null);
                }
                let resp = resp.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;
        if response.is_null() {
            return Ok(Value::Null);
        }

        let articles = response["articles"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .take(MAX_NEWS_ARTICLES)
            .map(|article| {
                json!({
                    "title": truncate_string(article["title"].as_str().unwrap_or(""), MAX_STRING_CHARS),
                    "source": article["source"]["name"].as_str().unwrap_or(""),
                    "publishedAt": article["publishedAt"].as_str().unwrap_or("")
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({ "articles": articles }))
    }
}

struct FiveThirtyEightSource {
    urls: Vec<&'static str>,
}

impl FiveThirtyEightSource {
    fn new() -> Self {
        Self {
            urls: vec![
                "https://projects.fivethirtyeight.com/polls-page/data/president_polls.csv",
                "https://projects.fivethirtyeight.com/polls-page/data/senate_polls.csv",
                "https://projects.fivethirtyeight.com/polls-page/data/house_polls.csv",
                "https://projects.fivethirtyeight.com/polls-page/data/generic_ballot_polls.csv",
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
    hints: Vec<String>,
}

impl RealClearPoliticsSource {
    fn new() -> Self {
        Self { hints: Vec::new() }
    }

    fn with_hints(hints: Vec<String>) -> Self {
        Self { hints }
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

        let urls = discover_rcp_urls(client, self.hints.as_slice()).await?;
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
                .take(MAX_PREDICTIT_CONTRACTS)
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

            if filtered.len() >= MAX_PREDICTIT_MARKETS {
                break;
            }
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
                "liquidity": market["totalLiquidity"],
                "volume": market["volume"],
                "closeTime": market["closeTime"]
            }));

            if filtered.len() >= MAX_MANIFOLD_MARKETS {
                break;
            }
        }

        Ok(json!({ "markets": filtered }))
    }
}

struct AugurSource {
    rpc_url: Option<String>,
    contract: Option<String>,
    markets_url: Option<String>,
}

impl AugurSource {
    fn new(rpc_url: Option<String>, contract: Option<String>, markets_url: Option<String>) -> Self {
        Self {
            rpc_url,
            contract,
            markets_url,
        }
    }
}

#[async_trait]
impl ExternalDataSource for AugurSource {
    fn name(&self) -> &'static str {
        "augur_markets"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        if let Some(url) = &self.markets_url {
            let payload = retry(
                || async {
                    let resp = client.get(url).send().await?.error_for_status()?;
                    Ok(resp.json::<Value>().await?)
                },
                3,
            )
            .await?;
            return Ok(payload);
        }

        let (Some(rpc_url), Some(contract)) = (&self.rpc_url, &self.contract) else {
            return Ok(Value::Null);
        };

        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| AgentError::Internal(e.to_string()))?;
        let abi = AbiParser::default()
            .parse_str("function getMarkets() view returns (address[])")
            .map_err(|e| AgentError::Internal(e.to_string()))?;
        let address: Address = contract
            .parse::<Address>()
            .map_err(|e| AgentError::Internal(e.to_string()))?;

        let contract = Contract::new(address, abi, Arc::new(provider));
        let markets: Vec<Address> = contract
            .method::<_, Vec<Address>>("getMarkets", ())
            .map_err(|e| AgentError::Internal(e.to_string()))?
            .call()
            .await
            .map_err(|e| AgentError::Internal(e.to_string()))?;

        let formatted = markets
            .into_iter()
            .map(|addr| format!("{:?}", addr))
            .collect::<Vec<_>>();

        Ok(json!({ "markets": formatted }))
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
        let default_url = "https://www.cftc.gov/dea/newcot/f_disagg.txt".to_string();
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
    key_id: Option<String>,
    private_key_b64: Option<String>,
    base_url: String,
}

impl KalshiSource {
    fn new(
        api_key: Option<String>,
        key_id: Option<String>,
        private_key_b64: Option<String>,
        base_url: String,
    ) -> Self {
        Self {
            api_key,
            key_id,
            private_key_b64,
            base_url,
        }
    }
}

#[async_trait]
impl ExternalDataSource for KalshiSource {
    fn name(&self) -> &'static str {
        "kalshi_markets"
    }

    async fn fetch(&self, client: &Client) -> Result<Value, AgentError> {
        let path = "/trade-api/v2/markets";
        let url = format!(
            "{}{}?status=open&limit=1000",
            self.base_url, path
        );

        let key_id = self.key_id.clone().or(self.api_key.clone());
        if let (Some(key_id), Some(priv_raw)) = (key_id, &self.private_key_b64) {
            let private_key = parse_kalshi_private_key(priv_raw)?;

            let timestamp = Utc::now().timestamp_millis();
            let sign_str = format!("{}{}{}", timestamp, "GET", path);
            let mut hasher = Sha256::new();
            hasher.update(sign_str.as_bytes());
            let hash = hasher.finalize();
            let mut rng = rand::rngs::OsRng;
            let signature = private_key
                .sign_with_rng(&mut rng, rsa::Pss::new::<Sha256>(), &hash)
                .map_err(|e| AgentError::Parse(format!("kalshi sign: {}", e)))?;
            let sig_b64 = general_purpose::STANDARD.encode(signature);

            let payload = retry(
                || async {
                    let resp = client
                        .get(&url)
                        .header("KALSHI-ACCESS-KEY", key_id.clone())
                        .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
                        .header("KALSHI-ACCESS-SIGNATURE", sig_b64.clone())
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(resp.json::<Value>().await?)
                },
                3,
            )
            .await?;

            return Ok(format_kalshi_markets(payload));
        }

        let payload = retry(
            || async {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json::<Value>().await?)
            },
            3,
        )
        .await?;

        Ok(format_kalshi_markets(payload))
    }
}

fn parse_kalshi_private_key(raw: &str) -> Result<RsaPrivateKey, AgentError> {
    if raw.contains("BEGIN RSA PRIVATE KEY") {
        return RsaPrivateKey::from_pkcs1_pem(raw)
            .map_err(|e| AgentError::Parse(format!("kalshi key parse: {}", e)));
    }
    if raw.contains("BEGIN PRIVATE KEY") {
        return RsaPrivateKey::from_pkcs8_pem(raw)
            .map_err(|e| AgentError::Parse(format!("kalshi key parse: {}", e)));
    }
    if raw.contains("BEGIN") {
        let pkcs8 = RsaPrivateKey::from_pkcs8_pem(raw);
        if let Ok(key) = pkcs8 {
            return Ok(key);
        }
        let pkcs1 = RsaPrivateKey::from_pkcs1_pem(raw);
        if let Ok(key) = pkcs1 {
            return Ok(key);
        }
        return Err(AgentError::Parse("kalshi key parse: unsupported PEM label".to_string()));
    }

    let decoded = general_purpose::STANDARD
        .decode(raw)
        .map_err(|e| AgentError::Parse(format!("kalshi key decode: {}", e)))?;

    if let Ok(pem) = std::str::from_utf8(&decoded) {
        if pem.contains("BEGIN RSA PRIVATE KEY") {
            return RsaPrivateKey::from_pkcs1_pem(pem)
                .map_err(|e| AgentError::Parse(format!("kalshi key parse: {}", e)));
        }
        if pem.contains("BEGIN PRIVATE KEY") {
            return RsaPrivateKey::from_pkcs8_pem(pem)
                .map_err(|e| AgentError::Parse(format!("kalshi key parse: {}", e)));
        }
        if pem.contains("BEGIN") {
            let pkcs8 = RsaPrivateKey::from_pkcs8_pem(pem);
            if let Ok(key) = pkcs8 {
                return Ok(key);
            }
            let pkcs1 = RsaPrivateKey::from_pkcs1_pem(pem);
            if let Ok(key) = pkcs1 {
                return Ok(key);
            }
        }
    }

    if let Ok(key) = RsaPrivateKey::from_pkcs8_der(&decoded) {
        return Ok(key);
    }
    if let Ok(key) = RsaPrivateKey::from_pkcs1_der(&decoded) {
        return Ok(key);
    }
    Err(AgentError::Parse("kalshi key parse: unsupported key format".to_string()))
}

fn format_kalshi_markets(payload: Value) -> Value {
    let markets = payload["markets"].as_array().cloned().unwrap_or_default();
    let mut filtered = Vec::new();
    for market in markets {
        let category = market["category"]
            .as_str()
            .unwrap_or("")
            .to_lowercase();
        if !(category.contains("politics")
            || category.contains("weather")
            || category.contains("sports")
            || category.contains("crypto"))
        {
            continue;
        }

        let yes_raw = market["yes_bid"]
            .as_f64()
            .or_else(|| market["yes_ask"].as_f64())
            .or_else(|| market["last_price"].as_f64())
            .unwrap_or(0.0);
        if yes_raw <= 0.0 {
            continue;
        }
        let yes_prob = if yes_raw > 1.0 { yes_raw / 100.0 } else { yes_raw };

        filtered.push(json!({
            "ticker": market["ticker"],
            "title": market["title"],
            "subtitle": market["subtitle"],
            "yes_prob": yes_prob,
            "volume_24h": market["volume_24h"],
            "liquidity": market["liquidity"]
        }));

        if filtered.len() >= MAX_KALSHI_MARKETS {
            break;
        }
    }

    json!({ "markets": filtered })
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
            return Ok(json!({
                "parse_failed": true,
                "rows": []
            }));
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
        let url = "https://www.betexplorer.com/";
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
            return Ok(json!({
                "parse_failed": true,
                "rows": []
            }));
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
            return Ok(json!({
                "parse_failed": true,
                "rows": []
            }));
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

fn compact_value(value: &Value) -> Value {
    let compacted = compact_value_inner(value);
    let serialized = serde_json::to_string(&compacted).unwrap_or_default();
    if serialized.len() > MAX_SOURCE_CHARS {
        Value::String(truncate_string(&serialized, MAX_SOURCE_CHARS))
    } else {
        compacted
    }
}

fn compact_value_inner(value: &Value) -> Value {
    match value {
        Value::String(text) => Value::String(truncate_string(text, MAX_STRING_CHARS)),
        Value::Array(items) => {
            let trimmed = items
                .iter()
                .take(MAX_ARRAY_ITEMS)
                .map(compact_value_inner)
                .collect::<Vec<_>>();
            Value::Array(trimmed)
        }
        Value::Object(map) => {
            let mut trimmed = Map::new();
            for (key, val) in map.iter().take(MAX_OBJECT_FIELDS) {
                trimmed.insert(key.clone(), compact_value_inner(val));
            }
            Value::Object(trimmed)
        }
        other => other.clone(),
    }
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

async fn discover_rcp_urls(
    client: &Client,
    hints: &[String],
) -> Result<Vec<String>, AgentError> {
    let main_url = "https://www.realclearpolling.com/latest-polls";
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
    let selectors = [
        Selector::parse("a[href^='/latest-polls/']").unwrap(),
        Selector::parse("a[href^='/polls/']").unwrap(),
    ];
    let mut urls = HashSet::new();
    for selector in &selectors {
        for link in doc.select(selector) {
            let Some(href) = link.value().attr("href") else {
                continue;
            };
            let href_lower = href.to_lowercase();
            if !(href_lower.contains("2026")
                || href_lower.contains("2028")
                || href_lower.contains("president")
                || href_lower.contains("senate")
                || href_lower.contains("house")
                || href_lower.contains("generic")
                || href_lower.contains("election")
                || href_lower.contains("governor"))
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
    }

    let mut urls: Vec<String> = urls.into_iter().collect();
    urls.sort();
    if urls.is_empty() {
        urls.push(main_url.to_string());
    }

    if !hints.is_empty() {
        let hint_set = hints
            .iter()
            .map(|h| h.to_lowercase())
            .collect::<Vec<_>>();
        let filtered = urls
            .iter()
            .cloned()
            .filter(|url| {
                let lower = url.to_lowercase();
                hint_set.iter().any(|hint| lower.contains(hint))
            })
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            urls = filtered;
        }
    }

    if urls.len() > 8 {
        urls.truncate(8);
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

fn is_optional_http_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND
            | StatusCode::PAYMENT_REQUIRED
            | StatusCode::TOO_MANY_REQUESTS
    )
}
