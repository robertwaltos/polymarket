use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SocialSource {
    X,
    Reddit,
    Manual,
}

impl SocialSource {
    pub fn code(&self) -> &'static str {
        match self {
            Self::X => "x",
            Self::Reddit => "reddit",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocialClaim {
    pub id: Uuid,
    pub source: SocialSource,
    pub query: Option<String>,
    pub author: Option<String>,
    pub community: Option<String>,
    pub posted_at: Option<DateTime<Utc>>,
    pub captured_at: DateTime<Utc>,
    pub url: Option<String>,
    pub text: String,
    pub brag_score: f64,
    pub hype_score: f64,
    pub evidence_flags: Vec<String>,
    pub strategy_tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManualClaimInput {
    #[serde(default)]
    pub source: Option<SocialSource>,
    pub text: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub community: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub posted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SocialScanRequest {
    #[serde(default)]
    pub sources: Option<Vec<SocialSource>>,
    #[serde(default)]
    pub queries: Option<Vec<String>>,
    #[serde(default)]
    pub lookback_days: Option<i64>,
    #[serde(default)]
    pub max_per_query: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceScanStatus {
    pub source: SocialSource,
    pub ok: bool,
    pub inserted: usize,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TagSignal {
    pub tag: String,
    pub mentions: usize,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SocialScanResult {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub inserted: usize,
    pub total_claims: usize,
    pub source_status: Vec<SourceScanStatus>,
    pub top_tags: Vec<TagSignal>,
}

#[derive(Debug, Clone)]
pub struct SocialScanOutcome {
    pub result: SocialScanResult,
    pub inserted_claims: Vec<SocialClaim>,
}

#[derive(Clone)]
pub struct SocialIntelStore {
    client: reqwest::Client,
    claims: Arc<Mutex<Vec<SocialClaim>>>,
    max_claims: usize,
    x_bearer_token: Option<String>,
    default_queries: Vec<String>,
}

impl SocialIntelStore {
    pub fn new_from_env() -> Self {
        let max_claims = env_u64("SOCIAL_INTEL_MAX_CLAIMS", 8_000) as usize;
        let default_queries = env_csv("SOCIAL_INTEL_QUERIES");
        let default_queries = if default_queries.is_empty() {
            vec![
                "multi agent trading".to_string(),
                "autonomous trading agent".to_string(),
                "LLM trading bot live".to_string(),
                "portfolio of agents trading".to_string(),
                "agentic trading system".to_string(),
            ]
        } else {
            default_queries
        };

        Self {
            client: reqwest::Client::builder()
                .user_agent("atlas-trading-intel/0.1")
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            claims: Arc::new(Mutex::new(Vec::new())),
            max_claims: max_claims.max(100),
            x_bearer_token: std::env::var("X_API_BEARER_TOKEN")
                .ok()
                .or_else(|| std::env::var("X_BEARER_TOKEN").ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            default_queries,
        }
    }

    pub fn claim_count(&self) -> Result<usize> {
        let guard = self
            .claims
            .lock()
            .map_err(|_| anyhow!("social intel lock poisoned"))?;
        Ok(guard.len())
    }

    pub fn replace_claims(&self, mut claims: Vec<SocialClaim>) -> Result<()> {
        let mut guard = self
            .claims
            .lock()
            .map_err(|_| anyhow!("social intel lock poisoned"))?;
        claims.sort_by(|a, b| b.captured_at.cmp(&a.captured_at));
        claims.truncate(self.max_claims);
        *guard = claims;
        Ok(())
    }

    pub fn list_claims(
        &self,
        limit: usize,
        source: Option<SocialSource>,
    ) -> Result<Vec<SocialClaim>> {
        let guard = self
            .claims
            .lock()
            .map_err(|_| anyhow!("social intel lock poisoned"))?;
        let mut claims: Vec<SocialClaim> = guard
            .iter()
            .filter(|claim| {
                source
                    .as_ref()
                    .map(|target| target == &claim.source)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        claims.sort_by(|a, b| b.captured_at.cmp(&a.captured_at));
        claims.truncate(limit.clamp(1, 1_000));
        Ok(claims)
    }

    pub fn ingest_manual_claim(&self, input: ManualClaimInput) -> Result<SocialClaim> {
        let text =
            normalize_text(&input.text).ok_or_else(|| anyhow!("claim text cannot be empty"))?;
        let source = input.source.unwrap_or(SocialSource::Manual);
        let claim = SocialClaim {
            id: Uuid::new_v4(),
            source,
            query: normalize_text_opt(input.query),
            author: normalize_text_opt(input.author),
            community: normalize_text_opt(input.community),
            posted_at: input.posted_at,
            captured_at: Utc::now(),
            url: normalize_text_opt(input.url),
            brag_score: compute_brag_score(&text),
            hype_score: compute_hype_score(&text),
            evidence_flags: compute_evidence_flags(&text),
            strategy_tags: extract_strategy_tags(&text),
            text,
        };
        let _ = self.insert_claims(vec![claim.clone()])?;
        Ok(claim)
    }

    pub async fn run_scan(&self, request: SocialScanRequest) -> Result<SocialScanOutcome> {
        let started_at = Utc::now();
        let lookback_days = request.lookback_days.unwrap_or(4).clamp(1, 14);
        let max_per_query = request.max_per_query.unwrap_or(20).clamp(3, 100);
        let queries = normalize_queries(request.queries.unwrap_or_default(), &self.default_queries);
        let sources = if let Some(mut provided) = request.sources {
            provided.retain(|source| !matches!(source, SocialSource::Manual));
            if provided.is_empty() {
                vec![SocialSource::X, SocialSource::Reddit]
            } else {
                provided
            }
        } else {
            vec![SocialSource::X, SocialSource::Reddit]
        };

        let mut source_status = Vec::new();
        let mut inserted_total = 0usize;
        let mut inserted_claims = Vec::new();
        for source in sources {
            match source {
                SocialSource::X => {
                    match self
                        .scan_x_recent(&queries, lookback_days, max_per_query)
                        .await
                    {
                        Ok(claims) => {
                            let inserted = self.insert_claims(claims)?;
                            inserted_total += inserted.len();
                            inserted_claims.extend(inserted.clone());
                            source_status.push(SourceScanStatus {
                                source: SocialSource::X,
                                ok: true,
                                inserted: inserted.len(),
                                detail: None,
                            });
                        }
                        Err(err) => {
                            source_status.push(SourceScanStatus {
                                source: SocialSource::X,
                                ok: false,
                                inserted: 0,
                                detail: Some(err.to_string()),
                            });
                        }
                    }
                }
                SocialSource::Reddit => {
                    match self
                        .scan_reddit_recent(&queries, lookback_days, max_per_query)
                        .await
                    {
                        Ok(claims) => {
                            let inserted = self.insert_claims(claims)?;
                            inserted_total += inserted.len();
                            inserted_claims.extend(inserted.clone());
                            source_status.push(SourceScanStatus {
                                source: SocialSource::Reddit,
                                ok: true,
                                inserted: inserted.len(),
                                detail: None,
                            });
                        }
                        Err(err) => {
                            source_status.push(SourceScanStatus {
                                source: SocialSource::Reddit,
                                ok: false,
                                inserted: 0,
                                detail: Some(err.to_string()),
                            });
                        }
                    }
                }
                SocialSource::Manual => {}
            }
        }

        let top_tags = self.tag_signals(24, 12)?;
        let total_claims = self.claim_count()?;
        Ok(SocialScanOutcome {
            result: SocialScanResult {
                started_at,
                finished_at: Utc::now(),
                inserted: inserted_total,
                total_claims,
                source_status,
                top_tags,
            },
            inserted_claims,
        })
    }

    pub fn tag_signals(&self, window_hours: i64, top_n: usize) -> Result<Vec<TagSignal>> {
        let cutoff = Utc::now() - chrono::Duration::hours(window_hours.clamp(1, 24 * 30));
        let guard = self
            .claims
            .lock()
            .map_err(|_| anyhow!("social intel lock poisoned"))?;
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut brag_totals: HashMap<String, f64> = HashMap::new();
        let mut hype_totals: HashMap<String, f64> = HashMap::new();
        for claim in guard.iter() {
            let reference_time = claim.posted_at.unwrap_or(claim.captured_at);
            if reference_time < cutoff {
                continue;
            }
            for tag in &claim.strategy_tags {
                *counts.entry(tag.clone()).or_insert(0) += 1;
                *brag_totals.entry(tag.clone()).or_insert(0.0) += claim.brag_score;
                *hype_totals.entry(tag.clone()).or_insert(0.0) += claim.hype_score;
            }
        }

        let mut out: Vec<TagSignal> = counts
            .into_iter()
            .map(|(tag, mentions)| {
                let brag = brag_totals.get(&tag).copied().unwrap_or(0.0);
                let hype = hype_totals.get(&tag).copied().unwrap_or(0.0);
                let mentions_f = mentions as f64;
                let weight =
                    mentions_f + (brag / mentions_f.max(1.0)) - (0.5 * hype / mentions_f.max(1.0));
                TagSignal {
                    tag,
                    mentions,
                    weight,
                }
            })
            .collect();
        out.sort_by(|a, b| {
            b.weight
                .partial_cmp(&a.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(top_n.max(1));
        Ok(out)
    }

    pub fn tag_weights(&self, window_hours: i64) -> Result<HashMap<String, f64>> {
        let signals = self.tag_signals(window_hours, 100)?;
        let mut out = HashMap::new();
        for signal in signals {
            out.insert(signal.tag, signal.weight);
        }
        Ok(out)
    }

    fn insert_claims(&self, mut incoming: Vec<SocialClaim>) -> Result<Vec<SocialClaim>> {
        if incoming.is_empty() {
            return Ok(Vec::new());
        }
        let mut guard = self
            .claims
            .lock()
            .map_err(|_| anyhow!("social intel lock poisoned"))?;
        let mut keys: HashSet<String> = guard.iter().map(claim_dedup_key).collect();
        let mut inserted = Vec::new();
        incoming.sort_by(|a, b| b.captured_at.cmp(&a.captured_at));
        for claim in incoming {
            let key = claim_dedup_key(&claim);
            if keys.contains(&key) {
                continue;
            }
            keys.insert(key);
            guard.push(claim.clone());
            inserted.push(claim);
        }
        guard.sort_by(|a, b| b.captured_at.cmp(&a.captured_at));
        guard.truncate(self.max_claims);
        Ok(inserted)
    }

    async fn scan_x_recent(
        &self,
        queries: &[String],
        lookback_days: i64,
        max_per_query: usize,
    ) -> Result<Vec<SocialClaim>> {
        let Some(token) = self.x_bearer_token.as_deref() else {
            return Err(anyhow!(
                "X bearer token missing. set X_API_BEARER_TOKEN in environment"
            ));
        };

        let start_time = (Utc::now() - chrono::Duration::days(lookback_days))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let mut claims = Vec::new();
        for query in queries {
            let params = [
                ("query", format!("{query} lang:en -is:retweet")),
                ("max_results", max_per_query.to_string()),
                ("start_time", start_time.clone()),
                (
                    "tweet.fields",
                    "created_at,public_metrics,lang,author_id".to_string(),
                ),
                ("expansions", "author_id".to_string()),
                (
                    "user.fields",
                    "username,name,verified,public_metrics".to_string(),
                ),
            ];
            let response = self
                .client
                .get("https://api.x.com/2/tweets/search/recent")
                .bearer_auth(token)
                .query(&params)
                .send()
                .await
                .context("failed to call X search endpoint")?;
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read response".to_string());
            if !status.is_success() {
                return Err(anyhow!(
                    "X scan failed with {}: {}",
                    status.as_u16(),
                    summarize_error_body(&body)
                ));
            }
            let parsed: serde_json::Value = serde_json::from_str(&body)
                .with_context(|| "invalid JSON from X endpoint".to_string())?;
            let mut users = HashMap::<String, String>::new();
            if let Some(includes) = parsed
                .get("includes")
                .and_then(|v| v.get("users"))
                .and_then(|v| v.as_array())
            {
                for user in includes {
                    if let (Some(id), Some(username)) = (
                        user.get("id").and_then(|v| v.as_str()),
                        user.get("username").and_then(|v| v.as_str()),
                    ) {
                        users.insert(id.to_string(), username.to_string());
                    }
                }
            }
            let Some(data) = parsed.get("data").and_then(|v| v.as_array()) else {
                continue;
            };
            for tweet in data {
                let text = tweet
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if text.is_empty() {
                    continue;
                }
                let id = tweet.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let author_id = tweet
                    .get("author_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let author_username = users.get(author_id).cloned();
                let url = if !id.is_empty() {
                    if let Some(username) = &author_username {
                        Some(format!("https://x.com/{username}/status/{id}"))
                    } else {
                        Some(format!("https://x.com/i/web/status/{id}"))
                    }
                } else {
                    None
                };
                let posted_at = tweet
                    .get("created_at")
                    .and_then(|v| v.as_str())
                    .and_then(parse_datetime);
                claims.push(build_claim(
                    SocialSource::X,
                    text,
                    url,
                    query.clone(),
                    author_username,
                    None,
                    posted_at,
                ));
            }
        }
        Ok(claims)
    }

    async fn scan_reddit_recent(
        &self,
        queries: &[String],
        lookback_days: i64,
        max_per_query: usize,
    ) -> Result<Vec<SocialClaim>> {
        let cutoff = Utc::now() - chrono::Duration::days(lookback_days);
        let mut claims = Vec::new();
        for query in queries {
            let params = [
                ("q", query.to_string()),
                ("sort", "new".to_string()),
                ("t", "week".to_string()),
                ("limit", max_per_query.to_string()),
            ];
            let response = self
                .client
                .get("https://www.reddit.com/search.json")
                .query(&params)
                .header("accept", "application/json")
                .send()
                .await
                .context("failed to call reddit search endpoint")?;
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read response".to_string());
            if !status.is_success() {
                let prefix = if status == StatusCode::TOO_MANY_REQUESTS {
                    "reddit rate limited"
                } else {
                    "reddit scan failed"
                };
                return Err(anyhow!(
                    "{prefix} {}: {}",
                    status.as_u16(),
                    summarize_error_body(&body)
                ));
            }
            let parsed: serde_json::Value = serde_json::from_str(&body)
                .with_context(|| "invalid JSON from reddit endpoint".to_string())?;
            let Some(children) = parsed
                .get("data")
                .and_then(|v| v.get("children"))
                .and_then(|v| v.as_array())
            else {
                continue;
            };
            for child in children {
                let Some(data) = child.get("data") else {
                    continue;
                };
                let created_utc = data
                    .get("created_utc")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let posted_at = Utc.timestamp_opt(created_utc as i64, 0).single();
                if let Some(posted) = posted_at {
                    if posted < cutoff {
                        continue;
                    }
                }
                let title = data
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim();
                let selftext = data
                    .get("selftext")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim();
                let text = if selftext.is_empty() {
                    title.to_string()
                } else {
                    format!("{title}\n\n{selftext}")
                };
                if text.trim().is_empty() {
                    continue;
                }
                let permalink = data
                    .get("permalink")
                    .and_then(|v| v.as_str())
                    .map(|p| format!("https://www.reddit.com{p}"));
                let author = data
                    .get("author")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string());
                let community = data
                    .get("subreddit")
                    .and_then(|v| v.as_str())
                    .map(|v| format!("r/{v}"));
                claims.push(build_claim(
                    SocialSource::Reddit,
                    text,
                    permalink,
                    query.clone(),
                    author,
                    community,
                    posted_at,
                ));
            }
        }
        Ok(claims)
    }
}

fn build_claim(
    source: SocialSource,
    text: String,
    url: Option<String>,
    query: String,
    author: Option<String>,
    community: Option<String>,
    posted_at: Option<DateTime<Utc>>,
) -> SocialClaim {
    let cleaned_text = sanitize_text(&text);
    SocialClaim {
        id: Uuid::new_v4(),
        source,
        query: normalize_text_opt(Some(query)),
        author: normalize_text_opt(author),
        community: normalize_text_opt(community),
        posted_at,
        captured_at: Utc::now(),
        url: normalize_text_opt(url),
        brag_score: compute_brag_score(&cleaned_text),
        hype_score: compute_hype_score(&cleaned_text),
        evidence_flags: compute_evidence_flags(&cleaned_text),
        strategy_tags: extract_strategy_tags(&cleaned_text),
        text: cleaned_text,
    }
}

fn sanitize_text(text: &str) -> String {
    let compact = text.replace("\r", "").replace("\t", " ");
    compact
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn extract_strategy_tags(text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let mut tags = Vec::new();
    push_tag_if_any(
        &mut tags,
        &lower,
        "multi_agent",
        &["multi agent", "swarm", "orchestrator", "team of agents"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "momentum",
        &["momentum", "trend", "breakout"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "mean_reversion",
        &["mean reversion", "revert", "pairs trade"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "market_making",
        &["market making", "maker", "spread capture", "inventory risk"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "cross_venue",
        &["cross exchange", "cross venue", "arbitrage", "latency arb"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "event_driven",
        &["event", "news", "earnings", "kalshi", "prediction market"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "sentiment",
        &["sentiment", "social", "x data", "reddit"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "options_volatility",
        &["iv", "volatility", "options", "straddle", "gamma"],
    );
    push_tag_if_any(
        &mut tags,
        &lower,
        "risk_controls",
        &["drawdown", "risk", "kill switch", "guard", "stop loss"],
    );
    tags
}

fn compute_brag_score(text: &str) -> f64 {
    let lower = text.to_ascii_lowercase();
    let brag_terms = [
        "live pnl",
        "outperformed",
        "alpha",
        "winning",
        "profit",
        "real money",
        "production",
        "fully automated",
        "24/7",
    ];
    let mut score = 0.0;
    for term in brag_terms {
        if lower.contains(term) {
            score += 1.0;
        }
    }
    if lower.contains('%') {
        score += 0.6;
    }
    if lower.contains('$') {
        score += 0.4;
    }
    score
}

fn compute_hype_score(text: &str) -> f64 {
    let lower = text.to_ascii_lowercase();
    let hype_terms = [
        "100x",
        "moon",
        "guaranteed",
        "no risk",
        "infinite money",
        "get rich",
        "easy profits",
    ];
    let mut score = 0.0;
    for term in hype_terms {
        if lower.contains(term) {
            score += 1.0;
        }
    }
    score
}

fn compute_evidence_flags(text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let mut out = Vec::new();
    for (needle, flag) in [
        ("sharpe", "mentions_sharpe"),
        ("drawdown", "mentions_drawdown"),
        ("backtest", "mentions_backtest"),
        ("paper trade", "mentions_paper"),
        ("live", "mentions_live"),
        ("trade count", "mentions_trade_count"),
    ] {
        if lower.contains(needle) {
            out.push(flag.to_string());
        }
    }
    if lower.contains('%') || lower.contains("pnl") {
        out.push("mentions_performance_stats".to_string());
    }
    out
}

fn push_tag_if_any(tags: &mut Vec<String>, lower_text: &str, tag: &str, needles: &[&str]) {
    for needle in needles {
        if lower_text.contains(needle) {
            if !tags.iter().any(|existing| existing == tag) {
                tags.push(tag.to_string());
            }
            return;
        }
    }
}

fn claim_dedup_key(claim: &SocialClaim) -> String {
    let url = claim.url.clone().unwrap_or_default().to_ascii_lowercase();
    let text = claim.text.trim().to_ascii_lowercase();
    format!("{}|{}|{}", claim.source.code(), url, text)
}

fn normalize_queries(mut provided: Vec<String>, defaults: &[String]) -> Vec<String> {
    provided.retain(|entry| !entry.trim().is_empty());
    if provided.is_empty() {
        return defaults.to_vec();
    }
    provided
}

fn summarize_error_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.len() <= 220 {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..220])
    }
}

fn normalize_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_text_opt(value: Option<String>) -> Option<String> {
    value.and_then(|raw| normalize_text(&raw))
}

fn parse_datetime(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_csv(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}
