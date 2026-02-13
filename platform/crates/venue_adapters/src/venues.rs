use crate::config::{CoinbaseConfig, IbkrConfig, KalshiConfig};
use crate::error::{AdapterError, AdapterResult};
use crate::traits::{HealthReport, VenueAdapter, VenueOrderResult};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::Utc;
use domain::{BalanceSnapshot, MarketQuote, OrderRequest, OrderType, Side, Venue};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::{Pkcs1v15Sign, RsaPrivateKey};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use tracing::debug;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct KalshiAdapter {
    client: Client,
    config: KalshiConfig,
    private_key: RsaPrivateKey,
}

impl KalshiAdapter {
    pub fn new(config: KalshiConfig) -> AdapterResult<Self> {
        let private_key = parse_rsa_private_key(&config.rsa_private_key_pem)?;
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(AdapterError::Http)?;
        Ok(Self {
            client,
            config,
            private_key,
        })
    }

    fn base_url(&self) -> &str {
        self.config.base_url.trim_end_matches('/')
    }

    fn signed_headers(&self, method: &str, path_and_query: &str) -> AdapterResult<HeaderMap> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signing_payload = format!("{}{}{}", timestamp, method.to_uppercase(), path_and_query);
        let digest = Sha256::digest(signing_payload.as_bytes());
        let signature = self
            .private_key
            .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
            .map_err(|e| AdapterError::Crypto(format!("kalshi sign failed: {e}")))?;
        let signature_b64 = BASE64_STANDARD.encode(signature);

        let mut headers = HeaderMap::new();
        headers.insert(
            "KALSHI-ACCESS-KEY",
            HeaderValue::from_str(&self.config.key_id)
                .map_err(|e| AdapterError::Auth(format!("kalshi key header invalid: {e}")))?,
        );
        headers.insert(
            "KALSHI-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp)
                .map_err(|e| AdapterError::Auth(format!("kalshi timestamp header invalid: {e}")))?,
        );
        headers.insert(
            "KALSHI-ACCESS-SIGNATURE",
            HeaderValue::from_str(&signature_b64)
                .map_err(|e| AdapterError::Auth(format!("kalshi signature header invalid: {e}")))?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    async fn signed_get(&self, path_and_query: &str) -> AdapterResult<Value> {
        let url = format!("{}{}", self.base_url(), path_and_query);
        let headers = self.signed_headers("GET", path_and_query)?;
        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }

    async fn signed_post(&self, path: &str, body: &Value) -> AdapterResult<Value> {
        let url = format!("{}{}", self.base_url(), path);
        let headers = self.signed_headers("POST", path)?;
        let response = self
            .client
            .post(url)
            .headers(headers)
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }
}

#[async_trait]
impl VenueAdapter for KalshiAdapter {
    fn venue(&self) -> Venue {
        Venue::Kalshi
    }

    async fn health(&self) -> AdapterResult<HealthReport> {
        let started = Instant::now();
        let payload = self.signed_get("/trade-api/v2/portfolio/balance").await?;
        let detail = payload
            .get("balance")
            .and_then(parse_f64)
            .map(|v| format!("balance={v:.2}"))
            .unwrap_or_else(|| "auth_ok".to_string());
        Ok(HealthReport {
            venue: Venue::Kalshi,
            ok: true,
            latency_ms: started.elapsed().as_millis(),
            detail,
            checked_at: Utc::now(),
        })
    }

    async fn fetch_quote(&self, symbol: &str) -> AdapterResult<MarketQuote> {
        let path = format!("/trade-api/v2/markets/{symbol}");
        let payload = match self.signed_get(&path).await {
            Ok(v) => v,
            Err(_) => {
                let fallback = format!("/trade-api/v2/markets?limit=1&ticker={symbol}");
                self.signed_get(&fallback).await?
            }
        };

        let market = payload
            .get("market")
            .cloned()
            .or_else(|| payload.get("markets").and_then(|v| v.as_array()).and_then(|v| v.first().cloned()))
            .ok_or_else(|| AdapterError::InvalidRequest("kalshi quote payload missing market".to_string()))?;

        let bid = market.get("yes_bid").and_then(parse_f64).map(cents_to_prob);
        let ask = market.get("yes_ask").and_then(parse_f64).map(cents_to_prob);
        let last = market
            .get("last_price")
            .and_then(parse_f64)
            .or_else(|| market.get("yes_price").and_then(parse_f64))
            .map(cents_to_prob);

        Ok(MarketQuote {
            instrument: domain::Instrument {
                venue: Venue::Kalshi,
                symbol: symbol.to_string(),
                asset_class: domain::AssetClass::Prediction,
                quote_currency: "USD".to_string(),
            },
            bid,
            ask,
            last,
            timestamp: Utc::now(),
            source: "kalshi".to_string(),
        })
    }

    async fn fetch_balances(&self) -> AdapterResult<Vec<BalanceSnapshot>> {
        let payload = self.signed_get("/trade-api/v2/portfolio/balance").await?;
        let total = payload.get("balance").and_then(parse_f64).unwrap_or(0.0);
        let available = payload
            .get("available_balance")
            .and_then(parse_f64)
            .unwrap_or(total);
        Ok(vec![BalanceSnapshot {
            venue: Venue::Kalshi,
            account_id: self.config.key_id.clone(),
            currency: "USD".to_string(),
            total,
            available,
            timestamp: Utc::now(),
        }])
    }

    async fn place_live_order(&self, order: &OrderRequest) -> AdapterResult<VenueOrderResult> {
        if order.order_type != OrderType::Limit {
            return Err(AdapterError::Unsupported(
                "kalshi connector currently supports limit orders only".to_string(),
            ));
        }
        let limit_price = order
            .limit_price
            .ok_or_else(|| AdapterError::InvalidRequest("limit price is required".to_string()))?;
        let count = order.quantity.max(1.0).round() as i64;
        let action = match order.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };
        let body = json!({
            "ticker": order.instrument.symbol,
            "client_order_id": order.request_id.to_string(),
            "type": "limit",
            "action": action,
            "side": "yes",
            "count": count,
            "yes_price": (limit_price * 100.0).round() as i64,
        });
        debug!(target: "venue_adapters", ?body, "submitting kalshi order");
        let payload = self.signed_post("/trade-api/v2/portfolio/orders", &body).await?;
        let venue_order_id = payload
            .get("order")
            .and_then(|v| v.get("order_id"))
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .or_else(|| payload.get("order_id").and_then(|v| v.as_str()).map(ToString::to_string));

        Ok(VenueOrderResult {
            venue: Venue::Kalshi,
            accepted: venue_order_id.is_some(),
            message: payload.to_string(),
            venue_order_id,
        })
    }
}

#[derive(Clone)]
pub struct IbkrAdapter {
    client: Client,
    config: IbkrConfig,
}

impl IbkrAdapter {
    pub fn new(config: IbkrConfig) -> AdapterResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(AdapterError::Http)?;
        Ok(Self { client, config })
    }

    fn base_url(&self) -> &str {
        self.config.base_url.trim_end_matches('/')
    }

    fn with_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.config.session_token.as_deref() {
            builder.bearer_auth(token)
        } else {
            builder
        }
    }
}

#[async_trait]
impl VenueAdapter for IbkrAdapter {
    fn venue(&self) -> Venue {
        Venue::Ibkr
    }

    async fn health(&self) -> AdapterResult<HealthReport> {
        let started = Instant::now();
        let url = format!("{}/v1/api/iserver/auth/status", self.base_url());
        let payload = self
            .with_auth(self.client.get(url))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let detail = payload
            .get("authenticated")
            .and_then(|v| v.as_bool())
            .map(|a| format!("authenticated={a}"))
            .unwrap_or_else(|| "ok".to_string());
        Ok(HealthReport {
            venue: Venue::Ibkr,
            ok: true,
            latency_ms: started.elapsed().as_millis(),
            detail,
            checked_at: Utc::now(),
        })
    }

    async fn fetch_quote(&self, symbol: &str) -> AdapterResult<MarketQuote> {
        let url = format!("{}/v1/api/iserver/marketdata/snapshot", self.base_url());
        let payload = self
            .with_auth(
                self.client
                    .get(url)
                    .query(&[("conids", symbol), ("fields", "31,84,86")]),
            )
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        let row = payload
            .as_array()
            .and_then(|v| v.first())
            .ok_or_else(|| AdapterError::InvalidRequest("ibkr quote payload empty".to_string()))?;
        let bid = row.get("84").and_then(parse_f64);
        let ask = row.get("86").and_then(parse_f64);
        let last = row.get("31").and_then(parse_f64);
        Ok(MarketQuote {
            instrument: domain::Instrument {
                venue: Venue::Ibkr,
                symbol: symbol.to_string(),
                asset_class: domain::AssetClass::Other,
                quote_currency: "USD".to_string(),
            },
            bid,
            ask,
            last,
            timestamp: Utc::now(),
            source: "ibkr".to_string(),
        })
    }

    async fn fetch_balances(&self) -> AdapterResult<Vec<BalanceSnapshot>> {
        let url = format!(
            "{}/v1/api/portfolio/{}/ledger",
            self.base_url(),
            self.config.account_id
        );
        let payload = self
            .with_auth(self.client.get(url))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        let mut snapshots = Vec::new();
        if let Some(map) = payload.as_object() {
            for (currency, values) in map {
                let total = values
                    .get("cashbalance")
                    .and_then(parse_f64)
                    .or_else(|| values.get("netliquidationvalue").and_then(parse_f64))
                    .unwrap_or(0.0);
                let available = values
                    .get("cashbalance")
                    .and_then(parse_f64)
                    .unwrap_or(total);
                snapshots.push(BalanceSnapshot {
                    venue: Venue::Ibkr,
                    account_id: self.config.account_id.clone(),
                    currency: currency.to_uppercase(),
                    total,
                    available,
                    timestamp: Utc::now(),
                });
            }
        }

        if snapshots.is_empty() {
            return Err(AdapterError::InvalidRequest(
                "ibkr ledger payload missing balances".to_string(),
            ));
        }
        Ok(snapshots)
    }

    async fn place_live_order(&self, order: &OrderRequest) -> AdapterResult<VenueOrderResult> {
        let order_type = match order.order_type {
            OrderType::Limit => "LMT",
            _ => "MKT",
        };
        let mut broker_order = json!({
            "conid": order.instrument.symbol,
            "side": if matches!(order.side, Side::Buy) { "BUY" } else { "SELL" },
            "orderType": order_type,
            "quantity": order.quantity,
            "cOID": order.request_id.to_string(),
        });
        if order_type == "LMT" {
            let limit = order
                .limit_price
                .ok_or_else(|| AdapterError::InvalidRequest("limit price is required".to_string()))?;
            broker_order["price"] = json!(limit);
        }

        let payload = json!({ "orders": [broker_order] });
        debug!(target: "venue_adapters", ?payload, "submitting ibkr order");
        let url = format!(
            "{}/v1/api/iserver/account/{}/orders",
            self.base_url(),
            self.config.account_id
        );
        let response = self
            .with_auth(self.client.post(url))
            .json(&payload)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let venue_order_id = response
            .as_array()
            .and_then(|v| v.first())
            .and_then(|v| v.get("order_id").or_else(|| v.get("id")))
            .and_then(|v| v.as_i64().map(|x| x.to_string()).or_else(|| v.as_str().map(ToString::to_string)));

        Ok(VenueOrderResult {
            venue: Venue::Ibkr,
            accepted: venue_order_id.is_some(),
            message: response.to_string(),
            venue_order_id,
        })
    }
}

#[derive(Clone)]
pub struct CoinbaseAdapter {
    client: Client,
    config: CoinbaseConfig,
    secret_bytes: Vec<u8>,
}

impl CoinbaseAdapter {
    pub fn new(config: CoinbaseConfig) -> AdapterResult<Self> {
        let secret_bytes = BASE64_STANDARD
            .decode(config.api_secret.as_bytes())
            .unwrap_or_else(|_| config.api_secret.as_bytes().to_vec());
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(AdapterError::Http)?;
        Ok(Self {
            client,
            config,
            secret_bytes,
        })
    }

    fn base_url(&self) -> &str {
        self.config.base_url.trim_end_matches('/')
    }

    fn signed_headers(
        &self,
        method: &str,
        path_and_query: &str,
        body: &str,
    ) -> AdapterResult<HeaderMap> {
        let timestamp = Utc::now().timestamp().to_string();
        let message = format!("{}{}{}{}", timestamp, method.to_uppercase(), path_and_query, body);
        let mut mac = HmacSha256::new_from_slice(&self.secret_bytes)
            .map_err(|e| AdapterError::Crypto(format!("coinbase hmac init failed: {e}")))?;
        mac.update(message.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

        let mut headers = HeaderMap::new();
        headers.insert(
            "CB-ACCESS-KEY",
            HeaderValue::from_str(&self.config.api_key)
                .map_err(|e| AdapterError::Auth(format!("coinbase key header invalid: {e}")))?,
        );
        headers.insert(
            "CB-ACCESS-SIGN",
            HeaderValue::from_str(&signature)
                .map_err(|e| AdapterError::Auth(format!("coinbase sign header invalid: {e}")))?,
        );
        headers.insert(
            "CB-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp)
                .map_err(|e| AdapterError::Auth(format!("coinbase timestamp header invalid: {e}")))?,
        );
        headers.insert(
            "CB-ACCESS-PASSPHRASE",
            HeaderValue::from_str(&self.config.passphrase).map_err(|e| {
                AdapterError::Auth(format!("coinbase passphrase header invalid: {e}"))
            })?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    async fn signed_get(&self, path_and_query: &str) -> AdapterResult<Value> {
        let url = format!("{}{}", self.base_url(), path_and_query);
        let headers = self.signed_headers("GET", path_and_query, "")?;
        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }

    async fn signed_post(&self, path: &str, body: &Value) -> AdapterResult<Value> {
        let url = format!("{}{}", self.base_url(), path);
        let body_string = serde_json::to_string(body)?;
        let headers = self.signed_headers("POST", path, &body_string)?;
        let response = self
            .client
            .post(url)
            .headers(headers)
            .body(body_string)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }
}

#[async_trait]
impl VenueAdapter for CoinbaseAdapter {
    fn venue(&self) -> Venue {
        Venue::Coinbase
    }

    async fn health(&self) -> AdapterResult<HealthReport> {
        let started = Instant::now();
        let payload = self.signed_get("/api/v3/brokerage/accounts?limit=1").await?;
        let accounts = payload
            .get("accounts")
            .and_then(|v| v.as_array())
            .map(|v| v.len())
            .unwrap_or(0);
        Ok(HealthReport {
            venue: Venue::Coinbase,
            ok: true,
            latency_ms: started.elapsed().as_millis(),
            detail: format!("accounts={accounts}"),
            checked_at: Utc::now(),
        })
    }

    async fn fetch_quote(&self, symbol: &str) -> AdapterResult<MarketQuote> {
        let path = format!("/api/v3/brokerage/best_bid_ask?product_ids={symbol}");
        let payload = self.signed_get(&path).await?;
        let book = payload
            .get("pricebooks")
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .ok_or_else(|| AdapterError::InvalidRequest("coinbase quote payload empty".to_string()))?;

        let bid = book
            .get("bids")
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .and_then(|v| v.get("price"))
            .and_then(parse_f64);
        let ask = book
            .get("asks")
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .and_then(|v| v.get("price"))
            .and_then(parse_f64);
        let last = payload
            .get("pricebooks")
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .and_then(|v| v.get("last_price"))
            .and_then(parse_f64);

        Ok(MarketQuote {
            instrument: domain::Instrument {
                venue: Venue::Coinbase,
                symbol: symbol.to_string(),
                asset_class: domain::AssetClass::Crypto,
                quote_currency: "USD".to_string(),
            },
            bid,
            ask,
            last,
            timestamp: Utc::now(),
            source: "coinbase".to_string(),
        })
    }

    async fn fetch_balances(&self) -> AdapterResult<Vec<BalanceSnapshot>> {
        let payload = self.signed_get("/api/v3/brokerage/accounts").await?;
        let mut balances = Vec::new();
        for account in payload
            .get("accounts")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let currency = account
                .get("currency")
                .and_then(|v| v.as_str())
                .unwrap_or("USD")
                .to_string();
            let total = account
                .get("available_balance")
                .and_then(|v| v.get("value"))
                .and_then(parse_f64)
                .unwrap_or(0.0);
            let hold = account
                .get("hold")
                .and_then(|v| v.get("value"))
                .and_then(parse_f64)
                .unwrap_or(0.0);
            balances.push(BalanceSnapshot {
                venue: Venue::Coinbase,
                account_id: account
                    .get("uuid")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                currency: currency.clone(),
                total: total + hold,
                available: total,
                timestamp: Utc::now(),
            });
        }
        if balances.is_empty() {
            return Err(AdapterError::InvalidRequest(
                "coinbase accounts payload missing balances".to_string(),
            ));
        }
        Ok(balances)
    }

    async fn place_live_order(&self, order: &OrderRequest) -> AdapterResult<VenueOrderResult> {
        let order_configuration = match order.order_type {
            OrderType::Limit => {
                let limit_price = order.limit_price.ok_or_else(|| {
                    AdapterError::InvalidRequest("limit price is required".to_string())
                })?;
                json!({
                    "limit_limit_gtc": {
                        "base_size": order.quantity.to_string(),
                        "limit_price": limit_price.to_string()
                    }
                })
            }
            _ => json!({
                "market_market_ioc": {
                    "base_size": order.quantity.to_string()
                }
            }),
        };
        let body = json!({
            "client_order_id": order.request_id.to_string(),
            "product_id": order.instrument.symbol,
            "side": if matches!(order.side, Side::Buy) { "BUY" } else { "SELL" },
            "order_configuration": order_configuration
        });
        debug!(target: "venue_adapters", ?body, "submitting coinbase order");
        let payload = self.signed_post("/api/v3/brokerage/orders", &body).await?;
        let venue_order_id = payload
            .get("success_response")
            .and_then(|v| v.get("order_id"))
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .or_else(|| payload.get("order_id").and_then(|v| v.as_str()).map(ToString::to_string));
        Ok(VenueOrderResult {
            venue: Venue::Coinbase,
            accepted: venue_order_id.is_some(),
            message: payload.to_string(),
            venue_order_id,
        })
    }
}

fn parse_rsa_private_key(raw: &str) -> AdapterResult<RsaPrivateKey> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AdapterError::Auth(
            "missing RSA private key for Kalshi".to_string(),
        ));
    }

    if trimmed.contains("BEGIN") {
        if let Ok(pkcs8) = RsaPrivateKey::from_pkcs8_pem(trimmed) {
            return Ok(pkcs8);
        }
        if let Ok(pkcs1) = RsaPrivateKey::from_pkcs1_pem(trimmed) {
            return Ok(pkcs1);
        }
        return Err(AdapterError::Auth(
            "unable to parse Kalshi RSA key from PEM".to_string(),
        ));
    }

    let decoded = BASE64_STANDARD
        .decode(trimmed.as_bytes())
        .map_err(|e| AdapterError::Auth(format!("failed to decode Kalshi key base64: {e}")))?;

    RsaPrivateKey::from_pkcs8_der(&decoded)
        .or_else(|_| RsaPrivateKey::from_pkcs1_der(&decoded))
        .map_err(|e| AdapterError::Auth(format!("unable to parse Kalshi RSA key bytes: {e}")))
}

fn parse_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn cents_to_prob(value: f64) -> f64 {
    if value > 1.0 {
        (value / 100.0).clamp(0.0, 1.0)
    } else {
        value.clamp(0.0, 1.0)
    }
}
