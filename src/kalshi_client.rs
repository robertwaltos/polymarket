use crate::config::Config;
use crate::error::AgentError;
use crate::risk_engine::{TradeSignal, TradeSide};
use chrono::Utc;
use log::info;
use rand::rngs::OsRng;
use reqwest::Client;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::{Pss, RsaPrivateKey};
use serde_json::json;
use sha2::{Digest, Sha256};
use base64::engine::general_purpose;
use base64::Engine as _;
use std::env;
use std::time::Duration;
use std::fs;

pub struct KalshiClient {
    client: Client,
    base_url: String,
    key_id: String,
    private_key: RsaPrivateKey,
    shadow_mode: bool,
    post_only: bool,
    time_in_force: String,
}

impl KalshiClient {
    pub fn new(client: Client, config: Config) -> Result<Self, AgentError> {
        let key_id = config
            .kalshi_key_id
            .or(config.kalshi_api_key)
            .ok_or_else(|| AgentError::Kalshi("KALSHI_KEY_ID missing".to_string()))?;

        let private_key_raw = config
            .kalshi_private_key_b64
            .ok_or_else(|| AgentError::Kalshi("KALSHI_PRIVATE_KEY missing".to_string()))?;
        let private_key = parse_private_key(&private_key_raw)?;

        Ok(Self {
            client,
            base_url: config.kalshi_base_url,
            key_id,
            private_key,
            shadow_mode: config.shadow_mode,
            post_only: config.kalshi_post_only,
            time_in_force: config.kalshi_time_in_force,
        })
    }

    pub fn from_env_for_health() -> Result<Self, AgentError> {
        let key_id = load_env_value(&[
            "KALSHI_KEY_ID",
            "KALSHI_API_KEY_ID",
            "KALSHI_API_KEY",
        ])?;
        let private_key_raw = load_env_value(&["KALSHI_PRIVATE_KEY", "KALSHI_RSA_PRIVATE_KEY"])?;
        let base_url = load_env_value_optional("KALSHI_BASE_URL")
            .unwrap_or_else(|| "https://api.elections.kalshi.com".to_string());

        let client = Client::builder()
            .user_agent("polymarket-agent/0.1")
            .timeout(Duration::from_secs(15))
            .build()?;

        let private_key = parse_private_key(&private_key_raw)?;
        Ok(Self {
            client,
            base_url,
            key_id,
            private_key,
            shadow_mode: true,
            post_only: false,
            time_in_force: "good_till_canceled".to_string(),
        })
    }

    pub async fn get_cash_balance(&self) -> Result<f64, AgentError> {
        let path = "/trade-api/v2/portfolio/balance";
        let url = format!("{}{}", self.base_url, path);
        let headers = self.signed_headers("GET", path)?;

        let mut req = self.client.get(&url);
        for (key, value) in headers {
            req = req.header(key, value);
        }

        let payload = req.send().await?.error_for_status()?.json::<serde_json::Value>().await?;

        let balance_cents = payload
            .get("balance")
            .or_else(|| payload.get("available_cash"))
            .or_else(|| payload.get("cash"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        Ok(balance_cents as f64 / 100.0)
    }

    pub async fn place_limit_order(
        &self,
        ticker: &str,
        signal: &TradeSignal,
        price: f64,
    ) -> Result<(), AgentError> {
        if self.shadow_mode {
            info!(
                "[SHADOW] Would place Kalshi order: ticker={} side={:?} price={:.4} size=${:.2}",
                ticker, signal.side, price, signal.size_usdc
            );
            return Ok(());
        }

        let side = match signal.side {
            TradeSide::BuyYes => "yes",
            TradeSide::BuyNo => "no",
            TradeSide::None => return Ok(()),
        };

        let price_cents = (price * 100.0).round() as i64;
        if price_cents <= 0 || price_cents >= 100 {
            return Err(AgentError::Kalshi(format!(
                "Invalid Kalshi price cents: {}",
                price_cents
            )));
        }

        let count = (signal.size_usdc / price).floor() as i64;
        if count < 1 {
            return Err(AgentError::Kalshi("Order size below 1 contract".to_string()));
        }

        let path = "/trade-api/v2/portfolio/orders";
        let url = format!("{}{}", self.base_url, path);
        let client_order_id = format!("agent-{}", Utc::now().timestamp_millis());
        let mut body = json!({
            "ticker": ticker,
            "type": "limit",
            "action": "buy",
            "side": side,
            "count": count,
            "client_order_id": client_order_id,
            "post_only": self.post_only,
            "time_in_force": self.time_in_force,
        });

        if side == "yes" {
            body["yes_price"] = json!(price_cents);
        } else {
            body["no_price"] = json!(price_cents);
        }

        let body_string = serde_json::to_string(&body)
            .map_err(|e| AgentError::Kalshi(e.to_string()))?;
        let headers = self.signed_headers("POST", path)?;

        let mut req = self.client.post(&url).header("Content-Type", "application/json");
        for (key, value) in headers {
            req = req.header(key, value);
        }

        let resp = req.body(body_string).send().await?.error_for_status();
        match resp {
            Ok(_) => {
                info!(
                    "Kalshi order placed: ticker={} side={:?} price={:.4} size=${:.2}",
                    ticker, signal.side, price, signal.size_usdc
                );
                Ok(())
            }
            Err(err) => Err(AgentError::Kalshi(err.to_string())),
        }
    }

    fn signed_headers(&self, method: &str, path: &str) -> Result<Vec<(String, String)>, AgentError> {
        let timestamp = Utc::now().timestamp_millis();
        let sign_str = format!("{}{}{}", timestamp, method.to_uppercase(), path);
        let mut hasher = Sha256::new();
        hasher.update(sign_str.as_bytes());
        let hash = hasher.finalize();

        let mut rng = OsRng;
        let signature = self
            .private_key
            .sign_with_rng(&mut rng, Pss::new::<Sha256>(), &hash)
            .map_err(|e| AgentError::Kalshi(format!("Kalshi sign error: {}", e)))?;
        let sig_b64 = general_purpose::STANDARD.encode(signature);

        Ok(vec![
            ("KALSHI-ACCESS-KEY".to_string(), self.key_id.clone()),
            (
                "KALSHI-ACCESS-TIMESTAMP".to_string(),
                timestamp.to_string(),
            ),
            ("KALSHI-ACCESS-SIGNATURE".to_string(), sig_b64),
        ])
    }
}

fn parse_private_key(raw: &str) -> Result<RsaPrivateKey, AgentError> {
    if raw.contains("BEGIN RSA PRIVATE KEY") {
        return RsaPrivateKey::from_pkcs1_pem(raw)
            .map_err(|e| AgentError::Kalshi(format!("Kalshi key parse: {}", e)));
    }
    if raw.contains("BEGIN PRIVATE KEY") {
        return RsaPrivateKey::from_pkcs8_pem(raw)
            .map_err(|e| AgentError::Kalshi(format!("Kalshi key parse: {}", e)));
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
        return Err(AgentError::Kalshi("Kalshi key parse: unsupported PEM label".to_string()));
    }

    let decoded = general_purpose::STANDARD
        .decode(raw)
        .map_err(|e| AgentError::Kalshi(format!("Kalshi key decode: {}", e)))?;

    if let Ok(pem) = std::str::from_utf8(&decoded) {
        if pem.contains("BEGIN RSA PRIVATE KEY") {
            return RsaPrivateKey::from_pkcs1_pem(pem)
                .map_err(|e| AgentError::Kalshi(format!("Kalshi key parse: {}", e)));
        }
        if pem.contains("BEGIN PRIVATE KEY") {
            return RsaPrivateKey::from_pkcs8_pem(pem)
                .map_err(|e| AgentError::Kalshi(format!("Kalshi key parse: {}", e)));
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
    Err(AgentError::Kalshi("Kalshi key parse: unsupported key format".to_string()))
}

fn load_env_value(keys: &[&str]) -> Result<String, AgentError> {
    for key in keys {
        if let Some(value) = load_env_value_optional(key) {
            return Ok(value);
        }
    }
    Err(AgentError::Env(env::VarError::NotPresent))
}

fn load_env_value_optional(key: &str) -> Option<String> {
    if let Ok(value) = env::var(key) {
        if let Some(normalized) = normalize_secret_value(&value) {
            if normalized.starts_with("-----BEGIN ") && !normalized.contains("-----END ") {
                if let Some(from_file) = read_env_value_from_file(key) {
                    return Some(from_file);
                }
            }
            return Some(normalized);
        }
    }

    read_env_value_from_file(key)
}

fn read_env_value_from_file(key: &str) -> Option<String> {
    let contents = fs::read_to_string(".env").ok()?;
    let lines: Vec<&str> = contents.lines().collect();
    let mut idx = 0usize;
    while idx < lines.len() {
        let line = lines[idx].trim();
        if line.is_empty() || line.starts_with('#') {
            idx += 1;
            continue;
        }
        let Some((k, raw_v)) = line.split_once('=') else {
            idx += 1;
            continue;
        };
        if k.trim() != key {
            idx += 1;
            continue;
        }
        let value = raw_v.trim();
        if value.starts_with("-----BEGIN ") && !value.contains("-----END ") {
            let mut block = String::from(value);
            idx += 1;
            while idx < lines.len() {
                let next = lines[idx].trim_end();
                block.push('\n');
                block.push_str(next);
                if next.contains("-----END ") {
                    break;
                }
                idx += 1;
            }
            return normalize_secret_value(&block);
        }
        return normalize_secret_value(value);
    }
    None
}

fn normalize_secret_value(input: &str) -> Option<String> {
    let mut value = input.trim().trim_matches('"').trim_matches('\'').trim().to_string();
    if value.is_empty() {
        return None;
    }
    if value.contains("\\n") {
        value = value.replace("\\n", "\n");
    }
    let path = std::path::Path::new(&value);
    if path.exists() && path.is_file() {
        if let Ok(contents) = fs::read_to_string(path) {
            let trimmed = contents.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    Some(value)
}
