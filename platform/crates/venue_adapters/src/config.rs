use domain::ExecutionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub mode: ExecutionMode,
    pub allow_live_writes: bool,
}

impl ExecutionPolicy {
    pub fn for_mode(mode: ExecutionMode) -> Self {
        let allow_live_writes = mode.allows_live_execution();
        Self {
            mode,
            allow_live_writes,
        }
    }

    pub fn effective_mode(&self, requested: ExecutionMode) -> (ExecutionMode, Option<String>) {
        match requested {
            ExecutionMode::Live if !self.allow_live_writes => (
                ExecutionMode::Shadow,
                Some("live writes disabled by execution policy".to_string()),
            ),
            _ => (requested, None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiConfig {
    pub base_url: String,
    pub key_id: String,
    pub rsa_private_key_pem: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrConfig {
    pub base_url: String,
    pub account_id: String,
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinbaseConfig {
    pub base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub kalshi: KalshiConfig,
    pub ibkr: IbkrConfig,
    pub coinbase: CoinbaseConfig,
    pub policy: ExecutionPolicy,
}
