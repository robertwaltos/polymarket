use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),
    #[error("env var error: {0}")]
    Env(#[from] std::env::VarError),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("anthropic api error: {0}")]
    Anthropic(String),
    #[error("clob api error: {0}")]
    Clob(String),
    #[error("wallet error: {0}")]
    Wallet(String),
    #[error("internal error: {0}")]
    Internal(String),
}
