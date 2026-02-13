use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("authentication error: {0}")]
    Auth(String),
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("unsupported venue operation: {0}")]
    Unsupported(String),
    #[error("missing adapter for venue: {0}")]
    MissingAdapter(String),
    #[error("venue rejected request: {0}")]
    VenueRejected(String),
}

pub type AdapterResult<T> = Result<T, AdapterError>;
