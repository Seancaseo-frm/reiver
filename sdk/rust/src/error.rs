use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReiverError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Client not initialized")]
    NotInitialized,

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Rate limited by server")]
    RateLimited,

    #[error("Server error (HTTP {status}): {body}")]
    Server { status: u16, body: String },
}

pub type Result<T> = std::result::Result<T, ReiverError>;
