/// Why the client stopped, or why one connection attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Feishu refused the app: wrong credentials, no permission, or too many connections.
    /// Retrying does not help.
    #[error("Feishu rejected the connection (code {code}): {message}")]
    Rejected { code: i64, message: String },
    /// Feishu could not take the connection now; the client retries.
    #[error("Feishu is unavailable (code {code}): {message}")]
    Unavailable { code: i64, message: String },
    /// The client gave up after the attempts Feishu allows.
    #[error("could not reach Feishu after {0} attempts")]
    Unreachable(u32),
    #[error("Feishu endpoint request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Feishu WebSocket failed: {0}")]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("unexpected Feishu response: {0}")]
    Protocol(String),
    #[error("TLS setup failed: {0}")]
    Tls(#[from] rustls::Error),
}

impl Error {
    /// Whether the client stops instead of reconnecting.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::Rejected { .. } | Error::Unreachable(_) | Error::Tls(_)
        )
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(Box::new(error))
    }
}
