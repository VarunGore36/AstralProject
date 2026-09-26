use thiserror::Error;

use crate::feed::FeedError;
use crate::store::StoreError;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("book error: {0}")]
    Book(#[from] astra_book::BookError),
    #[error("unreadable snapshot: {0}")]
    Snapshot(String),
    #[error("feed error: {0}")]
    Feed(#[from] FeedError),
    #[error("socket error: {0}")]
    Socket(Box<tungstenite::Error>),
    #[error("read timeouts are not supported on this transport")]
    TransportUnsupported,
    #[error("signal handler error: {0}")]
    Signal(String),
}

impl From<tungstenite::Error> for RecordError {
    fn from(error: tungstenite::Error) -> Self {
        RecordError::Socket(Box::new(error))
    }
}
