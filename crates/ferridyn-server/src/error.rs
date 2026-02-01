//! Error types for the server client.

use thiserror::Error;

use crate::protocol::ErrorResponse;

/// Errors returned by `FerridynClient` methods.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(serde_json::Error),

    #[error("server disconnected")]
    Disconnected,

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("server error: {}: {}", .0.error, .0.message)]
    Server(ErrorResponse),

    #[error("version mismatch: expected {expected}, actual {actual}")]
    VersionMismatch { expected: u64, actual: u64 },
}
