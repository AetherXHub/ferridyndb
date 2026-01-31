//! Error types for all DynaMite operations.

use std::io;
use thiserror::Error;

/// Top-level error type for DynaMite operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Encoding(#[from] EncodingError),

    #[error(transparent)]
    Transaction(#[from] TxnError),

    #[error(transparent)]
    Schema(#[from] SchemaError),

    #[error(transparent)]
    Query(#[from] QueryError),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("corrupted page: {0}")]
    CorruptedPage(String),

    #[error("invalid magic bytes")]
    InvalidMagic,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("database file is locked")]
    FileLocked,

    #[error("page out of bounds: {page_id} >= {total_pages}")]
    PageOutOfBounds { page_id: u64, total_pages: u64 },
}

#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("NaN is not a valid key value")]
    NaN,

    #[error("document exceeds maximum size of {max} bytes (got {actual})")]
    DocumentTooLarge { max: usize, actual: usize },

    #[error("invalid key type tag: {0}")]
    InvalidTypeTag(u8),

    #[error("malformed encoded key")]
    MalformedKey,
}

#[derive(Debug, Error)]
pub enum TxnError {
    #[error("transaction aborted")]
    Aborted,

    #[error("transaction conflict")]
    Conflict,
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("key type mismatch for attribute '{name}': expected {expected:?}, got {actual:?}")]
    KeyTypeMismatch {
        name: String,
        expected: crate::types::KeyType,
        actual: crate::types::KeyType,
    },

    #[error("missing key attribute: {0}")]
    MissingKeyAttribute(String),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("invalid sort key condition: {0}")]
    InvalidCondition(String),

    #[error("partition key is required")]
    PartitionKeyRequired,

    #[error("sort key not supported on this table")]
    SortKeyNotSupported,
}

pub type Result<T> = std::result::Result<T, Error>;
