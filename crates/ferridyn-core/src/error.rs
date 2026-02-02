//! Error types for all FerridynDB operations.

use std::io;
use thiserror::Error;

/// Top-level error type for FerridynDB operations.
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

    #[error("null byte (0x00) is not allowed in string or binary keys")]
    NullByteInKey,

    #[error("key exceeds maximum size of {max} bytes (got {actual})")]
    KeyTooLarge { max: usize, actual: usize },
}

#[derive(Debug, Error)]
pub enum TxnError {
    #[error("transaction aborted")]
    Aborted,

    #[error("transaction conflict")]
    Conflict,

    #[error("version mismatch: expected {expected}, actual {actual}")]
    VersionMismatch { expected: u64, actual: u64 },
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

    #[error("partition schema not found: {0}")]
    PartitionSchemaNotFound(String),

    #[error("partition schema already exists: {0}")]
    PartitionSchemaAlreadyExists(String),

    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("index '{index}' references missing attribute '{attribute}'")]
    IndexKeyAttributeMissing { index: String, attribute: String },

    #[error("validation failed for prefix '{prefix}': {}", errors.join("; "))]
    ValidationFailed { prefix: String, errors: Vec<String> },

    #[error("cannot drop partition schema '{prefix}': has associated indexes")]
    PartitionSchemaHasIndexes { prefix: String },

    #[error("partition schemas require a String partition key")]
    PartitionSchemaRequiresStringKey,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("invalid sort key condition: {0}")]
    InvalidCondition(String),

    #[error("partition key is required")]
    PartitionKeyRequired,

    #[error("sort key not supported on this table")]
    SortKeyNotSupported,

    #[error("index key value is required")]
    IndexKeyRequired,
}

pub type Result<T> = std::result::Result<T, Error>;
