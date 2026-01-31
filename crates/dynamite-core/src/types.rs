use serde::{Deserialize, Serialize};

/// Page identifier (offset in units of PAGE_SIZE).
pub type PageId = u64;

/// Transaction identifier (monotonically increasing).
pub type TxnId = u64;

/// Size of every page in bytes.
pub const PAGE_SIZE: usize = 4096;

/// Size of the common page header in bytes.
pub const PAGE_HEADER_SIZE: usize = 32;

/// Maximum document size in bytes (400KB, matching DynamoDB).
pub const MAX_DOCUMENT_SIZE: usize = 400 * 1024;

/// Size of a single slot entry in a slotted page (offset: u16 + length: u16).
pub const SLOT_SIZE: usize = 4;

/// The type of a key attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyType {
    String,
    Number,
    Binary,
}

/// Schema definition for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub partition_key: KeyDefinition,
    pub sort_key: Option<KeyDefinition>,
}

/// A key attribute definition (name + type).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDefinition {
    pub name: String,
    pub key_type: KeyType,
}
