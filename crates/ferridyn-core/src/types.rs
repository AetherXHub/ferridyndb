//! Core types: page IDs, transaction IDs, key definitions, table schemas.

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

/// Maximum partition key size in bytes (2048, matching DynamoDB).
pub const MAX_PARTITION_KEY_SIZE: usize = 2048;

/// Maximum sort key size in bytes (1024, matching DynamoDB).
pub const MAX_SORT_KEY_SIZE: usize = 1024;

/// An item returned with its MVCC version number.
///
/// The `version` is the transaction ID that created the current version
/// of the document. Used for optimistic concurrency control: pass this
/// version to a conditional put to ensure no other writer has modified
/// the item since it was read.
#[derive(Debug, Clone)]
pub struct VersionedItem {
    pub item: serde_json::Value,
    pub version: TxnId,
}

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
    /// Optional TTL attribute name. Items whose TTL attribute value (Unix epoch
    /// seconds) is in the past are invisible to reads and eligible for cleanup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_attribute: Option<String>,
}

/// A key attribute definition (name + type).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDefinition {
    pub name: String,
    pub key_type: KeyType,
}
