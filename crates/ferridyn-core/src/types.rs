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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyDefinition {
    pub name: String,
    pub key_type: KeyType,
}

/// The type of a document attribute (for partition schema validation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttrType {
    String,
    Number,
    Boolean,
}

/// A document attribute definition within a partition schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeDef {
    pub name: String,
    pub attr_type: AttrType,
    pub required: bool,
}

/// A declared partition schema describing an entity type within a single-table design.
///
/// Documents are matched to a partition schema by extracting the prefix from the
/// partition key (everything before the first `#`). Only String partition keys
/// support prefix matching.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionSchema {
    /// The prefix to match (e.g., `"CONTACT"` matches `"CONTACT#toby"`).
    pub prefix: String,
    /// Human/agent-readable description of this entity type.
    pub description: String,
    /// Attribute definitions for documents under this prefix.
    pub attributes: Vec<AttributeDef>,
    /// When true, `put_item` validates documents against these attributes.
    pub validate: bool,
}

/// A secondary index definition scoped to a partition schema.
///
/// Each index maintains a separate B+Tree keyed by
/// `encode_composite(indexed_value, primary_key_as_binary)`.
/// V1 supports single-attribute indexes with KeysOnly projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Unique index name (e.g., `"contact-email-index"`).
    pub name: String,
    /// The partition schema prefix this index is scoped to.
    pub partition_schema: String,
    /// The document attribute to index on.
    pub index_key: KeyDefinition,
    /// B+Tree root page for this index.
    pub root_page: PageId,
}
