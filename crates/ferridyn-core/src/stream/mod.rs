//! Change stream (CDC): types, configuration, and stream record definitions.

pub mod ops;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::TxnId;

/// The type of data captured in stream records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamViewType {
    /// Only the key attributes of the modified item.
    KeysOnly,
    /// The entire item as it appeared after it was modified.
    NewImage,
    /// The entire item as it appeared before it was modified.
    OldImage,
    /// Both the new and old images of the item.
    NewAndOldImages,
}

/// The type of change that triggered the stream record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// A new item was created.
    Insert,
    /// An existing item was modified.
    Modify,
    /// An item was deleted.
    Remove,
}

/// Configuration for a table's change stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// What data to capture in stream records.
    pub view_type: StreamViewType,
    /// Whether the stream is currently capturing changes.
    pub enabled: bool,
    /// Maximum age of stream records in seconds (default: 7 days).
    #[serde(default)]
    pub max_age_secs: Option<u64>,
    /// Maximum number of stream records to retain.
    #[serde(default)]
    pub max_count: Option<usize>,
}

/// A single change event captured by the stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    /// The transaction ID that produced this record.
    pub sequence_number: TxnId,
    /// Sub-sequence within the transaction (for multi-write transactions).
    pub sub_sequence: u32,
    /// The type of change.
    pub event_type: EventType,
    /// The key attributes of the affected item.
    pub keys: Value,
    /// Unix epoch seconds when the record was created.
    pub timestamp: f64,
    /// The item after modification (if view type includes new image).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_image: Option<Value>,
    /// The item before modification (if view type includes old image).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_image: Option<Value>,
}

/// Summary information about a table's change stream.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Whether the stream is enabled.
    pub enabled: bool,
    /// The view type configured for the stream.
    pub view_type: StreamViewType,
    /// The oldest sequence number in the stream (None if empty).
    pub oldest_sequence: Option<u64>,
    /// The latest sequence number in the stream (None if empty).
    pub latest_sequence: Option<u64>,
    /// The total number of records in the stream.
    pub record_count: usize,
}
