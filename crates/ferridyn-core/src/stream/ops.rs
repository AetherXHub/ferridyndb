//! Stream B+Tree operations: append records, query by sequence, encode/decode keys.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::btree::PageStore;
use crate::btree::ops as btree_ops;
use crate::encoding::number;
use crate::error::{EncodingError, Error, StorageError};
use crate::types::PageId;

use super::{StreamConfig, StreamInfo, StreamRecord};

/// Encode a stream key from (txn_id, sub_seq) into 12 bytes.
///
/// Layout: `[encode_number(txn_id)][u32_be(sub_seq)]` = 12 bytes.
/// This ensures natural ordering by transaction ID, then by sub-sequence.
pub fn encode_stream_key(txn_id: u64, sub_seq: u32) -> Result<Vec<u8>, Error> {
    let txn_bytes = number::encode_number(txn_id as f64)?;
    let mut key = Vec::with_capacity(12);
    key.extend_from_slice(&txn_bytes);
    key.extend_from_slice(&sub_seq.to_be_bytes());
    Ok(key)
}

/// Decode a stream key back into (txn_id, sub_seq).
pub fn decode_stream_key(key: &[u8]) -> Result<(u64, u32), Error> {
    if key.len() < 12 {
        return Err(EncodingError::MalformedKey.into());
    }
    let txn_bytes: [u8; 8] = key[..8].try_into().unwrap();
    let txn_id = number::decode_number(&txn_bytes) as u64;
    let sub_seq = u32::from_be_bytes(key[8..12].try_into().unwrap());
    Ok((txn_id, sub_seq))
}

/// Append a stream record to the stream B+Tree.
///
/// Returns the (possibly new) stream root page ID.
pub fn append_stream_record(
    store: &mut impl PageStore,
    stream_root: PageId,
    record: &StreamRecord,
) -> Result<PageId, Error> {
    let key = encode_stream_key(record.sequence_number, record.sub_sequence)?;
    let value: Vec<u8> = rmp_serde::to_vec_named(record).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to serialize stream record: {e}"))
    })?;
    let new_root = btree_ops::insert(store, stream_root, &key, &value)?;
    Ok(new_root)
}

/// Query stream records after a given sequence number.
///
/// Returns up to `limit` records with `sequence_number > after_sequence`
/// (or all records from the beginning if `after_sequence` is `None`).
pub fn get_stream_records(
    store: &impl PageStore,
    stream_root: PageId,
    after_sequence: Option<u64>,
    limit: usize,
) -> Result<Vec<StreamRecord>, Error> {
    // Build the start key: if after_sequence is provided, start after (txn_id, u32::MAX).
    let start_key = if let Some(seq) = after_sequence {
        Some(encode_stream_key(seq, u32::MAX)?)
    } else {
        None
    };

    let entries = btree_ops::range_scan(store, stream_root, start_key.as_deref(), None)?;

    let mut records = Vec::with_capacity(limit.min(entries.len()));
    for (_key, value) in entries {
        if records.len() >= limit {
            break;
        }
        let record: StreamRecord = rmp_serde::from_slice(&value).map_err(|e| {
            StorageError::CorruptedPage(format!("failed to deserialize stream record: {e}"))
        })?;
        records.push(record);
    }

    Ok(records)
}

/// Get summary information about a stream.
pub fn get_stream_info(
    store: &impl PageStore,
    stream_root: PageId,
    config: &StreamConfig,
) -> Result<StreamInfo, Error> {
    let entries = btree_ops::range_scan(store, stream_root, None, None)?;

    let record_count = entries.len();
    let (oldest_sequence, latest_sequence) = if entries.is_empty() {
        (None, None)
    } else {
        let (oldest_txn, _) = decode_stream_key(&entries[0].0)?;
        let (latest_txn, _) = decode_stream_key(&entries[entries.len() - 1].0)?;
        (Some(oldest_txn), Some(latest_txn))
    };

    Ok(StreamInfo {
        enabled: config.enabled,
        view_type: config.view_type,
        oldest_sequence,
        latest_sequence,
        record_count,
    })
}

/// Get the current wall-clock time as Unix epoch seconds.
pub fn now_epoch_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Prune stream records that exceed retention limits.
///
/// Removes records older than `max_age_secs` and/or exceeding `max_count`.
/// Returns the (possibly new) stream root page ID and the number of records pruned.
pub fn prune_stream(
    store: &mut impl PageStore,
    stream_root: PageId,
    max_age_secs: Option<u64>,
    max_count: Option<usize>,
) -> Result<(PageId, usize), Error> {
    let entries = btree_ops::range_scan(store, stream_root, None, None)?;
    let total = entries.len();

    let mut keys_to_delete = Vec::new();

    // Prune by age: remove records older than max_age_secs.
    if let Some(max_age) = max_age_secs {
        let cutoff = now_epoch_secs() - max_age as f64;
        for (key, value) in &entries {
            let record: StreamRecord = rmp_serde::from_slice(value).map_err(|e| {
                StorageError::CorruptedPage(format!("failed to deserialize stream record: {e}"))
            })?;
            if record.timestamp < cutoff {
                keys_to_delete.push(key.clone());
            }
        }
    }

    // Prune by count: remove oldest records exceeding max_count.
    if let Some(max_count) = max_count
        && total > max_count
    {
        let excess = total - max_count;
        for (key, _value) in entries.iter().take(excess) {
            if !keys_to_delete.contains(key) {
                keys_to_delete.push(key.clone());
            }
        }
    }

    let pruned = keys_to_delete.len();
    let mut current_root = stream_root;
    for key in &keys_to_delete {
        current_root = btree_ops::delete(store, current_root, key)?;
    }

    Ok((current_root, pruned))
}
