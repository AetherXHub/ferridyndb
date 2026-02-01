//! MVCC-aware B+Tree operations.
//!
//! These operations wrap the underlying B+Tree with versioning semantics.
//! Values stored in the B+Tree are serialized `VersionedDocument` structs.
//! Reads use snapshot isolation to determine which version is visible.

use crate::btree::PageStore;
use crate::btree::ops as btree_ops;
use crate::error::StorageError;
use crate::types::{PageId, TxnId};

use super::version_chain;
use super::versioned::VersionedDocument;
use super::visibility;

/// Insert or update a key-value pair with MVCC versioning.
///
/// If the key already exists, the old version is archived to a version chain
/// and a new version is created. If the key is new, a fresh version is stored.
///
/// Returns the (possibly new) B+Tree root page ID.
pub fn mvcc_put(
    store: &mut impl PageStore,
    data_root: PageId,
    key: &[u8],
    value: &[u8],
    write_txn: TxnId,
) -> Result<PageId, StorageError> {
    // Check if the key already exists.
    let existing = btree_ops::search(store, data_root, key)?;

    let new_doc = if let Some(existing_bytes) = existing {
        // Deserialize the existing versioned document.
        let mut old_doc = VersionedDocument::deserialize(&existing_bytes)?;

        // Mark the old document as deleted at this transaction.
        old_doc.deleted_txn = Some(write_txn);

        // Write the old document to a version chain.
        let old_serialized = old_doc.serialize();
        let (chain_page, chain_len) = version_chain::write_version_chain(store, &old_serialized)?;

        // Create the new version pointing to the old version chain.
        VersionedDocument {
            created_txn: write_txn,
            deleted_txn: None,
            data: value.to_vec(),
            prev_version_page: chain_page,
            prev_version_len: chain_len,
        }
    } else {
        // New key: fresh version with no history.
        VersionedDocument::new(write_txn, value.to_vec())
    };

    // Serialize and insert into the B+Tree.
    let versioned_bytes = new_doc.serialize();
    btree_ops::insert(store, data_root, key, &versioned_bytes)
}

/// Read a value with snapshot isolation.
///
/// Searches the B+Tree for the key, then walks the version chain to find the
/// version visible to the given snapshot transaction ID.
///
/// Returns `None` if the key does not exist or no version is visible.
pub fn mvcc_get(
    store: &impl PageStore,
    data_root: PageId,
    key: &[u8],
    snapshot_txn: TxnId,
) -> Result<Option<Vec<u8>>, StorageError> {
    let raw = btree_ops::search(store, data_root, key)?;

    let Some(raw_bytes) = raw else {
        return Ok(None);
    };

    let latest = VersionedDocument::deserialize(&raw_bytes)?;
    let visible = visibility::find_visible_version(store, &latest, snapshot_txn)?;

    Ok(visible.map(|doc| doc.data))
}

/// Soft-delete a key with MVCC versioning.
///
/// Sets the `deleted_txn` on the latest version. Older snapshots will still
/// see the value. The key is NOT physically removed from the B+Tree.
///
/// Returns the (possibly new) B+Tree root page ID.
pub fn mvcc_delete(
    store: &mut impl PageStore,
    data_root: PageId,
    key: &[u8],
    write_txn: TxnId,
) -> Result<PageId, StorageError> {
    let raw = btree_ops::search(store, data_root, key)?;

    let Some(raw_bytes) = raw else {
        // Key doesn't exist: no-op.
        return Ok(data_root);
    };

    let mut doc = VersionedDocument::deserialize(&raw_bytes)?;

    if doc.deleted_txn.is_some() {
        // Already deleted: no-op.
        return Ok(data_root);
    }

    // Mark as deleted.
    doc.deleted_txn = Some(write_txn);

    // Re-insert the updated version into the B+Tree.
    let serialized = doc.serialize();
    btree_ops::insert(store, data_root, key, &serialized)
}

/// Read a value with snapshot isolation, returning the version number.
///
/// Like `mvcc_get` but also returns the `created_txn` of the visible version.
/// The version can be used for optimistic concurrency control via
/// `mvcc_put_conditional`.
pub fn mvcc_get_versioned(
    store: &impl PageStore,
    data_root: PageId,
    key: &[u8],
    snapshot_txn: TxnId,
) -> Result<Option<(Vec<u8>, TxnId)>, StorageError> {
    let raw = btree_ops::search(store, data_root, key)?;

    let Some(raw_bytes) = raw else {
        return Ok(None);
    };

    let latest = VersionedDocument::deserialize(&raw_bytes)?;
    let visible = visibility::find_visible_version(store, &latest, snapshot_txn)?;

    Ok(visible.map(|doc| (doc.data, doc.created_txn)))
}

/// Insert or update a key-value pair with optimistic concurrency control.
///
/// Checks that the current version's `created_txn` matches `expected_version`
/// before writing. Returns `TxnError::VersionMismatch` if:
/// - The key exists but its `created_txn` differs from `expected_version`
/// - The key does not exist (version 0 vs the expected version)
///
/// Returns the (possibly new) B+Tree root page ID.
pub fn mvcc_put_conditional(
    store: &mut impl PageStore,
    data_root: PageId,
    key: &[u8],
    value: &[u8],
    write_txn: TxnId,
    expected_version: TxnId,
) -> Result<PageId, crate::error::Error> {
    let existing = btree_ops::search(store, data_root, key)?;

    match existing {
        Some(existing_bytes) => {
            let mut old_doc = VersionedDocument::deserialize(&existing_bytes)?;

            if old_doc.created_txn != expected_version {
                return Err(crate::error::TxnError::VersionMismatch {
                    expected: expected_version,
                    actual: old_doc.created_txn,
                }
                .into());
            }

            // Version matches: archive old and insert new.
            old_doc.deleted_txn = Some(write_txn);
            let old_serialized = old_doc.serialize();
            let (chain_page, chain_len) =
                version_chain::write_version_chain(store, &old_serialized)?;

            let new_doc = VersionedDocument {
                created_txn: write_txn,
                deleted_txn: None,
                data: value.to_vec(),
                prev_version_page: chain_page,
                prev_version_len: chain_len,
            };

            let versioned_bytes = new_doc.serialize();
            Ok(btree_ops::insert(store, data_root, key, &versioned_bytes)?)
        }
        None => {
            // Key doesn't exist but caller expected a specific version.
            Err(crate::error::TxnError::VersionMismatch {
                expected: expected_version,
                actual: 0,
            }
            .into())
        }
    }
}

/// A key-value pair with raw byte vectors.
pub type RawKvPair = (Vec<u8>, Vec<u8>);

/// Range scan with snapshot isolation.
///
/// Scans the B+Tree for keys in [start_key, end_key) and applies MVCC
/// visibility filtering. Returns key-value pairs where the value is the
/// raw `data` from the visible VersionedDocument.
pub fn mvcc_range_scan(
    store: &impl PageStore,
    data_root: PageId,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    snapshot_txn: TxnId,
) -> Result<Vec<RawKvPair>, StorageError> {
    let raw_pairs = btree_ops::range_scan(store, data_root, start_key, end_key)?;
    let mut results = Vec::new();
    for (key, value_bytes) in raw_pairs {
        let latest = VersionedDocument::deserialize(&value_bytes)?;
        let visible = visibility::find_visible_version(store, &latest, snapshot_txn)?;
        if let Some(doc) = visible {
            results.push((key, doc.data));
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;
    use crate::btree::ops::create_tree;

    /// Helper to create a fresh B+Tree and return (store, root_id).
    fn setup() -> (InMemoryPageStore, PageId) {
        let mut store = InMemoryPageStore::new();
        let root = create_tree(&mut store).unwrap();
        (store, root)
    }

    #[test]
    fn test_mvcc_put_and_get() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"value1", 1).unwrap();
        let result = mvcc_get(&store, root, b"key1", 1).unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_mvcc_put_invisible_to_old_snapshot() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"value1", 5).unwrap();

        // Snapshot at txn 3 should not see the value (created at txn 5).
        let result = mvcc_get(&store, root, b"key1", 3).unwrap();
        assert_eq!(result, None);

        // Snapshot at txn 5 should see it.
        let result = mvcc_get(&store, root, b"key1", 5).unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_mvcc_put_overwrite() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"v1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key1", b"v2", 5).unwrap();

        // Snapshot at txn 3: sees old version (v1).
        let result = mvcc_get(&store, root, b"key1", 3).unwrap();
        assert_eq!(result, Some(b"v1".to_vec()));

        // Snapshot at txn 5: sees new version (v2).
        let result = mvcc_get(&store, root, b"key1", 5).unwrap();
        assert_eq!(result, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_mvcc_delete() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"value1", 1).unwrap();
        root = mvcc_delete(&mut store, root, b"key1", 5).unwrap();

        // Snapshot at txn 5: deleted, so not visible.
        let result = mvcc_get(&store, root, b"key1", 5).unwrap();
        assert_eq!(result, None);

        // Snapshot at txn 10: also deleted.
        let result = mvcc_get(&store, root, b"key1", 10).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_mvcc_delete_invisible_to_old_snapshot() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"value1", 1).unwrap();
        root = mvcc_delete(&mut store, root, b"key1", 5).unwrap();

        // Snapshot at txn 3: deleted_txn(5) > 3, so still visible.
        let result = mvcc_get(&store, root, b"key1", 3).unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_mvcc_put_multiple_keys() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"a", b"val_a", 1).unwrap();
        root = mvcc_put(&mut store, root, b"b", b"val_b", 3).unwrap();
        root = mvcc_put(&mut store, root, b"c", b"val_c", 7).unwrap();

        // Snapshot at txn 5.
        assert_eq!(
            mvcc_get(&store, root, b"a", 5).unwrap(),
            Some(b"val_a".to_vec())
        );
        assert_eq!(
            mvcc_get(&store, root, b"b", 5).unwrap(),
            Some(b"val_b".to_vec())
        );
        // c was created at txn 7 > 5, not visible.
        assert_eq!(mvcc_get(&store, root, b"c", 5).unwrap(), None);
    }

    #[test]
    fn test_mvcc_overwrite_twice() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key", b"v1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key", b"v2", 5).unwrap();
        root = mvcc_put(&mut store, root, b"key", b"v3", 10).unwrap();

        // Snapshot at txn 3: sees v1.
        assert_eq!(
            mvcc_get(&store, root, b"key", 3).unwrap(),
            Some(b"v1".to_vec())
        );

        // Snapshot at txn 7: sees v2.
        assert_eq!(
            mvcc_get(&store, root, b"key", 7).unwrap(),
            Some(b"v2".to_vec())
        );

        // Snapshot at txn 10: sees v3.
        assert_eq!(
            mvcc_get(&store, root, b"key", 10).unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[test]
    fn test_mvcc_delete_nonexistent() {
        let (mut store, root) = setup();
        // Deleting a non-existent key should be a no-op.
        let new_root = mvcc_delete(&mut store, root, b"nope", 1).unwrap();
        assert_eq!(new_root, root);
    }

    #[test]
    fn test_mvcc_range_scan_basic() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"a", b"val_a", 1).unwrap();
        root = mvcc_put(&mut store, root, b"b", b"val_b", 2).unwrap();
        root = mvcc_put(&mut store, root, b"c", b"val_c", 3).unwrap();

        // Snapshot at txn 5: all visible.
        let results = mvcc_range_scan(&store, root, None, None, 5).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"a".to_vec(), b"val_a".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"val_b".to_vec()));
        assert_eq!(results[2], (b"c".to_vec(), b"val_c".to_vec()));
    }

    #[test]
    fn test_mvcc_range_scan_visibility() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"a", b"val_a", 1).unwrap();
        root = mvcc_put(&mut store, root, b"b", b"val_b", 5).unwrap();
        root = mvcc_put(&mut store, root, b"c", b"val_c", 10).unwrap();

        // Snapshot at txn 3: only "a" visible.
        let results = mvcc_range_scan(&store, root, None, None, 3).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"a".to_vec(), b"val_a".to_vec()));

        // Snapshot at txn 7: "a" and "b" visible.
        let results = mvcc_range_scan(&store, root, None, None, 7).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_mvcc_range_scan_with_bounds() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"a", b"va", 1).unwrap();
        root = mvcc_put(&mut store, root, b"b", b"vb", 1).unwrap();
        root = mvcc_put(&mut store, root, b"c", b"vc", 1).unwrap();
        root = mvcc_put(&mut store, root, b"d", b"vd", 1).unwrap();

        // Range [b, d) should return b and c.
        let results = mvcc_range_scan(&store, root, Some(b"b"), Some(b"d"), 5).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"b".to_vec());
        assert_eq!(results[1].0, b"c".to_vec());
    }

    #[test]
    fn test_mvcc_get_versioned() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"value1", 3).unwrap();

        let result = mvcc_get_versioned(&store, root, b"key1", 3).unwrap();
        assert!(result.is_some());
        let (data, version) = result.unwrap();
        assert_eq!(data, b"value1");
        assert_eq!(version, 3);
    }

    #[test]
    fn test_mvcc_get_versioned_not_found() {
        let (store, root) = setup();
        let result = mvcc_get_versioned(&store, root, b"nope", 5).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mvcc_get_versioned_after_overwrite() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"v1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key1", b"v2", 5).unwrap();

        // Snapshot at 5: sees v2 created at txn 5.
        let (data, version) = mvcc_get_versioned(&store, root, b"key1", 5)
            .unwrap()
            .unwrap();
        assert_eq!(data, b"v2");
        assert_eq!(version, 5);

        // Snapshot at 3: sees v1 created at txn 1.
        let (data, version) = mvcc_get_versioned(&store, root, b"key1", 3)
            .unwrap()
            .unwrap();
        assert_eq!(data, b"v1");
        assert_eq!(version, 1);
    }

    #[test]
    fn test_mvcc_put_conditional_success() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"v1", 3).unwrap();

        // Conditional put with correct expected version.
        root = mvcc_put_conditional(&mut store, root, b"key1", b"v2", 7, 3).unwrap();

        // Should see v2 now.
        let result = mvcc_get(&store, root, b"key1", 7).unwrap();
        assert_eq!(result, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_mvcc_put_conditional_version_mismatch() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"v1", 3).unwrap();

        // Conditional put with wrong expected version.
        let result = mvcc_put_conditional(&mut store, root, b"key1", b"v2", 7, 99);
        assert!(result.is_err());

        match result {
            Err(crate::error::Error::Transaction(crate::error::TxnError::VersionMismatch {
                expected: 99,
                actual: 3,
            })) => {}
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_mvcc_put_conditional_key_not_found() {
        let (mut store, root) = setup();

        // Conditional put on non-existent key.
        let result = mvcc_put_conditional(&mut store, root, b"nope", b"v1", 5, 1);
        assert!(result.is_err());

        match result {
            Err(crate::error::Error::Transaction(crate::error::TxnError::VersionMismatch {
                expected: 1,
                actual: 0,
            })) => {}
            other => panic!("expected VersionMismatch with actual=0, got {other:?}"),
        }
    }

    #[test]
    fn test_mvcc_put_conditional_preserves_history() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"key1", b"v1", 1).unwrap();
        root = mvcc_put_conditional(&mut store, root, b"key1", b"v2", 5, 1).unwrap();

        // Old snapshot at txn 3 should still see v1.
        let result = mvcc_get(&store, root, b"key1", 3).unwrap();
        assert_eq!(result, Some(b"v1".to_vec()));

        // New snapshot at txn 5 should see v2.
        let result = mvcc_get(&store, root, b"key1", 5).unwrap();
        assert_eq!(result, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_mvcc_range_scan_deleted() {
        let (mut store, mut root) = setup();
        root = mvcc_put(&mut store, root, b"a", b"val_a", 1).unwrap();
        root = mvcc_put(&mut store, root, b"b", b"val_b", 1).unwrap();
        root = mvcc_delete(&mut store, root, b"a", 5).unwrap();

        // Snapshot at txn 10: "a" deleted, only "b" visible.
        let results = mvcc_range_scan(&store, root, None, None, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"b".to_vec(), b"val_b".to_vec()));

        // Snapshot at txn 3: "a" still visible (deleted at 5 > 3).
        let results = mvcc_range_scan(&store, root, None, None, 3).unwrap();
        assert_eq!(results.len(), 2);
    }
}
