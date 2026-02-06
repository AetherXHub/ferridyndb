//! Garbage collection for MVCC version chains.
//!
//! Frees old versions that are no longer visible to any active snapshot and
//! removes fully-dead entries from the B+Tree.

use crate::btree::PageStore;
use crate::btree::ops as btree_ops;
use crate::error::StorageError;
use crate::types::{PageId, TxnId};

use super::version_chain;
use super::versioned::VersionedDocument;

/// Maximum number of cleanups per GC invocation (batch limit).
const GC_BATCH_LIMIT: usize = 100;

/// Run garbage collection on the MVCC B+Tree.
///
/// Scans all entries and:
/// 1. Frees version chains for old versions that are invisible to all snapshots.
/// 2. Removes fully-dead entries (deleted and invisible) from the B+Tree.
///
/// `oldest_snapshot` is the oldest active snapshot transaction ID. If `None`,
/// all old versions can be freed.
///
/// Returns `(new_data_root, number_of_cleanups)`.
pub fn gc_versions(
    store: &mut impl PageStore,
    data_root: PageId,
    oldest_snapshot: Option<TxnId>,
) -> Result<(PageId, usize), StorageError> {
    // Take a snapshot of all keys and values via range scan.
    let entries = btree_ops::range_scan(store, data_root, None, None)?;

    let mut current_root = data_root;
    let mut freed_count = 0usize;

    for (key, value_bytes) in &entries {
        if freed_count >= GC_BATCH_LIMIT {
            break;
        }

        // Re-read the current value from the tree (it may have changed due to
        // prior GC modifications in this loop).
        let current_value = btree_ops::search(store, current_root, key)?;
        let Some(current_bytes) = current_value else {
            continue;
        };

        let mut doc = VersionedDocument::deserialize(&current_bytes)?;

        // Use the original value to match the expected entry from the scan.
        // If the current bytes differ from the scan bytes, skip (already processed).
        if current_bytes != *value_bytes {
            continue;
        }

        // Case 1: Free old version chains that are invisible.
        if doc.prev_version_page != 0 {
            let prev_bytes = version_chain::read_version_chain(
                store,
                doc.prev_version_page,
                doc.prev_version_len,
            )?;
            let prev_doc = VersionedDocument::deserialize(&prev_bytes)?;

            let can_free = if let Some(oldest) = oldest_snapshot {
                // The previous version is fully invisible if:
                // - It has a deleted_txn AND that deleted_txn <= oldest_snapshot
                prev_doc.deleted_txn.is_some_and(|d| d <= oldest)
            } else {
                // No active snapshots: all old versions can be freed.
                true
            };

            if can_free {
                // Recursively free any deeper chain links from the freed version.
                if prev_doc.prev_version_page != 0 {
                    free_chain_recursive(store, &prev_doc)?;
                }

                // Free the immediate previous version chain.
                version_chain::free_version_chain(store, doc.prev_version_page)?;

                // Update the latest version to remove the chain link.
                doc.prev_version_page = 0;
                doc.prev_version_len = 0;

                let serialized = doc.serialize();
                current_root = btree_ops::insert(store, current_root, key, &serialized)?;

                freed_count += 1;

                // Re-read after modification for the dead-entry check below.
                let updated = btree_ops::search(store, current_root, key)?;
                if let Some(updated_bytes) = updated {
                    doc = VersionedDocument::deserialize(&updated_bytes)?;
                } else {
                    continue;
                }
            }
        }

        // Case 2: Remove fully dead entries from the B+Tree.
        // An entry is fully dead if:
        // - It is deleted (deleted_txn.is_some())
        // - It has no previous version chain
        // - The deletion is invisible to all snapshots
        if let Some(deleted_txn) = doc.deleted_txn
            && doc.prev_version_page == 0
        {
            let can_remove = if let Some(oldest) = oldest_snapshot {
                deleted_txn <= oldest
            } else {
                true
            };

            if can_remove {
                current_root = btree_ops::delete(store, current_root, key)?;
                freed_count += 1;
            }
        }
    }

    Ok((current_root, freed_count))
}

/// Run incremental garbage collection using tombstone entries.
///
/// Instead of scanning the full B-tree, processes only the keys listed in
/// `tombstones`. Each tombstone identifies a key that was deleted and may
/// be eligible for cleanup.
///
/// Returns `(new_data_root, number_of_cleanups)`.
pub fn gc_versions_incremental(
    store: &mut impl PageStore,
    data_root: PageId,
    oldest_snapshot: Option<TxnId>,
    tombstones: &[(TxnId, &[u8])],
) -> Result<(PageId, usize), StorageError> {
    let mut current_root = data_root;
    let mut freed_count = 0usize;

    for &(deleted_txn, key) in tombstones {
        if freed_count >= GC_BATCH_LIMIT {
            break;
        }

        // Check if this tombstone is reclaimable.
        let can_reclaim = if let Some(oldest) = oldest_snapshot {
            deleted_txn <= oldest
        } else {
            true
        };
        if !can_reclaim {
            continue;
        }

        // Look up the key in the data tree.
        let current_value = btree_ops::search(store, current_root, key)?;
        let Some(current_bytes) = current_value else {
            // Already removed by a previous GC pass.
            continue;
        };

        let mut doc = VersionedDocument::deserialize(&current_bytes)?;

        // Free old version chains if present.
        if doc.prev_version_page != 0 {
            let prev_bytes = version_chain::read_version_chain(
                store,
                doc.prev_version_page,
                doc.prev_version_len,
            )?;
            let prev_doc = VersionedDocument::deserialize(&prev_bytes)?;

            let can_free = if let Some(oldest) = oldest_snapshot {
                prev_doc.deleted_txn.is_some_and(|d| d <= oldest)
            } else {
                true
            };

            if can_free {
                if prev_doc.prev_version_page != 0 {
                    free_chain_recursive(store, &prev_doc)?;
                }
                version_chain::free_version_chain(store, doc.prev_version_page)?;
                doc.prev_version_page = 0;
                doc.prev_version_len = 0;

                let serialized = doc.serialize();
                current_root = btree_ops::insert(store, current_root, key, &serialized)?;
                freed_count += 1;

                // Re-read after modification.
                let updated = btree_ops::search(store, current_root, key)?;
                if let Some(updated_bytes) = updated {
                    doc = VersionedDocument::deserialize(&updated_bytes)?;
                } else {
                    continue;
                }
            }
        }

        // Remove fully dead entries from the B+Tree.
        if doc.deleted_txn.is_some() && doc.prev_version_page == 0 {
            current_root = btree_ops::delete(store, current_root, key)?;
            freed_count += 1;
        }
    }

    Ok((current_root, freed_count))
}

/// Recursively free version chains from a previous document.
fn free_chain_recursive(
    store: &mut impl PageStore,
    doc: &VersionedDocument,
) -> Result<(), StorageError> {
    if doc.prev_version_page == 0 {
        return Ok(());
    }

    // Read the deeper version.
    let deeper_bytes =
        version_chain::read_version_chain(store, doc.prev_version_page, doc.prev_version_len)?;
    let deeper_doc = VersionedDocument::deserialize(&deeper_bytes)?;

    // Recurse first (free deepest chains first).
    if deeper_doc.prev_version_page != 0 {
        free_chain_recursive(store, &deeper_doc)?;
    }

    // Free this level's chain.
    version_chain::free_version_chain(store, doc.prev_version_page)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;
    use crate::btree::ops::create_tree;
    use crate::mvcc::ops::{mvcc_get, mvcc_put};

    fn setup() -> (InMemoryPageStore, PageId) {
        let mut store = InMemoryPageStore::new();
        let root = create_tree(&mut store).unwrap();
        (store, root)
    }

    #[test]
    fn test_gc_frees_old_version() {
        let (mut store, mut root) = setup();

        // Put at txn 1, overwrite at txn 5.
        root = mvcc_put(&mut store, root, b"key", b"v1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key", b"v2", 5).unwrap();

        // Before GC, snapshot at txn 3 can see v1.
        assert_eq!(
            mvcc_get(&store, root, b"key", 3).unwrap(),
            Some(b"v1".to_vec())
        );

        // GC with no active snapshots: old version should be freed.
        let (new_root, count) = gc_versions(&mut store, root, None).unwrap();
        assert!(count > 0);
        root = new_root;

        // After GC, the latest version (v2) is still accessible.
        assert_eq!(
            mvcc_get(&store, root, b"key", 5).unwrap(),
            Some(b"v2".to_vec())
        );

        // After GC, snapshot at txn 3 can no longer see v1 (chain freed).
        assert_eq!(mvcc_get(&store, root, b"key", 3).unwrap(), None);
    }

    #[test]
    fn test_gc_respects_active_snapshot() {
        let (mut store, mut root) = setup();

        // Put at txn 1, overwrite at txn 5.
        root = mvcc_put(&mut store, root, b"key", b"v1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key", b"v2", 5).unwrap();

        // GC with oldest active snapshot at txn 3.
        // The old version (created at 1, deleted at 5) needs to be visible to
        // snapshot 3, so deleted_txn (5) > oldest_snapshot (3) means it should
        // NOT be freed.
        let (new_root, count) = gc_versions(&mut store, root, Some(3)).unwrap();
        assert_eq!(count, 0);
        root = new_root;

        // Old version should still be accessible.
        assert_eq!(
            mvcc_get(&store, root, b"key", 3).unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn test_gc_removes_dead_entries() {
        let (mut store, mut root) = setup();

        // Put at txn 1, delete at txn 5.
        root = mvcc_put(&mut store, root, b"key", b"value", 1).unwrap();
        root = crate::mvcc::ops::mvcc_delete(&mut store, root, b"key", 5).unwrap();

        // GC with no active snapshots: entry should be fully removed.
        let (new_root, count) = gc_versions(&mut store, root, None).unwrap();
        assert!(count > 0);
        root = new_root;

        // Key should be completely gone from the B+Tree.
        let raw = crate::btree::ops::search(&store, root, b"key").unwrap();
        assert!(raw.is_none());
    }

    #[test]
    fn test_gc_batch_limit() {
        let (mut store, mut root) = setup();

        // Create 150 overwritten entries.
        for i in 0..150u32 {
            let key = format!("key_{i:03}");
            root = mvcc_put(&mut store, root, key.as_bytes(), b"v1", 1).unwrap();
            root = mvcc_put(&mut store, root, key.as_bytes(), b"v2", 5).unwrap();
        }

        // GC with no active snapshots should process at most 100.
        let (_new_root, count) = gc_versions(&mut store, root, None).unwrap();
        assert!(count <= 100, "GC should process at most 100, got {count}");
        assert!(count > 0, "GC should process at least some entries");
    }

    #[test]
    fn test_incremental_gc_removes_dead_entry() {
        let (mut store, mut root) = setup();

        // Put at txn 1, delete at txn 5.
        root = mvcc_put(&mut store, root, b"key1", b"value1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key2", b"value2", 1).unwrap();
        root = crate::mvcc::ops::mvcc_delete(&mut store, root, b"key1", 5).unwrap();

        // Incremental GC with tombstone for key1 only.
        let tombstones: Vec<(u64, &[u8])> = vec![(5, b"key1")];
        let (new_root, count) =
            gc_versions_incremental(&mut store, root, None, &tombstones).unwrap();
        assert!(count > 0);
        root = new_root;

        // key1 should be fully removed.
        let raw = crate::btree::ops::search(&store, root, b"key1").unwrap();
        assert!(raw.is_none());

        // key2 should still exist.
        let val = mvcc_get(&store, root, b"key2", 5).unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_incremental_gc_respects_snapshot() {
        let (mut store, mut root) = setup();

        root = mvcc_put(&mut store, root, b"key", b"value", 1).unwrap();
        root = crate::mvcc::ops::mvcc_delete(&mut store, root, b"key", 5).unwrap();

        // Oldest snapshot is 3 — deletion at txn 5 is NOT reclaimable.
        let tombstones: Vec<(u64, &[u8])> = vec![(5, b"key")];
        let (new_root, count) =
            gc_versions_incremental(&mut store, root, Some(3), &tombstones).unwrap();
        assert_eq!(count, 0);
        root = new_root;

        // Key should still exist (visible to snapshot 3).
        let val = mvcc_get(&store, root, b"key", 3).unwrap();
        assert_eq!(val, Some(b"value".to_vec()));
    }

    #[test]
    fn test_incremental_gc_frees_version_chain() {
        let (mut store, mut root) = setup();

        // Put at txn 1, overwrite at txn 5, delete at txn 10.
        root = mvcc_put(&mut store, root, b"key", b"v1", 1).unwrap();
        root = mvcc_put(&mut store, root, b"key", b"v2", 5).unwrap();
        root = crate::mvcc::ops::mvcc_delete(&mut store, root, b"key", 10).unwrap();

        // Incremental GC with tombstone — no active snapshots.
        let tombstones: Vec<(u64, &[u8])> = vec![(10, b"key")];
        let (new_root, count) =
            gc_versions_incremental(&mut store, root, None, &tombstones).unwrap();
        assert!(count > 0);
        root = new_root;

        // Key should be completely gone.
        let raw = crate::btree::ops::search(&store, root, b"key").unwrap();
        assert!(raw.is_none());
    }
}
