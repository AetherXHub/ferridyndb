//! MVCC visibility rules for snapshot isolation.
//!
//! A versioned document is visible to a snapshot at `snapshot_txn` if:
//! - `created_txn <= snapshot_txn`, AND
//! - the document is not deleted, OR it was deleted after the snapshot
//!   (`deleted_txn > snapshot_txn`).

use crate::btree::PageStore;
use crate::error::StorageError;
use crate::types::TxnId;

use super::version_chain;
use super::versioned::VersionedDocument;

/// Check if a versioned document is visible to a snapshot at `snapshot_txn`.
///
/// Visible means: created before or at the snapshot AND (not deleted OR deleted
/// after the snapshot).
pub fn is_visible(doc: &VersionedDocument, snapshot_txn: TxnId) -> bool {
    doc.created_txn <= snapshot_txn && doc.deleted_txn.is_none_or(|d| d > snapshot_txn)
}

/// Starting from a deserialized `VersionedDocument` (the latest version from the
/// B+Tree), walk the version chain backward to find the first version visible to
/// `snapshot_txn`.
///
/// Returns `None` if no version in the chain is visible.
pub fn find_visible_version(
    store: &impl PageStore,
    latest: &VersionedDocument,
    snapshot_txn: TxnId,
) -> Result<Option<VersionedDocument>, StorageError> {
    // Check if the latest version is visible.
    if is_visible(latest, snapshot_txn) {
        return Ok(Some(latest.clone()));
    }

    // Walk backward through the version chain.
    let mut current = latest.clone();
    while current.prev_version_page != 0 {
        let chain_bytes = version_chain::read_version_chain(
            store,
            current.prev_version_page,
            current.prev_version_len,
        )?;
        let prev_doc = VersionedDocument::deserialize(&chain_bytes)?;

        if is_visible(&prev_doc, snapshot_txn) {
            return Ok(Some(prev_doc));
        }

        current = prev_doc;
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;

    #[test]
    fn test_visible_basic() {
        let doc = VersionedDocument::new(5, b"data".to_vec());
        assert!(is_visible(&doc, 5));
        assert!(is_visible(&doc, 10));
    }

    #[test]
    fn test_invisible_created_after() {
        let doc = VersionedDocument::new(10, b"data".to_vec());
        assert!(!is_visible(&doc, 5));
    }

    #[test]
    fn test_visible_not_deleted() {
        let doc = VersionedDocument::new(1, b"data".to_vec());
        // No deleted_txn => visible to any snapshot >= created_txn
        assert!(is_visible(&doc, 1));
        assert!(is_visible(&doc, 100));
    }

    #[test]
    fn test_invisible_deleted_before() {
        let mut doc = VersionedDocument::new(1, b"data".to_vec());
        doc.deleted_txn = Some(5);
        // Snapshot at 5: deleted_txn (5) is NOT > 5, so invisible
        assert!(!is_visible(&doc, 5));
        // Snapshot at 10: deleted_txn (5) is NOT > 10, so invisible
        assert!(!is_visible(&doc, 10));
    }

    #[test]
    fn test_visible_deleted_after() {
        let mut doc = VersionedDocument::new(1, b"data".to_vec());
        doc.deleted_txn = Some(10);
        // Snapshot at 5: deleted_txn (10) > 5, so visible
        assert!(is_visible(&doc, 5));
    }

    #[test]
    fn test_find_visible_latest() {
        let store = InMemoryPageStore::new();
        let doc = VersionedDocument::new(1, b"latest".to_vec());
        let result = find_visible_version(&store, &doc, 5).unwrap();
        assert_eq!(result, Some(doc));
    }

    #[test]
    fn test_find_visible_walks_chain() {
        let mut store = InMemoryPageStore::new();

        // Create an old version (created at txn 1, deleted at txn 5).
        let mut old_doc = VersionedDocument::new(1, b"old_data".to_vec());
        old_doc.deleted_txn = Some(5);

        // Write old version to a version chain.
        let (chain_page, chain_len) =
            version_chain::write_version_chain(&mut store, &old_doc.serialize()).unwrap();

        // Create the latest version (created at txn 5, not deleted) pointing to old.
        let latest = VersionedDocument {
            created_txn: 5,
            deleted_txn: None,
            data: b"new_data".to_vec(),
            prev_version_page: chain_page,
            prev_version_len: chain_len,
        };

        // Snapshot at txn 3: latest is invisible (created at 5 > 3),
        // old is visible (created at 1 <= 3, deleted at 5 > 3).
        let result = find_visible_version(&store, &latest, 3).unwrap();
        assert_eq!(result.unwrap().data, b"old_data");

        // Snapshot at txn 5: latest is visible.
        let result = find_visible_version(&store, &latest, 5).unwrap();
        assert_eq!(result.unwrap().data, b"new_data");
    }

    #[test]
    fn test_find_visible_no_version() {
        let store = InMemoryPageStore::new();
        // Document created at txn 10, no chain.
        let doc = VersionedDocument::new(10, b"data".to_vec());
        // Snapshot at txn 5: invisible and no previous version.
        let result = find_visible_version(&store, &doc, 5).unwrap();
        assert_eq!(result, None);
    }
}
