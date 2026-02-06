use crate::btree::PageStore;
use crate::error::StorageError;
use crate::storage::page::PageType;
use crate::types::{PAGE_SIZE, PageId, TxnId};

/// Offset within a PendingFree page where the next-page link is stored.
const NEXT_PAGE_OFFSET: usize = 40;

/// Offset within a PendingFree page where serialized data begins.
const DATA_OFFSET: usize = 48;

/// Usable data bytes per PendingFree page.
const DATA_PER_PAGE: usize = PAGE_SIZE - DATA_OFFSET;

/// Tracks pages freed by transactions that cannot yet be reclaimed.
///
/// Pages freed by a write transaction go here instead of the on-disk free list.
/// They become reclaimable only when no active snapshot predates the freeing transaction.
///
/// Persisted to disk as a chain of `PendingFree` pages rooted from the file header.
pub struct PendingFreeList {
    /// Each entry: (freeing_txn_id, list of freed page IDs).
    entries: Vec<(TxnId, Vec<PageId>)>,
}

impl PendingFreeList {
    /// Create an empty pending-free list.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Record that the given pages were freed by transaction `txn_id`.
    ///
    /// If `pages` is empty this is a no-op.
    pub fn add(&mut self, txn_id: TxnId, pages: Vec<PageId>) {
        if !pages.is_empty() {
            self.entries.push((txn_id, pages));
        }
    }

    /// Drain and return all pages that are reclaimable given the oldest active snapshot.
    ///
    /// A batch is reclaimable if `batch_txn_id <= oldest_active_snapshot`.
    /// If `oldest_active_snapshot` is `None` (no active readers), **all** pages are reclaimable.
    pub fn drain_reclaimable(&mut self, oldest_active_snapshot: Option<TxnId>) -> Vec<PageId> {
        let mut reclaimable = Vec::new();

        match oldest_active_snapshot {
            None => {
                // No active readers — everything is reclaimable.
                for (_txn_id, pages) in self.entries.drain(..) {
                    reclaimable.extend(pages);
                }
            }
            Some(oldest) => {
                let mut kept = Vec::new();
                for (txn_id, pages) in self.entries.drain(..) {
                    if txn_id <= oldest {
                        reclaimable.extend(pages);
                    } else {
                        kept.push((txn_id, pages));
                    }
                }
                self.entries = kept;
            }
        }

        reclaimable
    }

    /// Return the total number of pending pages across all batches.
    pub fn pending_count(&self) -> usize {
        self.entries.iter().map(|(_, pages)| pages.len()).sum()
    }

    /// Serialize the pending free list into newly allocated pages.
    ///
    /// Returns the root page ID of the chain (0 if the list is empty).
    /// Each page uses the `PendingFree` page type with a next-page link
    /// at offset 32 and serialized data starting at offset 40.
    pub fn serialize_to_pages(&self, store: &mut impl PageStore) -> Result<PageId, StorageError> {
        if self.entries.is_empty() {
            return Ok(0);
        }

        // Serialize to a flat byte array.
        let data = self.serialize_to_bytes();

        // Split into page-sized chunks and allocate pages in reverse so we
        // can set next-page links.
        let chunks: Vec<&[u8]> = data.chunks(DATA_PER_PAGE).collect();
        let mut next_page_id: PageId = 0;
        let mut root_page_id: PageId = 0;

        for chunk in chunks.iter().rev() {
            let mut page = store.allocate_page(PageType::PendingFree)?;
            let pid = page.page_id();

            // Write next-page link.
            page.data_mut()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 8]
                .copy_from_slice(&next_page_id.to_le_bytes());
            // Write data.
            page.data_mut()[DATA_OFFSET..DATA_OFFSET + chunk.len()].copy_from_slice(chunk);

            store.write_page(page)?;

            next_page_id = pid;
            root_page_id = pid;
        }

        Ok(root_page_id)
    }

    /// Deserialize a pending free list from a chain of pages on disk.
    ///
    /// Returns an empty list if `root_page_id` is 0.
    pub fn deserialize_from_pages(
        store: &impl PageStore,
        root_page_id: PageId,
    ) -> Result<Self, StorageError> {
        if root_page_id == 0 {
            return Ok(Self::new());
        }

        // Read all chained pages into a contiguous byte buffer.
        let mut data = Vec::new();
        let mut current = root_page_id;

        while current != 0 {
            let page = store.read_page(current)?;
            let next = u64::from_le_bytes(
                page.data()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 8]
                    .try_into()
                    .unwrap(),
            );
            data.extend_from_slice(&page.data()[DATA_OFFSET..]);
            current = next;
        }

        Self::deserialize_from_bytes(&data)
    }

    /// Collect all page IDs in a PendingFree chain (for freeing old chains).
    pub fn collect_chain_page_ids(
        store: &impl PageStore,
        root_page_id: PageId,
    ) -> Result<Vec<PageId>, StorageError> {
        let mut ids = Vec::new();
        let mut current = root_page_id;

        while current != 0 {
            ids.push(current);
            let page = store.read_page(current)?;
            current = u64::from_le_bytes(
                page.data()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 8]
                    .try_into()
                    .unwrap(),
            );
        }

        Ok(ids)
    }

    /// Serialize entries to a flat byte buffer.
    ///
    /// Format: `[entry_count: u32]` then for each entry:
    /// `[txn_id: u64, page_count: u32, page_ids: [u64; page_count]]`
    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (txn_id, pages) in &self.entries {
            buf.extend_from_slice(&txn_id.to_le_bytes());
            buf.extend_from_slice(&(pages.len() as u32).to_le_bytes());
            for &pid in pages {
                buf.extend_from_slice(&pid.to_le_bytes());
            }
        }
        buf
    }

    /// Deserialize entries from a flat byte buffer.
    fn deserialize_from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 4 {
            return Err(StorageError::CorruptedPage(
                "pending free list too short".to_string(),
            ));
        }
        let entry_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut entries = Vec::with_capacity(entry_count);

        for _ in 0..entry_count {
            if offset + 12 > data.len() {
                return Err(StorageError::CorruptedPage(
                    "pending free list truncated".to_string(),
                ));
            }
            let txn_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let page_count =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            let needed = page_count * 8;
            if offset + needed > data.len() {
                return Err(StorageError::CorruptedPage(
                    "pending free list truncated".to_string(),
                ));
            }
            let mut pages = Vec::with_capacity(page_count);
            for _ in 0..page_count {
                let pid = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                pages.push(pid);
            }
            entries.push((txn_id, pages));
        }

        Ok(Self { entries })
    }
}

impl Default for PendingFreeList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_drain_all() {
        let mut pfl = PendingFreeList::new();
        pfl.add(1, vec![10, 20, 30]);
        pfl.add(2, vec![40, 50]);

        assert_eq!(pfl.pending_count(), 5);

        // No active snapshot — drain all.
        let mut pages = pfl.drain_reclaimable(None);
        pages.sort();
        assert_eq!(pages, vec![10, 20, 30, 40, 50]);
        assert_eq!(pfl.pending_count(), 0);
    }

    #[test]
    fn test_drain_respects_snapshot() {
        let mut pfl = PendingFreeList::new();
        pfl.add(5, vec![100, 200]); // txn 5
        pfl.add(10, vec![300, 400]); // txn 10

        // Oldest active snapshot is 7 — only txn 5 batch is reclaimable.
        let mut pages = pfl.drain_reclaimable(Some(7));
        pages.sort();
        assert_eq!(pages, vec![100, 200]);

        // txn 10 batch is still pending
        assert_eq!(pfl.pending_count(), 2);

        // Now drain with snapshot 10 — txn 10 batch becomes reclaimable.
        let mut pages = pfl.drain_reclaimable(Some(10));
        pages.sort();
        assert_eq!(pages, vec![300, 400]);
        assert_eq!(pfl.pending_count(), 0);
    }

    #[test]
    fn test_drain_nothing_reclaimable() {
        let mut pfl = PendingFreeList::new();
        pfl.add(10, vec![100, 200]);

        // Oldest active snapshot is 5 — txn 10 batch is NOT reclaimable
        // because the batch was freed at txn 10 which is > oldest snapshot 5.
        let pages = pfl.drain_reclaimable(Some(5));
        assert!(pages.is_empty());
        assert_eq!(pfl.pending_count(), 2);
    }

    #[test]
    fn test_pending_count() {
        let mut pfl = PendingFreeList::new();
        assert_eq!(pfl.pending_count(), 0);

        pfl.add(1, vec![10, 20]);
        assert_eq!(pfl.pending_count(), 2);

        pfl.add(2, vec![30]);
        assert_eq!(pfl.pending_count(), 3);

        // Drain the first batch
        let _ = pfl.drain_reclaimable(Some(1));
        assert_eq!(pfl.pending_count(), 1);

        // Drain the rest
        let _ = pfl.drain_reclaimable(None);
        assert_eq!(pfl.pending_count(), 0);
    }
}
