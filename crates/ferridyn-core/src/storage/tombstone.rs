use crate::btree::PageStore;
use crate::error::StorageError;
use crate::storage::page::PageType;
use crate::types::{PAGE_SIZE, PageId, TxnId};

/// Offset within a Tombstone page where the next-page link is stored.
const NEXT_PAGE_OFFSET: usize = 40;

/// Offset within a Tombstone page where serialized data begins.
const DATA_OFFSET: usize = 48;

/// Usable data bytes per Tombstone page.
const DATA_PER_PAGE: usize = PAGE_SIZE - DATA_OFFSET;

/// A tombstone entry: a key that was deleted from a specific table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneEntry {
    pub table: String,
    pub key: Vec<u8>,
}

/// Tracks keys that were deleted and need GC attention.
///
/// When a delete marks an entry as dead, the key is appended here so that
/// GC can target specific entries instead of doing a full B-tree scan.
///
/// Persisted to disk as a chain of `Tombstone` pages rooted from the file header.
pub struct TombstoneQueue {
    /// Each batch: (txn_id that performed the delete, list of tombstone entries).
    entries: Vec<(TxnId, Vec<TombstoneEntry>)>,
}

impl TombstoneQueue {
    /// Create an empty tombstone queue.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Record that the given keys were deleted by transaction `txn_id`.
    pub fn add(&mut self, txn_id: TxnId, tombstones: Vec<TombstoneEntry>) {
        if !tombstones.is_empty() {
            self.entries.push((txn_id, tombstones));
        }
    }

    /// Drain and return all tombstones that are reclaimable given the oldest active snapshot.
    ///
    /// A batch is reclaimable if `batch_txn_id <= oldest_active_snapshot`.
    /// If `oldest_active_snapshot` is `None` (no active readers), **all** entries are reclaimable.
    pub fn drain_reclaimable(
        &mut self,
        oldest_active_snapshot: Option<TxnId>,
    ) -> Vec<(TxnId, TombstoneEntry)> {
        let mut reclaimable = Vec::new();

        match oldest_active_snapshot {
            None => {
                for (txn_id, entries) in self.entries.drain(..) {
                    for entry in entries {
                        reclaimable.push((txn_id, entry));
                    }
                }
            }
            Some(oldest) => {
                let mut kept = Vec::new();
                for (txn_id, entries) in self.entries.drain(..) {
                    if txn_id <= oldest {
                        for entry in entries {
                            reclaimable.push((txn_id, entry));
                        }
                    } else {
                        kept.push((txn_id, entries));
                    }
                }
                self.entries = kept;
            }
        }

        reclaimable
    }

    /// Return the total number of tombstone entries across all batches.
    pub fn pending_count(&self) -> usize {
        self.entries.iter().map(|(_, entries)| entries.len()).sum()
    }

    /// Serialize the tombstone queue into newly allocated pages.
    ///
    /// Returns the root page ID of the chain (0 if the queue is empty).
    pub fn serialize_to_pages(&self, store: &mut impl PageStore) -> Result<PageId, StorageError> {
        if self.entries.is_empty() {
            return Ok(0);
        }

        let data = self.serialize_to_bytes();
        let chunks: Vec<&[u8]> = data.chunks(DATA_PER_PAGE).collect();
        let mut next_page_id: PageId = 0;
        let mut root_page_id: PageId = 0;

        for chunk in chunks.iter().rev() {
            let mut page = store.allocate_page(PageType::Tombstone)?;
            let pid = page.page_id();

            page.data_mut()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 8]
                .copy_from_slice(&next_page_id.to_le_bytes());
            page.data_mut()[DATA_OFFSET..DATA_OFFSET + chunk.len()].copy_from_slice(chunk);

            store.write_page(page)?;

            next_page_id = pid;
            root_page_id = pid;
        }

        Ok(root_page_id)
    }

    /// Deserialize a tombstone queue from a chain of pages on disk.
    ///
    /// Returns an empty queue if `root_page_id` is 0.
    pub fn deserialize_from_pages(
        store: &impl PageStore,
        root_page_id: PageId,
    ) -> Result<Self, StorageError> {
        if root_page_id == 0 {
            return Ok(Self::new());
        }

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

    /// Collect all page IDs in a Tombstone chain (for freeing old chains).
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
    /// Format: `[batch_count: u32]` then for each batch:
    /// `[txn_id: u64, entry_count: u32]` then for each entry:
    /// `[table_len: u32, table_bytes: [u8], key_len: u32, key_bytes: [u8]]`
    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (txn_id, entries) in &self.entries {
            buf.extend_from_slice(&txn_id.to_le_bytes());
            buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for entry in entries {
                let table_bytes = entry.table.as_bytes();
                buf.extend_from_slice(&(table_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(table_bytes);
                buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                buf.extend_from_slice(&entry.key);
            }
        }
        buf
    }

    /// Deserialize entries from a flat byte buffer.
    fn deserialize_from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 4 {
            return Err(StorageError::CorruptedPage(
                "tombstone queue too short".to_string(),
            ));
        }
        let batch_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut entries = Vec::with_capacity(batch_count);

        for _ in 0..batch_count {
            if offset + 12 > data.len() {
                return Err(StorageError::CorruptedPage(
                    "tombstone queue truncated".to_string(),
                ));
            }
            let txn_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let entry_count =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            let mut batch = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                if offset + 4 > data.len() {
                    return Err(StorageError::CorruptedPage(
                        "tombstone queue truncated".to_string(),
                    ));
                }
                let table_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + table_len > data.len() {
                    return Err(StorageError::CorruptedPage(
                        "tombstone queue truncated".to_string(),
                    ));
                }
                let table = String::from_utf8(data[offset..offset + table_len].to_vec())
                    .map_err(|e| StorageError::CorruptedPage(format!("invalid table name: {e}")))?;
                offset += table_len;

                if offset + 4 > data.len() {
                    return Err(StorageError::CorruptedPage(
                        "tombstone queue truncated".to_string(),
                    ));
                }
                let key_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + key_len > data.len() {
                    return Err(StorageError::CorruptedPage(
                        "tombstone queue truncated".to_string(),
                    ));
                }
                let key = data[offset..offset + key_len].to_vec();
                offset += key_len;

                batch.push(TombstoneEntry { table, key });
            }
            entries.push((txn_id, batch));
        }

        Ok(Self { entries })
    }
}

impl Default for TombstoneQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_drain_all() {
        let mut tq = TombstoneQueue::new();
        tq.add(
            1,
            vec![TombstoneEntry {
                table: "items".to_string(),
                key: b"key1".to_vec(),
            }],
        );
        tq.add(
            2,
            vec![TombstoneEntry {
                table: "items".to_string(),
                key: b"key2".to_vec(),
            }],
        );

        assert_eq!(tq.pending_count(), 2);

        let drained = tq.drain_reclaimable(None);
        assert_eq!(drained.len(), 2);
        assert_eq!(tq.pending_count(), 0);
    }

    #[test]
    fn test_drain_respects_snapshot() {
        let mut tq = TombstoneQueue::new();
        tq.add(
            5,
            vec![TombstoneEntry {
                table: "t".to_string(),
                key: b"a".to_vec(),
            }],
        );
        tq.add(
            10,
            vec![TombstoneEntry {
                table: "t".to_string(),
                key: b"b".to_vec(),
            }],
        );

        let drained = tq.drain_reclaimable(Some(7));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1.key, b"a");
        assert_eq!(tq.pending_count(), 1);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut tq = TombstoneQueue::new();
        tq.add(
            1,
            vec![
                TombstoneEntry {
                    table: "users".to_string(),
                    key: b"alice".to_vec(),
                },
                TombstoneEntry {
                    table: "orders".to_string(),
                    key: b"order_001".to_vec(),
                },
            ],
        );
        tq.add(
            5,
            vec![TombstoneEntry {
                table: "users".to_string(),
                key: b"bob".to_vec(),
            }],
        );

        let bytes = tq.serialize_to_bytes();
        let restored = TombstoneQueue::deserialize_from_bytes(&bytes).unwrap();

        assert_eq!(restored.pending_count(), 3);

        // Verify structure matches.
        let mut restored_mut = restored;
        let all = restored_mut.drain_reclaimable(None);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].0, 1);
        assert_eq!(all[0].1.table, "users");
        assert_eq!(all[0].1.key, b"alice");
        assert_eq!(all[1].0, 1);
        assert_eq!(all[1].1.table, "orders");
        assert_eq!(all[2].0, 5);
        assert_eq!(all[2].1.table, "users");
        assert_eq!(all[2].1.key, b"bob");
    }
}
