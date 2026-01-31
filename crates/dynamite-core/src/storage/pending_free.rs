use crate::types::{PageId, TxnId};

/// Tracks pages freed by transactions that cannot yet be reclaimed.
///
/// Pages freed by a write transaction go here instead of the on-disk free list.
/// They become reclaimable only when no active snapshot predates the freeing transaction.
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
