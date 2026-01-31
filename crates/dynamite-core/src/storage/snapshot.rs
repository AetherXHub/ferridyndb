use std::collections::BTreeSet;

use parking_lot::Mutex;

use crate::types::TxnId;

/// Tracks active read snapshots to prevent premature page reclamation.
///
/// Each active read transaction registers a snapshot with the tracker.
/// The page manager uses [`oldest_active`](Self::oldest_active) to decide
/// which freed pages can be safely reclaimed.
pub struct SnapshotTracker {
    active: Mutex<BTreeSet<TxnId>>,
}

impl SnapshotTracker {
    /// Create a new, empty snapshot tracker.
    pub fn new() -> Self {
        Self {
            active: Mutex::new(BTreeSet::new()),
        }
    }

    /// Register a new active snapshot. Returns an RAII guard that automatically
    /// deregisters the snapshot when dropped.
    pub fn register(&self, txn_id: TxnId) -> SnapshotGuard<'_> {
        self.active.lock().insert(txn_id);
        SnapshotGuard {
            tracker: self,
            txn_id,
        }
    }

    /// Return the oldest (minimum) active snapshot, or `None` if no snapshots are active.
    pub fn oldest_active(&self) -> Option<TxnId> {
        self.active.lock().iter().next().copied()
    }

    /// Return the number of active snapshots.
    pub fn active_count(&self) -> usize {
        self.active.lock().len()
    }
}

impl Default for SnapshotTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that deregisters a snapshot when dropped.
pub struct SnapshotGuard<'a> {
    tracker: &'a SnapshotTracker,
    txn_id: TxnId,
}

impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        self.tracker.active.lock().remove(&self.txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_oldest() {
        let tracker = SnapshotTracker::new();

        let _g1 = tracker.register(5);
        let _g2 = tracker.register(10);
        let _g3 = tracker.register(15);

        assert_eq!(tracker.oldest_active(), Some(5));
        assert_eq!(tracker.active_count(), 3);
    }

    #[test]
    fn test_deregister_on_drop() {
        let tracker = SnapshotTracker::new();

        {
            let _guard = tracker.register(5);
            assert_eq!(tracker.oldest_active(), Some(5));
            assert_eq!(tracker.active_count(), 1);
        }
        // Guard dropped — snapshot deregistered.
        assert_eq!(tracker.oldest_active(), None);
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn test_oldest_updates() {
        let tracker = SnapshotTracker::new();

        let g1 = tracker.register(5);
        let _g2 = tracker.register(10);

        assert_eq!(tracker.oldest_active(), Some(5));

        // Drop snapshot 5
        drop(g1);
        assert_eq!(tracker.oldest_active(), Some(10));
    }

    #[test]
    fn test_active_count() {
        let tracker = SnapshotTracker::new();
        assert_eq!(tracker.active_count(), 0);

        let g1 = tracker.register(1);
        assert_eq!(tracker.active_count(), 1);

        let g2 = tracker.register(2);
        assert_eq!(tracker.active_count(), 2);

        drop(g1);
        assert_eq!(tracker.active_count(), 1);

        drop(g2);
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn test_no_active_snapshots() {
        let tracker = SnapshotTracker::new();
        assert_eq!(tracker.oldest_active(), None);
        assert_eq!(tracker.active_count(), 0);
    }
}
