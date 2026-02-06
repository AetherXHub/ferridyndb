# PRD: Concurrent Readers & Incremental GC

**Priority:** 18
**Status:** Approved
**Origin:** redb design analysis — concurrency and scale improvements

**Roadmap Sequencing:** Do **between PRD-10** (Change Streams) **and PRD-11** (Sort Key Range Queries). Change stream consumers poll reads while writes are happening; the current exclusive-lock model blocks them entirely during commits. Incremental GC prevents O(n) full scans as the database grows with GSIs + streams.

## Summary

Two coordinated improvements to FerridynDB's concurrency and garbage collection:
1. **Concurrent readers during writes:** Allow read transactions to proceed using their snapshot while a write transaction is in progress, using the existing `SnapshotTracker` infrastructure (currently `#[allow(dead_code)]`).
2. **Incremental GC:** Replace the full B-tree range scan in `gc.rs` with a targeted approach that only visits entries known to need cleanup, reducing GC cost from O(total_entries) to O(deleted_entries).

## Problem Statement

### Reader Contention

The `transact()` method acquires an exclusive write lock (`state.write()` at `database.rs:446`) that blocks ALL readers for the entire transaction duration. `read_snapshot()` acquires a read lock (`state.read()` at `database.rs:478`). Since these use `parking_lot::RwLock`, readers and the writer are mutually exclusive.

This means:
- A long-running write transaction blocks all reads
- Change stream consumers (PRD-10) cannot poll during writes
- Multi-threaded read workloads stall when any write is in progress

The `SnapshotTracker` (`storage/snapshot.rs`) and `PendingFreeList` (`storage/pending_free.rs`) already exist to support concurrent readers — they track active snapshots and defer page reclamation — but they are marked `#[allow(dead_code)]` and unused in the commit path.

### GC Scalability

The current GC (`mvcc/gc.rs:33`) performs `btree_ops::range_scan(store, data_root, None, None)` — a full scan of the entire B-tree — to find entries with `deleted_txn` set. With a `GC_BATCH_LIMIT` of 100, it processes at most 100 entries per invocation but still scans the entire tree to find them. For a database with 100K items and 50 deleted, GC reads 100K entries to find 50.

With GSIs (PRD-09) and change streams (PRD-10), there are more B-trees and more versions to clean up, making O(n) GC increasingly expensive.

## Scope

### In Scope

#### Change 1: Concurrent Readers During Writes

Enable read transactions to use a snapshot of the database state (header, catalog root, txn_counter) captured at read-start time, independent of any in-progress write. The writer only needs exclusive access to the mutable state (page allocation, pending free list, header slot), not to the entire `DatabaseState`.

Key changes:
- Split `DatabaseState` into immutable snapshot data (readable by many) and mutable writer state (exclusive to writer)
- `read_snapshot()` captures a lightweight snapshot (header copy + catalog root) without blocking writes
- Register active snapshots in `SnapshotTracker` so GC knows the oldest active reader
- Pages freed by a commit are deferred in `PendingFreeList` until no snapshot references them
- Writer still has exclusive write access (single-writer model is preserved)

#### Change 2: Incremental GC

Replace full-scan GC with a targeted approach. Options:
1. **Tombstone queue:** When a delete marks an entry as dead (`deleted_txn = Some(txn_id)`), append `(txn_id, key)` to a persistent queue. GC processes the queue instead of scanning.
2. **Dirty page tracking:** Track which leaf pages have dead entries. GC only visits those pages.
3. **Epoch watermark:** Track the oldest active snapshot's txn_id. GC scans entries with `deleted_txn <= watermark` using a secondary index on `deleted_txn`.

### Out of Scope

- Multi-writer concurrency — keep single-writer model
- Read-your-writes within the same thread during a snapshot — snapshots see committed state only
- B-tree rebalancing during GC — still mark-dead only, just find them faster
- Cross-process concurrent readers — `flock` still provides inter-process exclusivity

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Snapshot data structure | Copy of `FileHeader` (52 bytes) | Cheap to copy, contains all root pointers needed for reads |
| Reader registration | `SnapshotTracker::register()` returns a guard that auto-deregisters on drop | RAII prevents leaked registrations |
| GC approach | Option 1: Tombstone queue (persistent) | Most direct — O(deleted) work, no secondary index needed, naturally pairs with persistent pending free list from PRD-17 |
| Tombstone queue storage | System B-tree keyed by `(deleted_txn, key)` | Reuses B-tree infrastructure; ordered by txn allows efficient range-based cleanup |
| Lock granularity | `RwLock<ReadState>` + `Mutex<WriteState>` | Readers never block on writers; writers never block on readers reading |

## Phases

### Phase 1: Concurrent Readers

| Component | Change |
|-----------|--------|
| `api/database.rs` | Split `DatabaseState` into `ReadState` (RwLock, header snapshot) and `WriteState` (Mutex, allocation state) |
| `api/database.rs` | `read_snapshot()` reads current committed header without blocking writes |
| `api/database.rs` | `transact()` acquires `WriteState` mutex only, updates `ReadState` atomically after commit |
| `storage/snapshot.rs` | Wire up `SnapshotTracker`: register on `read_snapshot()`, deregister when snapshot is dropped |
| `storage/pending_free.rs` | Wire up `PendingFreeList`: check `SnapshotTracker::min_active_txn()` before reclaiming |
| `api/database.rs` | Remove `#[allow(dead_code)]` from `pending_free` and `snapshot_tracker` |
| Tests | Concurrent read + write test: reader sees consistent snapshot while writer commits |
| Tests | Stress test: many readers + one writer, no data races |

### Phase 2: Incremental GC

| Component | Change |
|-----------|--------|
| `storage/header.rs` | Add `tombstone_root: Option<PageId>` to header (or reuse pending_free system B-tree from PRD-17) |
| `btree/ops.rs` | On `delete_entry` (mark-dead), append `(deleted_txn, encoded_key)` to tombstone queue |
| `mvcc/gc.rs` | Replace full range scan with tombstone queue drain: read entries where `deleted_txn <= min_active_txn`, delete them from both the data tree and the tombstone queue |
| `mvcc/gc.rs` | Keep `GC_BATCH_LIMIT` for incremental processing |
| Tests | Delete 50 items in 100K database, verify GC only reads ~50 entries |
| Benchmarks | Compare GC time: full-scan vs incremental on 10K, 100K, 1M item databases |

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Snapshot leak (reader never drops) | Pages never freed, file grows unbounded | RAII guard; add timeout/warning for long-lived snapshots |
| Tombstone queue grows large | Extra storage overhead | Queue entries are small (txn_id + key); cleaned up during GC |
| Lock splitting correctness | Subtle bugs if reader sees inconsistent state | `ReadState` is updated atomically (swap header pointer) after commit completes; readers always see a consistent committed snapshot |
| Ordering: reader starts during commit | Reader might see pre-commit or post-commit state | Reader captures header BEFORE writer updates it; writer updates atomically after fsync |

## Success Criteria

- [ ] Read operations do not block during write transactions
- [ ] Write transactions do not block during read operations
- [ ] Concurrent readers see consistent snapshots (no torn reads)
- [ ] GC processes only deleted entries, not the full tree
- [ ] GC time is O(deleted_entries), not O(total_entries)
- [ ] `SnapshotTracker` and `PendingFreeList` are actively used (no more `#[allow(dead_code)]`)
- [ ] All existing tests pass
- [ ] No increase in memory usage per reader beyond ~100 bytes (header copy)
