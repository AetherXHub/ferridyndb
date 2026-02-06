# PRD: Copy-on-Write Commit Hardening

**Priority:** 17
**Status:** Complete
**Origin:** redb design analysis — crash safety improvements

**Roadmap Sequencing:** Do **between PRD-08** (MessagePack Wire Protocol) **and PRD-09** (Global Secondary Indexes). GSIs multiply the pages touched per commit; fixing the commit protocol before that prevents crash-corruption of both primary and index trees.

## Summary

Harden the commit protocol to eliminate the possibility of crash-induced data corruption. Three coordinated changes inspired by redb's design: (1) true copy-on-write for all modified pages so the old header's tree is never damaged, (2) a 1-phase-plus-checksum (1PC+C) commit mode that detects partial writes via header checksum verification on recovery, and (3) persisting the pending free list so crashed transactions don't permanently leak pages.

## Problem Statement

The current commit path (`database.rs:commit_txn`) writes modified pages to their **original page_id** on disk before writing the new header. If a crash occurs mid-flush:
- The old header still references pages that have been partially overwritten
- The database cannot safely recover because the old tree's pages are damaged
- This violates the copy-on-write guarantee that makes double-buffered headers safe

Additionally:
- The `PendingFreeList` is purely in-memory; crashed transactions leak pages permanently
- There is no mechanism to detect or recover from partial header writes beyond "pick the valid one with highest txn_counter"

## Scope

### In Scope

#### Change 1: True Copy-on-Write Page Allocation

When a B-tree operation modifies an existing page (e.g., inserting into an existing leaf), allocate a **new page_id** for the modified version instead of writing back to the original page_id. The original page remains untouched until the new header is committed, at which point the old page is added to the free list.

This requires changes to `BufferedPageStore` to intercept writes to existing page_ids and redirect them to fresh allocations, and propagating the new page_id up through parent internal nodes.

#### Change 2: 1PC+C Commit Mode

Enhance the commit protocol with checksum-based partial-write detection (redb's default strategy):
1. Write all data pages (all to new page_ids per Change 1)
2. Write the new header to the alternate slot, including a checksum of the entire header
3. Single `fsync`
4. On recovery: verify the header checksum of the primary slot. If invalid (partial write), fall back to the other slot.

The existing header already has an xxHash64 checksum of bytes 0..44. Verify this is checked on recovery and that the recovery path correctly falls back.

#### Change 3: Persist Pending Free List

Write the pending free list entries to a dedicated system structure during commit so that after a crash, pages freed by committed transactions can still be reclaimed. Options:
1. **System B-tree:** Store `(txn_id, Vec<page_id>)` entries in a B-tree rooted from the header (similar to redb's freed tree)
2. **Header extension:** Store a root page pointer in the header to a linked list of pending-free trunk pages (similar to existing free list format)

### Out of Scope

- 2-phase commit (two fsyncs) — overkill for embedded local-first use case
- Buddy allocator / variable-size pages — deferred to v2
- B-tree rebalancing — separate concern, deferred
- Savepoints — useful but independent feature, can be added later

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| COW granularity | Page-level (every modified page gets new ID) | Simplest correct approach; matches redb |
| Where to persist pending free list | System B-tree rooted from header | Reuses existing B-tree infrastructure; supports arbitrary size; matches redb's freed tree pattern |
| Commit mode default | 1PC+C (single fsync + checksum verify) | Good balance of durability and performance; redb's default |
| Keep `SyncMode::None`? | Yes, as opt-in | Some users (tests, ephemeral data) want maximum speed |
| Header checksum algorithm | Keep xxHash64 (already used) | Sufficient for partial-write detection; upgrade to XXH3_128 can come later with Merkle tree (PRD-19) |

## Phases

### Phase 1: True Copy-on-Write Pages

| Component | Change |
|-----------|--------|
| `api/page_store.rs` | `BufferedPageStore::write_page()` detects writes to existing page_ids (< `file_total_pages`) and redirects to new allocation |
| `api/page_store.rs` | Track old-to-new page_id mapping for parent pointer updates |
| `btree/ops.rs` | After any page write, propagate new page_id to parent internal node (already partially done for splits, needs generalization) |
| `api/database.rs` | `commit_txn` adds original page_ids of COW-replaced pages to pending free list |
| Tests | Crash simulation: verify old header's tree is intact after partial flush |

### Phase 2: 1PC+C Commit Protocol

| Component | Change |
|-----------|--------|
| `api/database.rs` | Verify `commit_txn` writes all data pages before header (already the case) |
| `storage/header.rs` | Verify header checksum is validated on `read_header` and recovery falls back to alternate slot on failure |
| `api/database.rs` | Add recovery path: on open, validate both headers, pick valid one with highest txn_counter, mark recovery_needed if primary was invalid |
| `api/database.rs` | After recovery, rebuild free list state from the valid header's tree |
| Tests | Simulate partial header write, verify recovery to alternate slot |

### Phase 3: Persistent Pending Free List

| Component | Change |
|-----------|--------|
| `storage/header.rs` | Add `pending_free_root: Option<PageId>` field to `FileHeader` |
| `storage/pending_free.rs` | Replace in-memory `Vec` with B-tree-backed persistent storage |
| `api/database.rs` | On commit: write pending free entries to the system B-tree before writing the header |
| `api/database.rs` | On open/recovery: read pending free list from the system B-tree and reclaim pages from committed transactions with no active readers |
| `mvcc/gc.rs` | Integrate with persistent pending free list for reclamation |
| Tests | Crash after commit, reopen, verify no leaked pages |

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Write amplification increase | Every modify = allocate new page + free old page, roughly 2x page writes | Monitor benchmarks; the free list should absorb steady-state; only net-new growth increases file size |
| Parent pointer propagation complexity | Must update all ancestors up to root when a page gets a new ID | B-tree splits already do this; generalize the existing mechanism |
| File format change (header) | Adding pending_free_root to header | Reserve space in current header padding bytes; bump format version |
| Performance regression | Extra allocation + free list bookkeeping per commit | Benchmark before/after; the single-fsync 1PC+C should offset some cost vs current approach |

## Success Criteria

- [ ] No existing page_id is overwritten during a transaction (all writes go to new pages)
- [ ] Crash during commit leaves the previous header's tree fully intact and readable
- [ ] Recovery correctly falls back to alternate header when primary has invalid checksum
- [ ] Pending free list survives crash and pages are reclaimed on next open
- [ ] Write amplification increase is < 2x on standard benchmarks
- [ ] All existing tests pass (511+ tests)
- [ ] `#[allow(dead_code)]` removed from `pending_free` and `snapshot_tracker` fields
