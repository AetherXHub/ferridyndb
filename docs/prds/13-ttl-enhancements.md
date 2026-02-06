# PRD: TTL Enhancements

**Priority:** 13
**Status:** Approved
**Proposal:** [docs/proposals/accepted/ttl-enhancements.md](../proposals/accepted/ttl-enhancements.md)

## Summary

Enhance the existing TTL system with convenience methods for managing item expiry and an optional background reaper for automatic cleanup. FerridynDB already supports TTL via `ttl_attribute` on table schemas, read-side filtering, and explicit `sweep_expired_ttl()`. This PRD adds `set_ttl`, `remove_ttl`, and a configurable background sweep.

## Current State

Already implemented:
- `TableBuilder::ttl_attribute()` -- designate a numeric attribute as the TTL field
- `is_ttl_expired()` -- read-side filtering on get, query, scan, query_index
- `sweep_expired_ttl()` -- explicit batch deletion of expired items (100 per call)
- DynamoDB semantics: TTL=0 means never expires, non-numeric ignored, missing attribute = permanent

Missing:
- No convenience methods for setting/removing TTL on existing items
- No background/automatic cleanup -- requires explicit `sweep_expired_ttl()` calls
- No server protocol support for TTL management

## Scope

### In Scope

- `set_ttl(table, pk, sk, ttl_seconds)` -- compute `expires_at = now + ttl_seconds`, update the TTL attribute
- `remove_ttl(table, pk, sk)` -- remove the TTL attribute (or set to 0), making the item permanent
- `get_ttl(table, pk, sk) -> Option<u64>` -- return remaining seconds until expiry (None if no TTL)
- Background reaper thread (opt-in, configurable interval)
- Server protocol support for `SetTTL`, `RemoveTTL`, `GetTTL`, and `SweepExpiredTTL`
- Client methods for all TTL operations

### Out of Scope

- Changing the TTL storage model (keep attribute-based approach)
- TTL on write (`put_item(..., ttl_seconds)`) -- callers can set the TTL attribute directly or call `set_ttl` after
- Per-item TTL attribute names -- TTL attribute is per-table
- Stream events for TTL expiration -- defer to change streams PRD

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| `set_ttl` implementation? | Use `update_item` internally (SET ttl_attribute) | Reuses existing update infrastructure |
| `remove_ttl` implementation? | Use `update_item` internally (SET ttl_attribute to 0) | TTL=0 already means "never expires" per DynamoDB semantics |
| Background reaper model? | Dedicated thread with configurable sleep interval | Simple; no async runtime needed in core crate |
| Default reaper interval? | 1 hour | Sensible default; expired items are already invisible to reads |
| Reaper opt-in? | Yes, via `FerridynDB::start_reaper(interval)` | Embedded users may not want a background thread |
| Reaper batch size? | Reuse existing 100-item batch from `sweep_expired_ttl` | Proven approach; repeat until 0 per sweep cycle |

## Implementation Phases

### Phase 1: Convenience Methods

**Deliverables:**
- `FerridynDB::set_ttl(table, pk, sk, ttl_seconds)` -- uses `update_item` to SET the table's TTL attribute to `now + ttl_seconds`
- `FerridynDB::remove_ttl(table, pk, sk)` -- uses `update_item` to SET TTL attribute to 0
- `FerridynDB::get_ttl(table, pk, sk) -> Result<Option<u64>>` -- reads item, extracts TTL attribute, computes remaining seconds
- Validation: error if table has no `ttl_attribute` configured

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/database.rs` | Add `set_ttl()`, `remove_ttl()`, `get_ttl()` methods |

**Tests:**
- `test_set_ttl_updates_expiry`
- `test_set_ttl_on_table_without_ttl_attribute` (error)
- `test_set_ttl_on_nonexistent_item` (error)
- `test_remove_ttl_makes_permanent`
- `test_get_ttl_returns_remaining_seconds`
- `test_get_ttl_no_ttl_set` (returns None)
- `test_get_ttl_already_expired` (returns 0 or None)

### Phase 2: Background Reaper

**Deliverables:**
- `FerridynDB::start_reaper(interval: Duration) -> ReaperHandle`
- `ReaperHandle::stop()` to shut down the background thread
- Reaper thread: loop over all tables with `ttl_attribute`, call `sweep_expired_ttl` repeatedly until 0, sleep for interval
- Graceful shutdown on `FerridynDB::drop` if reaper is running

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/reaper.rs` | New: `ReaperHandle`, background thread logic |
| `ferridyn-core/src/api/database.rs` | Add `start_reaper()`, store `ReaperHandle` |
| `ferridyn-core/src/api/mod.rs` | Export reaper module |

**Tests:**
- `test_reaper_cleans_expired_items` (start reaper with short interval, verify cleanup)
- `test_reaper_skips_tables_without_ttl`
- `test_reaper_stop` (graceful shutdown)
- `test_reaper_does_not_delete_live_items`

### Phase 3: Server Protocol Integration

**Deliverables:**
- `SetTTL`, `RemoveTTL`, `GetTTL`, `SweepExpiredTTL` request/response variants
- Server handlers forwarding to core API
- `FerridynClient` async methods for all TTL operations

**Tests:**
- `test_set_ttl_over_wire`
- `test_remove_ttl_over_wire`
- `test_get_ttl_over_wire`
- `test_sweep_ttl_over_wire`

## Acceptance Criteria

1. `set_ttl` makes an item expire after the specified duration
2. `remove_ttl` makes an item permanent (survives indefinitely)
3. `get_ttl` returns accurate remaining seconds
4. Background reaper automatically cleans expired items at configured interval
5. Reaper shuts down cleanly on stop or database drop
6. All operations error appropriately on tables without TTL configured
7. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD #1 (UpdateItem) -- `set_ttl` and `remove_ttl` use `update_item` internally (already complete)

## Dependents

- None
