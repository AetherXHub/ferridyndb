# PRD: BatchGetItem

**Priority:** 5
**Status:** Complete
**Proposal:** [docs/proposals/accepted/batch-get-item.md](../proposals/accepted/batch-get-item.md)

## Summary

Add `batch_get_item` for retrieving multiple items by key in a single call. Provides ergonomic improvement for embedded use and significant latency reduction for server use (N round-trips reduced to 1). All items are read from a single MVCC snapshot, providing stronger consistency than DynamoDB's BatchGetItem.

## Scope

### In Scope

- Single-table `BatchGetItemBuilder` with `.key()` chaining
- Positional `Vec<Option<Value>>` result (None for missing items)
- Single MVCC snapshot for all reads (consistent point-in-time view)
- Server protocol support (`BatchGetItem` request/response)
- Client method (`FerridynClient::batch_get_item`)
- Server-side batch size limit (1000 items)

### Out of Scope

- Cross-table batch (`batch_get_items` across multiple tables) -- defer to Phase 2
- Versioned batch reads (`execute_versioned()`) -- defer
- ProjectionExpression integration -- defer until PRD #7 lands
- Partial failure handling (UnprocessedKeys) -- all-or-nothing for now

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Result format? | Positional `Vec<Option<Value>>` | Simple, matches input order, DynamoDB-like |
| Cross-table? | Defer | Single-table covers 90% of use cases |
| Batch size limit? | 1000 (server), unlimited (embedded) | Generous for local IPC; prevents oversized requests |
| Versioned reads? | Defer | Niche use case; add later without breaking changes |
| Partial failure? | All-or-nothing | Simpler; server-side partial failure not needed yet |

## Implementation

### Phase 1: Core API

**Deliverables:**
- `BatchGetItemBuilder` struct with `.key()` and `.execute()` methods
- `FerridynDB::batch_get_item(table)` entry point
- Implementation: single `read_snapshot()`, loop B+Tree lookups, collect results
- Key encoding reuses existing `encode_composite_key` logic

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/builders.rs` | Add `BatchGetItemBuilder` |
| `ferridyn-core/src/api/database.rs` | Add `batch_get_item()` method |

**Tests:**
- `test_batch_get_basic` (3 keys, all exist)
- `test_batch_get_some_missing` (3 keys, 1 missing -> None in that position)
- `test_batch_get_empty` (0 keys -> empty vec)
- `test_batch_get_single` (degenerate: 1 key)
- `test_batch_get_snapshot_consistency` (all items from same snapshot)
- `test_batch_get_with_sort_key` (composite key table)

### Phase 2: Server Protocol

**Deliverables:**
- `BatchGetItem` request/response in `protocol.rs`
- Server handler: validate batch size <= 1000, call `db.batch_get_item()`
- `FerridynClient::batch_get_item()` async method
- Batch size exceeded error

**Tests:**
- `test_batch_get_over_wire`
- `test_batch_get_exceeds_limit` (> 1000 items returns error)

## Acceptance Criteria

1. Results are positional: `results[i]` corresponds to `keys[i]`
2. Missing items return `None`, not errors
3. All items come from the same MVCC snapshot
4. Server enforces 1000-item limit with clear error message
5. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None

## Dependents

- PRD #7 (Projection Expressions) could integrate `.projection()` on batch reads
