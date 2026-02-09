# PRD: Count on Index Queries

**Priority:** 16
**Status:** Approved

## Summary

Extend the `count` operation (PRD-14) to support secondary index queries. Add `count_index(table, index_name)` that returns the number of matching items in a secondary index without transferring document bodies. Same parameters as `query_index` (index key value, sort key condition, filter) but returns only `usize`.

## Scope

### In Scope

- `CountIndexBuilder` with index key value, sort key conditions, and filter expression support
- `FerridynDB::count_index(table, index_name)` entry point
- Reuse `compute_index_scan_bounds()` for range computation
- TTL-aware: expired items are not counted
- Lazy GC: stale index entries (pointing to deleted/updated base items) are not counted
- Server protocol support (`CountIndex` request/response)
- Client method (`FerridynClient::count_index`)

### Out of Scope

- `SUM`, `AVG`, `MIN`, `MAX` aggregations — count only
- Pre-computed/cached index counts — exact count at query time only
- Count on scan (full-index count) — could add later

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Separate builder or extend CountBuilder? | Separate `CountIndexBuilder` | Mirrors `QueryBuilder` / `IndexQueryBuilder` separation; index queries need `index_name` and `key_value` instead of `partition_key` |
| Deserialization? | Same as PRD-14: skip if no TTL and no filter | Consistent behavior |
| Stale index entries? | Skip via base table lookup (same as `IndexQueryBuilder`) | Consistent with query_index behavior |
| Wire op name? | `count_index` | Mirrors `query_index` naming |

## Implementation

### Phase 1: Core Count Index API

**Deliverables:**
- `CountIndexBuilder` struct with `.key_value()`, sort key condition methods, `.filter()`
- `FerridynDB::count_index(table, index_name) -> CountIndexBuilder`
- Implementation: same index scan as `IndexQueryBuilder`, but increment counter instead of collecting items

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/builders.rs` | Add `CountIndexBuilder` |
| `ferridyn-core/src/api/database.rs` | Add `count_index()` method |
| `ferridyn-core/src/api/mod.rs` | Export `CountIndexBuilder` |

**Tests:**
- `test_count_index_basic` (count all items matching index key)
- `test_count_index_with_sort_condition` (count with sort key range on composite index)
- `test_count_index_empty` (returns 0 for no matches)
- `test_count_index_excludes_expired` (TTL-expired items not counted)
- `test_count_index_skips_stale` (deleted base items not counted)
- `test_count_index_matches_query_index_len` (count equals query_index result length)
- `test_count_index_with_filter` (count with filter expression)
- `test_count_index_local` (count on LSI)

### Phase 2: Server Protocol Integration

**Deliverables:**
- `CountIndex` request variant with table, index_name, key_value, sort_key_condition, filter
- `CountIndex` response variant (reuse `OkResponse::Count`)
- Server handler forwarding to core API
- `FerridynClient::count_index()` async method

**Tests:**
- `test_count_index_over_wire`
- `test_count_index_with_filter_over_wire`

## Acceptance Criteria

1. Returns correct count without transferring item bodies
2. Supports index key value, sort key condition, and filter expression
3. Expired items (TTL) are not counted
4. Stale index entries are not counted
5. Count of 0 for no matches (not an error)
6. Count result matches `query_index().execute().items.len()` for the same parameters
7. Works on both GSI and LSI
8. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD-14 (Count / Basic Aggregation) — establishes count patterns and wire protocol
- PRD-09 (Global Secondary Indexes) — index infrastructure

## Dependents

- None

## Documentation

**IMPORTANT:** Documentation MUST be updated to reflect the changes introduced by this PRD. This includes:

- **Root README** (`README.md`) — Feature list, code examples, design decisions
- **Server README** (`crates/ferridyn-server/README.md`) — Protocol examples, client usage, features list
- **CLAUDE.md** — Architecture description, workspace layout, test counts, API patterns
