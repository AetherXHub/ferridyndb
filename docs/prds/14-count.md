# PRD: Count / Basic Aggregation

**Priority:** 14
**Status:** Approved
**Proposal:** [docs/proposals/accepted/count.md](../proposals/accepted/count.md)

## Summary

Add a `count` operation that returns the number of matching items without transferring document bodies. Same parameters as `query` (partition key, sort key condition, filter) but returns only `usize`. Reduces wire transfer and client memory usage for statistics, health checks, and rate-limiting checks.

## Scope

### In Scope

- `CountBuilder` with partition key, sort key condition, and filter expression support
- `FerridynDB::count(table)` entry point
- B+Tree traversal that counts matching items without deserializing full documents (optimization)
- TTL-aware: expired items are not counted
- Server protocol support (`Count` request/response)
- Client method (`FerridynClient::count`)

### Out of Scope

- `SUM`, `AVG`, `MIN`, `MAX` aggregations -- count only for now
- `count` on `scan` (full-table count) -- could add later but low priority
- Approximate count (DynamoDB's `DescribeTable` item count) -- exact count only
- Count on index queries -- defer to later iteration

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Deserialization optimization? | Deserialize for filter evaluation; skip if no filter | Filters need document access; without filter, count keys only |
| Filter dependency? | Optional; count works without filters | Useful even without PRD #2 (filter expressions) |
| TTL handling? | Expired items excluded from count | Consistent with query behavior |
| Limit interaction? | No limit on count | Count returns total; use query with limit for pagination |

## Implementation

### Phase 1: Core Count API

**Deliverables:**
- `CountBuilder` struct with `.partition_key()`, `.sort_key_begins_with()`, and sort key range methods
- `FerridynDB::count(table) -> CountBuilder`
- Implementation: same B+Tree scan as query, but increment counter instead of collecting items
- Without filter: count encoded keys without full document deserialization
- With filter (if PRD #2 is implemented): deserialize and evaluate filter, count matches
- TTL check: deserialize TTL attribute only (partial deserialization or full if filter also present)

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/builders.rs` | Add `CountBuilder` |
| `ferridyn-core/src/api/database.rs` | Add `count()` method, implement count logic |

**Tests:**
- `test_count_basic` (count all items in partition)
- `test_count_with_sort_key_prefix` (count with begins_with)
- `test_count_with_sort_key_range` (count with range condition, if PRD #11 is implemented)
- `test_count_empty_partition` (returns 0)
- `test_count_excludes_expired` (TTL-expired items not counted)
- `test_count_nonexistent_table` (error)
- `test_count_matches_query_len` (verify count equals query result length)

### Phase 2: Filter Integration

**Prerequisite:** PRD #2 (Filter Expressions)

**Deliverables:**
- `.filter()` method on `CountBuilder`
- Count evaluates filter on each item, counts matches

**Tests:**
- `test_count_with_filter` (count items matching filter)
- `test_count_filter_no_matches` (returns 0)

### Phase 3: Server Protocol Integration

**Deliverables:**
- `Count` request variant with table, partition_key, sort_key_condition, filter
- `Count` response variant with `count: usize`
- Server handler forwarding to core API
- `FerridynClient::count()` async method

**Tests:**
- `test_count_over_wire`
- `test_count_with_filter_over_wire`

## Acceptance Criteria

1. Returns correct count without transferring item bodies
2. Supports partition key, sort key condition, and filter expression
3. Expired items (TTL) are not counted
4. Count of 0 for empty partitions or no matches (not an error)
5. Count result matches `query().execute().items.len()` for the same parameters
6. Performance: count without filter is faster than full query (no document deserialization)
7. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD #2 (Filter Expressions) -- for Phase 2 filter integration (Phase 1 works without it)

## Dependents

- None
