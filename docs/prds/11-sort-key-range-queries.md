# PRD: Sort Key Range Queries

**Priority:** 11
**Status:** Approved
**Proposal:** [docs/proposals/accepted/sort-key-range-queries.md](../proposals/accepted/sort-key-range-queries.md)

## Summary

Extend `QueryBuilder` sort key conditions beyond `begins_with` to support equality, range, and comparison operators. Enables time-range queries, efficient pagination via "greater than last seen key", and general-purpose range scans on sort keys.

## Scope

### In Scope

- Six new `SortKeyCondition` variants: `Equals`, `Between`, `GreaterThan`, `GreaterThanOrEqual`, `LessThan`, `LessThanOrEqual`
- Builder methods on `QueryBuilder`: `.sort_key_eq()`, `.sort_key_between()`, `.sort_key_gt()`, `.sort_key_gte()`, `.sort_key_lt()`, `.sort_key_lte()`
- Support for both String and Number sort keys
- Correct B+Tree scan bound computation for each condition
- Server protocol integration (sort key condition variants on Query request)
- Client method updates

### Out of Scope

- Composite sort key conditions (`gt AND lt` as separate calls) -- use `between` instead
- Sort key conditions on `scan` (scans are full-table by design)
- Descending range with exclusive bounds -- use existing `scan_forward(false)` + appropriate condition

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Between bounds? | Inclusive on both ends | Matches DynamoDB `BETWEEN` behavior |
| Type mismatch? | Return error | Sort key type is declared in schema; mismatched condition is a caller bug |
| Empty range? | Return empty result, not error | `Between("Z", "A")` is valid but empty |
| Naming? | `sort_key_gt`, not `sort_key_greater_than` | Concise, matches common convention |

## Implementation Phases

### Phase 1: Core Query Conditions

**Deliverables:**
- Extend scan bound computation in `query.rs` to handle six new conditions
- `Equals`: start bound = encoded key, end bound = encoded key + 1 byte (or exact match check)
- `Between`: start bound = encode(start), end bound = encode(end), inclusive
- `GreaterThan`: start bound = encode(value) + 1, end bound = partition prefix end
- `GreaterThanOrEqual`: start bound = encode(value), end bound = partition prefix end
- `LessThan`: start bound = partition prefix start, end bound = encode(value) - 1
- `LessThanOrEqual`: start bound = partition prefix start, end bound = encode(value)
- Builder methods on `QueryBuilder`

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/query.rs` | Extend `compute_scan_bounds` for new conditions |
| `ferridyn-core/src/api/builders.rs` | Add `.sort_key_eq()`, `.sort_key_between()`, `.sort_key_gt()`, `.sort_key_gte()`, `.sort_key_lt()`, `.sort_key_lte()` |

**Tests:**
- `test_sort_key_equals` (exact sort key match)
- `test_sort_key_between_string` (inclusive range on string keys)
- `test_sort_key_between_number` (inclusive range on numeric keys)
- `test_sort_key_gt` / `test_sort_key_gte` (greater than / greater than or equal)
- `test_sort_key_lt` / `test_sort_key_lte` (less than / less than or equal)
- `test_sort_key_between_empty_range` (start > end returns empty)
- `test_sort_key_range_with_limit` (range + limit + pagination)
- `test_sort_key_range_reverse` (range + scan_forward=false)

### Phase 2: Server Protocol Integration

**Deliverables:**
- Extend Query request sort key condition serialization to include new variants
- Client builder methods for sort key conditions
- Console `QUERY ... WHERE sk BETWEEN ... AND ...` syntax (stretch)

**Tests:**
- `test_sort_key_range_over_wire`
- `test_sort_key_eq_over_wire`

## Acceptance Criteria

1. All six new sort key conditions return correct result sets
2. Between is inclusive on both ends
3. Range queries work with both String and Number sort keys
4. Conditions compose correctly with partition key, filter expressions, and limit
5. Empty ranges return empty results (not errors)
6. Reverse scan (`scan_forward=false`) works correctly with all conditions
7. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None (extends existing query infrastructure)

## Dependents

- None (but enables richer query patterns for all consumers)
