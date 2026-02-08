# PRD: Projection Expressions

**Priority:** 7
**Status:** Complete
**Proposal:** [docs/proposals/accepted/projection-expressions.md](../proposals/accepted/projection-expressions.md)

## Summary

Add `ProjectionExpression` support so clients can specify which attributes to return from read operations. Reduces wire transfer size (especially for ferridyn-server) and simplifies client code. Key attributes (pk, sk) are always included regardless of projection.

## Scope

### In Scope

- `.projection(&[&str])` method on `GetItemBuilder`, `QueryBuilder`, `ScanBuilder`, `IndexQueryBuilder`
- Dot-separated nested path support (`"address.city"`)
- Key attributes always included in results
- Missing projected attributes silently omitted
- Empty projection returns full document (backward compatible)
- Server protocol integration
- Post-deserialization filtering (full document is always read from storage)

### Out of Scope

- Array indexing (`items[0].name`) -- DynamoDB doesn't support this either
- Partial deserialization (skip unused fields during MessagePack decode) -- requires storage format changes
- Wildcard paths (`address.*`)
- Projection size limits

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Array indexing? | No | DynamoDB doesn't support it; not justified |
| Missing attributes? | Silently omit | Matches DynamoDB; avoids errors on heterogeneous documents |
| Filter + projection order? | Filter first, then projection | Matches DynamoDB |
| TransactGetItems? | Yes, add projection support | Consistency |

## Implementation

### Phase 1: Projection Utility and Builder Integration

**Deliverables:**
- `apply_projection(doc, projection, key_attributes)` utility function
- Path traversal: split on `.`, navigate nested objects, build result preserving nesting
- Key attribute detection from `TableSchema`
- `.projection()` method on `GetItemBuilder`, `QueryBuilder`, `ScanBuilder`, `IndexQueryBuilder`
- Apply projection after filter evaluation (if both present), before adding to results

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/projection.rs` | New: `apply_projection()` and path traversal logic |
| `ferridyn-core/src/api/builders.rs` | Add `projection` field and `.projection()` to all read builders |
| `ferridyn-core/src/api/mod.rs` | Export projection module |

**Tests:**
- `test_projection_top_level` (2-3 attrs from 10-attr doc)
- `test_projection_nested` (`"address.city"` preserves nesting)
- `test_projection_keys_always_included` (project `["name"]`, pk/sk still present)
- `test_projection_missing_attr` (no error, attribute omitted)
- `test_projection_empty` (full document returned)
- `test_projection_query` (multiple docs)
- `test_projection_scan` (multiple docs)
- `test_projection_index_query`
- `test_projection_get_item`

### Phase 2: Server Protocol

**Deliverables:**
- `projection: Option<Vec<String>>` on GetItem, Query, Scan, IndexQuery requests
- Server handler passes through to core builders
- Client builder methods for `.projection()`

**Tests:**
- `test_projection_over_wire`

## Acceptance Criteria

1. Projected documents contain only requested attributes plus key attributes
2. Nested paths preserve document structure (`"address.city"` returns `{"address": {"city": ...}}`)
3. Missing attributes are silently omitted
4. Empty or no projection returns full document (no regression)
5. Works correctly with filter expressions (filter evaluates on full doc, projection applied after)
6. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None (standalone feature, but interacts with filter expressions if both are present)

## Dependents

- PRD #5 (BatchGetItem) could integrate `.projection()` on batch reads
