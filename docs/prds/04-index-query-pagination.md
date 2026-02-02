# PRD: Index Query Pagination

**Priority:** 4
**Status:** Approved
**Proposal:** [docs/proposals/accepted/index-query-pagination.md](../proposals/accepted/index-query-pagination.md)

## Summary

Add pagination support to secondary index queries (`query_index`). Primary table queries already support `exclusive_start_key` and `last_evaluated_key`; index queries only have `limit` with no way to resume. This closes a feature parity gap and enables incremental processing of large index result sets.

## Scope

### In Scope

- `.exclusive_start_key()` method on `IndexQueryBuilder`
- `last_evaluated_key` populated in `QueryResult` when more results exist
- Structured JSON cursor format (indexed attribute + primary key attributes)
- Forward and reverse scan pagination
- Cursor validation (missing/mistyped key attributes return clear errors)
- Server protocol update (add `exclusive_start_key` to QueryIndex request)
- Client update (`FerridynClient::query_index` accepts optional ESK)

### Out of Scope

- Opaque token cursors (use structured JSON for DynamoDB compatibility)
- Separate `IndexQueryResult` type (reuse `QueryResult`)
- Auto-fetching past deleted entries (keep current variable page size behavior)

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Cursor format? | Structured JSON (index attr + pk + sk) | Matches DynamoDB LastEvaluatedKey, debuggable |
| Result type? | Reuse `QueryResult` | Consistent API, less code |
| Lazy GC during pagination? | Keep current behavior (variable page sizes) | Matches DynamoDB eventual consistency; avoids unbounded scans |

## Implementation

This is a single-phase feature (low complexity, ~240 lines including tests).

**Deliverables:**
- Add `exclusive_start_key: Option<Value>` field to `IndexQueryBuilder`
- Add `.exclusive_start_key()` builder method
- Encode ESK to composite B+Tree format: `encode(indexed_value) ++ encode(primary_key)`
- Skip items `<= exclusive_start_key` during index B+Tree scan
- When limit is applied and more items exist, build `last_evaluated_key` JSON object:
  ```json
  {
    "<index_attr_name>": <indexed_value>,
    "<pk_name>": <pk_value>,
    "<sk_name>": <sk_value>
  }
  ```
- Return `QueryResult { items, last_evaluated_key }`

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/builders.rs` | Add ESK field/method to `IndexQueryBuilder`, encode/skip/build cursor logic |
| `ferridyn-server/src/protocol.rs` | Add `exclusive_start_key` to QueryIndex request |
| `ferridyn-server/src/server.rs` | Pass ESK through to builder |
| `ferridyn-server/src/client.rs` | Accept ESK parameter in `query_index` |

**Tests:**
| Test | Verifies |
|------|----------|
| `test_index_pagination_basic` | 25 items, limit 10, 3 pages (10+10+5), cursor progression |
| `test_index_pagination_exact_page` | 10 items, limit 10, no cursor on last page |
| `test_index_pagination_single_item_pages` | limit=1, 3 items, 3 pages |
| `test_index_pagination_reverse_scan` | scan_forward=false with cursor |
| `test_index_pagination_invalid_cursor_missing_index_attr` | Error returned |
| `test_index_pagination_invalid_cursor_missing_pk` | Error returned |
| `test_index_pagination_invalid_cursor_type_mismatch` | Error returned |
| `test_index_pagination_over_wire` | Server round-trip |

## Acceptance Criteria

1. Paginating through all items yields the same total as querying without limit
2. No duplicate items across pages
3. No missing items across pages
4. Reverse scan pagination produces results in correct order
5. Invalid cursors produce clear error messages
6. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None (builds on existing index query infrastructure)

## Dependents

- None
