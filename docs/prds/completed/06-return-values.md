# PRD: ReturnValues Support

**Priority:** 6
**Status:** Complete
**Proposal:** [docs/proposals/accepted/return-values.md](../../proposals/accepted/return-values.md)

## Summary

Add `ReturnValues` support to write operations so applications can receive the old or new document atomically, without a separate read. FerridynDB's write paths already read the old document internally (for MVCC, index maintenance, key checks), so surfacing it costs almost nothing.

## Scope

### In Scope

- `ReturnValues` enum: `None`, `AllOld`, `AllNew` (AllNew only for UpdateItem)
- `.return_old()` on `PutItemBuilder` and `DeleteItemBuilder` (changes return type to `Result<Option<Value>>`)
- `.return_old()` and `.return_new()` on `UpdateItemBuilder`
- Type-state builder pattern to enforce correct return type at compile time
- Server protocol: `return_values` field on write requests, `returned_item` in responses
- Transaction support

### Out of Scope

- `UPDATED_OLD` / `UPDATED_NEW` (attribute-level granularity) -- requires diff tracking, defer
- `AllNew` for PutItem -- redundant (stored doc = input doc)
- WriteBatch return values -- batches are fire-and-forget
- Breaking change to existing `put_item()` signature -- keep returning `()`

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| API approach? | `.return_old()` transforms builder (type-state) | Backward compatible, compile-time type safety |
| AllNew for PutItem? | No | Redundant -- stored document equals input |
| Transaction support? | Yes | Returned value reflects state within the transaction |
| WriteBatch support? | No | Batches are for throughput, not complex logic |
| On condition failure? | Return Err, no document | Old value only on successful write |

## Implementation Phases

### Phase 1: Core API (PutItem and DeleteItem)

**Deliverables:**
- `ReturnValues` enum in types
- Type-state builders: `PutItemBuilder<NoReturn>` / `PutItemBuilder<ReturnOld>`
- `.return_old()` on PutItemBuilder returns `PutItemBuilder<ReturnOld>` with `execute() -> Result<Option<Value>>`
- `.return_old()` on DeleteItemBuilder, same pattern
- Internal: clone old document before overwrite/delete when return_old requested

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/builders.rs` | Add type-state generics, `.return_old()` methods |
| `ferridyn-core/src/api/database.rs` | Modify internal write paths to optionally return old doc |
| `ferridyn-core/src/types.rs` | Add `ReturnValues` enum |

**Tests:**
- `test_put_returning_old_existing` (replace existing item, get old back)
- `test_put_returning_old_new` (insert new item, get None)
- `test_delete_returning_old_existing` (delete item, get old back)
- `test_delete_returning_old_nonexistent` (delete missing, get None)

### Phase 2: UpdateItem Integration

**Prerequisite:** PRD #1 (UpdateItem)

**Deliverables:**
- `.return_old()` on `UpdateItemBuilder` -- returns document before update
- `.return_new()` on `UpdateItemBuilder` -- returns document after update
- AllNew construction: apply update actions, clone result before writing

**Tests:**
- `test_update_returning_old`
- `test_update_returning_new`
- `test_update_returning_new_upsert` (new item created)

### Phase 3: Server Protocol and Transactions

**Deliverables:**
- `return_values: Option<ReturnValues>` field on PutItem, DeleteItem, UpdateItem requests
- `returned_item: Option<Value>` in corresponding responses
- Transaction builder methods for `.return_old()` / `.return_new()`
- Client methods exposing return values

**Tests:**
- `test_server_put_return_values`
- `test_server_delete_return_values`
- `test_txn_put_returning_old`

## Acceptance Criteria

1. `.return_old()` on put returns the previous document (or None if new)
2. `.return_old()` on delete returns the deleted document (or None if missing)
3. `.return_new()` on update returns the document after modification
4. Type-state pattern prevents calling `.execute()` expecting a return value without `.return_old()`
5. No performance regression when return values are not requested (default path unchanged)
6. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD #1 (UpdateItem) -- for Phase 2

## Dependents

- None
