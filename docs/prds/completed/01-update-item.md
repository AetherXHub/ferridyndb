# PRD: UpdateItem Support

**Priority:** 1
**Status:** Complete
**Proposal:** [docs/proposals/accepted/update-item.md](../proposals/accepted/update-item.md)

## Summary

Add `update_item` to FerridynDB for atomic partial document modifications. Eliminates the racy read-modify-write cycle required today and enables atomic counters.

## Scope

### In Scope

- `UpdateItemBuilder` with chainable `.set()`, `.remove()`, `.add()`, `.delete()` methods
- Dot-separated nested path resolution (`"address.city"`)
- Upsert behavior: update creates a new item if it doesn't exist (DynamoDB-compatible)
- Key attribute protection: reject updates targeting partition key or sort key fields
- Secondary index maintenance on updated attributes
- Server protocol support (`UpdateItem` request/response in ferridyn-server)
- Console `UPDATE` command
- Transaction support (`txn.update_item()`)

### Out of Scope

- Array indexing in paths (`items[0]`) -- defer to future enhancement
- Arithmetic SET expressions (`SET path = path + :val`) -- use ADD for increments
- ReturnValues (old/new document return) -- covered by PRD #6
- ConditionExpression on UpdateItem -- covered by PRD #3

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Condition expressions? | Add `.condition()` stub, implement when PRD #3 lands | Keeps builder API forward-compatible |
| Arithmetic in SET? | No. Use ADD for increments | Simpler; arithmetic expressions can be added later |
| ReturnValues? | No. Return `()` initially | Separate PRD (#6) covers this for all write operations |
| Key attribute updates? | Forbid. Return error | Matches DynamoDB behavior |
| ADD on non-existent attr? | Initialize to provided value | Matches DynamoDB behavior |
| Type coercion? | Return validation error | Strict typing matches DynamoDB |

## Implementation Phases

### Phase 1: SET and REMOVE

**Deliverables:**
- `UpdateAction` enum (`Set`, `Remove`)
- `UpdateItemBuilder` with `.set()`, `.remove()`, `.execute()`
- Path resolution: split on `.`, navigate/create intermediate objects
- Upsert behavior for non-existent items
- Key attribute validation (reject updates to pk/sk fields)
- `FerridynDB::update_item()` entry point
- Secondary index maintenance (diff old vs new indexed attributes)

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/api/update.rs` | New: `UpdateAction`, path resolution, apply logic |
| `ferridyn-core/src/api/builders.rs` | Add `UpdateItemBuilder` |
| `ferridyn-core/src/api/database.rs` | Add `update_item()`, `execute_update()` |
| `ferridyn-core/src/api/mod.rs` | Export new module |

**Tests:**
- `test_update_set_top_level`
- `test_update_set_nested`
- `test_update_set_create_intermediate`
- `test_update_remove`
- `test_update_remove_nonexistent` (silent success)
- `test_update_nonexistent_item` (upsert)
- `test_update_multiple_actions` (ordered application)
- `test_update_rejects_key_attribute`
- `test_update_index_maintenance`

### Phase 2: ADD and DELETE

**Deliverables:**
- `UpdateAction::Add` and `UpdateAction::Delete` variants
- `.add()` and `.delete()` methods on `UpdateItemBuilder`
- ADD behavior: numeric increment or set union
- DELETE behavior: set difference, remove attribute if empty
- Type validation (error on ADD to string, DELETE on number)

**Tests:**
- `test_update_add_number` (increment)
- `test_update_add_number_init` (initialize non-existent)
- `test_update_add_set` (array union)
- `test_update_delete_set` (array difference)
- `test_update_delete_empty_set` (remove attribute)
- `test_update_add_type_error`

### Phase 3: Server and Transaction Integration

**Deliverables:**
- `UpdateItem` request/response in `ferridyn-server/src/protocol.rs`
- Server handler in `server.rs`
- `FerridynClient::update_item()` async method
- `Transaction::update_item()` method
- Console `UPDATE` command in parser/executor

**Tests:**
- Server round-trip integration test
- Transaction with update_item
- Console UPDATE parsing and execution

## Acceptance Criteria

1. `cargo test -p ferridyn-core` passes with all new update tests
2. `cargo test -p ferridyn-server` passes with UpdateItem wire protocol tests
3. `cargo clippy --workspace -- -D warnings` clean
4. Secondary indexes are correctly maintained when indexed attributes change via update
5. Upsert creates items with correct key attributes + SET/ADD values
6. Concurrent updates via server are serialized correctly (single-writer guarantee)

## Dependencies

- None (standalone feature, builds on existing put_item infrastructure)

## Dependents

- PRD #3 (Condition Expressions) -- adds `.condition()` to UpdateItemBuilder
- PRD #6 (ReturnValues) -- adds AllOld/AllNew return support
