# PRD: Condition Expressions

**Priority:** 3
**Status:** Approved
**Proposal:** [docs/proposals/accepted/condition-expressions.md](../proposals/accepted/condition-expressions.md)

## Summary

Add `ConditionExpression` support to write operations (`put_item`, `delete_item`, `update_item`). Conditions evaluate predicates against the existing item and only proceed if the condition is met. Enables prevent-overwrite, attribute-level optimistic locking, and business rule enforcement at the database level.

## Scope

### In Scope

- Reuse `FilterExpr` enum as the condition expression type (shared evaluation engine)
- `.condition()` method on `PutItemBuilder`, `DeleteItemBuilder`, `UpdateItemBuilder`
- `ConditionCheckFailed` error variant
- Evaluation against existing document (or `None` for non-existent items)
- Per-operation conditions in transactions (abort entire transaction if any condition fails)
- Server protocol integration (condition field on write requests)

### Out of Scope

- Deprecation of `put_item_conditional` (version-based OCC) -- keep both
- `AttributeType` function -- defer to v2
- `Size` function -- defer to v2
- Attribute-to-attribute comparison (`Gt(Attr("end"), Attr("start"))`) -- defer to later iteration

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Shared type with FilterExpr? | Yes, use same enum | Identical predicate logic; implement evaluator once |
| Keep put_item_conditional? | Yes, keep both | Version-based OCC is fast and common; deserves dedicated API |
| Transaction conditions? | Yes, per-operation | DynamoDB TransactWriteItems supports this |
| TTL-expired items? | Treat as non-existent | Matches DynamoDB behavior |
| Expression depth limit? | 16 levels max | Prevents stack overflow on pathological input |
| Evaluation target? | Deserialized serde_json::Value | Simpler implementation; MessagePack eval deferred |

## Implementation Phases

### Phase 1: Single-Operation Conditions

**Prerequisite:** PRD #2 (Filter Expressions) -- for the shared `FilterExpr` type and evaluator.

**Deliverables:**
- `ConditionCheckFailed(String)` error variant in `error.rs`
- `.condition(expr)` method on `PutItemBuilder` (via new field `condition: Option<FilterExpr>`)
- `.condition(expr)` method on `DeleteItemBuilder`
- Write path modification: read existing doc within write lock, evaluate condition, proceed or fail
- Handle `None` document case: `AttributeNotExists` passes, most others fail

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/error.rs` | Add `ConditionCheckFailed` variant |
| `ferridyn-core/src/api/builders.rs` | Add `condition` field and `.condition()` to PutItemBuilder, DeleteItemBuilder |
| `ferridyn-core/src/api/database.rs` | Modify put/delete write paths to evaluate condition |

**Tests:**
- `test_put_condition_attribute_not_exists_passes` (new item)
- `test_put_condition_attribute_not_exists_fails` (existing item)
- `test_delete_condition_eq_passes`
- `test_delete_condition_eq_fails` (item remains)
- `test_condition_compound_and_or`
- `test_condition_on_nonexistent_item`
- `test_condition_check_failed_error`

### Phase 2: UpdateItem Conditions

**Prerequisite:** PRD #1 (UpdateItem)

**Deliverables:**
- `.condition(expr)` method on `UpdateItemBuilder`
- Same evaluation flow: read existing, evaluate, apply update or fail

**Tests:**
- `test_update_with_condition_passes`
- `test_update_with_condition_fails`
- `test_update_with_condition_on_nonexistent` (upsert with condition)

### Phase 3: Transaction Conditions

**Deliverables:**
- Condition field on transaction operation structs
- At commit time: evaluate all conditions after acquiring write lock, before applying writes
- If any condition fails, abort entire transaction
- `TransactionConditionFailed { operation_index, reason }` error variant

**Tests:**
- `test_txn_all_conditions_pass`
- `test_txn_one_condition_fails_aborts_all`
- `test_txn_no_partial_writes_on_condition_failure`

### Phase 4: Server Protocol Integration

**Deliverables:**
- `condition: Option<FilterExpr>` field on PutItem, DeleteItem, UpdateItem requests
- Server handler passes condition through to core API
- Client builder methods for `.condition()`

**Tests:**
- `test_server_put_with_condition`
- `test_server_delete_with_condition`
- `test_server_condition_check_failed_response`

## Acceptance Criteria

1. Condition evaluation correctly handles existing, non-existent, and TTL-expired items
2. Failed conditions return `ConditionCheckFailed` and do not modify data
3. Transaction conditions are all-or-nothing (no partial writes)
4. Expression depth limit (16) is enforced
5. Existing `put_item_conditional` (version-based) continues to work unchanged
6. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD #2 (Filter Expressions) -- shared `FilterExpr` type and evaluator
- PRD #1 (UpdateItem) -- for Phase 2 (UpdateItem conditions)

## Dependents

- None (end-of-chain feature)
