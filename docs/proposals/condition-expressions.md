# Proposal: ConditionExpression Support for Write Operations

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB's `ConditionExpression` allows writes to proceed only if the existing item meets certain criteria. This enables powerful patterns:

- **Prevent overwrites** — `attribute_not_exists(pk)` ensures a put only succeeds if the item doesn't already exist
- **Optimistic locking on any attribute** — `version = 5` checks application-level versioning fields, not just internal MVCC versions
- **Enforce business rules at the database level** — `status = 'pending'` ensures deletes only affect pending records, or `balance >= amount` prevents negative balances

FerridynDB currently offers `put_item_conditional(table, doc, expected_version)` which performs optimistic concurrency control (OCC) on the internal MVCC version number. This is useful but limited — it cannot express "only write if status == 'pending'" or "only delete if item exists" based on document attributes.

Condition expressions are foundational in DynamoDB, appearing on `PutItem`, `DeleteItem`, `UpdateItem`, and `TransactWriteItems`.

## Current State

- **Version-based conditional writes:** `put_item_conditional(table, doc, expected_version)` checks the MVCC version, returning `VersionMismatch` on conflict.
- **Versioned reads:** `get_item_versioned()` returns `VersionedItem { item, version }` for use with conditional puts.
- **No attribute-level conditions:** There's no way to condition writes on document attributes.
- **No transaction conditions:** Transaction writes (`txn.put_item`, `txn.delete_item`) have no condition support.
- **MVCC integration:** The version-based system is built into the MVCC layer (`mvcc/`).

## Proposed Design

Reuse the same expression enum as `FilterExpression` (if that proposal is implemented), since the predicate logic is identical — both evaluate a boolean expression against a document.

### API Examples

```rust
use ConditionExpr::*;

// Only put if item doesn't already exist
db.put_item("table", doc)
    .condition(AttributeNotExists("pk".into()))
    .execute()?;

// Only delete if status is "cancelled"
db.delete_item("table")
    .partition_key("order#123")
    .condition(Eq(Attr("status"), Literal(json!("cancelled"))))
    .execute()?;

// Only update if version matches (application-level versioning)
db.update_item("table")
    .partition_key("user#1")
    .set("name", json!("Alice"))
    .condition(Eq(Attr("version"), Literal(json!(5))))
    .execute()?;

// Compound condition: only delete if exists AND status is cancelled
db.delete_item("table")
    .partition_key("order#123")
    .condition(And(vec![
        AttributeExists("pk".into()),
        Eq(Attr("status"), Literal(json!("cancelled")))
    ]))
    .execute()?;
```

### Expression Types

Shared with `FilterExpression`:

| Category | Operations |
|----------|-----------|
| **Comparison** | `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge` |
| **Range** | `Between(attr, low, high)` |
| **String** | `BeginsWith(attr, prefix)`, `Contains(attr, substring)` |
| **Existence** | `AttributeExists(path)`, `AttributeNotExists(path)` |
| **Boolean** | `And(exprs)`, `Or(exprs)`, `Not(expr)` |
| **Type** | `AttributeType(path, type)` — check if attribute is String, Number, etc. |
| **Size** | `Size(path)` — get attribute size (string length, array count) for comparison |

### Failure Behavior

When a condition is not met, return a new error variant:

```rust
pub enum FerridynError {
    // ... existing variants
    ConditionCheckFailed(String),  // "Condition not met: attribute_not_exists(pk)"
}
```

This matches DynamoDB's `ConditionalCheckFailedException`. The write is **not applied** when the condition fails.

### Relationship to Existing `put_item_conditional`

Two options:

1. **Keep both APIs:**
   - `put_item_conditional(table, doc, expected_version)` remains as a fast path for MVCC version checks (avoids deserializing the document)
   - `.condition()` is the general path for attribute-level checks
   - Recommended approach: both APIs serve distinct use cases

2. **Deprecate version-based API:**
   - Reimplement `put_item_conditional` as `put_item().condition(VersionEquals(v))`
   - Simpler API surface, but requires adding a special `VersionEquals` condition type and is less efficient

**Recommendation:** Keep both. The MVCC version check is fast and common enough to deserve a dedicated API.

## Changes Required

### Core Database (`crates/ferridyn-core/src/`)

1. **New or shared: `ConditionExpr` enum**
   - Ideally the same enum as `FilterExpr` from the filter-expressions proposal
   - If expressions are shared, place in `src/expression.rs`
   - If separate, place in `src/condition.rs`

2. **Condition evaluation logic**
   - `fn evaluate_condition(expr: &ConditionExpr, document: Option<&Value>) -> bool`
   - Shared with filter evaluation if possible
   - Handles the case where `document = None` (item doesn't exist) — `AttributeNotExists` passes, others fail

3. **Modify write builders**
   - `PutItemBuilder` / `PutItemConditionalBuilder` — add `.condition(expr)` method
   - `DeleteItemBuilder` — add `.condition(expr)` method
   - `UpdateItemBuilder` (if UpdateItem exists) — add `.condition(expr)` method

4. **Modify write paths in `api/database.rs`**
   - For put with condition:
     1. Acquire write lock
     2. Read existing document (if any) from B+Tree
     3. Evaluate condition against existing document
     4. If condition passes, proceed with write
     5. If condition fails, release lock and return `ConditionCheckFailed`
   - Similar logic for delete/update

5. **Transaction support**
   - `txn.put_item().condition(...)` — store condition with pending operation
   - Evaluate conditions at commit time (after acquiring write lock, before applying writes)
   - If any condition fails, abort entire transaction
   - Return which operation failed: `TransactionConditionFailed { operation_index: usize, reason: String }`

6. **New error variant**
   - Add `ConditionCheckFailed(String)` to `FerridynError` in `src/error.rs`
   - Add `TransactionConditionFailed { operation_index: usize, reason: String }` for transaction condition failures

### Server Protocol (`crates/ferridyn-server/src/protocol.rs`)

Add condition field to write requests:

```rust
pub enum Request {
    PutItem {
        table: String,
        item: serde_json::Value,
        condition: Option<ConditionExpr>,  // NEW
    },
    DeleteItem {
        table: String,
        partition_key: serde_json::Value,
        sort_key: Option<serde_json::Value>,
        condition: Option<ConditionExpr>,  // NEW
    },
    // ... existing variants
}
```

Ensure `ConditionExpr` derives `serde::Serialize` and `serde::Deserialize` for wire transmission.

### Tests

New test cases in `crates/ferridyn-core/tests/`:

1. **Basic existence checks**
   - `attribute_exists` passes when attribute present
   - `attribute_not_exists` passes when attribute absent
   - `attribute_not_exists` passes when item doesn't exist

2. **Comparison conditions**
   - `Eq`, `Ne`, `Lt`, `Gt`, etc. on string, number, boolean attributes
   - Condition failure returns `ConditionCheckFailed`

3. **Compound conditions**
   - `And`, `Or`, `Not` combinations
   - Nested boolean logic

4. **Put with condition**
   - `put_item().condition(attribute_not_exists)` on new item (passes)
   - Same condition on existing item (fails)

5. **Delete with condition**
   - Delete only if status field matches
   - Condition fails, item remains

6. **Transaction conditions**
   - Multi-operation transaction with per-operation conditions
   - First operation passes, second fails → entire transaction aborted
   - Verify no partial writes

7. **Size and type functions**
   - `Size(Attr("name")) > 10`
   - `AttributeType("age", "Number")`

## Implementation Note: Evaluation Flow

For `PutItem` with a condition, the write path must:

1. **Read current document** (if any) within the write lock
2. **Evaluate condition** against the current document (or `None` if item doesn't exist)
3. **If condition passes** → proceed with the write
4. **If condition fails** → return `ConditionCheckFailed` without modifying anything

This is similar to the existing `put_item_conditional` flow but evaluates an arbitrary expression instead of just checking the MVCC version.

For transactions, all conditions are evaluated at commit time (after acquiring the write lock, before any writes are applied). This ensures atomicity — either all conditions pass and all writes succeed, or one condition fails and the entire transaction is aborted.

## Trade-offs

| Dimension | Impact |
|-----------|--------|
| **Complexity** | Adds optional condition checks to every write path; expression evaluation adds code surface |
| **Performance** | Every conditional write must deserialize the existing document and walk the expression tree |
| **API surface** | Significant increase — new builder methods, new error variants, expression DSL |
| **DynamoDB parity** | Foundational feature that many application patterns depend on |
| **Code reuse** | Sharing `ConditionExpr` with `FilterExpression` means evaluation engine implemented once |

### When to Use Conditions

- **Use conditions for:**
  - Preventing lost updates (check-and-set patterns)
  - Enforcing business rules (only delete if cancelled)
  - Idempotent operations (only insert if not exists)

- **Don't use conditions for:**
  - Simple version-based OCC → use `put_item_conditional` (faster)
  - Complex multi-document constraints → consider application-level validation

## Open Questions

1. **Evaluation on MessagePack vs JSON** — Should conditions be evaluated on the deserialized `serde_json::Value` (simpler) or on the raw MessagePack bytes (potentially faster for simple checks)? Recommend starting with `serde_json::Value` for simplicity.

2. **Deprecation of `put_item_conditional`** — Should the version-based API be deprecated in favor of the general condition system, or kept as a fast path? Recommend keeping both.

3. **Attribute-to-attribute comparison** — Should conditions support referencing other attributes in the same document, e.g., `Gt(Attr("end_date"), Attr("start_date"))`? DynamoDB supports this. Recommend yes, but defer to a later iteration if it complicates the initial implementation.

4. **TransactWriteItems conditions** — Should individual operations within a transaction support per-operation conditions? DynamoDB does. Recommend yes — conditions are evaluated at commit time, and any failure aborts the transaction.

5. **TTL interaction** — How should conditions interact with TTL-expired items? Should an expired item be treated as non-existent for `attribute_not_exists` checks? Recommend treating expired items as non-existent (same as DynamoDB behavior).

6. **Condition expression size limits** — Should there be a maximum depth or complexity limit for condition expressions to prevent pathological cases (e.g., deeply nested `And`/`Or` that cause stack overflows)? Recommend imposing a reasonable depth limit (e.g., 16 levels).

## Migration Path

This is a pure addition — no breaking changes:

1. **Phase 1:** Implement core condition evaluation and single-operation support (PutItem, DeleteItem)
2. **Phase 2:** Add transaction support (per-operation conditions in TransactWriteItems)
3. **Phase 3:** Add UpdateItem conditions (if/when UpdateItem is implemented)
4. **Phase 4:** (Optional) Add attribute-to-attribute comparisons if demand exists

Existing code continues to work unchanged. The `.condition()` builder method is optional.
