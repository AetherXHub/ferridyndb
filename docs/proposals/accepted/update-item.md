# Proposal: UpdateItem Support

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB's `UpdateItem` allows partial document modifications without reading the full document first. Currently FerridynDB requires a full read-modify-write cycle: `get_item` → modify in Rust → `put_item`. This has several drawbacks:

- **Inconvenient** — Every update requires two API calls and manual document manipulation in client code.
- **Racy** — Another writer can modify the document between get and put. While optimistic concurrency control via `put_item_conditional` mitigates this, it requires the client to retry the full cycle.
- **Performance overhead** — Reading, deserializing, modifying, re-serializing, and writing the entire document is wasteful when only a few fields change.
- **Atomic counters impossible** — Incrementing a number without read-modify-write cannot be done safely without UpdateItem.

Update operations are the most common write pattern in most applications. Native support would significantly improve the API ergonomics and reduce the risk of race conditions in client code.

## Current State

- Only `put_item` (full document replace) and `delete_item` exist.
- `put_item_conditional` provides optimistic concurrency control but still requires a full document.
- No partial update capability at any layer.
- Documents are stored as MessagePack blobs in B+Tree leaf nodes and overflow pages.
- The MVCC layer creates new versions by writing complete documents.

## Proposed Design

DynamoDB supports four update actions: **SET**, **REMOVE**, **ADD**, and **DELETE**. This proposal suggests implementing them incrementally.

### Phase 1: SET and REMOVE (highest value)

```rust
use ferridyn_core::api::UpdateAction::*;

db.update_item("Users")
    .partition_key("user#123")
    .sort_key("profile")
    .set("name", json!("Alice"))
    .set("address.city", json!("Portland"))    // nested path
    .remove("temp_field")
    .execute()?;
```

**SET** inserts or replaces the value at the given path. If intermediate paths do not exist, they are created (like `mkdir -p`).

**REMOVE** deletes the attribute at the given path.

### Phase 2: ADD and DELETE

```rust
db.update_item("Users")
    .partition_key("user#123")
    .sort_key("profile")
    .add("login_count", json!(1))              // atomic increment
    .delete("tags", json!(["old-tag"]))        // remove from set
    .execute()?;
```

**ADD** has two behaviors:
- If the target is a number, add the provided value to it. If the attribute does not exist, initialize it to the provided value.
- If the target is a set (array), perform a union with the provided set. Duplicates are removed.

**DELETE** removes specified values from a set (array). If the resulting set is empty, the attribute is removed.

## Internal Implementation

UpdateItem is internally a read-modify-write operation, but it is **atomic** because it happens within the write lock.

1. Acquire write lock (normal transaction start).
2. Read the current document from the B+Tree (if it exists).
3. Deserialize from MessagePack to `serde_json::Value`.
4. Apply each update action in order:
   - **SET**: Insert or replace at the given path. Create intermediate objects if necessary.
   - **REMOVE**: Delete the key at the given path. Silently succeed if the path does not exist.
   - **ADD**: If number, add to existing number (or initialize). If array, perform set union.
   - **DELETE**: If array, remove specified elements. Remove the attribute if the set becomes empty.
5. Serialize back to MessagePack.
6. Write as new version (normal MVCC put).
7. Commit the transaction.

This means UpdateItem is internally still a full document read-modify-write at the storage layer. The optimization is at the **API level** (convenience, atomicity, reduced race conditions) not the I/O level. A true partial-update storage format (e.g., delta encoding) would be much more complex and is not proposed here.

## Data Structures

### UpdateAction Enum

```rust
pub enum UpdateAction {
    Set { path: String, value: Value },
    Remove { path: String },
    Add { path: String, value: Value },
    Delete { path: String, value: Value },
}
```

### UpdateItemBuilder

```rust
pub struct UpdateItemBuilder<'a> {
    db: &'a FerridynDB,
    table_name: String,
    partition_key: Value,
    sort_key: Option<Value>,
    actions: Vec<UpdateAction>,
    condition: Option<ConditionExpression>,  // future: conditional updates
}

impl<'a> UpdateItemBuilder<'a> {
    pub fn set(mut self, path: impl Into<String>, value: Value) -> Self { ... }
    pub fn remove(mut self, path: impl Into<String>) -> Self { ... }
    pub fn add(mut self, path: impl Into<String>, value: Value) -> Self { ... }
    pub fn delete(mut self, path: impl Into<String>, value: Value) -> Self { ... }
    pub fn condition(mut self, expr: ConditionExpression) -> Self { ... }  // future
    pub fn execute(self) -> Result<(), Error> { ... }
}
```

## Path Resolution

Paths are **dot-separated** strings, like `"address.city"`.

- `"name"` → top-level field
- `"address.city"` → nested object: `document["address"]["city"]`
- `"items[0]"` → array indexing (stretch goal, not in initial implementation)

### SET on nested paths

If the path is `"address.city"` and `address` does not exist, SET should create `address` as an empty object, then set `city` within it.

```rust
// Before: {}
db.update_item("table")
    .set("address.city", json!("Portland"))
    .execute()?;
// After: { "address": { "city": "Portland" } }
```

### REMOVE on non-existent paths

Silently succeed (DynamoDB behavior).

## UpdateItem on Non-Existent Item

If the item does not exist, UpdateItem should create a new item with:
- The partition key and sort key (from the request).
- All SET values applied.
- ADD values applied (initialized to the provided value).
- REMOVE and DELETE are no-ops (nothing to remove).

This matches DynamoDB behavior.

```rust
// Before: item does not exist
db.update_item("Users")
    .partition_key("user#new")
    .set("name", json!("Bob"))
    .add("login_count", json!(1))
    .execute()?;
// After: { "PK": "user#new", "name": "Bob", "login_count": 1 }
```

## Changes Required

### New Files

- **`crates/ferridyn-core/src/api/update.rs`**
  - `UpdateAction` enum
  - Path resolution logic (split on `.`, navigate the document tree)
  - Apply logic for SET, REMOVE, ADD, DELETE

### Modified Files

- **`crates/ferridyn-core/src/api/builders.rs`**
  - Add `UpdateItemBuilder` with chainable methods

- **`crates/ferridyn-core/src/api/database.rs`**
  - Add `update_item()` method on `FerridynDB`:
    ```rust
    pub fn update_item(&self, table_name: impl Into<String>) -> UpdateItemBuilder {
        UpdateItemBuilder::new(self, table_name.into())
    }
    ```
  - Internal method to execute the update within a transaction:
    ```rust
    fn execute_update(
        &self,
        table_name: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        actions: Vec<UpdateAction>,
        condition: Option<ConditionExpression>,
    ) -> Result<(), Error> { ... }
    ```

- **`crates/ferridyn-core/src/api/transaction.rs`**
  - Add `update_item()` method on `Transaction` for transactional updates:
    ```rust
    pub fn update_item(&mut self, table_name: impl Into<String>) -> UpdateItemBuilder { ... }
    ```

- **`crates/ferridyn-core/src/mvcc/transaction_manager.rs`**
  - No structural changes required — UpdateItem reuses the existing write path (`put_item` internally)

- **`crates/ferridyn-core/src/catalog/table.rs`**
  - No changes required unless secondary indexes are involved (see below)

- **`crates/ferridyn-server/src/protocol.rs`**
  - Add `UpdateItem` variant to `Request` enum:
    ```rust
    UpdateItem {
        table_name: String,
        partition_key: Value,
        sort_key: Option<Value>,
        actions: Vec<UpdateAction>,
        condition: Option<ConditionExpression>,
    }
    ```
  - Add corresponding `Response` variant (or reuse existing success/error responses)

### Secondary Index Maintenance

If a secondary index exists on an attribute that is modified by UpdateItem, the index must be updated. This requires:

1. Read the old document.
2. Extract the old indexed attribute values.
3. Apply the update actions to get the new document.
4. Extract the new indexed attribute values.
5. If they differ, update the secondary index (delete old entry, insert new entry).

This is the same logic already required for `put_item` when secondary indexes exist. UpdateItem can delegate to the existing index maintenance code after computing the new document.

**Note:** Phase 1 (SET/REMOVE) is sufficient to implement index maintenance. ADD and DELETE do not introduce new complexity here.

## Tests

### Unit Tests

- `test_update_set_top_level` — SET a top-level attribute
- `test_update_set_nested` — SET a nested attribute with path `"address.city"`
- `test_update_set_create_intermediate` — SET on non-existent intermediate path creates objects
- `test_update_remove` — REMOVE an attribute
- `test_update_remove_nonexistent` — REMOVE on non-existent path silently succeeds
- `test_update_add_number` — ADD to a numeric attribute (increment)
- `test_update_add_number_init` — ADD on non-existent attribute initializes it
- `test_update_add_set` — ADD to a set (array union)
- `test_update_delete_set` — DELETE from a set (array difference)
- `test_update_delete_empty_set` — DELETE that results in empty set removes the attribute
- `test_update_nonexistent_item` — UpdateItem on non-existent item creates it
- `test_update_multiple_actions` — Multiple SET/REMOVE in one update, applied in order
- `test_update_with_condition` — UpdateItem with condition expression (future)
- `test_update_in_transaction` — UpdateItem within a transaction
- `test_update_index_maintenance` — UpdateItem that changes indexed attribute updates secondary index

### Integration Tests

- Round-trip via `ferridyn-server` — ensure UpdateItem request/response serialization works
- Concurrent updates — multiple clients updating different fields of the same document
- UpdateItem interleaved with get/put/delete

## Trade-offs

| Dimension | Current (put_item) | Proposed (update_item) |
|---|---|---|
| **API ergonomics** | Manual get-modify-put | Single call with declarative actions |
| **Atomicity** | Requires explicit OCC (`put_item_conditional`) | Always atomic within write lock |
| **Storage I/O** | Full document read + write | Full document read + write (same at storage layer) |
| **Network payload** (when using ferridyn-server) | Full document sent over wire | Only the update actions sent |
| **Complexity** | Simple | More complex (path resolution, action application) |

### When put_item is better

- Replacing the entire document is more natural (e.g., "save this object").
- The client already has the full new state.

### When update_item is better

- Modifying a subset of fields.
- Atomic counters (e.g., increment view count).
- Avoiding race conditions without manual OCC retry loops.
- Reducing network payload in client-server deployments.

## Open Questions

1. **Condition expressions** — Should UpdateItem support a condition expression (like DynamoDB's `ConditionExpression`)? This would allow "update only if X is true" logic.
   - **Recommendation:** Yes, add `condition()` method to `UpdateItemBuilder`. Reuse the existing `ConditionExpression` logic from `put_item_conditional`.

2. **Arithmetic expressions** — Should SET support expressions like `SET path = path + :val`? DynamoDB supports this. Or should we rely on ADD for numeric increments?
   - **Recommendation:** Use ADD for increments (simpler). SET only accepts literal values. Arithmetic expressions can be added later if needed.

3. **ReturnValues** — Should UpdateItem return the old document, new document, or nothing? DynamoDB supports `NONE`, `ALL_OLD`, `ALL_NEW`, `UPDATED_OLD`, `UPDATED_NEW`.
   - **Recommendation:** Start with returning nothing (void). Add ReturnValues in a follow-up if needed (applies to put_item and delete_item too).

4. **Key attribute updates** — Should updates to the partition key or sort key be allowed? DynamoDB forbids this — you must delete and re-create the item.
   - **Recommendation:** Forbid updates to key attributes. Return an error if an update action targets the partition key or sort key field.

5. **Non-existent attribute behavior for ADD** — How should ADD behave if the attribute does not exist? DynamoDB creates it with the provided value.
   - **Recommendation:** Match DynamoDB. ADD on non-existent number initializes to the provided value. ADD on non-existent set creates a new set with the provided elements.

6. **Array indexing** — Should paths support array indexing like `items[0]`?
   - **Recommendation:** Not in Phase 1. Dot-separated object paths only. Array indexing is a stretch goal for Phase 2 or later.

7. **Type coercion** — What happens if ADD is used on a string, or DELETE on a number?
   - **Recommendation:** Return a validation error. DynamoDB has strict type requirements for each action. We should match this.

## Migration Path

UpdateItem is a purely additive feature. No changes to existing APIs or on-disk format. Deployment:

1. Implement UpdateItem in `ferridyn-core`.
2. Add UpdateItem request/response to `ferridyn-server` protocol.
3. Add UpdateItem method to `FerridynClient`.
4. Document in public API docs.

No backward compatibility concerns.
