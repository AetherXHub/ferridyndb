# Proposal: ReturnValues Support for Write Operations

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB's `ReturnValues` option on `PutItem`, `DeleteItem`, and `UpdateItem` allows applications to receive the old or new document in the same operation that modifies it. This eliminates the need for a separate read operation, which is:

- **More efficient** — Avoids a round-trip and reduces I/O.
- **More correct** — The read-then-write pattern is racy. Another transaction might modify the item between the read and the write. ReturnValues guarantees atomicity: you see the exact old value as of the write.
- **Nearly free** — FerridynDB's write operations already read the old document internally for MVCC version chain management, secondary index maintenance, and key existence checks. Surfacing this data costs almost nothing.

Currently, FerridynDB's write operations return `()`:

```rust
db.put_item("table", json!({...}))?;  // Result<()>
db.delete_item("table").partition_key("pk").execute()?;  // Result<()>
```

Applications that need the old value must perform an extra `get_item` before the write:

```rust
let old = db.get_item("table").partition_key("pk").execute()?;
db.delete_item("table").partition_key("pk").execute()?;
```

This pattern is wasteful and prone to race conditions.

## Current State

### API Surface

- `put_item(table: &str, item: Value) -> Result<()>`
- `put_item_conditional(...) -> Result<()>` (or `TxnError::VersionMismatch`)
- `delete_item(table: &str) -> DeleteItemBuilder` → `.execute() -> Result<()>`
- `WriteBatch` operations: `batch.put(...)`, `batch.delete(...)` — no return values
- Transaction operations: `txn.put_item(...)`, `txn.delete_item(...)` — no return values

### Internal Implementation

The put path (`crates/ferridyn-core/src/api/database.rs`) already reads the old document to:

1. Check for an existing key (to distinguish insert vs. update).
2. Manage MVCC version chains (`created_txn`, `deleted_txn`).
3. Update secondary indexes (future: diff old vs. new attributes to detect changes).

The delete path similarly reads the document before marking it as deleted.

**This means the old document is already available at the point where the operation completes.** The challenge is threading it through the API.

## Proposed Design

DynamoDB's `ReturnValues` enum:

- `NONE` — return nothing (current behavior, default)
- `ALL_OLD` — return the entire item as it was before the operation
- `ALL_NEW` — return the entire item after the operation (only meaningful for UpdateItem)
- `UPDATED_OLD` — return only the updated attributes, old values (UpdateItem only)
- `UPDATED_NEW` — return only the updated attributes, new values (UpdateItem only)

For FerridynDB, propose:

```rust
pub enum ReturnValues {
    None,
    AllOld,
    AllNew,  // Only valid for UpdateItem
}
```

For now, exclude `UPDATED_OLD` and `UPDATED_NEW` (attribute-level granularity). These require tracking which fields changed, which is future work.

### API Changes

#### PutItem with ReturnValues

Option A: Separate method for returning variant:

```rust
let old: Option<Value> = db.put_item_returning("table", json!({
    "pk": "user-123",
    "name": "Alice"
}))
    .return_old()
    .execute()?;
// old is None if no previous item, Some(old_doc) if replaced
```

Option B: Change existing method to return `Option<Value>` (breaking change):

```rust
let old = db.put_item("table", json!({...}))?;  // Option<Value>
```

Option C: Builder with return type determined by terminal method:

```rust
db.put_item("table", json!({...})).execute()?;  // Result<()>
db.put_item("table", json!({...})).execute_returning()?.return_old()?;  // Result<Option<Value>>
```

**Recommendation:** Option A (separate `put_item_returning` method). Backward compatible, clear intent.

#### DeleteItem with ReturnValues

Add `.return_old()` to `DeleteItemBuilder`:

```rust
let old: Option<Value> = db.delete_item("table")
    .partition_key("pk")
    .sort_key("sk")
    .return_old()
    .execute()?;
// old is None if item did not exist, Some(old_doc) if deleted
```

Change `DeleteItemBuilder::execute` return type based on whether `.return_old()` was called:

```rust
impl DeleteItemBuilder {
    pub fn execute(self) -> Result<()> { /* ... */ }

    pub fn return_old(self) -> DeleteItemReturningBuilder { /* ... */ }
}

impl DeleteItemReturningBuilder {
    pub fn execute(self) -> Result<Option<Value>> { /* ... */ }
}
```

#### UpdateItem (future)

When UpdateItem is implemented, support both `AllOld` and `AllNew`:

```rust
let new_doc = db.update_item("table")
    .partition_key("pk")
    .set("name", json!("Alice"))
    .return_new()
    .execute()?;
// new_doc: Option<Value> — the document after the update
```

### Transaction API

Extend transaction methods to support ReturnValues:

```rust
let mut txn = db.begin_transaction();
let old = txn.put_item_returning("table", json!({...}))
    .return_old()
    .execute()?;
txn.commit()?;
```

### WriteBatch API

Batch operations are fire-and-forget, so returning individual old values is awkward. Two options:

1. **No ReturnValues support for batches** — simplest, matches the "batch for throughput, not for complex logic" philosophy.
2. **Return a `Vec<Option<Value>>`** — ordered by operation, but requires buffering all results in memory.

**Recommendation:** Start with option 1. If needed, add option 2 later.

## Changes Required

### `crates/ferridyn-core/src/api/database.rs`

- Modify `put_item` internal implementation to optionally capture and return the old document.
- Add `put_item_returning` method that returns a builder.
- Modify `PutItemReturningBuilder` to have `.return_old()` method and `.execute() -> Result<Option<Value>>`.

### `crates/ferridyn-core/src/api/delete.rs` (or equivalent)

- Modify `DeleteItemBuilder` to add `.return_old()` → returns `DeleteItemReturningBuilder`.
- Implement `DeleteItemReturningBuilder::execute() -> Result<Option<Value>>`.

### `crates/ferridyn-core/src/api/transaction.rs`

- Add `txn.put_item_returning(...)` method.
- Add `.return_old()` to `txn.delete_item(...)` builder.

### `crates/ferridyn-server/src/protocol.rs`

- Add `return_values: Option<ReturnValues>` field to `PutRequest`, `DeleteRequest`, and future `UpdateRequest`.
- Modify `Response::PutItem` and `Response::DeleteItem` to include `returned_item: Option<Value>`.

Example:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutRequest {
    pub table: String,
    pub item: serde_json::Value,
    pub return_values: Option<ReturnValues>,  // New field
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    PutItem {
        success: bool,
        returned_item: Option<serde_json::Value>,  // New field
    },
    DeleteItem {
        success: bool,
        returned_item: Option<serde_json::Value>,  // New field
    },
    // ...
}
```

### `crates/ferridyn-server/src/server.rs`

- When handling `Request::PutItem` and `Request::DeleteItem`, check `return_values` field and call the appropriate method.
- Pass the returned item back in the response.

### `crates/ferridyn-server/src/client.rs`

- Add builder methods for `.return_old()` on put and delete operations.
- Parse `returned_item` from responses.

### Tests

#### Unit tests (`crates/ferridyn-core`)

- `test_put_item_returning_old_existing` — Put an item, verify old item is returned.
- `test_put_item_returning_old_new` — Put a new item, verify `None` is returned.
- `test_delete_item_returning_old_existing` — Delete an item, verify old item is returned.
- `test_delete_item_returning_old_nonexistent` — Delete a non-existent item, verify `None` is returned.
- `test_put_item_returning_old_in_transaction` — Within a transaction, put item returning old, verify isolation.

#### Integration tests (`crates/ferridyn-server`)

- `test_server_put_item_return_values` — Client sends `PutRequest` with `return_values: Some(ReturnValues::AllOld)`, verifies response contains `returned_item`.
- `test_server_delete_item_return_values` — Client sends `DeleteRequest` with `return_values: Some(ReturnValues::AllOld)`, verifies response contains `returned_item`.

## Trade-offs

| Dimension | Without ReturnValues (current) | With ReturnValues (proposed) |
|---|---|---|
| **API simplicity** | Simpler — all writes return `()` | More complex — new builder methods, return types |
| **Efficiency** | Extra `get_item` call needed → 2 I/O operations | Single operation → 1 I/O operation |
| **Correctness** | Read-then-write is racy | Atomic — guaranteed to return the exact old value |
| **Performance cost** | Separate read overhead | Nearly zero — old doc already read internally |
| **Backward compatibility** | N/A | Preserved via separate method (`put_item_returning`) |

## Open Questions

1. **Should `put_item` change its return type to `Option<Value>` (breaking change), or should there be a separate `put_item_returning` method?**
   - Recommendation: Separate method. Preserves backward compatibility and is common in Rust APIs (e.g., `Vec::push` vs. `Vec::insert`).

2. **Should ReturnValues work within transactions (`txn.put_item_returning`)?**
   - Recommendation: Yes. The returned value should reflect the state at the point within the transaction, not necessarily committed state.

3. **For UpdateItem, should we support `UPDATED_OLD`/`UPDATED_NEW` (only changed attributes) or just `ALL_OLD`/`ALL_NEW`?**
   - Recommendation: Start with `ALL_OLD`/`ALL_NEW`. Attribute-level granularity is future work and requires diff tracking.

4. **Should WriteBatch support per-operation ReturnValues?**
   - Recommendation: No, not in v1. Batches are for throughput. Applications that need return values can use individual operations or transactions.

5. **What should happen if an operation fails (e.g., condition check fails)?**
   - Recommendation: Return `Err(...)`, no document. The old value is only returned on success.

6. **Should `ReturnValues::AllNew` be supported for PutItem?**
   - In DynamoDB, PutItem with `ALL_NEW` returns the item as stored (after defaults, type conversions, etc.). For FerridynDB, the document is stored as-is, so `ALL_NEW` for PutItem would be identical to the input. This is redundant.
   - Recommendation: Only support `AllOld` for PutItem and DeleteItem. Reserve `AllNew` for UpdateItem.

## Migration Path

Since FerridynDB is pre-1.0 and the API is evolving, introduce ReturnValues support incrementally:

1. **Phase 1:** Implement `put_item_returning` and `delete_item` with `.return_old()` in core (`ferridyn-core`).
2. **Phase 2:** Extend `ferridyn-server` protocol to support `return_values` field and `returned_item` response.
3. **Phase 3:** Update `ferridyn-client` to expose the new API.
4. **Phase 4:** When UpdateItem is implemented, add `AllNew` support.

Each phase is independently testable.

## Implementation Notes

### Internal Changes

The internal write path in `crates/ferridyn-core/src/api/database.rs` already performs:

```rust
let old_doc = btree.get(key)?;  // Read old document
// ... MVCC version chain update ...
// ... Secondary index update (future) ...
btree.put(key, new_doc)?;  // Write new document
```

To support ReturnValues:

```rust
let old_doc = btree.get(key)?;
let returned = if return_values == ReturnValues::AllOld {
    old_doc.clone()  // Clone to return
} else {
    None
};
// ... MVCC version chain update ...
btree.put(key, new_doc)?;
return Ok(returned);
```

The only additional cost is the clone (cheap for typical document sizes).

### Type-Safe Builder Pattern

Use Rust's type state pattern to enforce at compile time that `.return_old()` changes the return type:

```rust
pub struct PutItemBuilder<R = NoReturn> {
    table: String,
    item: Value,
    _return: PhantomData<R>,
}

pub struct NoReturn;
pub struct ReturnOld;

impl PutItemBuilder<NoReturn> {
    pub fn execute(self) -> Result<()> { /* ... */ }

    pub fn return_old(self) -> PutItemBuilder<ReturnOld> {
        PutItemBuilder {
            table: self.table,
            item: self.item,
            _return: PhantomData,
        }
    }
}

impl PutItemBuilder<ReturnOld> {
    pub fn execute(self) -> Result<Option<Value>> { /* ... */ }
}
```

This ensures users cannot accidentally call `.execute()` expecting a return value without calling `.return_old()` first.

## Conclusion

ReturnValues support is a high-value, low-cost feature that brings FerridynDB closer to DynamoDB API parity while improving efficiency and correctness. The implementation leverages data already read internally, making the performance impact negligible. The proposed API design preserves backward compatibility while providing a natural, type-safe interface.
