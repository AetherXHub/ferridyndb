# Proposal: BatchGetItem Support

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB's `BatchGetItem` retrieves up to 100 items by key in a single call. FerridynDB currently requires individual `get_item` calls for each key.

For the **embedded case**, this is mainly an **ergonomic improvement**. Each `get_item` is already fast since it's in-process — there's no I/O overhead beyond the B+Tree lookup itself. Batching doesn't reduce disk I/O or lock acquisition; it just provides a cleaner API for fetching multiple known keys.

For the **server case** (`ferridyn-server`), batching provides a **significant latency reduction**. Each `get_item` requires a round-trip over the Unix socket (send request, await response). Batching 100 items reduces 100 round-trips to 1, which is meaningful even for local IPC.

## Current State

- `get_item` reads a single item by partition key + optional sort key.
- `get_item_versioned` returns the item with its MVCC version (`(Value, u64)`).
- Both use `read_snapshot` internally for MVCC isolation.
- `WriteBatch` exists for batch writes (`put_item` / `delete_item`), but there is no read equivalent.
- The server protocol has individual `GetItem` request/response — no batching.

## Proposed Design

### Single-Table Batch (Phase 1)

Start with a single-table batch API. This covers the most common use case and keeps the initial implementation simple.

```rust
let results = db.batch_get_item("users")
    .key(json!({"pk": "user#1", "sk": "profile"}))
    .key(json!({"pk": "user#2", "sk": "profile"}))
    .key(json!({"pk": "user#3", "sk": "profile"}))
    .execute()?;
// results: Vec<Option<Value>> — positional, None for missing items
```

**Builder API:**

```rust
pub struct BatchGetItemBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    keys: Vec<Value>,
}

impl<'a> BatchGetItemBuilder<'a> {
    pub fn key(mut self, key: Value) -> Self { ... }
    pub fn execute(self) -> Result<Vec<Option<Value>>> { ... }
}
```

**Consistency guarantee:** All items in the batch are read from the **same MVCC snapshot**, providing a consistent point-in-time view. This is actually **stronger** than DynamoDB's `BatchGetItem`, which provides no cross-item consistency.

### Cross-Table Batch (Phase 2, optional)

DynamoDB allows fetching from multiple tables in a single call:

```rust
let results = db.batch_get_items()
    .table("users")
        .key(json!({"pk": "user#1"}))
        .key(json!({"pk": "user#2"}))
    .table("orders")
        .key(json!({"pk": "order#100"}))
    .execute()?;
// results: HashMap<String, Vec<Option<Value>>> keyed by table name
```

This is useful but less common. Recommend deferring until single-table is validated.

## Implementation

### Core Logic

```rust
// In BatchGetItemBuilder::execute()
1. Acquire a single read snapshot: let snapshot = self.db.read_snapshot()?;
2. Look up the table in the catalog: let table_info = snapshot.get_table_info(&self.table)?;
3. For each key in self.keys:
     a. Encode the key as composite B+Tree key
     b. Perform B+Tree lookup in snapshot: snapshot.get(&encoded_key)?
     c. Collect result (Some(document) or None)
4. Return Vec<Option<Value>> in the same order as input keys
```

The existing `read_snapshot` + B+Tree lookup infrastructure handles everything. The batch is just a loop with a shared snapshot.

### Changes Required

| Component | Change |
|-----------|--------|
| `crates/ferridyn-core/src/api/builders.rs` | Add `BatchGetItemBuilder` struct |
| `crates/ferridyn-core/src/api/database.rs` | Add `batch_get_item(&self, table: &str) -> BatchGetItemBuilder` method |
| `crates/ferridyn-server/src/protocol.rs` | Add `BatchGetItem` request/response variants |
| `crates/ferridyn-server/src/server.rs` | Handle `Request::BatchGetItem` → call `db.batch_get_item()` |
| `crates/ferridyn-server/src/client.rs` | Add `batch_get_item` method that sends `BatchGetItem` request |
| Tests | Basic batch, some missing keys, empty batch, single key (degenerate case), snapshot consistency across items |

### Optional: Integration with ProjectionExpression

If `ProjectionExpression` support exists (filtering returned attributes), add:

```rust
db.batch_get_item("users")
    .key(...)
    .projection("name,email") // Optional: limit returned fields
    .execute()?
```

This follows the pattern from `QueryBuilder`.

### Optional: Versioned Batch Reads

Add a variant that returns MVCC versions alongside documents:

```rust
pub fn execute_versioned(self) -> Result<Vec<Option<(Value, u64)>>> { ... }
```

This mirrors `get_item_versioned`. Useful for caching scenarios where the client wants to detect staleness.

## Trade-offs

| Dimension | Embedded Case | Server Case |
|---|---|---|
| **Latency reduction** | None — already in-process | **Significant** — N round-trips → 1 |
| **Disk I/O reduction** | None — each get is a separate B+Tree lookup | None — same as embedded |
| **Ergonomics** | Better — one call instead of N | Better — cleaner client code |
| **Lock contention** | Slightly worse — single snapshot held longer | Same as multiple individual gets |
| **API surface** | Larger — new builder, new protocol messages | Larger |

For **embedded use**, this is primarily a **convenience feature**.

For **server use**, the **round-trip savings** are real and meaningful.

### Comparison to DynamoDB

| Feature | DynamoDB | FerridynDB (proposed) |
|---------|----------|----------------------|
| **Batch size limit** | 100 items | None (embedded) / configurable limit (server, suggest 1000) |
| **Cross-table batch** | Yes | Phase 2 / optional |
| **Consistency** | None — each item is eventually consistent | **Snapshot isolation** — all items from same MVCC snapshot |
| **Partial failure handling** | Returns `UnprocessedKeys` | All-or-nothing for embedded; consider partial failure for server (rate limiting, malformed keys) |
| **ProjectionExpression** | Yes | Optional, depends on existing support |

## Open Questions

### 1. Result Format: Positional Vec or Keyed HashMap?

**Option A: Positional Vec** (recommended)

```rust
results: Vec<Option<Value>>
```

**Pros:** Simple, matches input order, efficient for client iteration.
**Cons:** Client must track which index corresponds to which key if order matters.

**Option B: Keyed HashMap**

```rust
results: HashMap<Value, Option<Value>>
```

**Pros:** Explicit key → result mapping.
**Cons:** Requires `Value` to implement `Hash + Eq` for use as HashMap key. Composite keys (partition + sort) are multi-field objects. More allocation overhead.

**Recommendation:** Use positional Vec. DynamoDB uses an unkeyed array in its response format.

### 2. Cross-Table Batch: Phase 1 or Defer?

**Recommendation:** Defer to Phase 2. Single-table covers 90% of use cases and keeps the initial implementation simple.

### 3. Batch Size Limit?

**Embedded:** No limit necessary. The caller controls the batch size and owns the performance trade-off.

**Server:** Recommend a limit to prevent oversized requests. Options:
- **100** — matches DynamoDB
- **1000** — more generous, reasonable for local IPC
- **Configurable** — allow tuning via server config

**Recommendation:** Start with 1000 for server, no limit for embedded API.

### 4. Versioned Batch Reads?

Add `execute_versioned()` to return `Vec<Option<(Value, u64)>>`?

**Recommendation:** Defer. The use case (cache invalidation) is niche. Can add later if needed without breaking changes.

### 5. ProjectionExpression Integration?

Should `BatchGetItemBuilder` support `.projection()` to filter returned attributes?

**Recommendation:** If `ProjectionExpression` already exists for `QueryBuilder`, add it here for consistency. Otherwise, defer.

### 6. Partial Failure Handling?

DynamoDB returns `UnprocessedKeys` when it cannot complete the entire batch (throttling, transient errors).

**Embedded:** All-or-nothing. Either all keys succeed or the call returns `Err`.

**Server:** Consider partial success for robustness:
- **Option A:** All-or-nothing (match embedded behavior).
- **Option B:** Return `(Vec<Option<Value>>, Vec<Value>)` where the second element is unprocessed keys.

**Recommendation:** Start with all-or-nothing. Server-side partial failure can be added later if rate limiting or multi-tenancy is introduced.

## Migration Path

1. **Phase 1:** Implement single-table `batch_get_item` in `ferridyn-core`.
2. **Phase 2:** Add `BatchGetItem` request/response to `ferridyn-server` protocol.
3. **Phase 3:** Add client-side `batch_get_item` method to `FerridynClient`.
4. **Phase 4 (optional):** Extend to cross-table batch (`batch_get_items`).

Each phase is independently testable and valuable.

## Benchmark Considerations

For the **embedded case**, expect minimal performance difference:
- Batch: 1 snapshot acquisition + N B+Tree lookups
- Individual: N × (1 snapshot acquisition + 1 B+Tree lookup)

If snapshot acquisition is cheap (likely), the difference is negligible.

For the **server case**, benchmark the round-trip reduction:
- Measure total latency for 100 individual `get_item` calls vs. 1 `batch_get_item` call
- Expect ~2x improvement (round-trip overhead amortization)

## Summary

`BatchGetItem` is a **high-value, low-complexity** addition:
- **Server use case:** Clear performance win from round-trip reduction
- **Embedded use case:** Ergonomic improvement, no performance regression
- **Implementation:** Straightforward loop over existing B+Tree lookup logic
- **API consistency:** Matches DynamoDB patterns users expect

Recommend starting with single-table batch, deferring cross-table and advanced features until usage validates the need.
