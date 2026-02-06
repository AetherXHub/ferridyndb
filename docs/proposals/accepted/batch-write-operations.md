# Proposal: Batch Write Operations (Server Protocol)

**Status:** Accepted
**Author:** --
**Date:** 2026-02-06

## Motivation

FerridynDB's embedded API already supports atomic batch writes via `WriteBatch`, which collects put and delete operations and commits them in a single transaction. However, this is not exposed over the ferridyn-server wire protocol.

Server clients (including ferridyn-console and ferridyn-memory) must issue individual `PutItem` and `DeleteItem` requests sequentially, each requiring a full round-trip over the Unix socket. For bulk operations like seeding data, pruning expired items, or consolidating memories, this means N round-trips instead of 1.

## Current State

- `WriteBatch` in `ferridyn-core/src/api/batch.rs` supports:
  - `.put_item(table, document)` -- queue a put
  - `.delete_item(table, pk, sk)` -- queue a delete
  - `.commit()` -- commit all ops atomically (all-or-nothing)
  - Mixed put/delete in one batch
  - Cross-table operations
- The server protocol has individual `PutItem` and `DeleteItem` -- no batch variant
- DynamoDB's `BatchWriteItem` supports up to 25 put/delete ops per call

## Proposed Design

### Wire Protocol

```json
{
  "action": "BatchWriteItem",
  "operations": [
    { "put": { "table": "memories", "item": { "category": "notes", "key": "a", "content": "..." } } },
    { "put": { "table": "memories", "item": { "category": "notes", "key": "b", "content": "..." } } },
    { "delete": { "table": "memories", "partition_key": "notes", "sort_key": "old-item" } }
  ]
}
```

Response:
```json
{
  "succeeded": 3
}
```

### Server Handler

1. Validate `operations.len() <= 25`
2. Build a `WriteBatch` from the operations
3. Call `batch.commit()`
4. Return succeeded count (all-or-nothing, so either all succeed or error)

### Client API

```rust
client.batch_write_item()
    .put("memories", json!({ "category": "notes", "key": "a", "content": "..." }))
    .put("memories", json!({ "category": "notes", "key": "b", "content": "..." }))
    .delete("memories", json!("notes"), Some(json!("old-item")))
    .execute()
    .await?;
```

## Trade-offs

| Dimension | Impact |
|-----------|--------|
| Implementation complexity | Very low -- wraps existing `WriteBatch` |
| API surface | +1 protocol message, +1 client method |
| Performance | Significant for server clients (N round-trips -> 1) |
| Failure mode | All-or-nothing (matches `WriteBatch` semantics) |

## Open Questions

### 1. Batch Size Limit?

**25** matches DynamoDB. For local IPC this is conservative, but it prevents accidentally oversized requests and matches user expectations from DynamoDB.

### 2. Update operations in batch?

DynamoDB's `BatchWriteItem` only supports put and delete, not update. Follow the same pattern -- use transactions for multi-item updates.

## Summary

This is a thin wire protocol layer over existing `WriteBatch` infrastructure. Low risk, straightforward implementation, meaningful latency improvement for server clients doing bulk operations.
