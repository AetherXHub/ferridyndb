# PRD: Batch Write Operations (Server Protocol)

**Priority:** 12
**Status:** Approved
**Proposal:** [docs/proposals/accepted/batch-write-operations.md](../proposals/accepted/batch-write-operations.md)

## Summary

Expose the existing `WriteBatch` API over the ferridyn-server wire protocol. The embedded API already supports atomic batch put/delete via `WriteBatch`, but the server protocol has no batch write operation, forcing server clients to make N sequential round-trips. This PRD adds `BatchWriteItem` to the wire protocol and client.

## Scope

### In Scope

- `BatchWriteItem` request/response in the server wire protocol
- `FerridynClient::batch_write_item()` async method
- Batch size limit enforcement (25 items, matches DynamoDB convention)
- Mixed put and delete operations in a single batch
- Cross-table operations within a single batch (already supported by `WriteBatch`)
- `BatchWriteResult` with succeeded count

### Out of Scope

- Partial failure reporting (per-item errors) -- keep existing all-or-nothing semantics from `WriteBatch`
- `UnprocessedItems` retry mechanism (DynamoDB pattern for throttling) -- not applicable to embedded/local IPC
- Batch update_item -- DynamoDB doesn't support this either; use transactions for multi-item updates
- Embedded API changes -- `WriteBatch` already exists and works

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Batch size limit? | 25 items | Matches DynamoDB convention; prevents oversized requests |
| Failure mode? | All-or-nothing | Matches existing `WriteBatch.commit()` behavior |
| Cross-table? | Yes | Already supported by `WriteBatch` |
| Result format? | `{ succeeded: usize }` | Simple; all-or-nothing means succeeded = total or error |

## Implementation

### Single Phase (Low Complexity)

The core logic already exists in `WriteBatch`. This PRD only adds wire protocol plumbing.

**Deliverables:**

**Protocol (`protocol.rs`):**
- `BatchWriteItem` request variant with `operations: Vec<BatchWriteOp>`
- `BatchWriteOp` enum: `Put { table, document }` | `Delete { table, partition_key, sort_key }`
- `BatchWriteItem` response variant with `succeeded: usize`

**Server (`server.rs`):**
- Handle `Request::BatchWriteItem`:
  1. Validate batch size <= 25
  2. Build `WriteBatch` from operations
  3. Call `batch.commit()`
  4. Return succeeded count

**Client (`client.rs`):**
- `FerridynClient::batch_write_item()` method
- Builder pattern matching the embedded `WriteBatch` API

**Files:**
| File | Change |
|------|--------|
| `ferridyn-server/src/protocol.rs` | Add `BatchWriteItem` request/response, `BatchWriteOp` enum |
| `ferridyn-server/src/server.rs` | Handle `BatchWriteItem` request |
| `ferridyn-server/src/client.rs` | Add `batch_write_item()` method |

**Tests:**
- `test_batch_write_put_over_wire` (batch of puts)
- `test_batch_write_delete_over_wire` (batch of deletes)
- `test_batch_write_mixed_over_wire` (puts + deletes in one batch)
- `test_batch_write_cross_table` (operations spanning tables)
- `test_batch_write_exceeds_limit` (> 25 items returns error)
- `test_batch_write_error_rolls_back` (invalid op aborts entire batch)
- `test_batch_write_empty` (empty batch succeeds as no-op)

## Acceptance Criteria

1. Batch of up to 25 put/delete operations commits atomically over the wire
2. Exceeding batch size limit returns clear error
3. Failed batches roll back entirely (no partial writes)
4. Cross-table operations work within a single batch
5. Performance: batch of 25 over wire is faster than 25 individual calls
6. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None (builds on existing `WriteBatch` infrastructure)

## Dependents

- None
