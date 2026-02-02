# Proposal: Change Data Capture (Streams) for FerridynDB

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB Streams provides an ordered log of all changes (INSERT, MODIFY, REMOVE) to a table, enabling event-driven patterns such as:

- **Triggers** — React to data changes in real-time (e.g., send notification when a new order is created)
- **Replication** — Propagate changes to other databases or caches
- **Materialized views** — Maintain derived aggregations or denormalized tables
- **Audit logs** — Track all modifications for compliance or debugging
- **Event sourcing** — Reconstruct application state from a sequence of events

FerridynDB currently has no mechanism to observe changes after they happen. Applications must poll to detect updates, which is inefficient and scales poorly. For the `ferridyn-memory` plugin use case (agent memory backing store), change streams would enable reactive updates — when the database writes a new memory entry, the agent runtime could be notified immediately rather than polling every N seconds.

Change data capture is increasingly considered a core database feature. This proposal brings DynamoDB Streams semantics to FerridynDB's embedded and server APIs.

## Current State

- **No change notification mechanism** at any layer. Writes are silent from the perspective of external observers.
- **MVCC version chains** track old versions for snapshot isolation, but they are internal and get garbage collected. They cannot serve as a change log.
- **Monotonic transaction IDs** — The MVCC transaction manager assigns strictly increasing transaction IDs. These could serve as sequence numbers for a change stream.
- **Single-writer serialization point** — All writes go through a single-writer lock, creating a natural choke point where changes can be captured in a deterministic order.

## Proposed Design

### Stream Model

Each table can have an optional change stream. When enabled, every write operation (`put`, `delete`, `update`) appends a record to the stream.

**Stream record structure:**

```rust
pub struct StreamRecord {
    pub sequence_number: u64,        // monotonic, derived from txn_id
    pub event_type: EventType,       // Insert, Modify, Remove
    pub table_name: String,
    pub keys: Value,                 // partition key + sort key (always present)
    pub old_image: Option<Value>,    // document before change (if configured)
    pub new_image: Option<Value>,    // document after change (if configured)
    pub timestamp: u64,              // unix epoch milliseconds
}

pub enum EventType {
    Insert,   // new item (no previous version)
    Modify,   // existing item updated (previous version existed)
    Remove,   // item deleted
}
```

**Stream view types** (matching DynamoDB semantics):

| View Type | Keys | Old Image | New Image |
|---|---|---|---|
| `KeysOnly` | ✓ | — | — |
| `NewImage` | ✓ | — | ✓ |
| `OldImage` | ✓ | ✓ | — |
| `NewAndOldImages` | ✓ | ✓ | ✓ |

- `KeysOnly` includes only the item's partition key and sort key. Minimal storage, but consumer must read the database to get full item state.
- `NewImage` includes the full document after the change. Useful for cache invalidation or replication.
- `OldImage` includes the full document before the change. Useful for audit logs or undo functionality.
- `NewAndOldImages` includes both before and after. Enables diff computation.

### Storage

**Two options for stream record storage:**

#### Option A: Separate B+Tree per stream (recommended)

- Each table with streaming enabled has a dedicated B+Tree keyed by `sequence_number`.
- Stream records are stored as MessagePack-encoded blobs (same as documents).
- The stream tree root page ID is stored in the table schema (catalog).

**Pros:**
- Clean separation: stream records do not pollute the main data tree.
- Efficient sequential reads: B+Tree iteration over sequence numbers is fast.
- Reuses existing B+Tree machinery (no new data structures).

**Cons:**
- Increases file size: every write writes to two trees (data + stream).
- Adds another tree to maintain (splits, page allocation, GC).

#### Option B: Ring buffer of dedicated pages

- Fixed-size circular buffer of pages dedicated to stream records for a table.
- Old records are automatically overwritten when the buffer wraps.

**Pros:**
- Bounded storage: stream records do not grow indefinitely.
- Simple page management: allocate N contiguous pages, wrap at the end.

**Cons:**
- Limited retention: once the buffer wraps, old records are lost.
- More complex page management: need to track write pointer, handle wrapping.
- No inherent ordering structure: consumers must parse all records to find gaps.

**Recommendation:** **Option A** with a configurable retention policy. A background sweep (similar to the proposed TTL sweep) prunes old stream records based on age or count. This provides:
- Predictable storage growth with tunable retention.
- Efficient range queries (`get_stream_records(after: 12345, limit: 100)`).
- Familiar semantics for DynamoDB Streams users (sequence numbers, shard-like partitioning is future work).

### Reading the Stream

**Embedded API:**

```rust
// Enable stream on table creation
db.create_table("orders")
    .partition_key("pk", KeyType::String)
    .stream(StreamViewType::NewAndOldImages)
    .execute()?;

// Read stream records starting after a sequence number
let records: Vec<StreamRecord> = db.get_stream_records("orders")
    .after_sequence(last_seen_sequence)
    .limit(100)
    .execute()?;

// Get stream metadata (for initial catchup or monitoring)
let info: StreamInfo = db.get_stream_info("orders")?;
// info.oldest_sequence: u64 — earliest available record (trim horizon)
// info.latest_sequence: u64 — most recent record
// info.enabled: bool
// info.view_type: StreamViewType
```

**Stream consumption pattern (poll-based):**

```rust
let mut last_sequence = db.get_stream_info("orders")?.oldest_sequence;
loop {
    let records = db.get_stream_records("orders")
        .after_sequence(last_sequence)
        .limit(100)
        .execute()?;

    for record in records {
        handle_change(&record);  // Process the change
        last_sequence = record.sequence_number;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

**Server API (ferridyn-server):**

- **Poll-based (initial implementation):** Client sends `GetStreamRecords { table_name, after_sequence, limit }` request. Server responds with `Vec<StreamRecord>`.
- **Push-based (future enhancement):** Server pushes stream records to connected clients that have subscribed to a table's stream. This would require changing the server from request-response to bidirectional messaging (WebSocket-style or similar) — a significant architectural change. Defer to later.

### Integration with Write Path

At the single-writer commit point (end of a transaction):

1. **Normal write proceeds**: B+Tree modification, MVCC version creation, page allocation.
2. **If stream is enabled for this table**, capture the change:
   - Determine event type:
     - `Insert` if no previous version exists (MVCC `created_txn` = this transaction, no prior version in chain)
     - `Modify` if a previous version exists (MVCC version chain has older entry)
     - `Remove` on delete (item marked as deleted in MVCC)
   - Capture old image (from MVCC version chain if needed) and new image (from current write) based on stream view type.
   - Assign sequence number = current transaction ID (guaranteed monotonic).
   - Create `StreamRecord` struct.
   - Serialize with `rmp_serde::to_vec()`.
   - Append to the stream B+Tree (key = sequence number, value = serialized record).
3. **Atomic commit**: Both the data tree and stream tree changes are part of the same copy-on-write commit. The header update atomically makes both visible.

This ensures:
- **Atomicity**: Stream record appears if and only if the data write commits.
- **Ordering**: Sequence numbers (= txn IDs) are strictly increasing and reflect serialization order.
- **No races**: Single-writer lock prevents concurrent writes from producing out-of-order records.

**Changes Required:**

| Component | Modification |
|---|---|
| `crates/ferridyn-core/src/stream/` | **New module** — `StreamRecord`, `StreamViewType`, `EventType` structs; stream B+Tree management; `get_stream_records`, `get_stream_info` functions. |
| `TableBuilder` | Add `.stream(StreamViewType)` method to enable streaming on table creation. |
| `TableSchema` / catalog | Store stream configuration: `enabled: bool`, `view_type: StreamViewType`, `stream_tree_root: PageId`. |
| Header / catalog | Add field for stream tree root page ID (or embed in `TableSchema` and store in catalog tree). |
| Write paths | Modify `put`, `delete`, `update`, `batch_write`, `transact_write_items` to capture changes and append to stream tree when enabled. |
| `crates/ferridyn-core/src/api/` | Add public `get_stream_records`, `get_stream_info`, `enable_stream`, `disable_stream` methods to `FerridynDB`. |
| Stream retention | Add sweep function (similar to TTL sweep) to prune old stream records based on retention policy (max age or max count). |
| `crates/ferridyn-server/src/protocol.rs` | Add `GetStreamRecords` and `GetStreamInfo` request/response variants. |
| `crates/ferridyn-server/src/server.rs` | Handle new request types, forward to embedded DB. |
| `crates/ferridyn-server/src/client.rs` | Add `get_stream_records`, `get_stream_info` methods to `FerridynClient`. |
| Tests | Basic stream capture, all event types, old/new images, sequence ordering, retention pruning, stream with batch writes, stream with transactions (multiple changes → multiple records), stream disabled (no overhead), stream enabled mid-lifetime (historical data not retroactively streamed). |

### Retention Policy

Stream records accumulate over time. Without pruning, they will grow indefinitely. Proposed retention strategies:

| Strategy | Description | Use Case |
|---|---|---|
| **Max age** | Prune records older than N hours/days | "Keep last 24 hours of changes" |
| **Max count** | Prune records beyond the last N records | "Keep last 10,000 changes" |
| **Manual** | No automatic pruning; user calls `prune_stream` explicitly | Audit logs, event sourcing |

**Default recommendation:** 7 days or 1 million records, whichever is reached first. Configurable per table at creation time or via `alter_table_stream_retention`.

**Pruning implementation:**
- Periodically scan the stream B+Tree from the beginning.
- Delete records that violate the retention policy.
- Mark freed pages for reuse (same as document delete).

## Trade-offs

| Dimension | Without Streams (current) | With Streams (proposed) |
|---|---|---|
| **Write performance** | Baseline | **Slower** — every data write also writes a stream record (extra B+Tree insert) |
| **Storage** | Data only | **Larger** — data + stream records (mitigated by retention policy) |
| **Complexity** | Simpler | **Significantly more complex** — new subsystem, stream B+Trees, retention, API surface |
| **Capabilities** | Poll-only, no change history | **Event-driven patterns**, audit trail, replication, materialized views |
| **Read performance** | Unaffected | Unaffected (stream reads are separate from data reads) |
| **Crash recovery** | No change | No change (stream records commit atomically with data) |

**Write amplification:**
- Every `put`/`delete`/`update` writes two entries: one to the data tree, one to the stream tree.
- With `NewAndOldImages`, the stream record contains two full copies of the document, effectively tripling write size for that record.
- Mitigation: Use `KeysOnly` view type when full images are not needed.

**Storage growth:**
- Stream records accumulate until pruned.
- Example: 1,000 writes/sec with `NewAndOldImages` (avg 1 KB/document) → ~170 GB/day of stream data (before pruning).
- Retention policy is critical for production use.

**Complexity:**
- This is the most architecturally complex proposal to date. It adds an entirely new subsystem with its own storage, API, and lifecycle.
- Testing surface expands significantly: must verify atomicity, ordering, retention, interaction with transactions, etc.

**Embedded vs. server use case:**
- **Embedded**: The application controls all writes and already knows what changed. Streams are less valuable here unless implementing event sourcing or audit logs.
- **Server**: Multiple clients write to the same database. Streams enable one client to react to another's changes. Much higher value.

## Open Questions

1. **Per-table or global stream?**
   - Proposed: per-table (matches DynamoDB).
   - Alternative: single global stream for all tables. Simpler, but harder to consume selectively.

2. **Stream record durability during GC:**
   - Should stream records survive MVCC garbage collection, or are they ephemeral?
   - Proposed: stream records are independent of MVCC version chains. They persist until explicitly pruned by retention policy.

3. **Default retention limits:**
   - What are sensible defaults? 7 days? 1 million records?
   - Should there be a hard cap to prevent unbounded growth? E.g., refuse to create a table with `retention = None` (unlimited).

4. **Server API: push vs. poll:**
   - Initial implementation: poll-based (client calls `get_stream_records` periodically).
   - Future: push-based (server pushes records to subscribed clients).
   - Push-based requires bidirectional messaging, which is a major architectural change to `ferridyn-server`. Worth it?

5. **Batch writes and transactions:**
   - Should a batch write of 10 items produce 10 separate stream records, or one aggregate record?
   - Proposed: 10 separate records. Each has the same `sequence_number` (same transaction ID), but consumers can see all individual changes.
   - DynamoDB Streams produces one record per item, even in batch writes.

6. **TTL expiration events:**
   - Should TTL-expired items generate `Remove` stream records?
   - DynamoDB does this. It's useful for cache invalidation.
   - Proposed: yes, but TTL sweep must be extended to write stream records during expiration.

7. **Enabling/disabling streams dynamically:**
   - Can a stream be enabled on an existing table without rebuilding it?
   - Proposed: yes. Historical data is not retroactively streamed, but new writes after enabling generate stream records.
   - Can a stream be disabled? Proposed: yes. Disabling stops new records but does not delete existing ones (user must manually prune if desired).

8. **Performance benchmarks:**
   - What is the acceptable write overhead? 10%? 20%?
   - Need benchmarks comparing baseline write throughput vs. streaming enabled (with different view types).
   - Preliminary estimate: `KeysOnly` adds ~5-10% overhead, `NewAndOldImages` adds ~15-25% (due to extra serialization).

9. **Sharding / partitioning:**
   - DynamoDB Streams partitions records into shards for parallel consumption.
   - FerridynDB has no sharding in v1. Should streams support it?
   - Proposed: defer sharding to later. Single linear stream per table is sufficient for embedded/low-volume use cases.

10. **Integration with `ferridyn-memory` plugin:**
    - Should the plugin subscribe to streams automatically, or require explicit configuration?
    - How should the plugin handle stream records during startup (replay from last checkpoint vs. start fresh)?

## Next Steps

1. **Validate the proposal** with potential users (e.g., `ferridyn-memory` author, early adopters).
2. **Prototype** stream capture and storage (minimal implementation: `KeysOnly` view, no retention policy).
3. **Benchmark** write overhead with streaming enabled vs. disabled.
4. **Implement** full feature set (all view types, retention, API).
5. **Document** usage patterns and best practices.
6. **Consider** push-based streaming in `ferridyn-server` as a follow-up enhancement.
