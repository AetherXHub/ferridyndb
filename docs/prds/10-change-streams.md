# PRD: Change Streams (Change Data Capture)

**Priority:** 10
**Status:** Approved
**Proposal:** [docs/proposals/accepted/change-streams.md](../proposals/accepted/change-streams.md)

## Summary

Add DynamoDB-Streams-compatible change data capture to FerridynDB. Every write operation (put, delete, update) appends a record to a per-table stream B+Tree. Consumers poll the stream by sequence number to process changes incrementally. Enables event-driven patterns: triggers, replication, materialized views, audit logs.

## Scope

### In Scope

- Per-table opt-in change streams with configurable view types (KeysOnly, NewImage, OldImage, NewAndOldImages)
- `StreamRecord` struct with sequence number, event type, keys, old/new images, timestamp
- Stream storage via dedicated B+Tree per table (keyed by sequence number)
- Atomic capture: stream record commits atomically with data write (same CoW commit)
- Poll-based consumption API: `get_stream_records(after_sequence, limit)`
- Stream metadata API: `get_stream_info()` (oldest/latest sequence, enabled, view type)
- Configurable retention policy (max age, max count)
- Enable/disable streams on existing tables
- Server protocol support (GetStreamRecords, GetStreamInfo requests)

### Out of Scope

- Push-based streaming (server pushes to subscribed clients) -- requires bidirectional messaging, major architectural change
- Stream sharding/partitioning -- single linear stream per table sufficient for embedded/low-volume
- TTL expiration stream events -- defer to later
- Batch writes producing aggregate records -- produce one record per item (DynamoDB behavior)

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Storage model? | Separate B+Tree per stream | Clean separation; efficient sequential reads; reuses existing machinery |
| Per-table or global? | Per-table | Matches DynamoDB; easier selective consumption |
| Sequence numbers? | Transaction IDs (monotonic) | Already available; guaranteed ordering |
| Default retention? | 7 days or 1M records, whichever first | Sensible defaults preventing unbounded growth |
| Batch writes? | One record per item | Matches DynamoDB; same txn_id for same batch |
| Enable on existing table? | Yes (no retroactive data) | Simpler; historical data not streamed |
| Disable stream? | Yes (preserves existing records) | User can manually prune |

## Implementation Phases

### Phase 1: Core Stream Capture (KeysOnly)

**Deliverables:**
- `StreamRecord`, `StreamViewType`, `EventType` structs in new `ferridyn-core/src/stream/` module
- `.stream(StreamViewType)` on `TableBuilder`
- Stream tree root page stored in `TableSchema`
- Write path integration: capture Insert/Modify/Remove events, assign sequence = txn_id
- `db.get_stream_records("table").after_sequence(n).limit(100).execute()`
- `db.get_stream_info("table")`
- Atomic commit: stream tree changes in same CoW page commit

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/stream/` | New module: record types, stream tree ops |
| `ferridyn-core/src/catalog/table.rs` | Add stream config to `TableSchema` |
| `ferridyn-core/src/api/database.rs` | Modify put/delete/update to append stream records |
| `ferridyn-core/src/api/builders.rs` | `.stream()` on TableBuilder, stream query builders |
| `ferridyn-core/src/api/mod.rs` | Export stream module |

**Tests:**
- `test_stream_insert_event`
- `test_stream_modify_event`
- `test_stream_remove_event`
- `test_stream_sequence_ordering`
- `test_stream_keys_only_view`
- `test_stream_disabled_no_records` (no overhead when disabled)

### Phase 2: Full Image Views

**Deliverables:**
- `NewImage`, `OldImage`, `NewAndOldImages` view types
- Capture old document from MVCC read, new document from write
- `StreamRecord.old_image` and `.new_image` populated based on view type

**Tests:**
- `test_stream_new_image`
- `test_stream_old_image`
- `test_stream_new_and_old_images`
- `test_stream_modify_images_correct` (old = before, new = after)

### Phase 3: Retention and Pruning

**Deliverables:**
- Retention policy config: `max_age` (Duration), `max_count` (usize)
- `prune_stream("table")` explicit method
- Background/on-demand sweep: scan stream B+Tree from beginning, delete expired records
- `enable_stream` / `disable_stream` methods for existing tables

**Tests:**
- `test_stream_retention_max_count`
- `test_stream_retention_max_age`
- `test_stream_enable_on_existing_table`
- `test_stream_disable_preserves_records`

### Phase 4: Server Protocol

**Deliverables:**
- `GetStreamRecords` and `GetStreamInfo` request/response variants
- Server handlers forwarding to core API
- `FerridynClient::get_stream_records()` and `get_stream_info()` async methods

**Tests:**
- `test_stream_records_over_wire`
- `test_stream_info_over_wire`

### Phase 5: Transaction and Batch Integration

**Deliverables:**
- Transaction commits produce stream records for each operation
- Batch writes produce one stream record per item (same sequence number)
- Correct event type detection within transactions (Insert vs Modify vs Remove)

**Tests:**
- `test_stream_transaction_multiple_records`
- `test_stream_batch_write_records`

## Acceptance Criteria

1. Stream records appear if and only if the data write commits (atomic)
2. Sequence numbers are strictly increasing and reflect serialization order
3. Consuming all records from oldest to latest reproduces every change
4. Retention pruning correctly removes old records and reclaims pages
5. Stream disabled = zero write overhead (no tree operations)
6. Write performance with KeysOnly streams degrades < 15%
7. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD #1 (UpdateItem) -- for update event capture in Phase 1
- PRD #6 (ReturnValues) -- old/new images reuse similar internal read logic

## Dependents

- ferridyn-memory plugin (reactive updates)

## Risk

This is the most architecturally complex feature in the roadmap. It adds a new subsystem with its own storage, lifecycle, and API surface. The write amplification (2x B+Tree writes per operation) and storage growth require careful retention policy tuning.

Recommend prototyping Phase 1 (KeysOnly, no retention) first to validate the approach and measure write overhead before committing to the full implementation.
