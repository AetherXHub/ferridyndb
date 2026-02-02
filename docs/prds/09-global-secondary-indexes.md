# PRD: Global Secondary Indexes

**Priority:** 9
**Status:** Approved
**Proposal:** [docs/proposals/global-secondary-indexes.md](../proposals/global-secondary-indexes.md)

## Summary

Extend the secondary index system beyond partition-schema-scoped indexes. Add unscoped (global) indexes that span the entire table, composite index keys (partition + sort), index projections (KeysOnly, Include, All), and local secondary indexes. This brings FerridynDB closer to full DynamoDB GSI/LSI semantics.

## Scope

### In Scope (4 phases)

- **Phase 1:** Unscoped (global) indexes -- index all items with the attribute, no prefix requirement
- **Phase 2:** Composite index keys -- index partition key + sort key for range queries within indexes
- **Phase 3:** Index projections -- store additional attributes in index entries to avoid primary table fetches
- **Phase 4:** Local secondary indexes -- same partition key, alternate sort key

### Out of Scope

- Async/background backfill -- start with synchronous
- Maximum index count limits -- no hard limit initially
- DynamoDB-style provisioned throughput on indexes

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Keep scoped indexes? | Yes, alongside global | Scoped indexes are more efficient for single-entity-type queries |
| Max GSIs per table? | No hard limit | Embedded use case; no multi-tenant contention |
| Backfill mode? | Synchronous initially | Simpler; async backfill added if blocking becomes an issue |
| All projection update behavior? | Update on any doc change | Matches DynamoDB; full doc stored in index |
| Sparse indexes? | Yes | Items without the indexed attribute are not indexed (DynamoDB-compatible) |
| Type mismatch on indexed attr? | Silently skip (not indexed) | Matches DynamoDB |
| LSI creation timing? | Allow any time (not just at table creation) | Simpler API; we use separate B+Trees |
| Index drop page reclamation? | Implement in Phase 1 | Fix existing page leak issue |

## Implementation Phases

### Phase 1: Unscoped (Global) Indexes

**Deliverables:**
- Make `IndexDefinition.partition_schema` an `Option<String>` (None = global)
- Make `.partition_schema()` optional on `CreateIndexBuilder`
- Skip prefix check in write-path index maintenance when partition_schema is None
- Backfill scans entire table when partition_schema is None
- Fix index drop to reclaim pages (walk tree, add to free list)
- On-disk format migration: old `String` field deserializes as `Some(String)` via serde default

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/src/types.rs` | `partition_schema: Option<String>` |
| `ferridyn-core/src/api/builders.rs` | Optional `.partition_schema()`, update index maintenance |
| `ferridyn-core/src/catalog/ops.rs` | Accept `Option<String>`, update backfill logic |
| `ferridyn-core/src/api/database.rs` | Update put/delete index maintenance for global indexes |

**Tests:**
- `test_create_global_index` (no partition_schema)
- `test_global_index_backfill` (indexes all items with attribute)
- `test_global_index_query` (finds items across entity types)
- `test_global_index_sparse` (items without attribute not indexed)
- `test_global_index_type_mismatch_skipped`
- `test_scoped_index_still_works` (backward compatibility)
- `test_drop_index_reclaims_pages`

### Phase 2: Composite Index Keys

**Deliverables:**
- `index_sort_key: Option<KeyDefinition>` on `IndexDefinition`
- `.index_sort_key()` on `CreateIndexBuilder`
- Three-part B+Tree key encoding: `(index_pk, index_sk, primary_key)`
- Sort key condition methods on `IndexQueryBuilder`: `.sort_key_eq()`, `.sort_key_gt()`, `.sort_key_between()`, etc.
- Scan bound computation for index range queries

**Tests:**
- `test_composite_index_create`
- `test_composite_index_range_query`
- `test_composite_index_between`
- `test_composite_index_backfill`

### Phase 3: Index Projections

**Deliverables:**
- `IndexProjection` enum: `KeysOnly`, `Include(Vec<String>)`, `All`
- `.projection_type()` on `CreateIndexBuilder`
- Store projected attributes in index entry values (MessagePack)
- `IndexQueryBuilder`: return projected attrs when sufficient, fetch from primary table otherwise
- `All` projection: update index on every document change

**Tests:**
- `test_index_projection_keys_only` (default, current behavior)
- `test_index_projection_include`
- `test_index_projection_all`
- `test_index_projection_all_updates_on_any_change`

### Phase 4: Local Secondary Indexes

**Deliverables:**
- `CreateLocalIndexBuilder` (or flag on existing builder)
- LSI key format: `(table_pk, index_sk, table_sk)`
- LSI query: require partition key, support sort key conditions on LSI sort key
- Allow LSI creation on existing tables (diverges from DynamoDB)

**Tests:**
- `test_local_index_create`
- `test_local_index_query`
- `test_local_index_range_scan`

## Acceptance Criteria

1. Global indexes correctly index all items with the target attribute across the full table
2. Scoped indexes continue to work (backward compatible)
3. Composite keys enable range queries on the index sort key
4. Index projections reduce primary table fetches when projected attributes are sufficient
5. LSIs correctly partition by table pk and sort by index sk
6. Dropped indexes reclaim their B+Tree pages
7. On-disk format migration is seamless (old indexes deserialize correctly)
8. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None (extends existing index infrastructure)

## Dependents

- None (but enables richer query patterns for all consumers)
