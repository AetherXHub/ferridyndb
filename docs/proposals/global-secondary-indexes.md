# Proposal: Global Secondary Indexes

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

FerridynDB's current secondary indexes are **scoped** to a partition schema prefix — they only index items whose partition key matches a specific prefix pattern. This design works well for single-entity-type queries but breaks down for cross-entity access patterns common in DynamoDB single-table design.

DynamoDB provides two types of secondary indexes:

- **Global Secondary Indexes (GSIs)** — re-partition data on any attribute across the entire table. The GSI has its own partition key and optional sort key, independent of the base table's key schema.
- **Local Secondary Indexes (LSIs)** — same partition key as the table, alternate sort key. Queries still target a single partition but use a different sort order.

True GSIs would enable:

- **Query any attribute across the full table** — e.g., index "email" across all `USER#`, `ADMIN#`, and `GUEST#` prefixed items in a single index.
- **Support access patterns that cross entity type boundaries** — e.g., "find all items with `status=active`" regardless of whether they're orders, tasks, or projects.
- **Match DynamoDB semantics** — critical for systems like ferridyn-memory that aim to be DynamoDB-compatible.

## Current State

### Scoped Index Implementation

Indexes are defined with a partition schema scope:

```rust
db.create_index("table")
    .name("status-index")
    .partition_schema("ORDER")   // only indexes ORDER# items
    .index_key("status", KeyType::String)
    .execute()?;
```

**Key characteristics:**

| Property | Behavior |
|----------|----------|
| **Scope** | Partition schema prefix (e.g., `ORDER#`) |
| **Key format** | `(index_key_value, primary_key_bytes)` |
| **Projection** | KEYS_ONLY — stores primary key bytes, fetches full doc from primary table |
| **Maintenance** | Automatic on put/delete, checks prefix match before indexing |
| **Backfill** | Synchronous on index creation, scans only matching-prefix items |
| **Composite keys** | No — single attribute only |
| **Drop behavior** | Leaks pages (no reclamation) |

**Storage:**

- `IndexDefinition` struct (`types.rs` lines 101-116):
  - `name: String`
  - `partition_schema: String` — the prefix this index is scoped to
  - `index_key: KeyDefinition` — single attribute + type
  - `root_page: PageId` — B+Tree root for index

- `CreateIndexBuilder` (`builders.rs` lines 963-1042):
  - Requires `.partition_schema(prefix)` and `.index_key(attr, type)`
  - Backfills synchronously within the transaction

- `IndexQueryBuilder` (`builders.rs` lines 751-883):
  - Scans index B+Tree for entries matching `key_value`
  - Fetches full documents from primary table via MVCC
  - Silently skips deleted/invisible documents (lazy GC)

## Proposed Design

### Phase 1: Unscoped (Global) Secondary Indexes

Allow creating indexes **without** a partition schema scope. These indexes span the entire table, indexing any item that contains the indexed attribute.

#### API

```rust
// Current: scoped index
db.create_index("table")
    .name("status-index")
    .partition_schema("ORDER")   // only indexes ORDER# items
    .index_key("status", KeyType::String)
    .execute()?;

// Proposed: global index (no partition_schema)
db.create_index("table")
    .name("email-index")
    .index_key("email", KeyType::String)
    .execute()?;
// Indexes ALL items in the table that have an "email" attribute
```

#### Implementation

- **Modify `CreateIndexBuilder`:** Make `.partition_schema()` **optional**.
- **Modify index maintenance:** When `partition_schema` is `None`, skip the prefix check and index every item that contains the indexed attribute.
- **Modify backfill:** When `partition_schema` is `None`, scan the entire table instead of filtering by prefix.
- **Modify `IndexDefinition`:** Make `partition_schema: Option<String>`. This is a breaking schema change — requires migration.
- **Modify catalog storage:** Store unscoped index definitions (MessagePack serialization handles `Option<String>` automatically).

**Backward compatibility:** Existing scoped indexes continue to work. The `partition_schema` field in `IndexDefinition` becomes `Option<String>` — `Some(prefix)` for scoped indexes, `None` for global indexes.

### Phase 2: Composite Index Keys

DynamoDB GSIs have both a **partition key** and an optional **sort key** for the index itself. This allows range queries within the index.

#### API

```rust
db.create_index("table")
    .name("status-date-index")
    .index_key("status", KeyType::String)           // index partition key
    .index_sort_key("created_at", KeyType::Number)  // index sort key
    .execute()?;

// Query with both index partition key and sort condition
db.query_index("table", "status-date-index")
    .key_value("active")
    .sort_key_greater_than(json!(1706800000))
    .execute()?;
```

#### Implementation

- **Modify `IndexDefinition`:** Add `index_sort_key: Option<KeyDefinition>`.
- **Modify index B+Tree key encoding:** Change from `(index_key_value, primary_key_bytes)` to `(index_pk_value, index_sk_value, primary_key_bytes)`. For indexes without a sort key, the `index_sk_value` component is omitted (same as current encoding).
- **Modify `IndexQueryBuilder`:**
  - Add `.sort_key_eq()`, `.sort_key_gt()`, `.sort_key_between()`, etc. methods (mirror the main table `QueryBuilder`).
  - Compute scan bounds for the index key range: `(index_pk_value, sort_start) .. (index_pk_value, sort_end)`.
- **Modify index maintenance:** Encode composite key when inserting/updating/deleting index entries.

**Why this matters:** Without composite keys, you can only query by equality on the indexed attribute. With composite keys, you can do range queries like "find all items with `status=active` and `created_at > X`".

### Phase 3: Index Projections

Support projecting additional attributes into the index to avoid fetching from the primary table.

#### API

```rust
db.create_index("table")
    .name("email-index")
    .index_key("email", KeyType::String)
    .projection_type(IndexProjection::Include(vec!["name".into(), "status".into()]))
    .execute()?;
```

#### Projection Types

DynamoDB supports three projection types:

| Projection | Behavior | Use Case |
|------------|----------|----------|
| **KeysOnly** | Only primary key stored (current behavior) | Minimal storage, fetch full doc from table |
| **Include(Vec\<String\>)** | Specified attributes stored alongside key | Read common fields without table fetch |
| **All** | Full document copy stored in index | Fastest queries, 2x storage per index |

#### Implementation

- **Modify `IndexDefinition`:** Add `projection: IndexProjection` enum.
- **Modify index storage:** For `Include` and `All` projections, serialize the projected attributes (MessagePack) into the index entry value field. Current indexes use an empty value (`[]`).
- **Modify `IndexQueryBuilder`:**
  - For `KeysOnly` projection, fetch full document from primary table (current behavior).
  - For `Include` projection, deserialize projected attributes from index entry. If query needs additional fields, fetch from primary table.
  - For `All` projection, return full document from index entry without primary table fetch.
- **Modify index maintenance:** On put/delete, serialize projected attributes into the index entry. For `All` projection, the index must be updated on **every** document change, even if the indexed attribute didn't change.

**Trade-offs:** Full document projection (`All`) doubles storage for each index but eliminates primary table lookups. This can significantly improve query performance at the cost of write amplification and storage.

### Phase 4: Local Secondary Indexes (LSIs)

LSIs share the same partition key as the table but use a different sort key. These are conceptually simpler than GSIs but require the table to have a sort key.

#### API

```rust
db.create_local_index("table")
    .name("created-at-index")
    .sort_key("created_at", KeyType::Number)
    .execute()?;

// Query: same partition key, different sort key
db.query_index("table", "created-at-index")
    .partition_key("user#123")
    .sort_key_greater_than(json!(1706800000))
    .execute()?;
```

#### Implementation

- **Add `CreateLocalIndexBuilder`:** New builder similar to `CreateIndexBuilder`, but only accepts `.sort_key()` (not `.index_key()` or `.partition_schema()`).
- **LSI key format:** `(table_partition_key, index_sort_key_value, table_sort_key_value)`.
  - The table's partition key is used as the LSI partition key.
  - The index sort key is the new attribute to sort on.
  - The table's sort key is appended for uniqueness (in case multiple items have the same index sort key value).
- **Modify `IndexDefinition`:** Add `is_local: bool` field (or use a separate `LocalIndexDefinition` struct).
- **Modify index query:** For LSIs, require `.partition_key()` (same as table queries) and support sort key conditions on the LSI sort key.

**DynamoDB LSI semantics:** LSIs must be created when the table is created (cannot be added later). We may choose to relax this restriction for simplicity, but it's worth noting for compatibility.

## Changes Required

### Phase 1: Unscoped Indexes

| File | Change |
|------|--------|
| `types.rs` | Make `IndexDefinition.partition_schema: Option<String>` |
| `builders.rs` | Make `CreateIndexBuilder.partition_schema()` optional |
| `builders.rs` | Skip prefix check in index maintenance when `partition_schema` is `None` |
| `catalog/ops.rs` | Update `create_index()` to accept `Option<String>` for partition schema |
| `api/database.rs` | Update write paths (`put_item`, `delete_item`) to index all items when `partition_schema` is `None` |
| `catalog/ops.rs` | Update backfill to scan entire table when `partition_schema` is `None` |

### Phase 2: Composite Keys

| File | Change |
|------|--------|
| `types.rs` | Add `IndexDefinition.index_sort_key: Option<KeyDefinition>` |
| `encoding/composite.rs` | Support encoding/decoding three-part keys: `(index_pk, index_sk, primary_key)` |
| `builders.rs` | Add `.index_sort_key()` to `CreateIndexBuilder` |
| `builders.rs` | Add sort key condition methods to `IndexQueryBuilder` |
| `builders.rs` | Compute scan bounds for index range queries |
| `api/database.rs` | Update index maintenance to encode composite keys |

### Phase 3: Projections

| File | Change |
|------|--------|
| `types.rs` | Add `IndexProjection` enum and `IndexDefinition.projection` field |
| `builders.rs` | Add `.projection_type()` to `CreateIndexBuilder` |
| `builders.rs` | Modify `IndexQueryBuilder` to return projected attributes when sufficient |
| `api/database.rs` | Update index maintenance to serialize projected attributes into index entries |
| `api/database.rs` | For `All` projection, update index on any document change (not just indexed attribute) |

### Phase 4: Local Indexes

| File | Change |
|------|--------|
| `types.rs` | Add `LocalIndexDefinition` struct (or `is_local: bool` flag) |
| `builders.rs` | Add `CreateLocalIndexBuilder` |
| `builders.rs` | Add local index query support to `IndexQueryBuilder` (or separate builder) |
| `encoding/composite.rs` | Support LSI key format: `(table_pk, index_sk, table_sk)` |
| `api/database.rs` | Update index maintenance for LSIs |

## Trade-offs

| Dimension | Scoped Indexes (current) | Global Indexes (proposed) |
|---|---|---|
| **Write cost** | Lower — only indexes matching-prefix items | Higher — every item with the attribute is indexed |
| **Storage** | Smaller — subset of items | Larger — all matching items |
| **Query flexibility** | Limited to one entity type | Cross-entity queries |
| **Backfill time** | Faster — scans subset | Slower — full table scan |
| **Complexity** | Simpler | More complex maintenance, more edge cases |
| **DynamoDB compatibility** | Non-standard | Matches DynamoDB semantics |

### Projection Trade-offs

| Projection | Storage Overhead | Query Performance | Write Amplification |
|------------|------------------|-------------------|---------------------|
| **KeysOnly** | None (1x) | Requires primary table fetch | Low (1x) |
| **Include** | Moderate (~1.2-1.5x) | Avoids fetch for common fields | Moderate (1.2-1.5x) |
| **All** | High (2x per index) | No primary table fetch | High (2x per index) |

### Composite Key Trade-offs

| Aspect | Single-Attribute Index | Composite Index |
|--------|------------------------|-----------------|
| **Range queries** | Not supported (equality only) | Supported on second key |
| **Storage** | Smaller keys | Larger keys |
| **Key encoding** | Simple | More complex |
| **Query patterns** | Limited | Flexible |

## Migration Path

### Schema Evolution

The `IndexDefinition` struct change (`partition_schema: String` → `partition_schema: Option<String>`) is a **breaking change** for on-disk format. Migration strategy:

1. **Version 1 (current):** `partition_schema: String` is always present.
2. **Version 2 (proposed):** `partition_schema: Option<String>`.

**Migration code:**

```rust
// During catalog deserialization, wrap old indexes in Some(...)
impl IndexDefinition {
    fn migrate_from_v1(mut self) -> Self {
        // If this is an old-format index, wrap the partition_schema in Some.
        // New indexes will have partition_schema already as Option.
        self
    }
}
```

MessagePack deserialization handles `Option<String>` gracefully — missing fields deserialize as `None`, present fields deserialize as `Some(...)`. This means:

- Old indexes (serialized with `partition_schema: String`) will deserialize as `Some(String)` when the field is changed to `Option<String>`.
- New indexes can be serialized with `partition_schema: None`.

**No explicit migration code needed** if we rely on serde's `#[serde(default)]` attribute.

### Rollout

| Phase | Change | Compatibility Impact |
|-------|--------|---------------------|
| **Phase 1** | Unscoped indexes | Backward compatible with existing scoped indexes |
| **Phase 2** | Composite keys | Additive — old indexes still work |
| **Phase 3** | Projections | Additive — old indexes default to `KeysOnly` |
| **Phase 4** | LSIs | New index type, no impact on existing GSIs |

## Open Questions

1. **Should scoped indexes be deprecated or kept alongside global indexes?**
   - **Keep:** Scoped indexes are more efficient when you only need to index a subset of items. For single-table designs where entity types are isolated, scoped indexes save storage and write cost.
   - **Deprecate:** Simpler mental model with only global indexes. Users can manually filter by prefix if needed.
   - **Recommendation:** Keep both. Scoped indexes are a useful optimization for the embedded use case.

2. **Maximum number of GSIs per table?**
   - DynamoDB allows 20 GSIs per table.
   - For embedded use, there's less need for a hard limit (no multi-tenant resource contention).
   - **Recommendation:** No hard limit initially. Add a configurable soft limit (e.g., 50) if needed.

3. **Should backfill be asynchronous (background thread) for large tables?**
   - **Current:** Synchronous backfill within the transaction blocks all reads/writes.
   - **Problem:** For tables with millions of items, backfill could take minutes.
   - **Proposed:** Add an `.async_backfill(bool)` option to `CreateIndexBuilder`. When true, create the index definition immediately (marking it as "building") and spawn a background thread to backfill. Queries against the index return an error until backfill completes.
   - **Recommendation:** Start with synchronous backfill (simpler). Add async backfill if users report blocking issues.

4. **For `All` projection, should the index be updated on every document change (even if the indexed attribute didn't change)?**
   - **DynamoDB behavior:** Yes. GSIs with `All` projection update on any document change, even if the GSI key attributes are unchanged.
   - **Why:** The full document is stored in the index, so any field change requires re-serializing the index entry.
   - **Recommendation:** Match DynamoDB behavior. This is expected for users coming from DynamoDB.

5. **Should we support sparse indexes (only index items where the attribute exists)?**
   - **DynamoDB behavior:** GSIs are inherently sparse — items without the GSI key attribute are not indexed.
   - **Current behavior:** Scoped indexes are also sparse (only items with the indexed attribute are indexed).
   - **Recommendation:** Keep sparse behavior for all indexes. This is consistent with DynamoDB and avoids indexing `null` values.

6. **How to handle index key type mismatches?**
   - **Problem:** If an index is defined as `KeyType::Number` but a document has a string value for that attribute, should the item be skipped (sparse) or rejected (validation error)?
   - **DynamoDB behavior:** Type mismatches are silently ignored — the item is not indexed.
   - **Recommendation:** Match DynamoDB behavior. Log a warning (tracing level) but don't fail the write.

7. **Should LSIs be restricted to table creation time (matching DynamoDB)?**
   - **DynamoDB restriction:** LSIs must be defined when the table is created. They cannot be added or removed later.
   - **Reason:** LSIs share storage with the base table in DynamoDB. Adding an LSI after creation would require a full table rebuild.
   - **FerridynDB context:** We use separate B+Trees for indexes, so there's no technical reason to restrict LSI creation.
   - **Recommendation:** Allow LSIs to be added at any time (simpler API). Document the divergence from DynamoDB.

8. **How to handle index drop and page reclamation?**
   - **Current behavior:** Dropping an index leaks pages (no reclamation).
   - **Problem:** For large indexes, this wastes significant storage.
   - **Proposed solution:** Add a `DROP INDEX` operation that:
     1. Removes the index definition from the catalog.
     2. Walks the index B+Tree and adds all pages to the free list.
     3. Commits the updated free list in the transaction.
   - **Recommendation:** Implement this as part of Phase 1 (not tied to GSIs).
