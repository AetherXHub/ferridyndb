# FerridynDB Feature Requests

Requested by ferridyn-memory. These are database-level primitives that any application built on FerridynDB would benefit from. They are listed in priority order based on how many downstream application features they unblock.

## Context

FerridynDB currently supports: `put_item` (full document replace), `get_item` (exact key lookup), `query` (partition key + sort key `begins_with` + limit), `delete_item`, `list_partition_keys`, `list_sort_key_prefixes`, partition schemas with typed attributes, and secondary indexes with `query_index`.

The features below address gaps that force application layers to implement inefficient client-side workarounds (full scans with local filtering, fetch-modify-put cycles, client-side TTL).

---

## Feature 1: Partial Update (PATCH)

### Current Behavior

The only write operation is `put_item`, which replaces the entire document. To change a single attribute, the caller must `get_item`, modify the JSON locally, then `put_item` the whole document back. This is:
- 2 round-trips instead of 1
- Race-prone (another writer can modify between get and put)
- Wastes bandwidth sending unchanged attributes

### Proposed API

```
update_item(table, partition_key, sort_key, operations) -> Result<Item>
```

**Operations:**
- `SET { attr: value }` — set one or more attributes to new values
- `REMOVE [attr, ...]` — delete attributes from the document
- `INCREMENT { attr: amount }` — atomically add to a numeric attribute
- `APPEND { attr: value }` — append to a string or array attribute
- `SET_IF_NOT_EXISTS { attr: value }` — set only if the attribute doesn't already exist

**Return:** The updated item (post-update state).

**Error cases:**
- Item does not exist → error (not an upsert; use `put_item` for creation)
- Type mismatch (INCREMENT on a string) → error
- Attribute name conflicts with key attributes (category, key) → error

### Client Protocol

For the Unix socket protocol, add a new request variant:

```json
{
  "action": "UpdateItem",
  "table": "memories",
  "partition_key": "decisions",
  "sort_key": "jwt-auth",
  "operations": {
    "set": { "resolved": true, "updated_at": "2026-02-06T12:00:00Z" },
    "remove": ["temporary_note"],
    "increment": { "access_count": 1 }
  }
}
```

### Motivation

Unblocks in ferridyn-memory:
- Importance score decay (increment/decrement a counter without full replace)
- Toggling `resolved` on issues
- Removing `expires_at` during promote (currently requires full get/modify/put)
- Appending to notes without overwriting
- `access_count` tracking for relevance scoring

### Acceptance Criteria

- [ ] `update_item` with SET modifies specified attributes, leaves others unchanged
- [ ] `update_item` with REMOVE deletes specified attributes
- [ ] `update_item` with INCREMENT atomically adds to numeric attributes
- [ ] `update_item` on nonexistent item returns error (not upsert)
- [ ] Type mismatch errors are descriptive
- [ ] Available through both in-process (`FerridynDB`) and client (`FerridynClient`) APIs
- [ ] Existing tests continue to pass

---

## Feature 2: Attribute Filter Expressions

### Current Behavior

`query` only filters on partition key (exact match) and sort key (`begins_with`). To find "all issues where `resolved = false`" or "all contacts where `team = backend`", the caller must fetch the entire partition and filter client-side. This scales poorly as partitions grow.

### Proposed API

Add an optional `filter` parameter to `query` and `query_index`:

```
query(table, partition_key, sort_key_condition?, filter?, limit?) -> QueryResult
```

**Filter expression language:**

```
attribute = value              // equality
attribute != value             // inequality
attribute > value              // comparison (>, <, >=, <=) for numbers/strings
attribute BETWEEN a AND b      // range (inclusive)
attribute BEGINS_WITH prefix   // string prefix
attribute CONTAINS substring   // string contains
attribute EXISTS               // attribute is present
attribute NOT_EXISTS           // attribute is absent
expr AND expr                  // conjunction
expr OR expr                   // disjunction
NOT expr                       // negation
```

**Data types in filters:** String, Number, Boolean.

### Client Protocol

```json
{
  "action": "Query",
  "table": "memories",
  "partition_key": "issues",
  "filter": "resolved = false AND area = 'auth'",
  "limit": 50
}
```

### Evaluation Model

Filters are applied **after** partition key + sort key selection but **before** returning results. They reduce the result set without requiring client-side post-processing. The `limit` applies to filtered results (i.e., "return up to N items that match the filter").

### Motivation

Unblocks in ferridyn-memory:
- "all unresolved issues" → `filter: "resolved = false"`
- "contacts on backend team" → `filter: "team = 'backend'"`
- "high-importance memories" → `filter: "importance >= 7"`
- Relationship traversal: "all items referencing decisions/jwt-auth" → `filter: "relates_to CONTAINS 'decisions/jwt-auth'"`
- Expired item exclusion at DB level → `filter: "expires_at NOT_EXISTS OR expires_at > '2026-02-06T...'"` (partial TTL improvement even without native TTL)

### Acceptance Criteria

- [ ] Equality, inequality, comparison operators work on String, Number, Boolean
- [ ] BETWEEN works on numbers and strings (lexicographic)
- [ ] BEGINS_WITH and CONTAINS work on strings
- [ ] EXISTS / NOT_EXISTS check attribute presence
- [ ] AND, OR, NOT combine expressions correctly
- [ ] Filters apply to `query` results (partition + sort key selection first, then filter)
- [ ] Filters apply to `query_index` results
- [ ] `limit` counts only items that pass the filter
- [ ] Filter on nonexistent attribute: item is excluded (not an error)
- [ ] Filter expression parsing errors return descriptive messages

---

## Feature 3: Sort Key Range Queries

### Current Behavior

The sort key condition only supports `begins_with(prefix)`. There is no way to query a range like "sort keys between A and B" or "sort keys greater than X". This makes date-ordered data (events, logs) hard to query by time range.

### Proposed API

Extend `SortKeyCondition` with new variants:

```
SortKeyCondition:
  BeginsWith { prefix }          // existing
  Equals { value }               // new: exact sort key match
  Between { start, end }         // new: inclusive range
  GreaterThan { value }          // new
  GreaterThanOrEqual { value }   // new
  LessThan { value }             // new
  LessThanOrEqual { value }      // new
```

### Client Protocol

```json
{
  "action": "Query",
  "table": "memories",
  "partition_key": "events",
  "sort_key_condition": {
    "between": { "start": "2026-02-01", "end": "2026-02-07" }
  },
  "limit": 50
}
```

### Motivation

Unblocks in ferridyn-memory:
- "events this week" → partition `events`, sort key between `2026-02-03` and `2026-02-09` (if keys are date-prefixed)
- "memories created in last 7 days" → range on `created_at` if used as sort key, or via attribute filter
- Efficient pagination with "greater than last seen key"
- Temporal range queries for any date-keyed data

### Acceptance Criteria

- [ ] All six new sort key conditions work correctly
- [ ] Between is inclusive on both ends
- [ ] Range queries work with both String and Number sort keys
- [ ] Conditions compose correctly with partition key and filter expressions
- [ ] Empty range returns empty result (not an error)
- [ ] Available in both `FerridynDB` (builder API) and `FerridynClient`

---

## Feature 4: Batch Operations

### Current Behavior

All operations are single-item. Storing 10 items requires 10 sequential `put_item` calls, each a separate round-trip over the Unix socket.

### Proposed API

```
batch_put_items(table, items: Vec<Item>) -> Result<BatchResult>
batch_get_items(table, keys: Vec<(pk, sk)>) -> Result<Vec<Option<Item>>>
batch_delete_items(table, keys: Vec<(pk, sk)>) -> Result<BatchResult>
```

**BatchResult:**
```
{
  succeeded: usize,
  failed: Vec<{ key: (pk, sk), error: String }>
}
```

**Constraints:**
- Maximum batch size: 25 items (matches DynamoDB convention)
- Partial failure: succeeded items are committed, failed items are reported
- Items within a batch can span different partition keys

### Client Protocol

```json
{
  "action": "BatchPutItems",
  "table": "memories",
  "items": [
    { "category": "notes", "key": "a", "content": "..." },
    { "category": "notes", "key": "b", "content": "..." }
  ]
}
```

### Motivation

Unblocks in ferridyn-memory:
- Bulk import (seed data, migration from other systems)
- Consolidation (delete N items + write 1 summary in one batch)
- Export/backup workflows
- Agent batch-storing multiple memories from a single interaction
- `prune` deleting all expired items without N sequential round-trips

### Acceptance Criteria

- [ ] batch_put_items writes up to 25 items in a single call
- [ ] batch_get_items retrieves multiple items by key
- [ ] batch_delete_items removes multiple items by key
- [ ] Partial failures are reported per-item (succeeded items still committed)
- [ ] Exceeding max batch size returns an error
- [ ] Items can span different partition keys within one batch
- [ ] Performance: batch of 25 is faster than 25 individual calls

---

## Feature 5: Native TTL / Expiry

### Current Behavior

FerridynDB has no concept of item expiry. ferridyn-memory implements TTL entirely client-side:
- Stores an `expires_at` RFC 3339 string attribute
- Filters expired items from query results after fetching them
- Requires explicit `prune` command to delete expired items (full table scan)

Problems: expired items waste storage and query bandwidth, no background cleanup, client must implement filtering everywhere.

### Proposed API

**On write:**
```
put_item(table, item, ttl_seconds?) -> Result<()>
update_item(table, pk, sk, ..., ttl_seconds?) -> Result<Item>
```

When `ttl_seconds` is set, the DB computes `expires_at = now + ttl_seconds` and stores it internally.

**On read:**
Expired items are **never returned** by `get_item`, `query`, `query_index`, `batch_get_items`. They behave as if deleted.

**Background reaper:**
A periodic background task (configurable interval, default 1 hour) deletes expired items from storage. This is best-effort and asynchronous — reads filter expired items immediately, the reaper reclaims disk space.

**Explicit TTL management:**
```
remove_ttl(table, pk, sk) -> Result<()>       // make item permanent
set_ttl(table, pk, sk, ttl_seconds) -> Result<()>  // update expiry
```

### Client Protocol

```json
{
  "action": "PutItem",
  "table": "memories",
  "item": { "category": "scratchpad", "key": "temp", "content": "..." },
  "ttl_seconds": 86400
}
```

### Motivation

Unblocks in ferridyn-memory:
- Scratchpad 24h default TTL without client-side filtering
- Events auto-expiry without manual prune
- Efficient queries: no wasted bandwidth fetching dead items
- `promote` simplifies to `remove_ttl` instead of get-modify-put
- Background cleanup eliminates need for explicit `prune` command

### Acceptance Criteria

- [ ] Items with TTL are not returned after expiry by any read operation
- [ ] Background reaper deletes expired items periodically
- [ ] `remove_ttl` makes an item permanent
- [ ] `set_ttl` updates an existing item's expiry
- [ ] TTL is optional — items without TTL never expire
- [ ] Reaper interval is configurable
- [ ] TTL precision is seconds
- [ ] Expired items don't count toward query `limit`

---

## Feature 6: Conditional Writes

### Current Behavior

`put_item` is unconditional — it always succeeds, silently overwriting any existing item. There is no way to say "create only if this key doesn't exist" or "update only if the item hasn't changed since I read it."

### Proposed API

Add optional condition expressions to write operations:

```
put_item(table, item, condition?) -> Result<()>
update_item(table, pk, sk, operations, condition?) -> Result<Item>
delete_item(table, pk, sk, condition?) -> Result<()>
```

**Condition expressions:**
```
attribute_not_exists(key)           // item doesn't exist (for create-if-new)
attribute_exists(key)               // item exists (for update-if-exists)
attribute = value                   // attribute matches expected value
version = N                        // optimistic concurrency (if using version attribute)
```

**On condition failure:** Return a `ConditionCheckFailed` error (not a generic error).

### Client Protocol

```json
{
  "action": "PutItem",
  "table": "memories",
  "item": { "category": "notes", "key": "unique-id", "content": "..." },
  "condition": "attribute_not_exists(key)"
}
```

### Motivation

Unblocks in ferridyn-memory:
- Safe concurrent agent access: two agents storing to same key get conflict detection instead of silent overwrite
- "remember only if new" semantics for deduplication
- Optimistic locking for read-modify-write cycles
- Safe promote: delete source only if target write succeeded

### Acceptance Criteria

- [ ] `attribute_not_exists` prevents overwriting existing items
- [ ] `attribute_exists` prevents writing to nonexistent items
- [ ] Attribute value conditions work for String, Number, Boolean
- [ ] Failed conditions return a specific `ConditionCheckFailed` error type
- [ ] Conditions work on `put_item`, `update_item`, and `delete_item`
- [ ] Conditions compose with AND

---

## Feature 7: Count / Basic Aggregation

### Current Behavior

The only way to count items is to fetch them all and count client-side. There is no `COUNT` operation.

### Proposed API

```
count(table, partition_key, sort_key_condition?, filter?) -> Result<usize>
```

Same parameters as `query` but returns only the count, without transferring item data.

### Client Protocol

```json
{
  "action": "Count",
  "table": "memories",
  "partition_key": "issues",
  "filter": "resolved = false"
}
```

### Motivation

Unblocks in ferridyn-memory:
- Memory statistics: "how many items per category" without fetching all data
- Dashboard / health checks: "42 unresolved issues, 15 expired items"
- Prune estimation: "N items would be deleted" before committing
- Rate limiting: "don't store more than 100 scratchpad items"

### Acceptance Criteria

- [ ] Returns count without transferring item bodies
- [ ] Supports partition key, sort key condition, and filter expression
- [ ] Counts only non-expired items (if native TTL is implemented)
- [ ] Performance: significantly cheaper than fetching all items

---

## Feature 8: Vector / Embedding Index

### Current Behavior

No support for storing or querying vector embeddings. The only search strategy is exact key lookup, sort key prefix, and secondary index on discrete attribute values.

### Proposed API

**Index creation:**
```
create_vector_index(table, name, partition_schema, attribute, dimensions, metric?) -> Result<()>
```
- `attribute`: the item attribute containing the float vector
- `dimensions`: vector length (e.g., 1536 for OpenAI ada-002, 1024 for Cohere)
- `metric`: `cosine` (default), `euclidean`, `dot_product`

**Query:**
```
query_vector_index(table, index_name, vector, top_k, filter?) -> Result<Vec<ScoredItem>>
```
- `vector`: the query vector
- `top_k`: number of nearest neighbors to return
- `filter`: optional attribute filter applied pre- or post-ANN search
- Returns items with a `score` field (similarity/distance)

**Storage:**
Items include a vector attribute alongside regular attributes:
```json
{
  "category": "notes",
  "key": "rust-ownership",
  "content": "Rust uses ownership...",
  "embedding": [0.012, -0.034, ...]
}
```

### Motivation

Unblocks in ferridyn-memory:
- Semantic search: "find memories similar to this concept" without LLM round-trip per query
- Reduces per-query cost: embedding lookup is cheaper than LLM-powered query resolution
- Better recall: finds conceptually related memories even when keywords don't match
- Hybrid search: combine vector similarity with attribute filters ("similar to X, in category Y")

### Implementation Notes

This is the largest feature and could be deferred. If embedding storage is supported but ANN search is deferred, ferridyn-memory can still compute cosine similarity client-side for small datasets (< 10k items). The main value of DB-level support is scalability.

### Acceptance Criteria

- [ ] Vector attributes can be stored as float arrays
- [ ] Vector index can be created on a specific attribute
- [ ] ANN query returns top-k nearest neighbors with scores
- [ ] Cosine, euclidean, and dot product metrics are supported
- [ ] Vector queries can be combined with attribute filters
- [ ] Index updates automatically when items are added/modified/deleted

---

## Dependency Graph

```
(independent)
├── Feature 1: Partial Update
├── Feature 2: Attribute Filter Expressions
├── Feature 3: Sort Key Range Queries
├── Feature 4: Batch Operations
├── Feature 7: Count / Aggregation
│
(depends on Feature 1)
├── Feature 6: Conditional Writes (shares write-path changes)
│
(depends on Feature 2)
├── Feature 5: Native TTL (reaper uses filter to find expired; reads filter expired)
│
(independent, can be deferred)
└── Feature 8: Vector / Embedding Index
```

## Suggested Implementation Order

1. **Partial Update** — smallest scope, highest downstream impact
2. **Attribute Filter Expressions** — enables TTL, relationship traversal, most query improvements
3. **Sort Key Range Queries** — natural extension of existing query path
4. **Batch Operations** — independent, straightforward
5. **Native TTL** — depends on filters for read-side exclusion
6. **Conditional Writes** — shares write-path with partial update
7. **Count** — simple once filters exist
8. **Vector Index** — largest scope, can be deferred
