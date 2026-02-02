# Proposal: Pagination Support for Secondary Index Queries

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

Primary table `Query` and `Scan` operations already support full pagination via `exclusive_start_key` and `last_evaluated_key`. This allows clients to fetch large result sets incrementally without loading everything into memory.

Secondary index queries (`query_index`) currently have a `limit` parameter but no pagination cursor mechanism. If an index query hits its limit, there is no way to resume and fetch the next page. This is a gap in feature parity — all DynamoDB query operations support pagination uniformly.

**Current behavior:**

```rust
// First query gets 10 results
let results = db.query_index("users", "status-index")
    .key_value("active")
    .limit(10)
    .execute()?;

// results.items: Vec<Value>
// results.last_evaluated_key: None (missing!)

// No way to get the next page!
```

**Why this matters:**

- Large indexes may contain thousands of entries for a single indexed value
- Without pagination, clients must either load all results (memory exhaustion) or lose data (arbitrary limit)
- Inconsistent API — primary queries paginate but index queries do not
- DynamoDB's GSI queries support pagination; this is expected behavior

## Current State

### API Surface

**`IndexQueryBuilder`** (`crates/ferridyn-core/src/api/builders.rs`, lines 759-883):

```rust
pub struct IndexQueryBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    index_name: String,
    key_value: Option<Value>,
    limit: Option<usize>,
    scan_forward: bool,
}

impl<'a> IndexQueryBuilder<'a> {
    pub fn key_value(mut self, val: impl Into<Value>) -> Self { ... }
    pub fn limit(mut self, n: usize) -> Self { ... }
    pub fn scan_forward(mut self, forward: bool) -> Self { ... }
    pub fn execute(self) -> Result<QueryResult, Error> { ... }
}
```

**Missing:** `.exclusive_start_key()` method

### Internal Implementation

**Index query execution** (`crates/ferridyn-core/src/api/builders.rs`, lines 799-882):

1. Convert the search value to a `KeyValue`
2. Compute B+Tree scan bounds using `compute_scan_bounds(&indexed_kv, None, None)`
3. Scan the index B+Tree with `btree_ops::range_scan(store, index.root_page, start_key, end_key)`
4. For each index entry:
   - Extract the primary key bytes
   - Fetch the full document via `mvcc_ops::mvcc_get`
   - Check TTL expiration
   - Collect visible documents
5. Apply `scan_forward` direction
6. Apply `limit` via `Vec::truncate`
7. **Return `QueryResult { items, last_evaluated_key: None }`** — pagination info is always `None`

### Index B+Tree Key Format

Secondary index keys are composite: `(indexed_value, primary_key_bytes)`. This is critical for pagination because the cursor must encode both components to position correctly in the B+Tree.

Example for a user status index:
```
Index B+Tree key: encode("active") ++ encode_composite(pk, sk)
                  └─────┬──────┘    └──────────┬──────────┘
                  indexed value       primary key
```

### Primary Query Pagination (Reference Implementation)

**`QueryBuilder`** (`crates/ferridyn-core/src/api/builders.rs`, lines 304-476):

- Has `.exclusive_start_key(key: impl Into<Value>)` method (line 377)
- Encodes the exclusive start key via `encode_exclusive_start_key(esk, schema)` (line 419)
- Skips items `<= exclusive_start_key` during iteration (lines 428-432)
- Computes `last_evaluated_key` from the last item's key bytes (lines 462-465)
- Returns `QueryResult { items, last_evaluated_key }`

## Proposed Design

### API Changes

#### 1. Add `.exclusive_start_key()` to `IndexQueryBuilder`

```rust
impl<'a> IndexQueryBuilder<'a> {
    /// Resume pagination from a previous query result.
    ///
    /// The `key` should be the `last_evaluated_key` from a previous
    /// index query response. It must include both the indexed attribute
    /// and the primary key attributes.
    pub fn exclusive_start_key(mut self, key: impl Into<Value>) -> Self {
        self.exclusive_start_key = Some(key.into());
        self
    }
}
```

Add field to struct:

```rust
pub struct IndexQueryBuilder<'a> {
    // ... existing fields ...
    exclusive_start_key: Option<Value>,
}
```

#### 2. Return Pagination Cursor from `.execute()`

```rust
pub fn execute(self) -> Result<QueryResult, Error> {
    // ... (scan index, fetch documents, filter TTL) ...

    // Apply limit and compute last_evaluated_key.
    let (limited, last_key_info) = if let Some(limit) = self.limit {
        if items.len() > limit {
            let last_item = &items[limit - 1];
            let last_indexed_kv = extract_indexed_value(last_item, &index.index_key)?;
            let last_pk = extract_primary_key(last_item, &schema)?;
            items.truncate(limit);
            (items, Some((last_indexed_kv, last_pk)))
        } else {
            (items, None)
        }
    } else {
        (items, None)
    };

    let last_evaluated_key = if let Some((indexed_kv, pk_composite)) = last_key_info {
        Some(build_index_last_evaluated_key(indexed_kv, pk_composite, schema)?)
    } else {
        None
    };

    Ok(QueryResult {
        items: limited,
        last_evaluated_key,
    })
}
```

#### 3. Cursor Format (Structured JSON)

The cursor is a JSON object containing:

```json
{
  "status": "active",       // The indexed attribute (name from IndexDefinition)
  "user_id": "alice",       // Primary partition key (name from TableSchema)
  "timestamp": 1672531200   // Primary sort key (if present)
}
```

**Why structured instead of opaque:**

- **Matches DynamoDB:** DynamoDB GSI queries return `LastEvaluatedKey` as a structured map with all key attributes
- **Debuggable:** Clients can inspect the cursor to understand position
- **Stable format:** No encoding changes break existing cursors
- **Simple validation:** Clients can verify cursor completeness before sending

**Encoding for B+Tree positioning:**

When processing `exclusive_start_key`:

1. Extract indexed attribute value → encode as `KeyValue`
2. Extract primary key attributes → encode as composite key bytes
3. Concatenate: `encode(indexed_kv) ++ primary_key_bytes`
4. Use this as the B+Tree range scan start position

### Implementation Steps

#### 1. Modify `IndexQueryBuilder` in `builders.rs`

- Add `exclusive_start_key: Option<Value>` field
- Add `.exclusive_start_key()` method
- Update `execute()`:
  - Encode exclusive start key to composite B+Tree format
  - Skip items `<= exclusive_start_key` during iteration
  - Capture last item's key info when limit is applied
  - Build structured `last_evaluated_key` JSON object
  - Return `QueryResult { items, last_evaluated_key }`

#### 2. Add Helper Functions in `builders.rs`

```rust
/// Encode exclusive_start_key for index query into B+Tree composite format.
fn encode_index_exclusive_start_key(
    esk: &Value,
    schema: &TableSchema,
    index: &IndexDefinition,
) -> Result<Vec<u8>, Error> {
    // Extract indexed attribute value.
    let indexed_val = esk.get(&index.index_key.name)
        .ok_or(QueryError::IndexKeyRequired)?;
    let indexed_kv = key_utils::json_to_key_value(
        indexed_val,
        index.index_key.key_type,
        &index.index_key.name,
    )?;

    // Extract primary key attributes.
    let pk_val = esk.get(&schema.partition_key.name)
        .ok_or(QueryError::PartitionKeyRequired)?;
    let pk = key_utils::json_to_key_value(
        pk_val,
        schema.partition_key.key_type,
        &schema.partition_key.name,
    )?;

    let sk = match &schema.sort_key {
        Some(sk_def) => {
            if let Some(sk_val) = esk.get(&sk_def.name) {
                Some(key_utils::json_to_key_value(
                    sk_val,
                    sk_def.key_type,
                    &sk_def.name,
                )?)
            } else {
                None
            }
        }
        None => None,
    };

    // Encode composite: indexed_value ++ primary_key_bytes
    let primary_key_bytes = composite::encode_composite(&pk, sk.as_ref())?;
    let indexed_bytes = composite::encode_composite(&indexed_kv, None)?;

    let mut composite = Vec::new();
    composite.extend_from_slice(&indexed_bytes);
    composite.extend_from_slice(&primary_key_bytes);
    Ok(composite)
}

/// Build last_evaluated_key JSON object for index query.
fn build_index_last_evaluated_key(
    indexed_value: &Value,
    primary_key: &Value,
    schema: &TableSchema,
    index: &IndexDefinition,
) -> Result<Value, Error> {
    let mut obj = serde_json::Map::new();

    // Add indexed attribute.
    obj.insert(index.index_key.name.clone(), indexed_value.clone());

    // Add primary key attributes.
    if let Some(pk_val) = primary_key.get(&schema.partition_key.name) {
        obj.insert(schema.partition_key.name.clone(), pk_val.clone());
    }
    if let Some(sk_def) = &schema.sort_key {
        if let Some(sk_val) = primary_key.get(&sk_def.name) {
            obj.insert(sk_def.name.clone(), sk_val.clone());
        }
    }

    Ok(Value::Object(obj))
}
```

#### 3. Update `protocol.rs` in ferridyn-server

Add `QueryIndex` variant to the `Request` enum:

```rust
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    // ... existing variants ...

    QueryIndex {
        table: String,
        index_name: String,
        key_value: Value,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        scan_forward: Option<bool>,
        #[serde(default)]
        exclusive_start_key: Option<Value>,
    },
}
```

The server's request handler already returns `Response::ok_items(items, last_evaluated_key)`, which includes pagination fields. No changes needed to response types.

#### 4. Handle QueryIndex in server.rs

```rust
Request::QueryIndex {
    table,
    index_name,
    key_value,
    limit,
    scan_forward,
    exclusive_start_key,
} => {
    let mut builder = db.query_index(&table, &index_name).key_value(key_value);
    if let Some(lim) = limit {
        builder = builder.limit(lim);
    }
    if let Some(forward) = scan_forward {
        builder = builder.scan_forward(forward);
    }
    if let Some(esk) = exclusive_start_key {
        builder = builder.exclusive_start_key(esk);
    }
    let result = builder.execute()?;
    Response::ok_items(result.items, result.last_evaluated_key)
}
```

#### 5. Update client.rs

Add `query_index` method to `FerridynClient`:

```rust
pub async fn query_index(
    &self,
    table: &str,
    index_name: &str,
    key_value: Value,
    limit: Option<usize>,
    scan_forward: Option<bool>,
    exclusive_start_key: Option<Value>,
) -> Result<(Vec<Value>, Option<Value>), String> {
    let request = json!({
        "op": "query_index",
        "table": table,
        "index_name": index_name,
        "key_value": key_value,
        "limit": limit,
        "scan_forward": scan_forward,
        "exclusive_start_key": exclusive_start_key,
    });
    let response = self.send_request(request).await?;
    // Parse Items response...
}
```

### Test Cases

#### 1. Basic Pagination

```rust
// Create table with index.
db.create_table("orders")
    .partition_key("order_id", KeyType::String)
    .execute()?;

db.create_partition_schema("orders")
    .prefix("ORDER")
    .execute()?;

db.create_index("orders")
    .name("status-index")
    .partition_schema("ORDER")
    .index_key("status", KeyType::String)
    .execute()?;

// Insert 25 items with status="pending".
for i in 0..25 {
    db.put_item("orders", json!({
        "order_id": format!("ORDER#order-{:03}", i),
        "status": "pending",
    }))?;
}

// Page 1: limit 10.
let page1 = db.query_index("orders", "status-index")
    .key_value("pending")
    .limit(10)
    .execute()?;
assert_eq!(page1.items.len(), 10);
assert!(page1.last_evaluated_key.is_some());

// Page 2: continue from page1.
let page2 = db.query_index("orders", "status-index")
    .key_value("pending")
    .limit(10)
    .exclusive_start_key(page1.last_evaluated_key.unwrap())
    .execute()?;
assert_eq!(page2.items.len(), 10);
assert!(page2.last_evaluated_key.is_some());

// Page 3: final page (5 items).
let page3 = db.query_index("orders", "status-index")
    .key_value("pending")
    .limit(10)
    .exclusive_start_key(page2.last_evaluated_key.unwrap())
    .execute()?;
assert_eq!(page3.items.len(), 5);
assert!(page3.last_evaluated_key.is_none()); // No more pages.
```

#### 2. Empty Second Page

```rust
// Insert exactly 10 items.
for i in 0..10 {
    db.put_item("orders", json!({
        "order_id": format!("ORDER#order-{:03}", i),
        "status": "shipped",
    }))?;
}

// Page 1: gets all 10.
let page1 = db.query_index("orders", "status-index")
    .key_value("shipped")
    .limit(10)
    .execute()?;
assert_eq!(page1.items.len(), 10);
assert!(page1.last_evaluated_key.is_none()); // No more data.

// No page 2 needed — cursor is None.
```

#### 3. Single-Item Pages

```rust
// Insert 3 items.
for i in 0..3 {
    db.put_item("orders", json!({
        "order_id": format!("ORDER#order-{}", i),
        "status": "processing",
    }))?;
}

// Paginate with limit=1.
let mut cursor = None;
for expected in 0..3 {
    let mut builder = db.query_index("orders", "status-index")
        .key_value("processing")
        .limit(1);
    if let Some(esk) = cursor {
        builder = builder.exclusive_start_key(esk);
    }
    let result = builder.execute()?;
    assert_eq!(result.items.len(), 1);
    cursor = result.last_evaluated_key;
}

// Fourth call should return empty.
let last = db.query_index("orders", "status-index")
    .key_value("processing")
    .limit(1)
    .exclusive_start_key(cursor.unwrap())
    .execute()?;
assert_eq!(last.items.len(), 0);
assert!(last.last_evaluated_key.is_none());
```

#### 4. Reverse Scan Pagination

```rust
// Insert 20 items.
for i in 0..20 {
    db.put_item("orders", json!({
        "order_id": format!("ORDER#order-{:02}", i),
        "status": "delivered",
    }))?;
}

// Reverse scan: last 10 items first.
let page1 = db.query_index("orders", "status-index")
    .key_value("delivered")
    .limit(10)
    .scan_forward(false)
    .execute()?;
assert_eq!(page1.items.len(), 10);
assert!(page1.last_evaluated_key.is_some());

// Page 2: continue backward.
let page2 = db.query_index("orders", "status-index")
    .key_value("delivered")
    .limit(10)
    .scan_forward(false)
    .exclusive_start_key(page1.last_evaluated_key.unwrap())
    .execute()?;
assert_eq!(page2.items.len(), 10);
assert!(page2.last_evaluated_key.is_none());
```

#### 5. Invalid Cursor Handling

```rust
// Missing indexed attribute.
let result = db.query_index("orders", "status-index")
    .key_value("pending")
    .exclusive_start_key(json!({"order_id": "ORDER#123"}))
    .execute();
assert!(result.is_err()); // QueryError::IndexKeyRequired

// Missing primary key.
let result = db.query_index("orders", "status-index")
    .key_value("pending")
    .exclusive_start_key(json!({"status": "pending"}))
    .execute();
assert!(result.is_err()); // QueryError::PartitionKeyRequired

// Type mismatch.
let result = db.query_index("orders", "status-index")
    .key_value("pending")
    .exclusive_start_key(json!({"status": "pending", "order_id": 123}))
    .execute();
assert!(result.is_err()); // EncodingError::InvalidKeyType
```

## Trade-offs

### Benefits

- **Feature parity:** Index queries match primary query capabilities
- **Memory efficiency:** Clients can process arbitrarily large result sets incrementally
- **DynamoDB compatibility:** Cursor format and behavior align with GSI pagination
- **Minimal complexity:** Reuses existing pagination infrastructure from primary queries

### Costs

- **Cursor complexity:** The cursor must encode both indexed value and primary key (3+ attributes), making it slightly larger than primary query cursors
- **Documentation burden:** Users need to understand that index cursors differ from primary cursors (more fields)
- **Validation overhead:** Must validate that cursors contain all required key attributes

### Implementation Cost

**Low.** The B+Tree infrastructure already supports start positions. This is primarily wiring:

1. Add field to builder (1 line)
2. Add builder method (5 lines)
3. Encode cursor for scan start (reuse `encode_composite`, ~30 lines)
4. Skip items <= cursor (copy from `QueryBuilder`, ~5 lines)
5. Build `last_evaluated_key` from last item (new helper, ~20 lines)
6. Update protocol + server handlers (~30 lines)
7. Tests (~150 lines)

**Estimated effort:** 4-6 hours for implementation + tests.

## Open Questions

### 1. Cursor Format: Structured JSON vs. Opaque Token?

**Option A: Structured JSON (Proposed)**

```json
{
  "status": "active",
  "user_id": "alice",
  "timestamp": 1672531200
}
```

**Pros:**
- Matches DynamoDB's `LastEvaluatedKey` behavior
- Human-readable for debugging
- Clients can validate cursor completeness
- Stable format across versions

**Cons:**
- Exposes internal key structure
- Slightly verbose for large composite keys

**Option B: Opaque Token (base64-encoded bytes)**

```json
{
  "cursor": "AQIDBAUGBwgJCgsMDQ4PEA=="
}
```

**Pros:**
- Implementation flexibility (can change encoding)
- Compact representation
- Hides internal details

**Cons:**
- Opaque to clients (harder to debug)
- Breaks if encoding changes
- Not DynamoDB-compatible

**Recommendation:** Use **Structured JSON (Option A)** to match DynamoDB and provide better user experience. The slight verbosity is acceptable for debuggability.

### 2. Separate `IndexQueryResult` Type?

**Option A: Reuse `QueryResult` (Proposed)**

```rust
pub struct QueryResult {
    pub items: Vec<Value>,
    pub last_evaluated_key: Option<Value>,
}
```

**Pros:**
- Consistent API (primary queries and index queries return same type)
- Less code duplication
- Cursor format is self-describing (contains index key + primary key)

**Cons:**
- The cursor structure differs between primary and index queries (more attributes in index cursors)

**Option B: New `IndexQueryResult` Type**

```rust
pub struct IndexQueryResult {
    pub items: Vec<Value>,
    pub last_evaluated_key: Option<Value>, // Still a Value, just documented differently
}
```

**Pros:**
- Type system distinguishes index query results
- Could add index-specific metadata later

**Cons:**
- More types to maintain
- Same underlying data (just a cursor with different fields)
- Clients still need to handle both types

**Recommendation:** Use **QueryResult (Option A)** for simplicity. The cursor JSON is self-describing, so the type can remain generic.

### 3. Lazy GC During Pagination?

Currently, index queries silently skip deleted documents (lazy GC). If a page has many deleted entries, the returned page may be smaller than the requested limit.

**Behavior:**

```rust
// 100 index entries for status="pending".
// 50 documents have been deleted (lazy GC not yet run).

let page1 = db.query_index("orders", "status-index")
    .key_value("pending")
    .limit(10)
    .execute()?;

// page1.items.len() might be < 10 if deleted entries are encountered.
// page1.last_evaluated_key will still advance the cursor.
```

**Should we:**

1. **Keep current behavior (proposed):** Return fewer items when deletions are encountered. Matches DynamoDB's behavior with eventual consistency.
2. **Auto-fetch more:** Keep scanning until `limit` visible items are found. More consistent page sizes but higher latency and potential for deep scans.

**Recommendation:** **Keep current behavior (Option 1)**. Clients should be resilient to variable page sizes. Auto-fetching could cause unbounded scans if many deletions exist.

---

## Summary

Adding pagination to index queries is a straightforward extension of existing pagination infrastructure. It uses the same `QueryResult` type, structured JSON cursors, and range scan mechanics as primary queries. The main difference is the cursor must encode both the indexed attribute and the primary key components.

Implementation is minimal (~240 lines including tests) and provides full feature parity with DynamoDB's GSI pagination.
