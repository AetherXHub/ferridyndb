# Proposal: ProjectionExpression Support for FerridynDB

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB's `ProjectionExpression` allows clients to specify which attributes to return, reducing payload size when only a subset of a document is needed. Without this feature, FerridynDB always returns full documents — wasteful when:

- Listing names from documents with large nested data (e.g., user profiles with embedded photos/metadata)
- Displaying summary views where only 2-3 attributes are needed from documents with dozens of fields
- Fetching primary keys for reference resolution without transferring entire documents
- Reducing wire transfer size for `ferridyn-server` clients

This matters most for Query and Scan operations that return many documents.

## Current State

- `GetItem`, `Query`, and `Scan` always return complete `serde_json::Value` documents.
- No way to request a subset of attributes.
- Builders are in `crates/ferridyn-core/src/api/builders.rs`.
- Documents are deserialized from MessagePack into full `serde_json::Value` before being returned.
- The full document is always transferred over the wire in `ferridyn-server`.

## Proposed Design

### API

Add a `.projection()` method to the existing query builders:

```rust
// GetItem
db.get_item("users")
    .partition_key("user#123")
    .projection(&["name", "email", "address.city"])
    .execute()?;

// Query
db.query("users")
    .partition_key("user#123")
    .projection(&["name", "status", "last_login"])
    .execute()?;

// Scan
db.scan("users")
    .projection(&["pk", "sk", "name"])
    .execute()?;

// IndexQuery
db.query_index("users", "email-index")
    .partition_key("alice@example.com")
    .projection(&["pk", "name"])
    .execute()?;
```

### Projection Semantics

1. **Post-read filtering:** Projection is applied AFTER the document is read and deserialized. We cannot avoid reading the full document from disk because MessagePack does not support random attribute access. The optimization is purely on the returned payload size.

2. **Dot notation for nested attributes:** Support DynamoDB-style paths like `"address.city"`:
   ```rust
   // Original document
   {
       "pk": "user#123",
       "name": "Alice",
       "address": {
           "city": "Portland",
           "state": "OR",
           "zip": "97201"
       }
   }

   // Projection: ["name", "address.city"]
   // Result
   {
       "pk": "user#123",  // key always included
       "name": "Alice",
       "address": {
           "city": "Portland"
       }
   }
   ```
   The nesting structure is preserved to maintain attribute paths.

3. **Key attributes always included:** The partition key and sort key (if present) are ALWAYS included in the result, regardless of whether they appear in the projection. This matches DynamoDB behavior and ensures returned documents are always identifiable.

4. **Missing attributes are silently omitted:** If a projected attribute does not exist in the document, it is omitted from the result without error. This matches DynamoDB behavior.

5. **Empty projection returns full document:** If no projection is specified (or an empty slice is passed), the full document is returned. This is backward-compatible with existing code.

### Implementation Approach

Create a projection utility function in `crates/ferridyn-core/src/api/`:

```rust
/// Apply projection to a document value.
///
/// Always includes key attributes (partition key, sort key).
/// Supports dot notation for nested paths (e.g., "address.city").
/// Silently omits attributes that don't exist.
pub(crate) fn apply_projection(
    doc: &serde_json::Value,
    projection: &[&str],
    key_attributes: &[&str],
) -> serde_json::Value {
    // Implementation:
    // 1. If projection is empty, return doc.clone()
    // 2. Create new Map
    // 3. For each key in key_attributes, copy to result
    // 4. For each path in projection:
    //    a. Split on '.' to get path components
    //    b. Navigate through doc following the path
    //    c. If found, insert into result preserving nesting
    // 5. Return result as Value::Object
}
```

Update the builders:

```rust
// In GetItemBuilder, QueryBuilder, ScanBuilder, IndexQueryBuilder:

pub fn projection(mut self, attrs: &[&str]) -> Self {
    self.projection = Some(attrs.iter().map(|s| s.to_string()).collect());
    self
}
```

Apply projection in result construction:

```rust
// In query execution path (simplified example):
let doc = deserialize_document(&bytes)?;
let result_doc = if let Some(ref proj) = builder.projection {
    apply_projection(&doc, proj, &key_attrs)
} else {
    doc
};
```

## Changes Required

### `crates/ferridyn-core/src/api/mod.rs`

Add `projection.rs` module with `apply_projection()` function and supporting logic for path traversal.

### `crates/ferridyn-core/src/api/builders.rs`

- Add `projection: Option<Vec<String>>` field to:
  - `GetItemBuilder`
  - `QueryBuilder`
  - `ScanBuilder`
  - `IndexQueryBuilder`
- Add `.projection(&[&str])` method to each builder.

### `crates/ferridyn-core/src/api/mod.rs` (execution paths)

- Modify `GetItem` execution to apply projection before returning.
- Modify `Query` execution to apply projection to each document in results.
- Modify `Scan` execution to apply projection to each document in results.
- Modify `IndexQuery` execution to apply projection to each document in results.
- Key attributes must be determined from the table schema (partition key name, sort key name if present).

### `crates/ferridyn-server/src/protocol.rs`

- Add `projection: Option<Vec<String>>` field to:
  - `GetItemRequest`
  - `QueryRequest`
  - `ScanRequest`
  - `IndexQueryRequest`
- Pass through to core API when executing requests.

### `crates/ferridyn-server/src/client.rs`

- Add `.projection(&[&str])` methods to client builder APIs to populate the request fields.

### Tests

Add tests in `crates/ferridyn-core/tests/`:

- Basic projection: request 2-3 top-level attributes from a document with 10 attributes.
- Nested paths: request `"address.city"`, verify structure is preserved.
- Key attributes always included: project `["name"]` on a document, verify `pk` and `sk` appear in result.
- Missing attributes: project `["name", "nonexistent"]`, verify no error and `nonexistent` is omitted.
- Empty projection: verify full document is returned.
- Multiple documents: Query/Scan with projection, verify each document is projected.

## Trade-offs

| Dimension | No Projection (current) | Projection (proposed) |
|---|---|---|
| **Disk I/O** | Full document read | Same — full document read |
| **Deserialization cost** | Full document deserialized | Same — full document deserialized |
| **Memory after return** | Caller receives full document | Caller receives filtered document |
| **Wire transfer (embedded)** | Full document copied | Filtered document copied (modest savings) |
| **Wire transfer (server)** | Full document sent over socket | Filtered document sent (meaningful savings) |
| **API complexity** | Simple | Slightly more complex |
| **Code maintenance** | No projection logic | Projection logic + path traversal |

### When Projection Matters Most

- **ferridyn-server clients:** Wire transfer savings compound over many documents. A Query returning 100 documents where each document is 10 KB but only 500 bytes are needed = 950 KB saved per query.
- **Memory-constrained environments:** Reducing in-memory document size after return.
- **API ergonomics:** Clients don't need to manually filter documents after receiving them.

### When Projection Matters Less

- **Embedded use case (ferridyn-core):** The caller already has the full document in memory. The savings are modest unless the caller is immediately discarding most fields.
- **Small documents:** Overhead of projection logic may exceed savings.
- **All attributes needed:** No benefit if the projection would include everything anyway.

### Limitations

- Projection does **not** reduce disk I/O or deserialization cost. The full document is still read from storage and deserialized into `serde_json::Value`. The optimization is purely post-deserialization.
- For true I/O savings, we would need column-oriented storage or sparse document encoding, which is out of scope for v1.

## Migration Path

This is a backward-compatible addition:

1. Add the feature with `projection` fields defaulting to `None` (no projection).
2. Existing code continues to work unchanged — full documents are returned by default.
3. Clients opt in by calling `.projection(&[...])` on builders.

No breaking changes to existing APIs.

## Open Questions

1. **Array indexing:** Should projection support array indexing (e.g., `"items[0].name"`)? DynamoDB **does not** support this in `ProjectionExpression`. Recommendation: **No.** Arrays are uncommon in key-value workloads, and the added complexity is not justified.

2. **Missing attribute behavior:** Should we return an error if a projected attribute doesn't exist, or silently omit it? DynamoDB silently omits. Recommendation: **Silently omit** to match DynamoDB and avoid spurious errors on heterogeneous documents.

3. **Interaction with filter expressions:** If both projection and filter expressions exist, which applies first? DynamoDB applies **filter first, then projection**. Recommendation: Match DynamoDB order. (Note: FilterExpression is not yet implemented, so this is forward-looking.)

4. **Projection on transactional reads (`transact_get_items`):** Should projection be supported in transaction read operations? DynamoDB supports this. Recommendation: **Yes**, add projection to `TransactGetItemsBuilder` for consistency.

5. **Performance cost:** Path traversal and map construction have CPU cost. Should there be a limit on projection list size (e.g., max 20 attributes) to prevent abuse? Recommendation: **Start without limits.** The cost is proportional to projection size, which is naturally bounded by the user's intent.

## Future Enhancements

- **Partial deserialization:** If ferridyn-core adopts a custom binary format (not MessagePack), projection could be pushed down to the storage layer to skip deserializing unused fields. Out of scope for v1.
- **Wildcards:** Support `"address.*"` to include all fields under a nested object. DynamoDB does not support this, so it's non-standard.
