# Proposal: FilterExpression Support for Query and Scan

**Status:** Draft
**Author:** —
**Date:** 2026-02-02

## Motivation

DynamoDB supports `FilterExpression` on Query and Scan operations, allowing server-side post-evaluation filtering on any attribute. Without this, applications must fetch all results matching the key conditions and filter client-side, wasting bandwidth, memory, and compute cycles.

Consider a table with partition key `user_id` and sort key `timestamp`, storing user activity events. To find all events for a user where `status="active"` and `priority>5`, an application today must:

```rust
let result = db.query("events")
    .partition_key("user#123")
    .execute()?;

// Client-side filtering — every event was serialized, transmitted, deserialized.
let filtered: Vec<_> = result.items.into_iter()
    .filter(|doc| {
        doc.get("status").and_then(|v| v.as_str()) == Some("active")
            && doc.get("priority").and_then(|v| v.as_u64()) > Some(5)
    })
    .collect();
```

This is particularly wasteful on Scan operations across large tables where only a small fraction of items match the filter criteria. Supporting server-side FilterExpression allows the database to discard non-matching items before serialization and transmission, improving both performance and API ergonomics.

## Current State

**Query operations** (`crates/ferridyn-core/src/api/builders.rs`, `QueryBuilder`):
- Support partition key + optional sort key conditions (Eq, Lt, Le, Gt, Ge, Between, BeginsWith)
- Support limit, pagination (`exclusive_start_key`), and scan direction (forward/reverse)
- No post-fetch filtering — all items matching the key conditions are returned

**Scan operations** (`ScanBuilder`):
- Support limit and pagination
- No filtering at all — entire table is scanned

**Query execution** (`crates/ferridyn-core/src/api/query.rs`):
- MVCC range scan returns all visible documents within the key bounds
- TTL expiration check is applied
- Results are serialized to MessagePack internally, deserialized to `serde_json::Value`, and returned

Applications must filter results in Rust code after receiving them from the database. There is no API to express filtering logic to the engine.

## Proposed Design

Add a `FilterExpression` API to both `QueryBuilder` and `ScanBuilder` that evaluates predicates on deserialized documents before adding them to the result set.

### Two Design Options

#### Option A: Closure-based API (Rust-native)

Provide a `.filter()` method accepting a Rust closure:

```rust
use ferridyn_core::FerridynDB;

db.query("events")
    .partition_key("user#123")
    .filter(|doc: &serde_json::Value| {
        doc.get("status").and_then(|v| v.as_str()) == Some("active")
            && doc.get("priority").and_then(|v| v.as_u64()) > Some(5)
    })
    .execute()?;

db.scan("events")
    .filter(|doc| doc.get("archived").and_then(|v| v.as_bool()) == Some(false))
    .execute()?;
```

**Pros:**
- Fully type-safe — leverages Rust's type system and borrow checker
- Zero parsing overhead — closure is compiled native code
- Flexible — supports arbitrary Rust expressions, including external function calls
- Familiar — idiomatic for embedded Rust library use

**Cons:**
- Not expressible over the wire protocol (`ferridyn-server`) — closures cannot be serialized
- Cannot be stored, logged, or inspected as data
- Debugging is harder — no structured representation of the filter logic
- Incompatible with DynamoDB's expression model (users migrating from AWS SDK would find different semantics)

#### Option B: Expression Enum API (DynamoDB-like, serializable)

Define a `FilterExpr` enum representing filter expressions as structured data:

```rust
use ferridyn_core::api::filter::FilterExpr;
use serde_json::json;

db.query("events")
    .partition_key("user#123")
    .filter(FilterExpr::And(vec![
        FilterExpr::Eq(FilterExpr::Attr("status".into()), FilterExpr::Literal(json!("active"))),
        FilterExpr::Gt(FilterExpr::Attr("priority".into()), FilterExpr::Literal(json!(5))),
    ]))
    .execute()?;
```

The `FilterExpr` enum would include:

| Variant | Semantics | Example |
|---------|-----------|---------|
| `Attr(String)` | Attribute reference (supports nested paths like `"address.city"`) | `Attr("status")` |
| `Literal(Value)` | JSON literal value | `Literal(json!("active"))` |
| `Eq(Box<Expr>, Box<Expr>)` | Equality | `Eq(Attr("status"), Literal(json!("active")))` |
| `Ne(Box<Expr>, Box<Expr>)` | Inequality | `Ne(Attr("status"), Literal(json!("deleted")))` |
| `Lt(Box<Expr>, Box<Expr>)` | Less-than | `Lt(Attr("age"), Literal(json!(30)))` |
| `Le(Box<Expr>, Box<Expr>)` | Less-than-or-equal | `Le(Attr("score"), Literal(json!(100)))` |
| `Gt(Box<Expr>, Box<Expr>)` | Greater-than | `Gt(Attr("priority"), Literal(json!(5)))` |
| `Ge(Box<Expr>, Box<Expr>)` | Greater-than-or-equal | `Ge(Attr("count"), Literal(json!(10)))` |
| `Between(Box<Expr>, Box<Expr>, Box<Expr>)` | Inclusive range | `Between(Attr("timestamp"), Literal(json!(1000)), Literal(json!(2000)))` |
| `BeginsWith(Box<Expr>, String)` | String prefix match | `BeginsWith(Attr("name"), "John".into())` |
| `Contains(Box<Expr>, Value)` | String substring or set membership | `Contains(Attr("tags"), json!("urgent"))` |
| `AttributeExists(String)` | Attribute presence check | `AttributeExists("email".into())` |
| `AttributeNotExists(String)` | Attribute absence check | `AttributeNotExists("deleted_at".into())` |
| `And(Vec<Expr>)` | Logical AND | `And(vec![...])` |
| `Or(Vec<Expr>)` | Logical OR | `Or(vec![...])` |
| `Not(Box<Expr>)` | Logical negation | `Not(Box::new(Eq(...)))` |

The enum derives `Serialize` and `Deserialize` (serde) for wire protocol support.

**Pros:**
- **Serializable** — can be sent over the wire protocol (JSON or MessagePack)
- **Inspectable** — expressions are structured data that can be logged, stored, or analyzed
- **DynamoDB-compatible semantics** — matches AWS SDK's expression model conceptually
- **Debuggable** — structured representation aids debugging and error reporting
- **Future-proof** — opens the door to query optimization, indexing hints, or cost estimation

**Cons:**
- **More verbose** — requires constructing enum variants instead of writing closures
- **Requires an evaluator** — more implementation complexity (expression interpreter)
- **Less flexible** — cannot express arbitrary Rust logic without extending the enum

### Recommendation: Option B (Expression Enum)

Option B is more work upfront but necessary for `ferridyn-server` support and better aligns with DynamoDB's model. The expression enum is the correct long-term design.

A closure-based convenience wrapper could be added later as syntactic sugar for embedded library use, internally converting simple closures to expression trees (where possible).

## Implementation Details

### Filter Evaluation Point

Filters are evaluated **after** the MVCC visibility check and document deserialization, but **before** adding the document to the result vector.

**Query execution flow (in `QueryBuilder::execute`):**

```
1. Compute scan bounds from partition key + sort condition
2. mvcc_range_scan → raw (key_bytes, value_bytes) pairs (MVCC-filtered)
3. For each (key, value):
   a. Deserialize value_bytes → serde_json::Value
   b. Check TTL expiration → skip if expired
   c. Evaluate FilterExpression (if present) → skip if false
   d. Add to results vector
4. Apply scan direction (reverse if needed)
5. Apply limit, compute last_evaluated_key
6. Return QueryResult
```

**Scan execution flow (in `ScanBuilder::execute`):**

Similar, but step 1 uses `None, None` for full table scan bounds.

### Limit Semantics (DynamoDB-compatible)

**Critical detail:** DynamoDB's `Limit` parameter specifies the maximum number of items to **evaluate** (read from storage), not the maximum number to **return** after filtering.

This means:
- With `Limit=10` and a filter, you may receive **fewer than 10 results** if some items fail the filter.
- Pagination (`exclusive_start_key`) advances based on the last item evaluated, not the last item returned.

This is subtle but important for correctness. FerridynDB should match this behavior:

```rust
// Pseudocode for limit + filter interaction
let mut evaluated_count = 0;
let mut results = vec![];
for (key, value) in mvcc_range_scan(...) {
    if evaluated_count >= limit { break; }
    evaluated_count += 1;

    let doc = deserialize(value);
    if ttl_expired(doc) { continue; }
    if filter_expr.is_some() && !eval_filter(&filter_expr, &doc) { continue; }

    results.push(doc);
}
```

**Alternative (user-friendly) semantics:** Limit could count **returned** items instead of **evaluated** items. This would be more intuitive but diverge from DynamoDB. Recommend following DynamoDB's model for compatibility, but document this behavior prominently in the API docs.

## Changes Required

### New Module: `crates/ferridyn-core/src/api/filter.rs`

Define the `FilterExpr` enum and evaluation logic:

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    Attr(String),
    Literal(Value),
    Eq(Box<FilterExpr>, Box<FilterExpr>),
    Ne(Box<FilterExpr>, Box<FilterExpr>),
    Lt(Box<FilterExpr>, Box<FilterExpr>),
    Le(Box<FilterExpr>, Box<FilterExpr>),
    Gt(Box<FilterExpr>, Box<FilterExpr>),
    Ge(Box<FilterExpr>, Box<FilterExpr>),
    Between(Box<FilterExpr>, Box<FilterExpr>, Box<FilterExpr>),
    BeginsWith(Box<FilterExpr>, String),
    Contains(Box<FilterExpr>, Value),
    AttributeExists(String),
    AttributeNotExists(String),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    /// Evaluate the filter expression against a document.
    pub fn eval(&self, doc: &Value) -> Result<bool, FilterError> {
        // Recursive expression evaluator implementation
    }
}
```

Helper functions:
- `resolve_attr(doc: &Value, path: &str) -> Option<&Value>` — Navigate nested attribute paths (`"address.city"`)
- `compare_values(op: CompareOp, left: &Value, right: &Value) -> Option<bool>` — Type-aware comparison (numbers, strings, etc.)

### Modify: `crates/ferridyn-core/src/api/builders.rs`

**QueryBuilder:**

```rust
pub struct QueryBuilder<'a> {
    // ... existing fields ...
    filter: Option<FilterExpr>,
}

impl<'a> QueryBuilder<'a> {
    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.filter = Some(expr);
        self
    }
}
```

**ScanBuilder:**

```rust
pub struct ScanBuilder<'a> {
    // ... existing fields ...
    filter: Option<FilterExpr>,
}

impl<'a> ScanBuilder<'a> {
    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.filter = Some(expr);
        self
    }
}
```

### Modify: Query Execution

**In `QueryBuilder::execute` (around line 425-443):**

```rust
for (key_bytes, value_bytes) in raw_results {
    if let Some(ref sk) = skip_key && key_bytes <= *sk {
        continue;
    }

    let val: Value = rmp_serde::from_slice(&value_bytes).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
    })?;

    if is_ttl_expired(&val, schema) {
        continue;
    }

    // NEW: Apply filter expression.
    if let Some(ref filter) = self.filter {
        if !filter.eval(&val)? {
            continue;
        }
    }

    items.push((key_bytes, val));
}
```

**In `ScanBuilder::execute` (around line 526-540):**

Same logic — add filter evaluation after TTL check.

### Modify: `crates/ferridyn-server/src/protocol.rs`

Add `filter` field to Query and Scan request types:

```rust
pub enum Request {
    Query {
        table: String,
        partition_key: Value,
        sort_condition: Option<SortCondition>,
        filter: Option<FilterExpr>,  // NEW
        limit: Option<usize>,
        scan_forward: bool,
        exclusive_start_key: Option<Value>,
    },
    Scan {
        table: String,
        filter: Option<FilterExpr>,  // NEW
        limit: Option<usize>,
        exclusive_start_key: Option<Value>,
    },
    // ...
}
```

### Error Handling

Define `FilterError` in `crates/ferridyn-core/src/error.rs`:

```rust
#[derive(Debug, Clone, thiserror::Error)]
pub enum FilterError {
    #[error("type mismatch in filter expression: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("attribute not found: {0}")]
    AttributeNotFound(String),

    #[error("invalid attribute path: {0}")]
    InvalidPath(String),

    #[error("unsupported comparison between {left} and {right}")]
    UnsupportedComparison { left: String, right: String },
}
```

### Tests

Add comprehensive test coverage in `crates/ferridyn-core/src/api/filter.rs`:

| Test | Verifies |
|------|----------|
| `test_filter_eq` | Equality on string, number, boolean |
| `test_filter_comparison` | Lt, Le, Gt, Ge on numbers |
| `test_filter_between` | Inclusive range check |
| `test_filter_begins_with` | String prefix matching |
| `test_filter_attribute_exists` | Presence/absence checks |
| `test_filter_nested_attr` | Nested path resolution (`"address.city"`) |
| `test_filter_and_or_not` | Boolean logic |
| `test_query_with_filter` | Integration: query + filter |
| `test_scan_with_filter` | Integration: scan + filter |
| `test_filter_with_limit` | Limit counts evaluated items, not returned |
| `test_filter_with_pagination` | Filter + exclusive_start_key |
| `test_filter_type_errors` | Invalid comparisons return errors |

Add wire protocol tests in `crates/ferridyn-server/tests/integration.rs`:

```rust
#[tokio::test]
async fn test_query_with_filter_over_wire() {
    // Send Query request with FilterExpr, verify correct filtering
}
```

## Trade-offs

| Dimension | Closure API (Option A) | Expression Enum (Option B) |
|-----------|------------------------|----------------------------|
| **Type safety** | Fully type-safe at compile time | Runtime type checking in evaluator |
| **Expressiveness** | Arbitrary Rust logic | Limited to predefined operations |
| **Wire protocol support** | None — closures not serializable | Full support via serde JSON/MessagePack |
| **Debuggability** | Opaque closure, hard to inspect | Structured data, easy to log/inspect |
| **Implementation complexity** | Trivial (just call the closure) | Requires expression evaluator (~200 LOC) |
| **DynamoDB compatibility** | Incompatible semantics | Matches DynamoDB's expression model |
| **Performance** | Fastest (native code) | Slightly slower (interpreter overhead) |
| **API ergonomics** | Concise, idiomatic Rust | Verbose enum construction |

### When Closures Are Better

For one-off embedded use cases where the filter logic is highly specific and performance-critical, closures are ideal. Example: a local CLI tool filtering a small database.

### When Expressions Are Better

For any application using `ferridyn-server` (multi-process access) or needing to serialize/store filter logic, expressions are required. This is the expected architecture for most real-world deployments.

## Open Questions

1. **DynamoDB string expression syntax** — Should we support DynamoDB's string-based expression syntax (`"#status = :active AND #priority > :min"`) in addition to the Rust enum API? This would ease migration from AWS SDK but adds a parser. Recommend: defer to v2.

2. **Limit semantics** — Should `limit` count items **evaluated** (DynamoDB-compatible) or items **returned** (user-friendly)? Recommendation: match DynamoDB for compatibility, but document this prominently.

3. **Nested attribute paths** — Should filter expressions support arbitrary nested paths (`"address.city"`, `"tags[0]"`)? Recommendation: support dot-separated paths (`"address.city"`), defer array indexing to v2.

4. **`size()` function** — DynamoDB supports `size(attribute)` to check the length of strings, sets, or lists. Useful for conditions like `"size(name) > 10"`. Recommendation: defer to v2, add as `Size(Box<Expr>)` variant later.

5. **Performance impact** — Filter evaluation adds ~10-50 µs per document depending on expression complexity. For queries returning thousands of items, this could add milliseconds. Is this acceptable? Recommendation: yes — the alternative is client-side filtering which is far more expensive due to serialization/network overhead.

6. **Filter on key attributes** — Should filters be allowed on partition key or sort key attributes? These are already covered by key conditions, but users might write filters like `Eq(Attr("user_id"), Literal("123"))`. Recommendation: allow it (no harm), but document that key conditions are more efficient.

7. **Short-circuit evaluation** — Should `And`/`Or` expressions short-circuit (stop evaluating on first false/true)? Recommendation: yes — standard boolean logic behavior, minor performance win.