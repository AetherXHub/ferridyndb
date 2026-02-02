# PRD: Filter Expressions

**Priority:** 2
**Status:** Approved
**Proposal:** [docs/proposals/accepted/filter-expressions.md](../proposals/accepted/filter-expressions.md)

## Summary

Add server-side `FilterExpression` support to Query and Scan operations. Filters evaluate predicates on deserialized documents before returning them, eliminating wasteful client-side filtering and reducing wire transfer size.

## Scope

### In Scope

- `FilterExpr` enum with comparison, boolean, existence, and string operations
- Serializable via serde (JSON and MessagePack) for wire protocol support
- Expression evaluator that operates on `serde_json::Value` documents
- `.filter()` method on `QueryBuilder`, `ScanBuilder`, and `IndexQueryBuilder`
- DynamoDB-compatible limit semantics (limit counts evaluated items, not returned items)
- Dot-separated nested attribute path resolution (`"address.city"`)
- Short-circuit evaluation for `And`/`Or`
- Server protocol integration (filter field on Query/Scan requests)

### Out of Scope

- DynamoDB string expression syntax (`"#status = :active"`) -- defer to v2
- Array indexing in paths (`items[0]`) -- defer to v2
- `size()` function -- defer to v2
- Closure-based API -- expression enum only (closures not serializable)

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Closure vs expression enum? | Expression enum (Option B) | Required for wire protocol serialization |
| Limit semantics? | Count evaluated items (DynamoDB-compatible) | Compatibility and correct pagination behavior |
| Nested paths? | Dot-separated only | Array indexing deferred |
| Short-circuit evaluation? | Yes | Standard boolean logic, minor perf win |
| Filter on key attributes? | Allow | No harm; document that key conditions are more efficient |

## Data Structures

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    // Leaf nodes
    Attr(String),
    Literal(Value),

    // Comparisons
    Eq(Box<FilterExpr>, Box<FilterExpr>),
    Ne(Box<FilterExpr>, Box<FilterExpr>),
    Lt(Box<FilterExpr>, Box<FilterExpr>),
    Le(Box<FilterExpr>, Box<FilterExpr>),
    Gt(Box<FilterExpr>, Box<FilterExpr>),
    Ge(Box<FilterExpr>, Box<FilterExpr>),
    Between(Box<FilterExpr>, Box<FilterExpr>, Box<FilterExpr>),

    // String operations
    BeginsWith(Box<FilterExpr>, String),
    Contains(Box<FilterExpr>, Value),

    // Existence checks
    AttributeExists(String),
    AttributeNotExists(String),

    // Boolean logic
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
}
```

## Implementation Phases

### Phase 1: Core Expression Evaluator

**Deliverables:**
- `FilterExpr` enum in new `ferridyn-core/src/api/filter.rs`
- `FilterExpr::eval(&self, doc: &Value) -> Result<bool, FilterError>`
- `resolve_attr(doc, path)` helper for nested path navigation
- `compare_values(left, right)` helper for type-aware comparison
- `FilterError` enum in `error.rs`

**Tests:**
- `test_filter_eq` (string, number, boolean)
- `test_filter_comparison` (Lt, Le, Gt, Ge)
- `test_filter_between`
- `test_filter_begins_with`
- `test_filter_contains`
- `test_filter_attribute_exists` / `attribute_not_exists`
- `test_filter_nested_attr`
- `test_filter_and_or_not`
- `test_filter_short_circuit`
- `test_filter_type_errors`

### Phase 2: Query and Scan Integration

**Deliverables:**
- Add `filter: Option<FilterExpr>` field to `QueryBuilder`, `ScanBuilder`, `IndexQueryBuilder`
- Add `.filter()` method to each builder
- Evaluate filter after TTL check, before adding to results
- Correct limit semantics (count evaluated, not returned)
- Update `last_evaluated_key` to reflect last evaluated item (not last returned)

**Tests:**
- `test_query_with_filter`
- `test_scan_with_filter`
- `test_index_query_with_filter`
- `test_filter_with_limit` (verify DynamoDB-compatible limit behavior)
- `test_filter_with_pagination` (filter + exclusive_start_key)
- `test_filter_no_matches` (empty result with pagination cursor)

### Phase 3: Server Protocol Integration

**Deliverables:**
- Add `filter: Option<FilterExpr>` to Query and Scan request types in `protocol.rs`
- Server handler passes filter through to core API
- `FerridynClient` methods accept optional filter parameter
- Console `QUERY ... WHERE` and `SCAN ... WHERE` syntax (stretch)

**Tests:**
- `test_query_with_filter_over_wire`
- `test_scan_with_filter_over_wire`

## Acceptance Criteria

1. All filter expression unit tests pass
2. Query/Scan with filter return correct subsets
3. Limit counts evaluated items, not returned items (DynamoDB-compatible)
4. Filter expressions serialize/deserialize correctly over wire protocol
5. `cargo clippy --workspace -- -D warnings` clean
6. No performance regression on queries without filters (Option::None check is O(1))

## Dependencies

- None (standalone feature)

## Dependents

- PRD #3 (Condition Expressions) -- reuses `FilterExpr` as `ConditionExpr`
