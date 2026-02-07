//! Filter expression evaluator for Query and Scan operations.
//!
//! Filters are evaluated on deserialized `serde_json::Value` documents after
//! key-based retrieval, eliminating wasteful client-side filtering.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::FilterError;

/// A filter expression that can be evaluated against a JSON document.
///
/// Designed to be serializable for wire protocol transport (JSON / MessagePack).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterExpr {
    // Leaf nodes
    /// Reference to a document attribute. Supports dot-separated nested paths.
    Attr(String),
    /// A literal JSON value.
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

/// Maximum nesting depth for filter/condition expressions.
const MAX_EXPRESSION_DEPTH: usize = 16;

impl FilterExpr {
    /// Evaluate this filter expression against a document.
    ///
    /// Returns `true` if the document passes the filter, `false` otherwise.
    /// Enforces a maximum nesting depth of 16 levels to prevent stack overflow.
    pub fn eval(&self, doc: &Value) -> Result<bool, FilterError> {
        self.eval_inner(doc, 0)
    }

    fn eval_inner(&self, doc: &Value, depth: usize) -> Result<bool, FilterError> {
        if depth > MAX_EXPRESSION_DEPTH {
            return Err(FilterError::InvalidExpression(
                "expression depth exceeds maximum of 16".to_string(),
            ));
        }

        match self {
            FilterExpr::Attr(_) | FilterExpr::Literal(_) => Err(FilterError::InvalidExpression(
                "leaf node cannot be evaluated as a boolean".to_string(),
            )),

            FilterExpr::Eq(left, right) => {
                let l = resolve_expr(left, doc)?;
                let r = resolve_expr(right, doc)?;
                Ok(compare_values(&l, &r) == Some(std::cmp::Ordering::Equal))
            }
            FilterExpr::Ne(left, right) => {
                let l = resolve_expr(left, doc)?;
                let r = resolve_expr(right, doc)?;
                Ok(compare_values(&l, &r) != Some(std::cmp::Ordering::Equal))
            }
            FilterExpr::Lt(left, right) => {
                let l = resolve_expr(left, doc)?;
                let r = resolve_expr(right, doc)?;
                Ok(compare_values(&l, &r) == Some(std::cmp::Ordering::Less))
            }
            FilterExpr::Le(left, right) => {
                let l = resolve_expr(left, doc)?;
                let r = resolve_expr(right, doc)?;
                Ok(matches!(
                    compare_values(&l, &r),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ))
            }
            FilterExpr::Gt(left, right) => {
                let l = resolve_expr(left, doc)?;
                let r = resolve_expr(right, doc)?;
                Ok(compare_values(&l, &r) == Some(std::cmp::Ordering::Greater))
            }
            FilterExpr::Ge(left, right) => {
                let l = resolve_expr(left, doc)?;
                let r = resolve_expr(right, doc)?;
                Ok(matches!(
                    compare_values(&l, &r),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ))
            }
            FilterExpr::Between(val, low, high) => {
                let v = resolve_expr(val, doc)?;
                let lo = resolve_expr(low, doc)?;
                let hi = resolve_expr(high, doc)?;
                let ge_low = matches!(
                    compare_values(&v, &lo),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                );
                let le_high = matches!(
                    compare_values(&v, &hi),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                Ok(ge_low && le_high)
            }

            FilterExpr::BeginsWith(expr, prefix) => {
                let val = resolve_expr(expr, doc)?;
                match val {
                    Value::String(s) => Ok(s.starts_with(prefix.as_str())),
                    Value::Null => Ok(false),
                    _ => Ok(false),
                }
            }
            FilterExpr::Contains(expr, search) => {
                let val = resolve_expr(expr, doc)?;
                match (&val, search) {
                    (Value::String(s), Value::String(needle)) => Ok(s.contains(needle.as_str())),
                    (Value::Array(arr), item) => Ok(arr.contains(item)),
                    (Value::Null, _) => Ok(false),
                    _ => Ok(false),
                }
            }

            FilterExpr::AttributeExists(path) => Ok(!resolve_attr(doc, path).is_null()),
            FilterExpr::AttributeNotExists(path) => Ok(resolve_attr(doc, path).is_null()),

            FilterExpr::And(exprs) => {
                for expr in exprs {
                    if !expr.eval_inner(doc, depth + 1)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            FilterExpr::Or(exprs) => {
                for expr in exprs {
                    if expr.eval_inner(doc, depth + 1)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            FilterExpr::Not(expr) => Ok(!expr.eval_inner(doc, depth + 1)?),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve an expression to a concrete JSON value.
fn resolve_expr(expr: &FilterExpr, doc: &Value) -> Result<Value, FilterError> {
    match expr {
        FilterExpr::Attr(path) => Ok(resolve_attr(doc, path).clone()),
        FilterExpr::Literal(val) => Ok(val.clone()),
        _ => Err(FilterError::InvalidExpression(
            "expected attribute or literal in comparison position".to_string(),
        )),
    }
}

/// Resolve a dot-separated attribute path on a document.
///
/// Returns `Value::Null` if any segment is missing.
pub fn resolve_attr<'a>(doc: &'a Value, path: &str) -> &'a Value {
    let mut current = doc;
    for segment in path.split('.') {
        match current.get(segment) {
            Some(v) => current = v,
            None => return &Value::Null,
        }
    }
    current
}

/// Compare two JSON values, returning an ordering if the types are comparable.
///
/// - Numbers: compared as f64
/// - Strings: compared lexicographically
/// - Booleans: false < true
/// - Null == Null
/// - Mismatched types: returns `None`
pub fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    match (left, right) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Number(a), Value::Number(b)) => {
            let fa = a.as_f64()?;
            let fb = b.as_f64()?;
            fa.partial_cmp(&fb)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

impl FilterExpr {
    /// Create an attribute reference.
    pub fn attr(name: impl Into<String>) -> Self {
        FilterExpr::Attr(name.into())
    }

    /// Create a literal value.
    pub fn literal(val: impl Into<Value>) -> Self {
        FilterExpr::Literal(val.into())
    }

    /// `attr == value`
    pub fn eq(left: FilterExpr, right: FilterExpr) -> Self {
        FilterExpr::Eq(Box::new(left), Box::new(right))
    }

    /// `attr != value`
    pub fn ne(left: FilterExpr, right: FilterExpr) -> Self {
        FilterExpr::Ne(Box::new(left), Box::new(right))
    }

    /// `attr < value`
    pub fn lt(left: FilterExpr, right: FilterExpr) -> Self {
        FilterExpr::Lt(Box::new(left), Box::new(right))
    }

    /// `attr <= value`
    pub fn le(left: FilterExpr, right: FilterExpr) -> Self {
        FilterExpr::Le(Box::new(left), Box::new(right))
    }

    /// `attr > value`
    pub fn gt(left: FilterExpr, right: FilterExpr) -> Self {
        FilterExpr::Gt(Box::new(left), Box::new(right))
    }

    /// `attr >= value`
    pub fn ge(left: FilterExpr, right: FilterExpr) -> Self {
        FilterExpr::Ge(Box::new(left), Box::new(right))
    }

    /// `attr BETWEEN low AND high`
    pub fn between(val: FilterExpr, low: FilterExpr, high: FilterExpr) -> Self {
        FilterExpr::Between(Box::new(val), Box::new(low), Box::new(high))
    }

    /// `begins_with(attr, prefix)`
    pub fn begins_with(expr: FilterExpr, prefix: impl Into<String>) -> Self {
        FilterExpr::BeginsWith(Box::new(expr), prefix.into())
    }

    /// `contains(attr, value)`
    pub fn contains(expr: FilterExpr, search: impl Into<Value>) -> Self {
        FilterExpr::Contains(Box::new(expr), search.into())
    }

    /// `attribute_exists(path)`
    pub fn attribute_exists(path: impl Into<String>) -> Self {
        FilterExpr::AttributeExists(path.into())
    }

    /// `attribute_not_exists(path)`
    pub fn attribute_not_exists(path: impl Into<String>) -> Self {
        FilterExpr::AttributeNotExists(path.into())
    }

    /// `expr1 AND expr2 AND ...`
    pub fn and(exprs: Vec<FilterExpr>) -> Self {
        FilterExpr::And(exprs)
    }

    /// `expr1 OR expr2 OR ...`
    pub fn or(exprs: Vec<FilterExpr>) -> Self {
        FilterExpr::Or(exprs)
    }

    /// `NOT expr`
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: FilterExpr) -> Self {
        FilterExpr::Not(Box::new(expr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_doc() -> Value {
        json!({
            "id": "user123",
            "name": "Alice",
            "age": 30,
            "active": true,
            "score": 95.5,
            "address": {
                "city": "Portland",
                "state": "OR",
                "zip": "97201"
            },
            "tags": ["admin", "user"],
            "metadata": null
        })
    }

    // -----------------------------------------------------------------------
    // Equality
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_eq_string() {
        let doc = sample_doc();
        let f = FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice"));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Bob"));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_eq_number() {
        let doc = sample_doc();
        let f = FilterExpr::eq(FilterExpr::attr("age"), FilterExpr::literal(30));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::eq(FilterExpr::attr("age"), FilterExpr::literal(25));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_eq_boolean() {
        let doc = sample_doc();
        let f = FilterExpr::eq(FilterExpr::attr("active"), FilterExpr::literal(true));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::eq(FilterExpr::attr("active"), FilterExpr::literal(false));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_ne() {
        let doc = sample_doc();
        let f = FilterExpr::ne(FilterExpr::attr("name"), FilterExpr::literal("Bob"));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::ne(FilterExpr::attr("name"), FilterExpr::literal("Alice"));
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Comparisons
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_lt() {
        let doc = sample_doc();
        let f = FilterExpr::lt(FilterExpr::attr("age"), FilterExpr::literal(40));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::lt(FilterExpr::attr("age"), FilterExpr::literal(30));
        assert!(!f.eval(&doc).unwrap());

        let f = FilterExpr::lt(FilterExpr::attr("age"), FilterExpr::literal(20));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_le() {
        let doc = sample_doc();
        let f = FilterExpr::le(FilterExpr::attr("age"), FilterExpr::literal(30));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::le(FilterExpr::attr("age"), FilterExpr::literal(29));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_gt() {
        let doc = sample_doc();
        let f = FilterExpr::gt(FilterExpr::attr("age"), FilterExpr::literal(20));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::gt(FilterExpr::attr("age"), FilterExpr::literal(30));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_ge() {
        let doc = sample_doc();
        let f = FilterExpr::ge(FilterExpr::attr("age"), FilterExpr::literal(30));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::ge(FilterExpr::attr("age"), FilterExpr::literal(31));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_string_comparison() {
        let doc = sample_doc();
        // "Alice" < "Bob" lexicographically
        let f = FilterExpr::lt(FilterExpr::attr("name"), FilterExpr::literal("Bob"));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::gt(FilterExpr::attr("name"), FilterExpr::literal("Bob"));
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Between
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_between() {
        let doc = sample_doc();
        let f = FilterExpr::between(
            FilterExpr::attr("age"),
            FilterExpr::literal(25),
            FilterExpr::literal(35),
        );
        assert!(f.eval(&doc).unwrap());

        // Exactly at bounds
        let f = FilterExpr::between(
            FilterExpr::attr("age"),
            FilterExpr::literal(30),
            FilterExpr::literal(30),
        );
        assert!(f.eval(&doc).unwrap());

        // Out of range
        let f = FilterExpr::between(
            FilterExpr::attr("age"),
            FilterExpr::literal(31),
            FilterExpr::literal(40),
        );
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // BeginsWith
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_begins_with() {
        let doc = sample_doc();
        let f = FilterExpr::begins_with(FilterExpr::attr("name"), "Ali");
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::begins_with(FilterExpr::attr("name"), "Bob");
        assert!(!f.eval(&doc).unwrap());

        // Full match
        let f = FilterExpr::begins_with(FilterExpr::attr("name"), "Alice");
        assert!(f.eval(&doc).unwrap());

        // Non-string attribute returns false
        let f = FilterExpr::begins_with(FilterExpr::attr("age"), "3");
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Contains
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_contains_string() {
        let doc = sample_doc();
        let f = FilterExpr::contains(FilterExpr::attr("name"), "lic");
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::contains(FilterExpr::attr("name"), "xyz");
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_contains_array() {
        let doc = sample_doc();
        let f = FilterExpr::contains(FilterExpr::attr("tags"), "admin");
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::contains(FilterExpr::attr("tags"), "superadmin");
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Existence
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_attribute_exists() {
        let doc = sample_doc();
        let f = FilterExpr::attribute_exists("name");
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::attribute_exists("nonexistent");
        assert!(!f.eval(&doc).unwrap());

        // Null attribute counts as not existing (Null is returned for missing)
        let f = FilterExpr::attribute_exists("metadata");
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_attribute_not_exists() {
        let doc = sample_doc();
        let f = FilterExpr::attribute_not_exists("nonexistent");
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::attribute_not_exists("name");
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Nested attributes
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_nested_attr() {
        let doc = sample_doc();
        let f = FilterExpr::eq(
            FilterExpr::attr("address.city"),
            FilterExpr::literal("Portland"),
        );
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::eq(FilterExpr::attr("address.state"), FilterExpr::literal("OR"));
        assert!(f.eval(&doc).unwrap());

        // Missing nested path
        let f = FilterExpr::eq(
            FilterExpr::attr("address.country"),
            FilterExpr::literal("US"),
        );
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_deeply_nested() {
        let doc = json!({
            "a": { "b": { "c": { "d": 42 } } }
        });
        let f = FilterExpr::eq(FilterExpr::attr("a.b.c.d"), FilterExpr::literal(42));
        assert!(f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Boolean logic
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_and() {
        let doc = sample_doc();
        let f = FilterExpr::and(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice")),
            FilterExpr::gt(FilterExpr::attr("age"), FilterExpr::literal(20)),
        ]);
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::and(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice")),
            FilterExpr::gt(FilterExpr::attr("age"), FilterExpr::literal(40)),
        ]);
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_or() {
        let doc = sample_doc();
        let f = FilterExpr::or(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Bob")),
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice")),
        ]);
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::or(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Bob")),
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Carol")),
        ]);
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_not() {
        let doc = sample_doc();
        let f = FilterExpr::not(FilterExpr::eq(
            FilterExpr::attr("name"),
            FilterExpr::literal("Bob"),
        ));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::not(FilterExpr::eq(
            FilterExpr::attr("name"),
            FilterExpr::literal("Alice"),
        ));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_and_empty() {
        let doc = sample_doc();
        // Empty AND is vacuously true.
        let f = FilterExpr::and(vec![]);
        assert!(f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_or_empty() {
        let doc = sample_doc();
        // Empty OR is vacuously false.
        let f = FilterExpr::or(vec![]);
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Short-circuit evaluation
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_short_circuit_and() {
        let doc = sample_doc();
        // AND should stop evaluating after first false.
        // If And short-circuits, a bad second expression won't be reached.
        let f = FilterExpr::and(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Bob")),
            // This would fail if evaluated (leaf node as boolean).
            FilterExpr::Attr("name".to_string()),
        ]);
        // First expr is false, so short-circuit: result is false, no error.
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_short_circuit_or() {
        let doc = sample_doc();
        // OR should stop evaluating after first true.
        let f = FilterExpr::or(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice")),
            // This would fail if evaluated.
            FilterExpr::Attr("name".to_string()),
        ]);
        // First expr is true, so short-circuit: result is true, no error.
        assert!(f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // Type mismatches and edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_type_mismatch_returns_false() {
        let doc = sample_doc();
        // Comparing string to number: types don't match, returns false (not equal).
        let f = FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal(42));
        assert!(!f.eval(&doc).unwrap());

        // Lt with mismatched types: no ordering, returns false.
        let f = FilterExpr::lt(FilterExpr::attr("name"), FilterExpr::literal(42));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_missing_attribute() {
        let doc = sample_doc();
        // Missing attribute resolves to null.
        let f = FilterExpr::eq(
            FilterExpr::attr("nonexistent"),
            FilterExpr::Literal(Value::Null),
        );
        assert!(f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_null_comparison() {
        let doc = sample_doc();
        let f = FilterExpr::eq(
            FilterExpr::attr("metadata"),
            FilterExpr::Literal(Value::Null),
        );
        assert!(f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_leaf_eval_error() {
        let doc = sample_doc();
        let f = FilterExpr::Attr("name".to_string());
        assert!(f.eval(&doc).is_err());

        let f = FilterExpr::Literal(json!(42));
        assert!(f.eval(&doc).is_err());
    }

    // -----------------------------------------------------------------------
    // Serialization roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_serde_roundtrip_json() {
        let filter = FilterExpr::and(vec![
            FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice")),
            FilterExpr::gt(FilterExpr::attr("age"), FilterExpr::literal(18)),
            FilterExpr::attribute_exists("email"),
        ]);

        let json = serde_json::to_string(&filter).unwrap();
        let deserialized: FilterExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(filter, deserialized);
    }

    #[test]
    fn test_filter_serde_roundtrip_msgpack() {
        let filter = FilterExpr::or(vec![
            FilterExpr::begins_with(FilterExpr::attr("name"), "A"),
            FilterExpr::contains(FilterExpr::attr("tags"), "admin"),
        ]);

        let bytes = rmp_serde::to_vec(&filter).unwrap();
        let deserialized: FilterExpr = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(filter, deserialized);
    }

    // -----------------------------------------------------------------------
    // Complex / combined expressions
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_complex_combined() {
        let doc = sample_doc();
        // (name == "Alice" AND age >= 18) OR active == false
        let f = FilterExpr::or(vec![
            FilterExpr::and(vec![
                FilterExpr::eq(FilterExpr::attr("name"), FilterExpr::literal("Alice")),
                FilterExpr::ge(FilterExpr::attr("age"), FilterExpr::literal(18)),
            ]),
            FilterExpr::eq(FilterExpr::attr("active"), FilterExpr::literal(false)),
        ]);
        assert!(f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_between_strings() {
        let doc = sample_doc();
        // "Alice" between "A" and "B"
        let f = FilterExpr::between(
            FilterExpr::attr("name"),
            FilterExpr::literal("A"),
            FilterExpr::literal("B"),
        );
        assert!(f.eval(&doc).unwrap());

        // "Alice" not between "B" and "C"
        let f = FilterExpr::between(
            FilterExpr::attr("name"),
            FilterExpr::literal("B"),
            FilterExpr::literal("C"),
        );
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_float_comparison() {
        let doc = sample_doc();
        let f = FilterExpr::gt(FilterExpr::attr("score"), FilterExpr::literal(90.0));
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::lt(FilterExpr::attr("score"), FilterExpr::literal(90.0));
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_nested_attribute_exists() {
        let doc = sample_doc();
        let f = FilterExpr::attribute_exists("address.city");
        assert!(f.eval(&doc).unwrap());

        let f = FilterExpr::attribute_exists("address.country");
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_contains_on_missing() {
        let doc = sample_doc();
        let f = FilterExpr::contains(FilterExpr::attr("nonexistent"), "foo");
        assert!(!f.eval(&doc).unwrap());
    }

    #[test]
    fn test_filter_begins_with_on_missing() {
        let doc = sample_doc();
        let f = FilterExpr::begins_with(FilterExpr::attr("nonexistent"), "foo");
        assert!(!f.eval(&doc).unwrap());
    }

    // -----------------------------------------------------------------------
    // resolve_attr
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_attr_top_level() {
        let doc = sample_doc();
        assert_eq!(resolve_attr(&doc, "name"), &json!("Alice"));
        assert_eq!(resolve_attr(&doc, "age"), &json!(30));
    }

    #[test]
    fn test_resolve_attr_nested() {
        let doc = sample_doc();
        assert_eq!(resolve_attr(&doc, "address.city"), &json!("Portland"));
    }

    #[test]
    fn test_resolve_attr_missing() {
        let doc = sample_doc();
        assert_eq!(resolve_attr(&doc, "missing"), &Value::Null);
        assert_eq!(resolve_attr(&doc, "address.missing"), &Value::Null);
        assert_eq!(resolve_attr(&doc, "a.b.c.d"), &Value::Null);
    }

    // -----------------------------------------------------------------------
    // compare_values
    // -----------------------------------------------------------------------

    #[test]
    fn test_compare_values_numbers() {
        assert_eq!(
            compare_values(&json!(1), &json!(2)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_values(&json!(2), &json!(2)),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            compare_values(&json!(3), &json!(2)),
            Some(std::cmp::Ordering::Greater)
        );
    }

    #[test]
    fn test_compare_values_strings() {
        assert_eq!(
            compare_values(&json!("a"), &json!("b")),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_values(&json!("b"), &json!("b")),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_compare_values_mixed_types() {
        assert_eq!(compare_values(&json!("a"), &json!(1)), None);
        assert_eq!(compare_values(&json!(true), &json!(1)), None);
    }

    #[test]
    fn test_compare_values_nulls() {
        assert_eq!(
            compare_values(&Value::Null, &Value::Null),
            Some(std::cmp::Ordering::Equal)
        );
    }
}
