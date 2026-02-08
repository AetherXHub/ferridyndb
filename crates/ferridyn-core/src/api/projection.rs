//! Projection expressions: return only selected attributes from documents.
//!
//! Projection is applied post-deserialization — the full document is always
//! read from storage, then trimmed before returning to the caller. Key
//! attributes (partition key, sort key) are always included.

use serde_json::{Map, Value};

use super::filter::resolve_attr;

/// Apply a projection to a document, returning only the specified paths
/// plus key attributes. If `paths` is empty, returns the full document.
pub fn apply_projection(doc: &Value, paths: &[String], key_attrs: &[&str]) -> Value {
    if paths.is_empty() {
        return doc.clone();
    }

    let mut result = Map::new();

    // Always include key attributes.
    if let Some(obj) = doc.as_object() {
        for &key in key_attrs {
            if let Some(v) = obj.get(key) {
                result.insert(key.to_string(), v.clone());
            }
        }
    }

    // Include each projected path.
    for path in paths {
        let val = resolve_attr(doc, path);
        if !val.is_null() {
            set_nested_path(&mut result, path, val.clone());
        }
    }

    Value::Object(result)
}

/// Set a value at a dot-separated path, creating intermediate objects as needed.
fn set_nested_path(target: &mut Map<String, Value>, path: &str, value: Value) {
    let segments: Vec<&str> = path.split('.').collect();

    if segments.len() == 1 {
        target.insert(segments[0].to_string(), value);
        return;
    }

    // Navigate/create intermediate objects.
    let mut current = target;
    for &seg in &segments[..segments.len() - 1] {
        current = current
            .entry(seg)
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("path conflict: intermediate segment is not an object");
    }
    current.insert(segments.last().unwrap().to_string(), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_empty_projection_returns_full_doc() {
        let doc = json!({"pk": "a", "name": "Alice", "age": 30});
        let result = apply_projection(&doc, &[], &["pk"]);
        assert_eq!(result, doc);
    }

    #[test]
    fn test_top_level_projection() {
        let doc = json!({"pk": "a", "name": "Alice", "age": 30, "email": "a@b.com"});
        let result = apply_projection(&doc, &["name".to_string(), "age".to_string()], &["pk"]);
        assert_eq!(result, json!({"pk": "a", "name": "Alice", "age": 30}));
    }

    #[test]
    fn test_nested_projection() {
        let doc = json!({"pk": "a", "address": {"city": "NYC", "zip": "10001"}, "name": "Alice"});
        let result = apply_projection(&doc, &["address.city".to_string()], &["pk"]);
        assert_eq!(result, json!({"pk": "a", "address": {"city": "NYC"}}));
    }

    #[test]
    fn test_keys_always_included() {
        let doc = json!({"pk": "a", "sk": "b", "name": "Alice", "age": 30});
        let result = apply_projection(&doc, &["name".to_string()], &["pk", "sk"]);
        assert_eq!(result, json!({"pk": "a", "sk": "b", "name": "Alice"}));
    }

    #[test]
    fn test_missing_attr_silently_omitted() {
        let doc = json!({"pk": "a", "name": "Alice"});
        let result = apply_projection(&doc, &["nonexistent".to_string()], &["pk"]);
        assert_eq!(result, json!({"pk": "a"}));
    }

    #[test]
    fn test_multiple_nested_paths() {
        let doc = json!({
            "pk": "a",
            "address": {"city": "NYC", "zip": "10001", "state": "NY"},
            "contact": {"email": "a@b.com", "phone": "555-1234"}
        });
        let result = apply_projection(
            &doc,
            &["address.city".to_string(), "contact.email".to_string()],
            &["pk"],
        );
        assert_eq!(
            result,
            json!({"pk": "a", "address": {"city": "NYC"}, "contact": {"email": "a@b.com"}})
        );
    }

    #[test]
    fn test_key_attr_not_duplicated_when_projected() {
        let doc = json!({"pk": "a", "name": "Alice"});
        let result = apply_projection(&doc, &["pk".to_string(), "name".to_string()], &["pk"]);
        // pk should appear once, not duplicated
        assert_eq!(result, json!({"pk": "a", "name": "Alice"}));
    }
}
