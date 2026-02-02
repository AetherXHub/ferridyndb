//! Update actions and path resolution for `update_item`.

use serde_json::Value;

use crate::error::{Error, SchemaError};

/// An individual update action to apply to a document.
pub enum UpdateAction {
    /// Set an attribute at the given dot-separated path to a value.
    /// Creates intermediate objects if they don't exist.
    Set { path: String, value: Value },
    /// Remove an attribute at the given dot-separated path.
    /// Silent no-op if the path doesn't exist.
    Remove { path: String },
    /// Add to a numeric attribute (increment) or an array attribute (set union).
    /// If the attribute doesn't exist, initializes it to the provided value.
    Add { path: String, value: Value },
    /// Remove elements from an array attribute (set difference).
    /// If the resulting array is empty, removes the attribute entirely.
    Delete { path: String, value: Value },
}

impl UpdateAction {
    /// Return the dot-separated path targeted by this action.
    fn path(&self) -> &str {
        match self {
            UpdateAction::Set { path, .. }
            | UpdateAction::Remove { path }
            | UpdateAction::Add { path, .. }
            | UpdateAction::Delete { path, .. } => path,
        }
    }
}

/// Validate that no update action targets a key attribute (partition key or sort key).
///
/// Only checks the top-level segment of each path (e.g. `"pk"` in `"pk.sub"` is
/// rejected, but `"data.pk"` is fine because the top-level segment is `"data"`).
pub fn validate_no_key_updates(
    actions: &[UpdateAction],
    pk_name: &str,
    sk_name: Option<&str>,
) -> Result<(), Error> {
    for action in actions {
        let top_level = action.path().split('.').next().unwrap_or(action.path());
        if top_level == pk_name {
            return Err(SchemaError::KeyAttributeUpdate(pk_name.to_string()).into());
        }
        if let Some(sk) = sk_name
            && top_level == sk
        {
            return Err(SchemaError::KeyAttributeUpdate(sk.to_string()).into());
        }
    }
    Ok(())
}

/// Apply a sequence of update actions to a mutable document.
///
/// Actions are applied in order. Each SET creates intermediate objects as
/// needed; each REMOVE silently skips missing paths; ADD increments numbers
/// or unions arrays; DELETE subtracts elements from arrays.
pub fn apply_updates(doc: &mut Value, actions: &[UpdateAction]) -> Result<(), Error> {
    for action in actions {
        match action {
            UpdateAction::Set { path, value } => {
                let segments: Vec<&str> = path.split('.').collect();
                resolve_path_set(doc, &segments, value.clone());
            }
            UpdateAction::Remove { path } => {
                let segments: Vec<&str> = path.split('.').collect();
                resolve_path_remove(doc, &segments);
            }
            UpdateAction::Add { path, value } => {
                let segments: Vec<&str> = path.split('.').collect();
                apply_add(doc, &segments, value, path)?;
            }
            UpdateAction::Delete { path, value } => {
                let segments: Vec<&str> = path.split('.').collect();
                apply_delete(doc, &segments, value, path)?;
            }
        }
    }
    Ok(())
}

/// Navigate to the target location and set the value, creating intermediate
/// objects for any missing segments.
fn resolve_path_set(doc: &mut Value, segments: &[&str], value: Value) {
    debug_assert!(!segments.is_empty());

    if segments.len() == 1 {
        // Leaf: set the value.
        if let Value::Object(map) = doc {
            map.insert(segments[0].to_string(), value);
        }
        return;
    }

    // Navigate deeper, creating intermediate objects as needed.
    if let Value::Object(map) = doc {
        let entry = map
            .entry(segments[0].to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        resolve_path_set(entry, &segments[1..], value);
    }
}

/// Get a mutable reference to the value at a dot-separated path, returning
/// `None` if any intermediate segment is missing or not an object.
fn resolve_path_get_mut<'a>(doc: &'a mut Value, segments: &[&str]) -> Option<&'a mut Value> {
    let mut current = doc;
    for segment in segments {
        current = current.as_object_mut()?.get_mut(*segment)?;
    }
    Some(current)
}

/// Apply ADD action: numeric increment or array union.
/// If the attribute doesn't exist, initializes it to the provided value.
fn apply_add(doc: &mut Value, segments: &[&str], value: &Value, path: &str) -> Result<(), Error> {
    match resolve_path_get_mut(doc, segments) {
        Some(existing) => {
            // Both must be numbers, or both must be arrays.
            match (existing.as_f64(), value.as_f64()) {
                (Some(a), Some(b)) => {
                    *existing = Value::from(a + b);
                    Ok(())
                }
                _ => match (existing.as_array().is_some(), value.as_array().is_some()) {
                    (true, true) => {
                        let arr = existing.as_array_mut().unwrap();
                        for elem in value.as_array().unwrap() {
                            if !arr.contains(elem) {
                                arr.push(elem.clone());
                            }
                        }
                        Ok(())
                    }
                    _ => Err(SchemaError::UpdateTypeMismatch {
                        attribute: path.to_string(),
                        message: format!(
                            "ADD requires both values to be numbers or both to be arrays, \
                             got existing={}, value={}",
                            type_name(existing),
                            type_name(value)
                        ),
                    }
                    .into()),
                },
            }
        }
        None => {
            // Attribute doesn't exist — initialize with the value.
            // Navigate to parent, create intermediates, set value.
            resolve_path_set(doc, segments, value.clone());
            // Validate the value is a number or array.
            if !value.is_number() && !value.is_array() {
                return Err(SchemaError::UpdateTypeMismatch {
                    attribute: path.to_string(),
                    message: format!(
                        "ADD requires a number or array value, got {}",
                        type_name(value)
                    ),
                }
                .into());
            }
            Ok(())
        }
    }
}

/// Apply DELETE action: array set difference.
/// If the resulting array is empty, removes the attribute.
fn apply_delete(
    doc: &mut Value,
    segments: &[&str],
    value: &Value,
    path: &str,
) -> Result<(), Error> {
    let leaf = *segments.last().unwrap();

    // Value must be an array.
    let to_remove = value
        .as_array()
        .ok_or_else(|| SchemaError::UpdateTypeMismatch {
            attribute: path.to_string(),
            message: format!("DELETE requires an array value, got {}", type_name(value)),
        })?;

    let Some(existing) = resolve_path_get_mut(doc, segments) else {
        // Attribute doesn't exist — silent no-op.
        return Ok(());
    };

    // Check type before taking mutable borrow.
    if !existing.is_array() {
        return Err(SchemaError::UpdateTypeMismatch {
            attribute: path.to_string(),
            message: format!(
                "DELETE requires existing attribute to be an array, got {}",
                type_name(existing)
            ),
        }
        .into());
    }

    let arr = existing.as_array_mut().unwrap();
    arr.retain(|elem| !to_remove.contains(elem));

    // If empty, remove the attribute entirely.
    if arr.is_empty() {
        // Navigate to parent and remove the leaf key.
        let parent_segments = &segments[..segments.len() - 1];
        if parent_segments.is_empty() {
            if let Value::Object(map) = doc {
                map.remove(leaf);
            }
        } else if let Some(Value::Object(map)) = resolve_path_get_mut(doc, parent_segments) {
            map.remove(leaf);
        }
    }

    Ok(())
}

/// Return a human-readable type name for a JSON value.
fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Navigate to the parent of the target location and remove the leaf key.
/// Silent no-op if any intermediate segment is missing or not an object.
fn resolve_path_remove(doc: &mut Value, segments: &[&str]) {
    debug_assert!(!segments.is_empty());

    if segments.len() == 1 {
        // Leaf: remove the key.
        if let Value::Object(map) = doc {
            map.remove(segments[0]);
        }
        return;
    }

    // Navigate deeper. Missing intermediate = silent no-op.
    if let Value::Object(map) = doc
        && let Some(child) = map.get_mut(segments[0])
    {
        resolve_path_remove(child, &segments[1..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_set_top_level() {
        let mut doc = json!({"pk": "a", "name": "Alice"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Set {
                path: "age".to_string(),
                value: json!(30),
            }],
        )
        .unwrap();
        assert_eq!(doc["age"], 30);
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn test_set_overwrite() {
        let mut doc = json!({"pk": "a", "name": "Alice"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Set {
                path: "name".to_string(),
                value: json!("Bob"),
            }],
        )
        .unwrap();
        assert_eq!(doc["name"], "Bob");
    }

    #[test]
    fn test_set_nested() {
        let mut doc = json!({"pk": "a"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Set {
                path: "address.city".to_string(),
                value: json!("NYC"),
            }],
        )
        .unwrap();
        assert_eq!(doc["address"]["city"], "NYC");
    }

    #[test]
    fn test_set_deeply_nested() {
        let mut doc = json!({"pk": "a"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Set {
                path: "a.b.c".to_string(),
                value: json!(42),
            }],
        )
        .unwrap();
        assert_eq!(doc["a"]["b"]["c"], 42);
    }

    #[test]
    fn test_remove_existing() {
        let mut doc = json!({"pk": "a", "name": "Alice", "age": 30});
        apply_updates(
            &mut doc,
            &[UpdateAction::Remove {
                path: "age".to_string(),
            }],
        )
        .unwrap();
        assert!(doc.get("age").is_none());
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut doc = json!({"pk": "a"});
        // Should not error.
        apply_updates(
            &mut doc,
            &[UpdateAction::Remove {
                path: "missing".to_string(),
            }],
        )
        .unwrap();
    }

    #[test]
    fn test_remove_nested() {
        let mut doc = json!({"pk": "a", "address": {"city": "NYC", "zip": "10001"}});
        apply_updates(
            &mut doc,
            &[UpdateAction::Remove {
                path: "address.city".to_string(),
            }],
        )
        .unwrap();
        assert!(doc["address"].get("city").is_none());
        assert_eq!(doc["address"]["zip"], "10001");
    }

    #[test]
    fn test_remove_nested_missing_intermediate() {
        let mut doc = json!({"pk": "a"});
        // address doesn't exist — should silently succeed.
        apply_updates(
            &mut doc,
            &[UpdateAction::Remove {
                path: "address.city".to_string(),
            }],
        )
        .unwrap();
    }

    #[test]
    fn test_multiple_actions_in_order() {
        let mut doc = json!({"pk": "a", "x": 1});
        apply_updates(
            &mut doc,
            &[
                UpdateAction::Set {
                    path: "y".to_string(),
                    value: json!(2),
                },
                UpdateAction::Remove {
                    path: "x".to_string(),
                },
                UpdateAction::Set {
                    path: "z".to_string(),
                    value: json!(3),
                },
            ],
        )
        .unwrap();
        assert!(doc.get("x").is_none());
        assert_eq!(doc["y"], 2);
        assert_eq!(doc["z"], 3);
    }

    #[test]
    fn test_add_number_increment() {
        let mut doc = json!({"pk": "a", "count": 10});
        apply_updates(
            &mut doc,
            &[UpdateAction::Add {
                path: "count".to_string(),
                value: json!(5),
            }],
        )
        .unwrap();
        assert_eq!(doc["count"], 15.0);
    }

    #[test]
    fn test_add_number_init() {
        let mut doc = json!({"pk": "a"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Add {
                path: "count".to_string(),
                value: json!(5),
            }],
        )
        .unwrap();
        assert_eq!(doc["count"], 5);
    }

    #[test]
    fn test_add_array_union() {
        let mut doc = json!({"pk": "a", "tags": ["x", "y"]});
        apply_updates(
            &mut doc,
            &[UpdateAction::Add {
                path: "tags".to_string(),
                value: json!(["y", "z"]),
            }],
        )
        .unwrap();
        let tags = doc["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert!(tags.contains(&json!("x")));
        assert!(tags.contains(&json!("y")));
        assert!(tags.contains(&json!("z")));
    }

    #[test]
    fn test_add_array_init() {
        let mut doc = json!({"pk": "a"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Add {
                path: "tags".to_string(),
                value: json!(["x", "y"]),
            }],
        )
        .unwrap();
        assert_eq!(doc["tags"], json!(["x", "y"]));
    }

    #[test]
    fn test_add_type_error_string_target() {
        let mut doc = json!({"pk": "a", "name": "Alice"});
        let result = apply_updates(
            &mut doc,
            &[UpdateAction::Add {
                path: "name".to_string(),
                value: json!(5),
            }],
        );
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("type mismatch"), "error: {msg}");
    }

    #[test]
    fn test_add_type_error_init_string() {
        let mut doc = json!({"pk": "a"});
        let result = apply_updates(
            &mut doc,
            &[UpdateAction::Add {
                path: "name".to_string(),
                value: json!("hello"),
            }],
        );
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("type mismatch"), "error: {msg}");
    }

    #[test]
    fn test_delete_array_elements() {
        let mut doc = json!({"pk": "a", "tags": ["x", "y", "z"]});
        apply_updates(
            &mut doc,
            &[UpdateAction::Delete {
                path: "tags".to_string(),
                value: json!(["y"]),
            }],
        )
        .unwrap();
        assert_eq!(doc["tags"], json!(["x", "z"]));
    }

    #[test]
    fn test_delete_empty_removes_attribute() {
        let mut doc = json!({"pk": "a", "tags": ["x"]});
        apply_updates(
            &mut doc,
            &[UpdateAction::Delete {
                path: "tags".to_string(),
                value: json!(["x"]),
            }],
        )
        .unwrap();
        assert!(doc.get("tags").is_none());
    }

    #[test]
    fn test_delete_nonexistent_silent() {
        let mut doc = json!({"pk": "a"});
        apply_updates(
            &mut doc,
            &[UpdateAction::Delete {
                path: "tags".to_string(),
                value: json!(["x"]),
            }],
        )
        .unwrap();
    }

    #[test]
    fn test_delete_type_error_number() {
        let mut doc = json!({"pk": "a", "count": 10});
        let result = apply_updates(
            &mut doc,
            &[UpdateAction::Delete {
                path: "count".to_string(),
                value: json!([5]),
            }],
        );
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("type mismatch"), "error: {msg}");
    }

    #[test]
    fn test_delete_value_must_be_array() {
        let mut doc = json!({"pk": "a", "tags": ["x"]});
        let result = apply_updates(
            &mut doc,
            &[UpdateAction::Delete {
                path: "tags".to_string(),
                value: json!("x"),
            }],
        );
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("type mismatch"), "error: {msg}");
    }

    #[test]
    fn test_validate_rejects_pk() {
        let actions = vec![UpdateAction::Set {
            path: "pk".to_string(),
            value: json!("new"),
        }];
        let result = validate_no_key_updates(&actions, "pk", None);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("pk"));
    }

    #[test]
    fn test_validate_rejects_sk() {
        let actions = vec![UpdateAction::Set {
            path: "sk".to_string(),
            value: json!(42),
        }];
        let result = validate_no_key_updates(&actions, "pk", Some("sk"));
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("sk"));
    }

    #[test]
    fn test_validate_allows_nested_with_key_name() {
        // "data.pk" should be fine — only top-level "pk" is rejected.
        let actions = vec![UpdateAction::Set {
            path: "data.pk".to_string(),
            value: json!("fine"),
        }];
        assert!(validate_no_key_updates(&actions, "pk", None).is_ok());
    }

    #[test]
    fn test_validate_allows_non_key() {
        let actions = vec![
            UpdateAction::Set {
                path: "name".to_string(),
                value: json!("Alice"),
            },
            UpdateAction::Remove {
                path: "age".to_string(),
            },
        ];
        assert!(validate_no_key_updates(&actions, "pk", Some("sk")).is_ok());
    }
}
