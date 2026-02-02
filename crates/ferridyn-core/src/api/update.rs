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
}

impl UpdateAction {
    /// Return the dot-separated path targeted by this action.
    fn path(&self) -> &str {
        match self {
            UpdateAction::Set { path, .. } | UpdateAction::Remove { path } => path,
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
/// needed; each REMOVE silently skips missing paths.
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
