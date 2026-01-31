use serde_json::Value;

use crate::encoding::KeyValue;
use crate::error::{EncodingError, Error, SchemaError};
use crate::types::{KeyDefinition, KeyType, MAX_DOCUMENT_SIZE};

/// Convert a [`serde_json::Value`] to a [`KeyValue`], given the expected [`KeyType`].
pub fn json_to_key_value(
    val: &Value,
    key_type: KeyType,
    attr_name: &str,
) -> Result<KeyValue, Error> {
    match key_type {
        KeyType::String => {
            let s = val.as_str().ok_or_else(|| SchemaError::KeyTypeMismatch {
                name: attr_name.to_string(),
                expected: KeyType::String,
                actual: infer_key_type(val),
            })?;
            Ok(KeyValue::String(s.to_string()))
        }
        KeyType::Number => {
            let n = val.as_f64().ok_or_else(|| SchemaError::KeyTypeMismatch {
                name: attr_name.to_string(),
                expected: KeyType::Number,
                actual: infer_key_type(val),
            })?;
            Ok(KeyValue::Number(n))
        }
        KeyType::Binary => {
            let arr = val.as_array().ok_or_else(|| SchemaError::KeyTypeMismatch {
                name: attr_name.to_string(),
                expected: KeyType::Binary,
                actual: infer_key_type(val),
            })?;
            let bytes: Vec<u8> = arr
                .iter()
                .map(|v| {
                    v.as_u64()
                        .map(|n| n as u8)
                        .ok_or_else(|| SchemaError::KeyTypeMismatch {
                            name: attr_name.to_string(),
                            expected: KeyType::Binary,
                            actual: infer_key_type(v),
                        })
                })
                .collect::<Result<_, _>>()?;
            Ok(KeyValue::Binary(bytes))
        }
    }
}

/// Infer a [`KeyType`] from a JSON value (for error messages).
fn infer_key_type(val: &Value) -> KeyType {
    if val.is_string() {
        KeyType::String
    } else if val.is_number() {
        KeyType::Number
    } else {
        KeyType::Binary
    }
}

/// Extract a key value from a JSON document by attribute name.
pub fn extract_key_from_doc(doc: &Value, key_def: &KeyDefinition) -> Result<KeyValue, Error> {
    let val = doc
        .get(&key_def.name)
        .ok_or_else(|| SchemaError::MissingKeyAttribute(key_def.name.clone()))?;
    json_to_key_value(val, key_def.key_type, &key_def.name)
}

/// Validate document size and return the serialized bytes.
pub fn validate_document_size(doc: &Value) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(doc).map_err(|e| {
        crate::error::StorageError::CorruptedPage(format!("JSON serialization error: {e}"))
    })?;
    if bytes.len() > MAX_DOCUMENT_SIZE {
        return Err(EncodingError::DocumentTooLarge {
            max: MAX_DOCUMENT_SIZE,
            actual: bytes.len(),
        }
        .into());
    }
    Ok(bytes)
}

/// Convert a [`KeyValue`] back to a [`serde_json::Value`].
pub fn key_value_to_json(kv: &KeyValue) -> Value {
    match kv {
        KeyValue::String(s) => Value::String(s.clone()),
        KeyValue::Number(n) => serde_json::json!(*n),
        KeyValue::Binary(b) => {
            let arr: Vec<Value> = b.iter().map(|&byte| Value::from(byte)).collect();
            Value::Array(arr)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_to_key_value_string() {
        let val = json!("hello");
        let kv = json_to_key_value(&val, KeyType::String, "pk").unwrap();
        assert_eq!(kv, KeyValue::String("hello".to_string()));
    }

    #[test]
    fn test_json_to_key_value_number() {
        let val = json!(42.5);
        let kv = json_to_key_value(&val, KeyType::Number, "pk").unwrap();
        assert_eq!(kv, KeyValue::Number(42.5));
    }

    #[test]
    fn test_json_to_key_value_binary() {
        let val = json!([1, 2, 3]);
        let kv = json_to_key_value(&val, KeyType::Binary, "pk").unwrap();
        assert_eq!(kv, KeyValue::Binary(vec![1, 2, 3]));
    }

    #[test]
    fn test_json_to_key_value_type_mismatch() {
        let val = json!(42);
        let result = json_to_key_value(&val, KeyType::String, "pk");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_key_from_doc() {
        let doc = json!({"user_id": "alice", "name": "Alice"});
        let key_def = KeyDefinition {
            name: "user_id".to_string(),
            key_type: KeyType::String,
        };
        let kv = extract_key_from_doc(&doc, &key_def).unwrap();
        assert_eq!(kv, KeyValue::String("alice".to_string()));
    }

    #[test]
    fn test_extract_key_missing() {
        let doc = json!({"name": "Alice"});
        let key_def = KeyDefinition {
            name: "user_id".to_string(),
            key_type: KeyType::String,
        };
        let result = extract_key_from_doc(&doc, &key_def);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_document_size_ok() {
        let doc = json!({"key": "value"});
        let result = validate_document_size(&doc);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_document_size_too_large() {
        // Create a document larger than MAX_DOCUMENT_SIZE (400KB).
        let big_string = "x".repeat(MAX_DOCUMENT_SIZE + 1);
        let doc = json!({"data": big_string});
        let result = validate_document_size(&doc);
        assert!(result.is_err());
    }

    #[test]
    fn test_key_value_to_json_roundtrip() {
        let kv_str = KeyValue::String("test".to_string());
        let json_val = key_value_to_json(&kv_str);
        assert_eq!(json_val, json!("test"));

        let kv_num = KeyValue::Number(3.14);
        let json_val = key_value_to_json(&kv_num);
        assert_eq!(json_val, json!(3.14));

        let kv_bin = KeyValue::Binary(vec![1, 2, 3]);
        let json_val = key_value_to_json(&kv_bin);
        assert_eq!(json_val, json!([1, 2, 3]));
    }
}
