use serde_json::Value;

use crate::encoding::KeyValue;
use crate::encoding::{binary, string};
use crate::error::{EncodingError, Error, SchemaError};
use crate::types::{
    IndexDefinition, KeyDefinition, KeyType, MAX_DOCUMENT_SIZE, MAX_PARTITION_KEY_SIZE,
    MAX_SORT_KEY_SIZE, TableSchema,
};

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
            string::validate_string_key(s)?;
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

/// Validate document size and return the serialized bytes (MessagePack).
pub fn validate_document_size(doc: &Value) -> Result<Vec<u8>, Error> {
    let bytes = rmp_serde::to_vec(doc).map_err(|e| {
        crate::error::StorageError::CorruptedPage(format!("MessagePack serialization error: {e}"))
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

/// Size in bytes of a key value (for limit checks).
fn key_value_byte_size(kv: &KeyValue) -> usize {
    match kv {
        KeyValue::String(s) => s.len(),
        KeyValue::Number(_) => 8,
        KeyValue::Binary(b) => b.len(),
    }
}

/// Validate that a partition key does not exceed the DynamoDB limit (2048 bytes).
pub fn validate_partition_key_size(kv: &KeyValue) -> Result<(), Error> {
    let size = key_value_byte_size(kv);
    if size > MAX_PARTITION_KEY_SIZE {
        return Err(EncodingError::KeyTooLarge {
            max: MAX_PARTITION_KEY_SIZE,
            actual: size,
        }
        .into());
    }
    Ok(())
}

/// Validate that a sort key does not exceed the DynamoDB limit (1024 bytes).
pub fn validate_sort_key_size(kv: &KeyValue) -> Result<(), Error> {
    let size = key_value_byte_size(kv);
    if size > MAX_SORT_KEY_SIZE {
        return Err(EncodingError::KeyTooLarge {
            max: MAX_SORT_KEY_SIZE,
            actual: size,
        }
        .into());
    }
    Ok(())
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

/// Extract the prefix from a partition key by splitting on `#`.
///
/// Returns `Some(prefix)` for String keys, `None` for Number/Binary keys.
/// - `KeyValue::String("CONTACT#toby")` → `Some("CONTACT")`
/// - `KeyValue::String("simple")` → `Some("simple")` (no delimiter = entire key)
/// - `KeyValue::Number(_)` → `None`
/// - `KeyValue::Binary(_)` → `None`
pub fn extract_pk_prefix(pk: &KeyValue) -> Option<String> {
    match pk {
        KeyValue::String(s) => {
            if let Some(idx) = s.find('#') {
                Some(s[..idx].to_string())
            } else {
                Some(s.clone())
            }
        }
        _ => None,
    }
}

/// Build the B+Tree key for a secondary index entry.
///
/// The index key encodes: `(indexed_attribute_value, primary_composite_key_as_Binary)`.
/// This ensures uniqueness and allows recovering the primary key from index scan results.
///
/// Returns `Ok(None)` if the document doesn't have the indexed attribute or the
/// attribute's type doesn't match the index's declared `KeyType` (silently skipped).
pub fn build_index_key(
    index: &IndexDefinition,
    doc: &Value,
    table_schema: &TableSchema,
) -> Result<Option<Vec<u8>>, Error> {
    use crate::encoding::{binary, composite};

    // Extract the indexed attribute value from the document.
    let index_attr_val = match doc.get(&index.index_key.name) {
        Some(val) => val,
        None => return Ok(None), // Document lacks indexed attribute — skip.
    };

    // Try to convert to KeyValue; type mismatch → skip silently.
    let index_kv = match json_to_key_value(
        index_attr_val,
        index.index_key.key_type,
        &index.index_key.name,
    ) {
        Ok(kv) => kv,
        Err(_) => return Ok(None), // Type mismatch — skip.
    };

    // Build the primary composite key bytes.
    let pk = extract_key_from_doc(doc, &table_schema.partition_key)?;
    let sk = match &table_schema.sort_key {
        Some(sk_def) => Some(extract_key_from_doc(doc, sk_def)?),
        None => None,
    };
    let primary_key_bytes = composite::encode_composite(&pk, sk.as_ref())?;

    // Build the index key manually: indexed_value + TAG_BINARY + primary_key_bytes
    // The primary key bytes are the LAST component, so we don't need a terminator.
    let mut index_key = Vec::new();

    // Part 1: encode the indexed attribute value
    index_key.push(match &index_kv {
        KeyValue::String(_) => composite::TAG_STRING,
        KeyValue::Number(_) => composite::TAG_NUMBER,
        KeyValue::Binary(_) => composite::TAG_BINARY,
    });
    index_key.extend(match &index_kv {
        KeyValue::String(s) => crate::encoding::string::encode_string(s),
        KeyValue::Number(n) => crate::encoding::number::encode_number(*n)?.to_vec(),
        KeyValue::Binary(b) => binary::encode_binary(b),
    });

    // Part 2: append the TAG_BINARY and raw primary key bytes (no terminator needed - it's the last field)
    index_key.push(composite::TAG_BINARY);
    index_key.extend(&primary_key_bytes);

    Ok(Some(index_key))
}

/// Decode the primary composite key bytes from an index entry's B+Tree key.
///
/// The index key is: indexed_value + TAG_BINARY + primary_key_bytes.
/// This extracts the primary key bytes (everything after the TAG_BINARY).
pub fn decode_primary_key_from_index_entry(index_key: &[u8]) -> Result<Vec<u8>, Error> {
    use crate::encoding::composite;

    if index_key.is_empty() {
        return Err(crate::error::EncodingError::MalformedKey.into());
    }

    // Decode the first component (indexed value)
    let first_tag = index_key[0];
    let first_type = composite::tag_to_key_type(first_tag)?;

    // Calculate how many bytes the first component consumed
    let bytes_consumed = match first_type {
        crate::types::KeyType::String => {
            // Find the null terminator
            let pos = index_key[1..]
                .iter()
                .position(|&b| b == 0x00)
                .ok_or(crate::error::EncodingError::MalformedKey)?;
            1 + pos + 1 // tag + string bytes + terminator
        }
        crate::types::KeyType::Number => {
            1 + 8 // tag + 8 bytes for f64
        }
        crate::types::KeyType::Binary => {
            let (_decoded, consumed) = binary::decode_binary(&index_key[1..])?;
            1 + consumed // tag + escaped binary with terminator
        }
    };

    // The rest should be: TAG_BINARY + primary_key_bytes
    if bytes_consumed >= index_key.len() {
        return Err(crate::error::EncodingError::MalformedKey.into());
    }

    let remaining = &index_key[bytes_consumed..];
    if remaining.is_empty() || remaining[0] != composite::TAG_BINARY {
        return Err(crate::error::StorageError::CorruptedPage(
            "index entry missing TAG_BINARY for primary key component".to_string(),
        )
        .into());
    }

    // Everything after the TAG_BINARY is the primary key bytes
    Ok(remaining[1..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{IndexDefinition, TableSchema};
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

    #[test]
    fn test_extract_pk_prefix_with_hash() {
        let pk = KeyValue::String("CONTACT#toby".to_string());
        assert_eq!(extract_pk_prefix(&pk), Some("CONTACT".to_string()));
    }

    #[test]
    fn test_extract_pk_prefix_no_hash() {
        let pk = KeyValue::String("simple_key".to_string());
        assert_eq!(extract_pk_prefix(&pk), Some("simple_key".to_string()));
    }

    #[test]
    fn test_extract_pk_prefix_multiple_hashes() {
        let pk = KeyValue::String("A#B#C".to_string());
        assert_eq!(extract_pk_prefix(&pk), Some("A".to_string()));
    }

    #[test]
    fn test_extract_pk_prefix_number() {
        let pk = KeyValue::Number(42.0);
        assert_eq!(extract_pk_prefix(&pk), None);
    }

    #[test]
    fn test_extract_pk_prefix_binary() {
        let pk = KeyValue::Binary(vec![1, 2, 3]);
        assert_eq!(extract_pk_prefix(&pk), None);
    }

    #[test]
    fn test_build_index_key_roundtrip() {
        let table_schema = TableSchema {
            name: "data".to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: Some(KeyDefinition {
                name: "sk".to_string(),
                key_type: KeyType::Number,
            }),
            ttl_attribute: None,
        };
        let index = IndexDefinition {
            name: "email-index".to_string(),
            partition_schema: "CONTACT".to_string(),
            index_key: KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            root_page: 0,
        };
        let doc = json!({
            "pk": "CONTACT#alice",
            "sk": 42.0,
            "email": "alice@example.com",
            "name": "Alice"
        });

        let index_key = build_index_key(&index, &doc, &table_schema).unwrap();
        assert!(index_key.is_some());

        let index_key = index_key.unwrap();
        let primary_bytes = decode_primary_key_from_index_entry(&index_key).unwrap();

        // Verify the primary key bytes match what encode_composite would produce
        use crate::encoding::composite;
        let expected_pk_bytes = composite::encode_composite(
            &KeyValue::String("CONTACT#alice".to_string()),
            Some(&KeyValue::Number(42.0)),
        )
        .unwrap();
        assert_eq!(primary_bytes, expected_pk_bytes);
    }

    #[test]
    fn test_build_index_key_missing_attribute() {
        let table_schema = TableSchema {
            name: "data".to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: None,
            ttl_attribute: None,
        };
        let index = IndexDefinition {
            name: "email-index".to_string(),
            partition_schema: "CONTACT".to_string(),
            index_key: KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            root_page: 0,
        };
        let doc = json!({"pk": "CONTACT#alice", "name": "Alice"});

        let result = build_index_key(&index, &doc, &table_schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_build_index_key_type_mismatch() {
        let table_schema = TableSchema {
            name: "data".to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: None,
            ttl_attribute: None,
        };
        let index = IndexDefinition {
            name: "email-index".to_string(),
            partition_schema: "CONTACT".to_string(),
            index_key: KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            root_page: 0,
        };
        // email is a number, but index expects String
        let doc = json!({"pk": "CONTACT#alice", "email": 42});

        let result = build_index_key(&index, &doc, &table_schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_build_index_key_no_sort_key() {
        let table_schema = TableSchema {
            name: "data".to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: None,
            ttl_attribute: None,
        };
        let index = IndexDefinition {
            name: "email-index".to_string(),
            partition_schema: "CONTACT".to_string(),
            index_key: KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            root_page: 0,
        };
        let doc = json!({"pk": "CONTACT#alice", "email": "alice@example.com"});

        let index_key = build_index_key(&index, &doc, &table_schema).unwrap();
        assert!(index_key.is_some());

        let primary_bytes = decode_primary_key_from_index_entry(&index_key.unwrap()).unwrap();
        use crate::encoding::composite;
        let expected =
            composite::encode_composite(&KeyValue::String("CONTACT#alice".to_string()), None)
                .unwrap();
        assert_eq!(primary_bytes, expected);
    }
}
