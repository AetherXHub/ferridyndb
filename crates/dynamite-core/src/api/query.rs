use serde_json::Value;

use crate::encoding::{KeyValue, composite};
use crate::error::Error;
use crate::types::KeyType;

use super::key_utils::json_to_key_value;

/// Sort key condition for queries.
#[derive(Debug, Clone)]
pub enum SortCondition {
    Eq(Value),
    Lt(Value),
    Le(Value),
    Gt(Value),
    Ge(Value),
    Between(Value, Value),
    BeginsWith(String),
}

/// Result of a query or scan operation.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub items: Vec<Value>,
    pub last_evaluated_key: Option<Value>,
}

/// Increment a byte string lexicographically.
///
/// Finds the rightmost byte < 0xFF, increments it, and truncates everything
/// after it. Returns `None` if all bytes are 0xFF (cannot increment).
fn increment_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut result = bytes.to_vec();
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            result.truncate(i + 1);
            return Some(result);
        }
    }
    None
}

/// Compute the partition end bound (exclusive) by incrementing the encoded
/// partition prefix.
fn partition_end(partition: &KeyValue) -> Result<Option<Vec<u8>>, Error> {
    let prefix = composite::encode_partition_prefix(partition)?;
    Ok(increment_bytes(&prefix))
}

/// A pair of optional byte-vector bounds for a range scan.
pub type ScanBounds = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Compute the (start_key, end_key) for a B+Tree range scan given partition +
/// sort condition.
///
/// Both bounds are byte vectors for the composite-encoded keys.
/// start_key is inclusive (>=), end_key is exclusive (<).
pub fn compute_scan_bounds(
    partition: &KeyValue,
    sort_condition: Option<&SortCondition>,
    sort_key_type: Option<KeyType>,
) -> Result<ScanBounds, Error> {
    let partition_prefix = composite::encode_partition_prefix(partition)?;
    let part_end = partition_end(partition)?;

    match sort_condition {
        None => {
            // No sort condition: scan the entire partition.
            Ok((Some(partition_prefix), part_end))
        }
        Some(cond) => {
            let sk_type = sort_key_type.unwrap_or(KeyType::String);
            match cond {
                SortCondition::Eq(v) => {
                    let sk = json_to_key_value(v, sk_type, "sort_key")?;
                    let start = composite::encode_composite(partition, Some(&sk))?;
                    let mut end = start.clone();
                    end.push(0xFF);
                    Ok((Some(start), Some(end)))
                }
                SortCondition::Lt(v) => {
                    let sk = json_to_key_value(v, sk_type, "sort_key")?;
                    let end = composite::encode_composite(partition, Some(&sk))?;
                    Ok((Some(partition_prefix), Some(end)))
                }
                SortCondition::Le(v) => {
                    let sk = json_to_key_value(v, sk_type, "sort_key")?;
                    let mut end = composite::encode_composite(partition, Some(&sk))?;
                    end.push(0xFF);
                    Ok((Some(partition_prefix), Some(end)))
                }
                SortCondition::Gt(v) => {
                    let sk = json_to_key_value(v, sk_type, "sort_key")?;
                    let mut start = composite::encode_composite(partition, Some(&sk))?;
                    start.push(0xFF);
                    Ok((Some(start), part_end))
                }
                SortCondition::Ge(v) => {
                    let sk = json_to_key_value(v, sk_type, "sort_key")?;
                    let start = composite::encode_composite(partition, Some(&sk))?;
                    Ok((Some(start), part_end))
                }
                SortCondition::Between(lo, hi) => {
                    let sk_lo = json_to_key_value(lo, sk_type, "sort_key")?;
                    let sk_hi = json_to_key_value(hi, sk_type, "sort_key")?;
                    let start = composite::encode_composite(partition, Some(&sk_lo))?;
                    let mut end = composite::encode_composite(partition, Some(&sk_hi))?;
                    end.push(0xFF);
                    Ok((Some(start), Some(end)))
                }
                SortCondition::BeginsWith(prefix) => {
                    let sk_start = KeyValue::String(prefix.clone());
                    let start = composite::encode_composite(partition, Some(&sk_start))?;

                    // Increment the prefix string to get the exclusive upper bound.
                    let end = if let Some(incremented) = increment_string_prefix(prefix) {
                        let sk_end = KeyValue::String(incremented);
                        Some(composite::encode_composite(partition, Some(&sk_end))?)
                    } else {
                        // All 0xFF bytes -- fall back to partition end.
                        part_end
                    };

                    Ok((Some(start), end))
                }
            }
        }
    }
}

/// Increment the last character of a string prefix for BeginsWith upper bound.
///
/// This works at the UTF-8 byte level: finds the last byte < 0xFF,
/// increments it, and truncates.
fn increment_string_prefix(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let incremented = increment_bytes(bytes)?;
    // The incremented bytes may not be valid UTF-8 in general, but for typical
    // ASCII prefixes this will work. For safety, fall back to raw bytes mode.
    String::from_utf8(incremented).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_increment_bytes() {
        assert_eq!(increment_bytes(&[0x01, 0x02]), Some(vec![0x01, 0x03]));
        assert_eq!(increment_bytes(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(increment_bytes(&[0xFF, 0xFF]), None);
        assert_eq!(increment_bytes(&[0x00]), Some(vec![0x01]));
    }

    #[test]
    fn test_compute_scan_bounds_no_sort() {
        let pk = KeyValue::String("users".to_string());
        let (start, end) = compute_scan_bounds(&pk, None, None).unwrap();
        assert!(start.is_some());
        assert!(end.is_some());
        // Start should be the partition prefix.
        let prefix = composite::encode_partition_prefix(&pk).unwrap();
        assert_eq!(start.unwrap(), prefix);
    }

    #[test]
    fn test_compute_scan_bounds_eq() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::Eq(json!(42.0));
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::Number)).unwrap();

        let sk = KeyValue::Number(42.0);
        let expected_start = composite::encode_composite(&pk, Some(&sk)).unwrap();
        let mut expected_end = expected_start.clone();
        expected_end.push(0xFF);

        assert_eq!(start.unwrap(), expected_start);
        assert_eq!(end.unwrap(), expected_end);
    }

    #[test]
    fn test_compute_scan_bounds_lt() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::Lt(json!(100.0));
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::Number)).unwrap();

        let prefix = composite::encode_partition_prefix(&pk).unwrap();
        let sk = KeyValue::Number(100.0);
        let expected_end = composite::encode_composite(&pk, Some(&sk)).unwrap();

        assert_eq!(start.unwrap(), prefix);
        assert_eq!(end.unwrap(), expected_end);
    }

    #[test]
    fn test_compute_scan_bounds_between() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::Between(json!(10.0), json!(20.0));
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::Number)).unwrap();

        let sk_lo = KeyValue::Number(10.0);
        let sk_hi = KeyValue::Number(20.0);
        let expected_start = composite::encode_composite(&pk, Some(&sk_lo)).unwrap();
        let mut expected_end = composite::encode_composite(&pk, Some(&sk_hi)).unwrap();
        expected_end.push(0xFF);

        assert_eq!(start.unwrap(), expected_start);
        assert_eq!(end.unwrap(), expected_end);
    }

    #[test]
    fn test_compute_scan_bounds_begins_with() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::BeginsWith("abc".to_string());
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::String)).unwrap();

        assert!(start.is_some());
        assert!(end.is_some());

        // Start should encode (pk, "abc") and end should encode (pk, "abd").
        let sk_start = KeyValue::String("abc".to_string());
        let expected_start = composite::encode_composite(&pk, Some(&sk_start)).unwrap();
        assert_eq!(start.unwrap(), expected_start);
    }

    #[test]
    fn test_compute_scan_bounds_ge() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::Ge(json!(50.0));
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::Number)).unwrap();

        let sk = KeyValue::Number(50.0);
        let expected_start = composite::encode_composite(&pk, Some(&sk)).unwrap();
        assert_eq!(start.unwrap(), expected_start);
        // End should be partition end.
        assert!(end.is_some());
    }

    #[test]
    fn test_compute_scan_bounds_gt() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::Gt(json!(50.0));
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::Number)).unwrap();

        let sk = KeyValue::Number(50.0);
        let mut expected_start = composite::encode_composite(&pk, Some(&sk)).unwrap();
        expected_start.push(0xFF);
        assert_eq!(start.unwrap(), expected_start);
        assert!(end.is_some());
    }

    #[test]
    fn test_compute_scan_bounds_le() {
        let pk = KeyValue::String("users".to_string());
        let cond = SortCondition::Le(json!(50.0));
        let (start, end) = compute_scan_bounds(&pk, Some(&cond), Some(KeyType::Number)).unwrap();

        let prefix = composite::encode_partition_prefix(&pk).unwrap();
        assert_eq!(start.unwrap(), prefix);

        let sk = KeyValue::Number(50.0);
        let mut expected_end = composite::encode_composite(&pk, Some(&sk)).unwrap();
        expected_end.push(0xFF);
        assert_eq!(end.unwrap(), expected_end);
    }
}
