use crate::error::EncodingError;
use crate::types::KeyType;

use super::KeyValue;

/// Type tag constants for composite key encoding.
pub const TAG_STRING: u8 = 0x01;
pub const TAG_NUMBER: u8 = 0x02;
pub const TAG_BINARY: u8 = 0x03;

/// Map a `KeyType` to its type tag byte.
pub fn key_type_tag(kt: KeyType) -> u8 {
    match kt {
        KeyType::String => TAG_STRING,
        KeyType::Number => TAG_NUMBER,
        KeyType::Binary => TAG_BINARY,
    }
}

/// Map a type tag byte back to a `KeyType`.
pub fn tag_to_key_type(tag: u8) -> Result<KeyType, EncodingError> {
    match tag {
        TAG_STRING => Ok(KeyType::String),
        TAG_NUMBER => Ok(KeyType::Number),
        TAG_BINARY => Ok(KeyType::Binary),
        _ => Err(EncodingError::InvalidTypeTag(tag)),
    }
}

/// Encode a single `KeyValue`, returning its encoded bytes (without a type tag prefix).
fn encode_key_value(kv: &KeyValue) -> Result<Vec<u8>, EncodingError> {
    match kv {
        KeyValue::String(s) => Ok(super::string::encode_string(s)),
        KeyValue::Number(n) => Ok(super::number::encode_number(*n)?.to_vec()),
        KeyValue::Binary(b) => Ok(super::binary::encode_binary(b)),
    }
}

/// Return the type tag for a `KeyValue`.
pub(crate) fn key_value_tag(kv: &KeyValue) -> u8 {
    match kv {
        KeyValue::String(_) => TAG_STRING,
        KeyValue::Number(_) => TAG_NUMBER,
        KeyValue::Binary(_) => TAG_BINARY,
    }
}

/// Decode a single key value starting at `data[offset]`, given its type tag.
///
/// Returns `(decoded_value, bytes_consumed)` where bytes_consumed does NOT include
/// the tag byte (the caller is expected to have already consumed the tag).
pub(crate) fn decode_key_value(tag: u8, data: &[u8]) -> Result<(KeyValue, usize), EncodingError> {
    match tag {
        TAG_STRING => {
            let (s, consumed) = super::string::decode_string(data)?;
            Ok((KeyValue::String(s), consumed))
        }
        TAG_NUMBER => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedKey);
            }
            let arr: [u8; 8] = data[..8]
                .try_into()
                .map_err(|_| EncodingError::MalformedKey)?;
            let n = super::number::decode_number(&arr);
            Ok((KeyValue::Number(n), 8))
        }
        TAG_BINARY => {
            let (b, consumed) = super::binary::decode_binary(data)?;
            Ok((KeyValue::Binary(b), consumed))
        }
        _ => Err(EncodingError::InvalidTypeTag(tag)),
    }
}

/// Encode a composite key (partition + optional sort).
///
/// Format:
/// - `[type_tag][encoded_partition_key]` (partition only)
/// - `[type_tag][encoded_partition_key][type_tag][encoded_sort_key]` (partition + sort)
pub fn encode_composite(
    partition: &KeyValue,
    sort: Option<&KeyValue>,
) -> Result<Vec<u8>, EncodingError> {
    let mut out = Vec::new();

    // Partition key.
    out.push(key_value_tag(partition));
    out.extend(encode_key_value(partition)?);

    // Sort key (optional).
    if let Some(sk) = sort {
        out.push(key_value_tag(sk));
        out.extend(encode_key_value(sk)?);
    }

    Ok(out)
}

/// Decode a composite key.
///
/// `has_sort_key` indicates whether a sort key follows the partition key.
pub fn decode_composite(
    data: &[u8],
    has_sort_key: bool,
) -> Result<(KeyValue, Option<KeyValue>), EncodingError> {
    if data.is_empty() {
        return Err(EncodingError::MalformedKey);
    }

    // Partition key.
    let pk_tag = data[0];
    let (pk, pk_consumed) = decode_key_value(pk_tag, &data[1..])?;
    let mut offset = 1 + pk_consumed;

    // Sort key (optional).
    let sk = if has_sort_key {
        if offset >= data.len() {
            return Err(EncodingError::MalformedKey);
        }
        let sk_tag = data[offset];
        offset += 1;
        let (sk_val, _sk_consumed) = decode_key_value(sk_tag, &data[offset..])?;
        Some(sk_val)
    } else {
        None
    };

    Ok((pk, sk))
}

/// Encode just a partition key prefix (for range scan start bounds).
///
/// This is the same as `encode_composite` with no sort key.
pub fn encode_partition_prefix(partition: &KeyValue) -> Result<Vec<u8>, EncodingError> {
    encode_composite(partition, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_string_only() {
        let pk = KeyValue::String("users".to_string());
        let encoded = encode_composite(&pk, None).unwrap();
        let (decoded_pk, decoded_sk) = decode_composite(&encoded, false).unwrap();
        assert_eq!(decoded_pk, pk);
        assert!(decoded_sk.is_none());
    }

    #[test]
    fn test_composite_string_number() {
        let pk = KeyValue::String("users".to_string());
        let sk = KeyValue::Number(42.0);
        let encoded = encode_composite(&pk, Some(&sk)).unwrap();
        let (decoded_pk, decoded_sk) = decode_composite(&encoded, true).unwrap();
        assert_eq!(decoded_pk, pk);
        assert_eq!(decoded_sk.unwrap(), sk);
    }

    #[test]
    fn test_composite_roundtrip_all_types() {
        // String partition key.
        let pk_str = KeyValue::String("table1".to_string());
        let enc = encode_composite(&pk_str, None).unwrap();
        let (dec, _) = decode_composite(&enc, false).unwrap();
        assert_eq!(dec, pk_str);

        // Number partition key.
        let pk_num = KeyValue::Number(99.5);
        let enc = encode_composite(&pk_num, None).unwrap();
        let (dec, _) = decode_composite(&enc, false).unwrap();
        assert_eq!(dec, pk_num);

        // Binary partition key.
        let pk_bin = KeyValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let enc = encode_composite(&pk_bin, None).unwrap();
        let (dec, _) = decode_composite(&enc, false).unwrap();
        assert_eq!(dec, pk_bin);

        // All types with sort keys.
        let sort_values = vec![
            KeyValue::String("sort_val".to_string()),
            KeyValue::Number(-3.14),
            KeyValue::Binary(vec![0x01, 0x02]),
        ];
        let partition_values = vec![
            KeyValue::String("pk".to_string()),
            KeyValue::Number(1.0),
            KeyValue::Binary(vec![0xFF]),
        ];
        for pk in &partition_values {
            for sk in &sort_values {
                let enc = encode_composite(pk, Some(sk)).unwrap();
                let (dec_pk, dec_sk) = decode_composite(&enc, true).unwrap();
                assert_eq!(&dec_pk, pk);
                assert_eq!(dec_sk.as_ref().unwrap(), sk);
            }
        }
    }

    #[test]
    fn test_composite_ordering() {
        // Same partition, different sort keys should group together and order by sort key.
        let pk = KeyValue::String("users".to_string());

        let sk1 = KeyValue::Number(1.0);
        let sk2 = KeyValue::Number(2.0);
        let sk3 = KeyValue::Number(3.0);

        let enc1 = encode_composite(&pk, Some(&sk1)).unwrap();
        let enc2 = encode_composite(&pk, Some(&sk2)).unwrap();
        let enc3 = encode_composite(&pk, Some(&sk3)).unwrap();

        assert!(enc1 < enc2, "sort key 1.0 should come before 2.0");
        assert!(enc2 < enc3, "sort key 2.0 should come before 3.0");

        // Different partition keys should sort by partition.
        let pk_a = KeyValue::String("aaa".to_string());
        let pk_b = KeyValue::String("bbb".to_string());
        let sk = KeyValue::Number(1.0);

        let enc_a = encode_composite(&pk_a, Some(&sk)).unwrap();
        let enc_b = encode_composite(&pk_b, Some(&sk)).unwrap();
        assert!(
            enc_a < enc_b,
            "partition 'aaa' should come before partition 'bbb'"
        );

        // Keys with same partition group together regardless of sort key.
        let pk_a_sk_high = encode_composite(&pk_a, Some(&KeyValue::Number(999.0))).unwrap();
        let pk_b_sk_low = encode_composite(&pk_b, Some(&KeyValue::Number(-999.0))).unwrap();
        assert!(
            pk_a_sk_high < pk_b_sk_low,
            "partition 'aaa' with high sort should still come before partition 'bbb'"
        );
    }

    #[test]
    fn test_partition_prefix() {
        let pk = KeyValue::String("users".to_string());
        let sk = KeyValue::Number(42.0);

        let prefix = encode_partition_prefix(&pk).unwrap();
        let full = encode_composite(&pk, Some(&sk)).unwrap();

        // The prefix should be a proper prefix of the full composite key.
        assert!(full.starts_with(&prefix));
        assert!(full.len() > prefix.len());
    }

    #[test]
    fn test_type_tag_roundtrip() {
        let types = vec![KeyType::String, KeyType::Number, KeyType::Binary];
        for kt in types {
            let tag = key_type_tag(kt);
            let decoded = tag_to_key_type(tag).unwrap();
            assert_eq!(decoded, kt);
        }
    }

    #[test]
    fn test_invalid_tag() {
        assert!(tag_to_key_type(0x00).is_err());
        assert!(tag_to_key_type(0x04).is_err());
        assert!(tag_to_key_type(0xFF).is_err());
    }
}
