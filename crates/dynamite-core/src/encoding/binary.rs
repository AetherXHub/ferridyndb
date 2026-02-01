use crate::error::EncodingError;

/// Encode arbitrary bytes using null-termination for lexicographic ordering.
///
/// The input must not contain any `0x00` bytes. The encoded form is the raw
/// bytes followed by a single `0x00` terminator.
pub fn encode_binary(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 1);
    out.extend_from_slice(data);
    out.push(0x00); // terminator
    out
}

/// Validate that binary data contains no null bytes before encoding as a key.
///
/// Call this before `encode_binary` when encoding user-supplied key values.
pub fn validate_binary_key(data: &[u8]) -> Result<(), EncodingError> {
    if data.contains(&0x00) {
        return Err(EncodingError::NullByteInKey);
    }
    Ok(())
}

/// Decode null-terminated binary data.
///
/// Returns `(decoded_bytes, bytes_consumed)` where `bytes_consumed` includes
/// the terminator byte.
pub fn decode_binary(data: &[u8]) -> Result<(Vec<u8>, usize), EncodingError> {
    let pos = data
        .iter()
        .position(|&b| b == 0x00)
        .ok_or(EncodingError::MalformedKey)?;
    Ok((data[..pos].to_vec(), pos + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let cases: Vec<&[u8]> = vec![
            &[],
            &[0x01],
            &[0x01, 0x02, 0x03],
            &[0xFF, 0xFE, 0xFD],
            &[0x42, 0xFF, 0x01],
        ];
        for data in cases {
            let encoded = encode_binary(data);
            let (decoded, consumed) = decode_binary(&encoded).unwrap();
            assert_eq!(decoded, data, "roundtrip failed for {:?}", data);
            assert_eq!(consumed, encoded.len(), "consumed mismatch for {:?}", data);
        }
    }

    #[test]
    fn test_validate_rejects_null_bytes() {
        assert!(validate_binary_key(&[0x00]).is_err());
        assert!(validate_binary_key(&[0x00, 0x01, 0x00]).is_err());
        assert!(validate_binary_key(&[0x42, 0x00, 0xFF]).is_err());
    }

    #[test]
    fn test_validate_accepts_normal_data() {
        assert!(validate_binary_key(&[]).is_ok());
        assert!(validate_binary_key(&[0x01, 0x02, 0x03]).is_ok());
        assert!(validate_binary_key(&[0xFF, 0xFE, 0xFD]).is_ok());
    }

    #[test]
    fn test_binary_ordering() {
        let items: Vec<&[u8]> = vec![&[1], &[1, 1], &[2]];
        let encoded: Vec<Vec<u8>> = items.iter().map(|d| encode_binary(d)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "expected {:?} < {:?} (encoded {:?} < {:?})",
                items[i],
                items[i + 1],
                encoded[i],
                encoded[i + 1],
            );
        }
    }

    #[test]
    fn test_empty_binary() {
        let encoded = encode_binary(&[]);
        assert_eq!(encoded, vec![0x00]);
        let (decoded, consumed) = decode_binary(&encoded).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_encoded_format() {
        let encoded = encode_binary(&[0xDE, 0xAD]);
        assert_eq!(encoded, vec![0xDE, 0xAD, 0x00]);
    }
}
