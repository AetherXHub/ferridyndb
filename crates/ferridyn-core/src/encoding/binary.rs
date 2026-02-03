use crate::error::EncodingError;

/// Encode arbitrary bytes using escaped-terminator scheme for lexicographic ordering.
///
/// Every `0x00` byte in the input is escaped as `0x00 0x01`. The encoded form
/// ends with `0x00 0x00` as the terminator. This preserves byte-level sort order
/// and allows null bytes in the input data.
pub fn encode_binary(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 2);
    for &b in data {
        if b == 0x00 {
            out.push(0x00);
            out.push(0x01); // escaped null
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00); // terminator
    out
}

/// Decode escaped-terminator binary data.
///
/// Returns `(decoded_bytes, bytes_consumed)` where `bytes_consumed` includes
/// the two-byte terminator.
pub fn decode_binary(data: &[u8]) -> Result<(Vec<u8>, usize), EncodingError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0x00 {
            if i + 1 >= data.len() {
                return Err(EncodingError::MalformedKey);
            }
            match data[i + 1] {
                0x00 => return Ok((out, i + 2)), // terminator
                0x01 => {
                    out.push(0x00); // escaped null
                    i += 2;
                }
                _ => return Err(EncodingError::MalformedKey),
            }
        } else {
            out.push(data[i]);
            i += 1;
        }
    }
    Err(EncodingError::MalformedKey) // no terminator found
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
            &[0x00],
            &[0x00, 0x00],
            &[0x00, 0x01],
            &[0x00, 0x01, 0x00],
            &[0x42, 0x00, 0xFF],
        ];
        for data in cases {
            let encoded = encode_binary(data);
            let (decoded, consumed) = decode_binary(&encoded).unwrap();
            assert_eq!(decoded, data, "roundtrip failed for {:?}", data);
            assert_eq!(consumed, encoded.len(), "consumed mismatch for {:?}", data);
        }
    }

    #[test]
    fn test_binary_ordering() {
        let items: Vec<&[u8]> = vec![
            &[],
            &[0x00],
            &[0x00, 0x00],
            &[0x00, 0x01],
            &[0x01],
            &[0x01, 0x00],
            &[0x01, 0x01],
            &[0x02],
            &[0xFF],
        ];
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
        assert_eq!(encoded, vec![0x00, 0x00]);
        let (decoded, consumed) = decode_binary(&encoded).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_encoded_format() {
        let encoded = encode_binary(&[0xDE, 0xAD]);
        assert_eq!(encoded, vec![0xDE, 0xAD, 0x00, 0x00]);
    }

    #[test]
    fn test_encoded_format_with_nulls() {
        let encoded = encode_binary(&[0x00]);
        assert_eq!(encoded, vec![0x00, 0x01, 0x00, 0x00]);

        let encoded = encode_binary(&[0x00, 0x00]);
        assert_eq!(encoded, vec![0x00, 0x01, 0x00, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn test_decode_malformed() {
        // Empty data — no terminator.
        assert!(decode_binary(&[]).is_err());

        // Single null — incomplete terminator.
        assert!(decode_binary(&[0x00]).is_err());

        // Data without terminator.
        assert!(decode_binary(&[0x41, 0x42]).is_err());

        // Invalid escape sequence (0x00 followed by 0x02).
        assert!(decode_binary(&[0x00, 0x02, 0x00, 0x00]).is_err());
    }
}
