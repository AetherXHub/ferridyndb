use crate::error::EncodingError;

/// Encode arbitrary bytes using the escaped-terminator scheme for lexicographic ordering.
///
/// Same scheme as string encoding:
/// - `0x00` bytes → `0x00 0xFF` (escape)
/// - All other bytes → as-is
/// - Terminator: `0x00 0x00`
pub fn encode_binary(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 2);
    for &b in data {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    // Terminator
    out.push(0x00);
    out.push(0x00);
    out
}

/// Decode binary data from escaped-terminator encoding.
///
/// Returns `(decoded_bytes, bytes_consumed)` where `bytes_consumed` is the
/// number of bytes read from `data` (including the terminator).
pub fn decode_binary(data: &[u8]) -> Result<(Vec<u8>, usize), EncodingError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0x00 {
            if i + 1 >= data.len() {
                return Err(EncodingError::MalformedKey);
            }
            match data[i + 1] {
                0x00 => {
                    // Terminator.
                    let consumed = i + 2;
                    return Ok((out, consumed));
                }
                0xFF => {
                    // Escaped 0x00.
                    out.push(0x00);
                    i += 2;
                }
                _ => {
                    return Err(EncodingError::MalformedKey);
                }
            }
        } else {
            out.push(data[i]);
            i += 1;
        }
    }
    // Reached end without terminator.
    Err(EncodingError::MalformedKey)
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
            &[0x00],
            &[0x00, 0x01, 0x00],
            &[0x42, 0x00, 0xFF, 0x00, 0x00],
        ];
        for data in cases {
            let encoded = encode_binary(data);
            let (decoded, consumed) = decode_binary(&encoded).unwrap();
            assert_eq!(decoded, data, "roundtrip failed for {:?}", data);
            assert_eq!(consumed, encoded.len(), "consumed mismatch for {:?}", data);
        }
    }

    #[test]
    fn test_binary_with_zeros() {
        let data = vec![0x00, 0x00, 0x00];
        let encoded = encode_binary(&data);
        // Each 0x00 becomes 0x00 0xFF, plus terminator 0x00 0x00.
        assert_eq!(
            encoded,
            vec![0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0x00]
        );
        let (decoded, consumed) = decode_binary(&encoded).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_binary_ordering() {
        let items: Vec<&[u8]> = vec![&[1], &[1, 0], &[1, 1], &[2]];
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
}
