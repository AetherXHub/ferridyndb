use crate::error::EncodingError;

/// Encode a string using the escaped-terminator scheme for lexicographic ordering.
///
/// For each byte `b` in the UTF-8 representation:
/// - If `b == 0x00`: emit `0x00 0xFF` (escape)
/// - Otherwise: emit `b` as-is
///
/// After all bytes: emit `0x00 0x00` (terminator).
pub fn encode_string(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() + 2);
    for &b in s.as_bytes() {
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

/// Decode a string from escaped-terminator encoding.
///
/// Returns `(decoded_string, bytes_consumed)` where `bytes_consumed` is the
/// number of bytes read from `data` (including the terminator).
pub fn decode_string(data: &[u8]) -> Result<(String, usize), EncodingError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0x00 {
            // Need at least one more byte after the 0x00.
            if i + 1 >= data.len() {
                return Err(EncodingError::MalformedKey);
            }
            match data[i + 1] {
                0x00 => {
                    // Terminator — end of string.
                    let consumed = i + 2;
                    let s = String::from_utf8(out).map_err(|_| EncodingError::MalformedKey)?;
                    return Ok((s, consumed));
                }
                0xFF => {
                    // Escaped 0x00 byte.
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
    // Reached end of data without finding a terminator.
    Err(EncodingError::MalformedKey)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let cases = vec![
            "",
            "hello",
            "hello world",
            "abc123",
            "\x00",
            "foo\x00bar",
            "\x00\x00\x00",
            "unicode: \u{1F600}",
            "a",
            "longer string with various characters: !@#$%^&*()",
        ];
        for s in cases {
            let encoded = encode_string(s);
            let (decoded, consumed) = decode_string(&encoded).unwrap();
            assert_eq!(decoded, s, "roundtrip failed for {:?}", s);
            assert_eq!(consumed, encoded.len(), "consumed mismatch for {:?}", s);
        }
    }

    #[test]
    fn test_string_ordering() {
        let strings = vec!["a", "aa", "ab", "b"];
        let encoded: Vec<Vec<u8>> = strings.iter().map(|s| encode_string(s)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "expected {:?} < {:?} (encoded {:?} < {:?})",
                strings[i],
                strings[i + 1],
                encoded[i],
                encoded[i + 1],
            );
        }
    }

    #[test]
    fn test_string_with_nul() {
        let s = "hello\x00world";
        let encoded = encode_string(s);
        // The \x00 byte should be escaped as 0x00 0xFF.
        assert!(encoded.contains(&0xFF));
        let (decoded, consumed) = decode_string(&encoded).unwrap();
        assert_eq!(decoded, s);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_empty_string() {
        let encoded = encode_string("");
        assert_eq!(encoded, vec![0x00, 0x00]);
        let (decoded, consumed) = decode_string(&encoded).unwrap();
        assert_eq!(decoded, "");
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_decode_malformed() {
        // Truncated: just a single 0x00 byte with no following byte.
        assert!(decode_string(&[0x00]).is_err());

        // Empty data.
        assert!(decode_string(&[]).is_err());

        // 0x00 followed by unexpected byte (not 0x00 or 0xFF).
        assert!(decode_string(&[0x00, 0x42]).is_err());

        // Data without terminator.
        assert!(decode_string(&[0x41, 0x42]).is_err());
    }
}
