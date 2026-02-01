use crate::error::EncodingError;

/// Encode a string using null-termination for lexicographic ordering.
///
/// The input must not contain any `0x00` bytes. The encoded form is simply
/// the raw UTF-8 bytes followed by a single `0x00` terminator.
pub fn encode_string(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() + 1);
    out.extend_from_slice(s.as_bytes());
    out.push(0x00); // terminator
    out
}

/// Validate that a string contains no null bytes before encoding as a key.
///
/// Call this before `encode_string` when encoding user-supplied key values.
pub fn validate_string_key(s: &str) -> Result<(), EncodingError> {
    if s.as_bytes().contains(&0x00) {
        return Err(EncodingError::NullByteInKey);
    }
    Ok(())
}

/// Decode a null-terminated string.
///
/// Returns `(decoded_string, bytes_consumed)` where `bytes_consumed` includes
/// the terminator byte.
pub fn decode_string(data: &[u8]) -> Result<(String, usize), EncodingError> {
    let pos = data
        .iter()
        .position(|&b| b == 0x00)
        .ok_or(EncodingError::MalformedKey)?;
    let s = String::from_utf8(data[..pos].to_vec()).map_err(|_| EncodingError::MalformedKey)?;
    Ok((s, pos + 1))
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
    fn test_validate_rejects_null_bytes() {
        assert!(validate_string_key("hello\x00world").is_err());
        assert!(validate_string_key("\x00").is_err());
        assert!(validate_string_key("\x00\x00\x00").is_err());
    }

    #[test]
    fn test_validate_accepts_normal_strings() {
        assert!(validate_string_key("").is_ok());
        assert!(validate_string_key("hello").is_ok());
        assert!(validate_string_key("unicode: \u{1F600}").is_ok());
    }

    #[test]
    fn test_empty_string() {
        let encoded = encode_string("");
        assert_eq!(encoded, vec![0x00]);
        let (decoded, consumed) = decode_string(&encoded).unwrap();
        assert_eq!(decoded, "");
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_decode_malformed() {
        // Empty data — no terminator.
        assert!(decode_string(&[]).is_err());

        // Data without terminator.
        assert!(decode_string(&[0x41, 0x42]).is_err());
    }

    #[test]
    fn test_encoded_format() {
        // "hello" → [h, e, l, l, o, 0x00]
        let encoded = encode_string("hello");
        assert_eq!(encoded, vec![b'h', b'e', b'l', b'l', b'o', 0x00]);
    }
}
