use crate::error::EncodingError;

/// Encode an f64 into 8 bytes that preserve numeric ordering under `memcmp`.
///
/// Algorithm:
/// 1. Reject NaN.
/// 2. Normalize -0.0 to +0.0.
/// 3. Convert to u64 bits via `f64::to_bits`.
/// 4. If the sign bit is set (negative): flip all bits (`!bits`).
/// 5. If the sign bit is clear (positive/zero): flip only the sign bit (`bits ^= 1 << 63`).
/// 6. Write as big-endian u64.
pub fn encode_number(value: f64) -> Result<[u8; 8], EncodingError> {
    if value.is_nan() {
        return Err(EncodingError::NaN);
    }

    // Normalize -0.0 to +0.0.
    let value = if value == 0.0 { 0.0_f64 } else { value };

    let mut bits = value.to_bits();

    if bits & (1u64 << 63) != 0 {
        // Negative: flip all bits.
        bits = !bits;
    } else {
        // Positive or zero: flip the sign bit.
        bits ^= 1u64 << 63;
    }

    Ok(bits.to_be_bytes())
}

/// Decode 8 bytes back into an f64, reversing the encoding transformation.
pub fn decode_number(data: &[u8; 8]) -> f64 {
    let mut bits = u64::from_be_bytes(*data);

    if bits & (1u64 << 63) != 0 {
        // Sign bit is set in encoded form → was positive or zero.
        bits ^= 1u64 << 63;
    } else {
        // Sign bit is clear in encoded form → was negative.
        bits = !bits;
    }

    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let values = vec![
            0.0,
            1.0,
            -1.0,
            0.5,
            -0.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e10,
            -1e10,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::MIN,
            0.0001,
            -0.0001,
            42.0,
            -42.0,
            std::f64::consts::PI,
            std::f64::consts::E,
        ];
        for v in values {
            let encoded = encode_number(v).unwrap();
            let decoded = decode_number(&encoded);
            assert_eq!(v.to_bits(), decoded.to_bits(), "roundtrip failed for {}", v);
        }
    }

    #[test]
    fn test_nan_rejected() {
        assert!(encode_number(f64::NAN).is_err());
    }

    #[test]
    fn test_negative_zero_normalized() {
        let pos = encode_number(0.0).unwrap();
        let neg = encode_number(-0.0).unwrap();
        assert_eq!(pos, neg, "-0.0 should encode the same as +0.0");
    }

    #[test]
    fn test_ordering() {
        let values = vec![
            f64::NEG_INFINITY,
            -1.0,
            -0.0001,
            0.0,
            0.0001,
            1.0,
            f64::INFINITY,
        ];
        let encoded: Vec<[u8; 8]> = values.iter().map(|&v| encode_number(v).unwrap()).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "expected {} < {} (encoded {:?} < {:?})",
                values[i],
                values[i + 1],
                encoded[i],
                encoded[i + 1],
            );
        }
    }

    #[test]
    fn test_negative_ordering() {
        let values = vec![-100.0, -10.0, -1.0, -0.1];
        let encoded: Vec<[u8; 8]> = values.iter().map(|&v| encode_number(v).unwrap()).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "expected {} < {} (encoded {:?} < {:?})",
                values[i],
                values[i + 1],
                encoded[i],
                encoded[i + 1],
            );
        }
    }
}
