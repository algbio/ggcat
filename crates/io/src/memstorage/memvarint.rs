const MAX_BYTES: usize = 7;
const FLAGS_OFFSET: usize = 4;

#[inline(always)]
pub fn compute_memvarint_bytes_count(value: u64) -> usize {
    debug_assert!(value <= ((1 << (MAX_BYTES * 8)) - 1));
    (8 - value.leading_zeros() / 8) as usize
}

#[inline(always)]
pub fn encode_memvarint_flags_with_size<const WITH_FLAGS: bool>(
    bytes_count: usize,
    value: u64,
    flags: u8,
    write_fn: impl FnOnce(&[u8; 8], usize),
) {
    let encoded = (value << 8)
        | (bytes_count as u64)
        | if WITH_FLAGS {
            (flags << FLAGS_OFFSET) as u64
        } else {
            0
        };
    write_fn(&encoded.to_le_bytes(), bytes_count + 1)
}

/// Encodes a memory varint, with up to 4 bits for flags
#[inline(always)]
pub fn encode_memvarint_flags<const WITH_FLAGS: bool>(
    value: u64,
    flags: u8,
    write_fn: impl FnOnce(&[u8; 8], usize),
) {
    let size = compute_memvarint_bytes_count(value);
    encode_memvarint_flags_with_size::<WITH_FLAGS>(size, value, flags, write_fn);
}

#[inline(always)]
pub fn decode_memvarint_flags<const WITH_FLAGS: bool>(data: &[u8; 8]) -> (usize, (u64, u8)) {
    let control = data[0];
    let num_bytes = control & 0xF;
    let flags = if WITH_FLAGS {
        control >> FLAGS_OFFSET
    } else {
        0
    };

    let value = u64::from_le_bytes(*data) >> 8;
    let mask = (1u64 << (num_bytes * 8)) - 1;

    ((num_bytes + 1) as usize, (value & mask, flags))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<const WITH_FLAGS: bool>(value: u64, flags: u8) {
        let mut written_data = [0u8; 8];
        let mut written_size = 0usize;

        // Encode
        encode_memvarint_flags::<WITH_FLAGS>(value, flags, |bytes, size| {
            written_data.copy_from_slice(bytes);
            written_size = size;
        });

        // Decode
        let (decoded_size, (decoded_value, decoded_flags)) =
            decode_memvarint_flags::<WITH_FLAGS>(&written_data);

        assert_eq!(
            decoded_size, written_size,
            "Mismatched size for value={value}"
        );
        assert_eq!(decoded_value, value, "Value mismatch for value={value}");
        if WITH_FLAGS {
            assert_eq!(decoded_flags, flags, "Flags mismatch for value={value}");
        } else {
            assert_eq!(decoded_flags, 0, "Flags should be 0 when WITH_FLAGS=false");
        }
    }

    #[test]
    fn test_basic_values_without_flags() {
        roundtrip::<false>(0, 0);
        roundtrip::<false>(1, 0);
        roundtrip::<false>(127, 0);
        roundtrip::<false>(255, 0);
        roundtrip::<false>(256, 0);
        roundtrip::<false>(0xFFFF, 0);
        roundtrip::<false>(0xFFFFFF, 0);
        roundtrip::<false>(0xFFFFFFFFFFFFFF, 0);
    }

    #[test]
    fn test_basic_values_with_flags() {
        roundtrip::<true>(0, 0b0001);
        roundtrip::<true>(42, 0b0010);
        roundtrip::<true>(255, 0b0100);
        roundtrip::<true>(0x123456, 0b1011);
        roundtrip::<true>(0xFFFFFFFF, 0b1111);
    }

    #[test]
    fn test_min_and_max_byte_lengths() {
        // Smallest: 1 byte (value fits in <8 bits)
        roundtrip::<false>(0x7F, 0);

        // Largest possible for 7 bytes
        let max_7_bytes = (1u64 << (7 * 8)) - 1;
        roundtrip::<false>(max_7_bytes, 0);

        // Just below limit
        roundtrip::<false>(max_7_bytes - 1, 0);
    }

    #[test]
    fn test_flags_disabled_ignore_flags_value() {
        // Even if flags are nonzero, they shouldn't affect decode result
        roundtrip::<false>(12345, 0xFF);
    }

    #[test]
    fn test_compute_memvarint_bytes_count_boundaries() {
        assert_eq!(compute_memvarint_bytes_count(0), 0);
        assert_eq!(compute_memvarint_bytes_count(0xFF), 1);
        assert_eq!(compute_memvarint_bytes_count(0xFFFF), 2);
        assert_eq!(compute_memvarint_bytes_count(0xFFFFFF), 3);
        assert_eq!(compute_memvarint_bytes_count(0xFFFFFFFF), 4);
        assert_eq!(compute_memvarint_bytes_count(0xFFFFFFFFFF), 5);
        assert_eq!(compute_memvarint_bytes_count(0xFFFFFFFFFFFF), 6);
        assert_eq!(compute_memvarint_bytes_count(0xFFFFFFFFFFFFFF), 7);
    }

    // #[test]
    // #[should_panic]
    // fn test_value_exceeds_max_bytes() {
    //     // Should panic because value > ((1 << (MAX_BYTES * 8)) - 1)
    //     let invalid_value = (1u64 << (MAX_BYTES * 8)) as u64;
    //     let _ = compute_memvarint_bytes_count(invalid_value);
    // }
}
