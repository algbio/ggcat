//! 64-byte block FASTA byte-classification kernels.
//!
//! Adapted from helicase (`helicase/src/simd`), MIT License, Copyright (c) 2025 Igor Martayan.
//! See the repository notice in `lib.rs`.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RawFastaMasks {
    pub open_brackets: u64,
    pub line_feeds: u64,
    pub carriage_returns: u64,
    pub two_bits: u128,
    pub mask_non_acgt: u64,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub fn extract_fasta_masks(buf: &[u8; 64]) -> RawFastaMasks {
    avx2::extract(buf)
}

#[cfg(all(
    target_arch = "x86_64",
    not(target_feature = "avx2"),
    target_feature = "ssse3"
))]
#[inline(always)]
pub fn extract_fasta_masks(buf: &[u8; 64]) -> RawFastaMasks {
    ssse3::extract(buf)
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline(always)]
pub fn extract_fasta_masks(buf: &[u8; 64]) -> RawFastaMasks {
    neon::extract(buf)
}

/// No vector byte shuffle on this target, so classify eight bytes at a time
/// with ordinary 64-bit arithmetic. Slower, but it keeps the crate portable.
#[cfg(not(any(
    all(target_arch = "x86_64", target_feature = "avx2"),
    all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        target_feature = "ssse3"
    ),
    all(target_arch = "aarch64", target_feature = "neon")
)))]
#[inline(always)]
pub fn extract_fasta_masks(buf: &[u8; 64]) -> RawFastaMasks {
    fallback::extract(buf)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
mod avx2 {
    use super::{RawFastaMasks, pack_two_bit_masks};
    use core::arch::x86_64::*;

    #[inline(always)]
    pub fn extract(buf: &[u8; 64]) -> RawFastaMasks {
        unsafe {
            let ptr = buf.as_ptr().cast::<__m256i>();
            let first = _mm256_loadu_si256(ptr);
            let second = _mm256_loadu_si256(ptr.add(1));
            let high = movemask(first, second, 5);
            let low = movemask(first, second, 6);
            let mask_upper = _mm256_set1_epi8(0b1101_1111u8 as i8);
            let mask_two_bits = _mm256_set1_epi8(0b110);
            let lut = _mm256_setr_epi8(
                b'A' as i8, b'_' as i8, b'C' as i8, b'_' as i8, b'T' as i8, b'_' as i8, b'G' as i8,
                b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8,
                b'_' as i8, b'_' as i8, b'A' as i8, b'_' as i8, b'C' as i8, b'_' as i8, b'T' as i8,
                b'_' as i8, b'G' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8,
                b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8,
            );
            let upper_first = _mm256_and_si256(first, mask_upper);
            let upper_second = _mm256_and_si256(second, mask_upper);
            let actg = cmp_mask(
                _mm256_cmpeq_epi8(
                    _mm256_shuffle_epi8(lut, _mm256_and_si256(first, mask_two_bits)),
                    upper_first,
                ),
                _mm256_cmpeq_epi8(
                    _mm256_shuffle_epi8(lut, _mm256_and_si256(second, mask_two_bits)),
                    upper_second,
                ),
            );
            RawFastaMasks {
                open_brackets: byte_mask(first, second, b'>'),
                line_feeds: byte_mask(first, second, b'\n'),
                carriage_returns: byte_mask(first, second, b'\r'),
                two_bits: pack_two_bit_masks(low, high),
                mask_non_acgt: !actg,
            }
        }
    }

    #[inline(always)]
    unsafe fn movemask(first: __m256i, second: __m256i, shift: i32) -> u64 {
        let a = unsafe { _mm256_movemask_epi8(_mm256_sll_epi16(first, _mm_cvtsi32_si128(shift))) }
            as u32 as u64;
        let b = unsafe { _mm256_movemask_epi8(_mm256_sll_epi16(second, _mm_cvtsi32_si128(shift))) }
            as u32 as u64;
        a | (b << 32)
    }

    #[inline(always)]
    unsafe fn byte_mask(first: __m256i, second: __m256i, byte: u8) -> u64 {
        let value = unsafe { _mm256_set1_epi8(byte as i8) };
        unsafe {
            cmp_mask(
                _mm256_cmpeq_epi8(first, value),
                _mm256_cmpeq_epi8(second, value),
            )
        }
    }

    #[inline(always)]
    unsafe fn cmp_mask(first: __m256i, second: __m256i) -> u64 {
        unsafe {
            (_mm256_movemask_epi8(first) as u32 as u64)
                | ((_mm256_movemask_epi8(second) as u32 as u64) << 32)
        }
    }
}

#[cfg(all(
    target_arch = "x86_64",
    not(target_feature = "avx2"),
    target_feature = "ssse3"
))]
mod ssse3 {
    use super::{RawFastaMasks, pack_two_bit_masks};
    use core::arch::x86_64::*;

    #[inline(always)]
    pub fn extract(buf: &[u8; 64]) -> RawFastaMasks {
        unsafe {
            let ptr = buf.as_ptr().cast::<__m128i>();
            let values = [
                _mm_loadu_si128(ptr),
                _mm_loadu_si128(ptr.add(1)),
                _mm_loadu_si128(ptr.add(2)),
                _mm_loadu_si128(ptr.add(3)),
            ];
            let high = movemask(values, 5);
            let low = movemask(values, 6);
            let mask_upper = _mm_set1_epi8(0b1101_1111u8 as i8);
            let mask_two_bits = _mm_set1_epi8(0b110);
            let lut = _mm_setr_epi8(
                b'A' as i8, b'_' as i8, b'C' as i8, b'_' as i8, b'T' as i8, b'_' as i8, b'G' as i8,
                b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8, b'_' as i8,
                b'_' as i8, b'_' as i8,
            );
            let actg_values = values.map(|value| {
                _mm_cmpeq_epi8(
                    _mm_shuffle_epi8(lut, _mm_and_si128(value, mask_two_bits)),
                    _mm_and_si128(value, mask_upper),
                )
            });
            RawFastaMasks {
                open_brackets: byte_mask(values, b'>'),
                line_feeds: byte_mask(values, b'\n'),
                carriage_returns: byte_mask(values, b'\r'),
                two_bits: pack_two_bit_masks(low, high),
                mask_non_acgt: !combine_masks(actg_values),
            }
        }
    }

    #[inline(always)]
    unsafe fn movemask(values: [__m128i; 4], shift: i32) -> u64 {
        let shift = unsafe { _mm_cvtsi32_si128(shift) };
        unsafe { combine_masks(values.map(|value| _mm_sll_epi16(value, shift))) }
    }

    #[inline(always)]
    unsafe fn byte_mask(values: [__m128i; 4], byte: u8) -> u64 {
        let needle = unsafe { _mm_set1_epi8(byte as i8) };
        unsafe { combine_masks(values.map(|value| _mm_cmpeq_epi8(value, needle))) }
    }

    #[inline(always)]
    unsafe fn combine_masks(values: [__m128i; 4]) -> u64 {
        unsafe {
            (_mm_movemask_epi8(values[0]) as u16 as u64)
                | ((_mm_movemask_epi8(values[1]) as u16 as u64) << 16)
                | ((_mm_movemask_epi8(values[2]) as u16 as u64) << 32)
                | ((_mm_movemask_epi8(values[3]) as u16 as u64) << 48)
        }
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod neon {
    use super::RawFastaMasks;
    use core::arch::aarch64::*;
    use core::mem::transmute;

    #[inline(always)]
    pub fn extract(buf: &[u8; 64]) -> RawFastaMasks {
        unsafe {
            let values = vld4q_u8(buf.as_ptr());
            let upper_mask = vdupq_n_u8(0b1101_1111);
            let two_mask = vdupq_n_u8(0b110);
            let lut = vld1q_u8(b"A_C_T_G_________".as_ptr());
            let shifted = map(values, |value| vshlq_n_u8::<5>(value));
            let packed = vsriq_n_u8(
                vsriq_n_u8(shifted.3, shifted.2, 2),
                vsriq_n_u8(shifted.1, shifted.0, 2),
                4,
            );
            let upper = map(values, |value| vandq_u8(value, upper_mask));
            let lookup = map(values, |value| vqtbl1q_u8(lut, vandq_u8(value, two_mask)));
            let actg = movemask(map_two(lookup, upper, |left, right| vceqq_u8(left, right)));
            RawFastaMasks {
                open_brackets: byte_mask(values, b'>'),
                line_feeds: byte_mask(values, b'\n'),
                carriage_returns: byte_mask(values, b'\r'),
                two_bits: transmute(packed),
                mask_non_acgt: !actg,
            }
        }
    }

    #[inline(always)]
    unsafe fn byte_mask(values: uint8x16x4_t, byte: u8) -> u64 {
        let needle = unsafe { vdupq_n_u8(byte) };
        unsafe { movemask(map(values, |value| vceqq_u8(value, needle))) }
    }

    #[inline(always)]
    fn map(values: uint8x16x4_t, mut f: impl FnMut(uint8x16_t) -> uint8x16_t) -> uint8x16x4_t {
        uint8x16x4_t(f(values.0), f(values.1), f(values.2), f(values.3))
    }

    #[inline(always)]
    fn map_two(
        left: uint8x16x4_t,
        right: uint8x16x4_t,
        mut f: impl FnMut(uint8x16_t, uint8x16_t) -> uint8x16_t,
    ) -> uint8x16x4_t {
        uint8x16x4_t(
            f(left.0, right.0),
            f(left.1, right.1),
            f(left.2, right.2),
            f(left.3, right.3),
        )
    }

    #[inline(always)]
    unsafe fn movemask(values: uint8x16x4_t) -> u64 {
        unsafe {
            let accumulated = vsriq_n_u8(
                vsriq_n_u8(values.3, values.2, 1),
                vsriq_n_u8(values.1, values.0, 1),
                2,
            );
            vget_lane_u64(
                vreinterpret_u64_u8(vshrn_n_u16(
                    vreinterpretq_u16_u8(vsriq_n_u8(accumulated, accumulated, 4)),
                    4,
                )),
                0,
            )
        }
    }
}

/// Portable SWAR kernel, for targets with no vector byte shuffle.
///
/// Every 64-bit word holds eight bytes, so one block costs eight word
/// operations instead of sixty-four byte ones. It is always compiled, so the
/// tests check it against the reference on every build, and it is only
/// dispatched to when none of the vector kernels apply.
pub mod fallback {
    use super::{RawFastaMasks, pack_two_bit_masks};

    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGHS: u64 = 0x8080_8080_8080_8080;
    const LOW_SEVEN: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    /// Gathers bit 0 of each byte into the top byte of the product.
    const PACK_LSB: u64 = 0x0102_0408_1020_4080;
    /// Clears bit 5 of every byte, which is what separates `a` from `A`.
    const UPPERCASE: u64 = 0xdfdf_dfdf_dfdf_dfdf;

    /// Bit `i` of the result is set when byte `i` of `word` is zero.
    ///
    /// The usual `(word - ONES) & !word & HIGHS` is not used here: its borrow
    /// crosses byte boundaries, so a zero byte makes the next byte report zero
    /// too whenever that byte is 1. Adding into the low seven bits instead can
    /// never carry out of a byte, because `0x7f + 0x7f < 0x100`.
    #[inline(always)]
    fn zero_bytes(word: u64) -> u8 {
        let low = (word & LOW_SEVEN) + LOW_SEVEN;
        // Bit 7 of `low | word` is set exactly when the byte is not zero.
        pack_lsb_by_byte((!(low | word) & HIGHS) >> 7)
    }

    /// Bit `i` of the result is set when byte `i` of `word` equals `byte`.
    #[inline(always)]
    fn eq_mask(word: u64, byte: u8) -> u8 {
        zero_bytes(word ^ (ONES * byte as u64))
    }

    /// Gathers bit 0 of each byte of `word` into one byte.
    #[inline(always)]
    fn pack_lsb_by_byte(word: u64) -> u8 {
        (((word & ONES).wrapping_mul(PACK_LSB)) >> 56) as u8
    }

    #[inline(always)]
    pub fn extract(buf: &[u8; 64]) -> RawFastaMasks {
        let mut open_brackets = 0u64;
        let mut line_feeds = 0u64;
        let mut carriage_returns = 0u64;
        let mut low_bits = 0u64;
        let mut high_bits = 0u64;
        let mut acgt = 0u64;

        for chunk in 0..8 {
            let shift = chunk * 8;
            let word = u64::from_le_bytes(buf[shift..shift + 8].try_into().unwrap());

            open_brackets |= (eq_mask(word, b'>') as u64) << shift;
            line_feeds |= (eq_mask(word, b'\n') as u64) << shift;
            carriage_returns |= (eq_mask(word, b'\r') as u64) << shift;

            // The two bits that distinguish the four bases are bits 1 and 2 of
            // the byte; `pack_lsb_by_byte` keeps bit 0 of each byte, so the
            // word shift leaking into the neighbour is masked away.
            low_bits |= (pack_lsb_by_byte(word >> 1) as u64) << shift;
            high_bits |= (pack_lsb_by_byte(word >> 2) as u64) << shift;

            let upper = word & UPPERCASE;
            let matched = eq_mask(upper, b'A')
                | eq_mask(upper, b'C')
                | eq_mask(upper, b'G')
                | eq_mask(upper, b'T');
            acgt |= (matched as u64) << shift;
        }

        RawFastaMasks {
            open_brackets,
            line_feeds,
            carriage_returns,
            two_bits: pack_two_bit_masks(low_bits, high_bits),
            mask_non_acgt: !acgt,
        }
    }
}

#[inline(always)]
fn spread_32(mut value: u64) -> u64 {
    value &= 0xffff_ffff;
    value = (value | (value << 16)) & 0x0000_ffff_0000_ffff;
    value = (value | (value << 8)) & 0x00ff_00ff_00ff_00ff;
    value = (value | (value << 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    value = (value | (value << 2)) & 0x3333_3333_3333_3333;
    (value | (value << 1)) & 0x5555_5555_5555_5555
}

#[inline(always)]
fn pack_two_bit_masks(low: u64, high: u64) -> u128 {
    let first = spread_32(low) | (spread_32(high) << 1);
    let second = spread_32(low >> 32) | (spread_32(high >> 32) << 1);
    first as u128 | ((second as u128) << 64)
}

#[inline(always)]
pub fn low_mask_u32(bits: usize) -> u32 {
    if bits == 32 {
        u32::MAX
    } else {
        (1u32 << bits) - 1
    }
}

#[inline(always)]
pub fn low_mask_u64(bits: usize) -> u64 {
    if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

#[inline(always)]
pub fn next_set_bit(mask: u64, start: usize) -> Option<usize> {
    let remaining = mask & (u64::MAX << start);
    (remaining != 0).then(|| remaining.trailing_zeros() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-at-a-time reference used both as the test oracle and as the
    /// documentation of what every vector kernel must produce.
    pub(crate) fn scalar_masks(buf: &[u8; 64]) -> RawFastaMasks {
        let mut result = RawFastaMasks {
            open_brackets: 0,
            line_feeds: 0,
            carriage_returns: 0,
            two_bits: 0,
            mask_non_acgt: 0,
        };
        for (index, &byte) in buf.iter().enumerate() {
            let bit = 1u64 << index;
            result.open_brackets |= ((byte == b'>') as u64) * bit;
            result.line_feeds |= ((byte == b'\n') as u64) * bit;
            result.carriage_returns |= ((byte == b'\r') as u64) * bit;
            result.two_bits |= (((byte >> 1) & 3) as u128) << (2 * index);
            let upper = byte & 0b1101_1111;
            if !matches!(upper, b'A' | b'C' | b'G' | b'T') {
                result.mask_non_acgt |= bit;
            }
        }
        result
    }

    #[test]
    fn simd_extractor_matches_scalar_reference() {
        let mut state = 0x9e37_79b9u32;
        for round in 0..256 {
            let mut block = [0u8; 64];
            for (index, byte) in block.iter_mut().enumerate() {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *byte = match (state as usize + index + round) % 17 {
                    0 => b'>',
                    1 => b'\n',
                    2 => b'\r',
                    3 => b'A',
                    4 => b'c',
                    5 => b'G',
                    6 => b't',
                    7 => b'N',
                    8 => b'n',
                    9 => b'R',
                    _ => (state >> 24) as u8,
                };
            }
            assert_eq!(extract_fasta_masks(&block), scalar_masks(&block));
        }
    }

    #[test]
    fn fallback_matches_scalar_reference() {
        let mut state = 0x9e37_79b9u32;
        for round in 0..256 {
            let mut block = [0u8; 64];
            for (index, byte) in block.iter_mut().enumerate() {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *byte = match (state as usize + index + round) % 17 {
                    0 => b'>',
                    1 => b'\n',
                    2 => b'\r',
                    3 => b'A',
                    4 => b'c',
                    5 => b'G',
                    6 => b't',
                    7 => b'N',
                    8 => b'n',
                    9 => b'R',
                    _ => (state >> 24) as u8,
                };
            }
            assert_eq!(fallback::extract(&block), scalar_masks(&block));
            // Whatever kernel this build selected has to agree with it too.
            assert_eq!(fallback::extract(&block), extract_fasta_masks(&block));
        }
    }

    /// A byte that matches, followed by one whose code differs in bit 0 only.
    /// A borrow-based byte comparison reports the second byte as a match too,
    /// so every adjacent pair is checked exhaustively.
    #[test]
    fn fallback_does_not_leak_between_adjacent_bytes() {
        for first in 0u16..256 {
            for second in 0u16..256 {
                let mut block = [b'x'; 64];
                block[0] = first as u8;
                block[1] = second as u8;
                assert_eq!(
                    fallback::extract(&block),
                    scalar_masks(&block),
                    "bytes {first:#04x} {second:#04x}"
                );
            }
        }
    }

    #[test]
    fn zero_bytes_are_not_acgt() {
        let block = [0u8; 64];
        let masks = extract_fasta_masks(&block);
        assert_eq!(masks.mask_non_acgt, u64::MAX);
        assert_eq!(masks.open_brackets, 0);
        assert_eq!(masks.line_feeds, 0);
    }
}
