//! The vector operations the SIMD code needs, [`HashVector`], one implementation per
//! [`HashKernel`]: plain arrays; AVX2 and AVX-512 (on 256 bits), one register for the eight
//! lanes; SSSE3 and NEON, two registers of four.

use super::{BLOCK_CHUNKS, SIMD_LANES, packed_word};
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// The operations the hash and the minimizer queue need, on `LANES` lanes of 32 bits:
/// implemented here for each [`HashKernel`], and used by the [`BaseLanes`](super::BaseLanes) to
/// hand out their chunks in the kernel's vectors.
///
/// # Safety
///
/// The methods may use the instructions of their kernel: they are to be called only where the
/// CPU has them, as [`CanonicalNtHashLanes::for_each`] does.
///
/// [`CanonicalNtHashLanes::for_each`]: super::nthash::CanonicalNtHashLanes::for_each
pub trait HashVector<const LANES: usize>: Copy {
    /// Four values to look up by base code.
    type Table: Copy;
    /// Some of the lanes, as comparisons pick them: all ones or zero in each lane of a vector, or
    /// the kernel's mask registers.
    type Mask: Copy;

    unsafe fn table(values: [u32; 4]) -> Self::Table;
    unsafe fn from_array(values: [u32; LANES]) -> Self;
    unsafe fn to_array(self) -> [u32; LANES];
    unsafe fn xor(self, other: Self) -> Self;
    unsafe fn rotate_left_1(self) -> Self;
    unsafe fn rotate_right_1(self) -> Self;
    /// The packed base at `offset` of every lane's chunk.
    unsafe fn packed_base_at(chunk: Self, offset: u32) -> Self;
    /// `table`'s value for the code in every lane.
    unsafe fn lookup(table: Self::Table, codes: Self) -> Self;
    /// See [`NtHashLanes::canonical`](super::nthash::NtHashLanes::canonical).
    unsafe fn canonical(forward: Self, reverse: Self) -> Self;
    /// See [`NtHashLanes::forward`](super::nthash::NtHashLanes::forward).
    unsafe fn forward_mask(forward: Self, reverse: Self) -> u32;
    unsafe fn splat(value: u32) -> Self;
    /// The lanes where `self` and `other` are equal.
    unsafe fn equal(self, other: Self) -> Self::Mask;
    /// The lanes where `self` is below `other`, unsigned.
    unsafe fn less(self, other: Self) -> Self::Mask;
    /// `chosen` in the lanes of `mask`, `other` in the others.
    unsafe fn select(mask: Self::Mask, chosen: Self, other: Self) -> Self;
    /// `self` with bit 0, a minimizer queue's unique flag, cleared in the lanes of `mask`.
    unsafe fn clear_flag(self, mask: Self::Mask) -> Self;
    /// Bit `lane` set for each lane of `mask`.
    unsafe fn mask_bits(mask: Self::Mask) -> u32;
    /// The chunks of block `block` of every lane of the packed `bases`, lane `i` from byte
    /// `first_bytes[i]`: chunk `j` is the 32 bits from byte `first_bytes[i] + 32 * block + 4 * j`
    /// of every lane, bytes past the buffer read as A. The first bytes go up with the lanes.
    unsafe fn load_block(
        bases: &[u8],
        first_bytes: &[u32; LANES],
        block: usize,
    ) -> [Self; BLOCK_CHUNKS];
}

/// Plain arrays: what any CPU runs, the scalar hash with one lane.
#[derive(Copy, Clone)]
pub(super) struct Portable<const LANES: usize>([u32; LANES]);

impl<const LANES: usize> HashVector<LANES> for Portable<LANES> {
    type Table = [u32; 4];
    type Mask = [bool; LANES];

    #[inline(always)]
    unsafe fn table(values: [u32; 4]) -> Self::Table {
        values
    }

    #[inline(always)]
    unsafe fn from_array(values: [u32; LANES]) -> Self {
        Self(values)
    }

    #[inline(always)]
    unsafe fn to_array(self) -> [u32; LANES] {
        self.0
    }

    #[inline(always)]
    unsafe fn xor(mut self, other: Self) -> Self {
        for lane in 0..LANES {
            self.0[lane] ^= other.0[lane];
        }
        self
    }

    #[inline(always)]
    unsafe fn rotate_left_1(mut self) -> Self {
        for value in &mut self.0 {
            *value = value.rotate_left(1);
        }
        self
    }

    #[inline(always)]
    unsafe fn rotate_right_1(mut self) -> Self {
        for value in &mut self.0 {
            *value = value.rotate_right(1);
        }
        self
    }

    #[inline(always)]
    unsafe fn packed_base_at(mut chunk: Self, offset: u32) -> Self {
        for value in &mut chunk.0 {
            *value = (*value >> (2 * offset)) & 3;
        }
        chunk
    }

    #[inline(always)]
    unsafe fn lookup(table: Self::Table, mut codes: Self) -> Self {
        for value in &mut codes.0 {
            *value = table[*value as usize & 3];
        }
        codes
    }

    #[inline(always)]
    unsafe fn canonical(forward: Self, reverse: Self) -> Self {
        let mut canonical = [0; LANES];
        for lane in 0..LANES {
            let (forward, reverse) = (forward.0[lane], reverse.0[lane]);
            canonical[lane] = (forward.min(reverse) << 1) | (forward != reverse) as u32;
        }
        Self(canonical)
    }

    #[inline(always)]
    unsafe fn forward_mask(forward: Self, reverse: Self) -> u32 {
        let mut mask = 0;
        for lane in 0..LANES {
            mask |= ((forward.0[lane] < reverse.0[lane]) as u32) << lane;
        }
        mask
    }

    #[inline(always)]
    unsafe fn splat(value: u32) -> Self {
        Self([value; LANES])
    }

    #[inline(always)]
    unsafe fn equal(self, other: Self) -> Self::Mask {
        let mut mask = [false; LANES];
        for lane in 0..LANES {
            mask[lane] = self.0[lane] == other.0[lane];
        }
        mask
    }

    #[inline(always)]
    unsafe fn less(self, other: Self) -> Self::Mask {
        let mut mask = [false; LANES];
        for lane in 0..LANES {
            mask[lane] = self.0[lane] < other.0[lane];
        }
        mask
    }

    #[inline(always)]
    unsafe fn select(mask: Self::Mask, mut chosen: Self, other: Self) -> Self {
        for lane in 0..LANES {
            if !mask[lane] {
                chosen.0[lane] = other.0[lane];
            }
        }
        chosen
    }

    #[inline(always)]
    unsafe fn clear_flag(mut self, mask: Self::Mask) -> Self {
        for lane in 0..LANES {
            self.0[lane] &= !(mask[lane] as u32);
        }
        self
    }

    #[inline(always)]
    unsafe fn mask_bits(mask: Self::Mask) -> u32 {
        let mut bits = 0;
        for lane in 0..LANES {
            bits |= (mask[lane] as u32) << lane;
        }
        bits
    }

    #[inline(always)]
    unsafe fn load_block(
        bases: &[u8],
        first_bytes: &[u32; LANES],
        block: usize,
    ) -> [Self; BLOCK_CHUNKS] {
        let mut chunks = [Self([0; LANES]); BLOCK_CHUNKS];
        for lane in 0..LANES {
            let start = first_bytes[lane] as usize + 4 * BLOCK_CHUNKS * block;
            for (index, chunk) in chunks.iter_mut().enumerate() {
                chunk.0[lane] = packed_word(bases, start + 4 * index);
            }
        }
        chunks
    }
}

/// AVX2: the eight lanes in one register.
#[cfg(target_arch = "x86_64")]
#[derive(Copy, Clone)]
pub(super) struct Avx2(__m256i);

#[cfg(target_arch = "x86_64")]
impl HashVector<SIMD_LANES> for Avx2 {
    type Table = __m256i;
    type Mask = __m256i;

    #[inline(always)]
    unsafe fn table(values: [u32; 4]) -> Self::Table {
        let [a, c, t, g] = values.map(|value| value as i32);
        unsafe { _mm256_setr_epi32(a, c, t, g, a, c, t, g) }
    }

    #[inline(always)]
    unsafe fn from_array(values: [u32; SIMD_LANES]) -> Self {
        Self(unsafe { _mm256_loadu_si256(values.as_ptr().cast()) })
    }

    #[inline(always)]
    unsafe fn to_array(self) -> [u32; SIMD_LANES] {
        let mut values = [0; SIMD_LANES];
        unsafe { _mm256_storeu_si256(values.as_mut_ptr().cast(), self.0) };
        values
    }

    #[inline(always)]
    unsafe fn xor(self, other: Self) -> Self {
        Self(unsafe { _mm256_xor_si256(self.0, other.0) })
    }

    #[inline(always)]
    unsafe fn rotate_left_1(self) -> Self {
        Self(unsafe {
            _mm256_or_si256(
                _mm256_slli_epi32::<1>(self.0),
                _mm256_srli_epi32::<31>(self.0),
            )
        })
    }

    #[inline(always)]
    unsafe fn rotate_right_1(self) -> Self {
        Self(unsafe {
            _mm256_or_si256(
                _mm256_srli_epi32::<1>(self.0),
                _mm256_slli_epi32::<31>(self.0),
            )
        })
    }

    #[inline(always)]
    unsafe fn packed_base_at(chunk: Self, offset: u32) -> Self {
        unsafe {
            let shifted = _mm256_srl_epi32(chunk.0, _mm_cvtsi32_si128((2 * offset) as i32));
            Self(_mm256_and_si256(shifted, _mm256_set1_epi32(3)))
        }
    }

    #[inline(always)]
    unsafe fn lookup(table: Self::Table, codes: Self) -> Self {
        Self(unsafe { _mm256_permutevar8x32_epi32(table, codes.0) })
    }

    #[inline(always)]
    unsafe fn canonical(forward: Self, reverse: Self) -> Self {
        unsafe {
            let smaller = _mm256_slli_epi32::<1>(_mm256_min_epu32(forward.0, reverse.0));
            let symmetric = _mm256_cmpeq_epi32(forward.0, reverse.0);
            let unique = _mm256_andnot_si256(symmetric, _mm256_set1_epi32(1));
            Self(_mm256_or_si256(smaller, unique))
        }
    }

    #[inline(always)]
    unsafe fn forward_mask(forward: Self, reverse: Self) -> u32 {
        unsafe {
            // No unsigned compare: forward is the smaller, and not equal.
            let not_greater = _mm256_cmpeq_epi32(_mm256_min_epu32(forward.0, reverse.0), forward.0);
            let symmetric = _mm256_cmpeq_epi32(forward.0, reverse.0);
            let less = _mm256_andnot_si256(symmetric, not_greater);
            _mm256_movemask_ps(_mm256_castsi256_ps(less)) as u32
        }
    }

    #[inline(always)]
    unsafe fn splat(value: u32) -> Self {
        Self(unsafe { _mm256_set1_epi32(value as i32) })
    }

    #[inline(always)]
    unsafe fn equal(self, other: Self) -> Self::Mask {
        unsafe { _mm256_cmpeq_epi32(self.0, other.0) }
    }

    #[inline(always)]
    unsafe fn less(self, other: Self) -> Self::Mask {
        unsafe {
            // No unsigned compare: signed, on flipped sign bits.
            let sign = _mm256_set1_epi32(i32::MIN);
            _mm256_cmpgt_epi32(
                _mm256_xor_si256(other.0, sign),
                _mm256_xor_si256(self.0, sign),
            )
        }
    }

    #[inline(always)]
    unsafe fn select(mask: Self::Mask, chosen: Self, other: Self) -> Self {
        Self(unsafe { _mm256_blendv_epi8(other.0, chosen.0, mask) })
    }

    #[inline(always)]
    unsafe fn clear_flag(self, mask: Self::Mask) -> Self {
        Self(unsafe { _mm256_andnot_si256(_mm256_and_si256(mask, _mm256_set1_epi32(1)), self.0) })
    }

    #[inline(always)]
    unsafe fn mask_bits(mask: Self::Mask) -> u32 {
        unsafe { _mm256_movemask_ps(_mm256_castsi256_ps(mask)) as u32 }
    }

    #[inline(always)]
    unsafe fn load_block(
        bases: &[u8],
        first_bytes: &[u32; SIMD_LANES],
        block: usize,
    ) -> [Self; BLOCK_CHUNKS] {
        let skip = 4 * BLOCK_CHUNKS * block;
        // The last lane reads furthest: if its 32 bytes are in the buffer, all are.
        if first_bytes[SIMD_LANES - 1] as usize + skip + 4 * BLOCK_CHUNKS > bases.len() {
            let chunks = unsafe { Portable::load_block(bases, first_bytes, block) };
            let mut vectors = [unsafe { Self::from_array([0; SIMD_LANES]) }; BLOCK_CHUNKS];
            for index in 0..BLOCK_CHUNKS {
                vectors[index] = unsafe { Self::from_array(chunks[index].0) };
            }
            return vectors;
        }
        unsafe {
            // A row per lane, its 8 chunks; turned into a vector per chunk, its 8 lanes.
            let row = |lane: usize| {
                let start = bases.as_ptr().add(first_bytes[lane] as usize + skip);
                _mm256_loadu_si256(start.cast())
            };
            let (r0, r1, r2, r3) = (row(0), row(1), row(2), row(3));
            let (r4, r5, r6, r7) = (row(4), row(5), row(6), row(7));
            let (t0, t1) = (_mm256_unpacklo_epi32(r0, r1), _mm256_unpackhi_epi32(r0, r1));
            let (t2, t3) = (_mm256_unpacklo_epi32(r2, r3), _mm256_unpackhi_epi32(r2, r3));
            let (t4, t5) = (_mm256_unpacklo_epi32(r4, r5), _mm256_unpackhi_epi32(r4, r5));
            let (t6, t7) = (_mm256_unpacklo_epi32(r6, r7), _mm256_unpackhi_epi32(r6, r7));
            let (u0, u1) = (_mm256_unpacklo_epi64(t0, t2), _mm256_unpackhi_epi64(t0, t2));
            let (u2, u3) = (_mm256_unpacklo_epi64(t1, t3), _mm256_unpackhi_epi64(t1, t3));
            let (u4, u5) = (_mm256_unpacklo_epi64(t4, t6), _mm256_unpackhi_epi64(t4, t6));
            let (u6, u7) = (_mm256_unpacklo_epi64(t5, t7), _mm256_unpackhi_epi64(t5, t7));
            [
                Self(_mm256_permute2x128_si256::<0x20>(u0, u4)),
                Self(_mm256_permute2x128_si256::<0x20>(u1, u5)),
                Self(_mm256_permute2x128_si256::<0x20>(u2, u6)),
                Self(_mm256_permute2x128_si256::<0x20>(u3, u7)),
                Self(_mm256_permute2x128_si256::<0x31>(u0, u4)),
                Self(_mm256_permute2x128_si256::<0x31>(u1, u5)),
                Self(_mm256_permute2x128_si256::<0x31>(u2, u6)),
                Self(_mm256_permute2x128_si256::<0x31>(u3, u7)),
            ]
        }
    }
}

/// AVX-512 on 256 bits: as AVX2, with its single-instruction rotations.
#[cfg(target_arch = "x86_64")]
#[derive(Copy, Clone)]
pub(super) struct Avx512(Avx2);

#[cfg(target_arch = "x86_64")]
impl HashVector<SIMD_LANES> for Avx512 {
    type Table = __m256i;
    type Mask = __mmask8;

    #[inline(always)]
    unsafe fn table(values: [u32; 4]) -> Self::Table {
        unsafe { Avx2::table(values) }
    }

    #[inline(always)]
    unsafe fn from_array(values: [u32; SIMD_LANES]) -> Self {
        Self(unsafe { Avx2::from_array(values) })
    }

    #[inline(always)]
    unsafe fn to_array(self) -> [u32; SIMD_LANES] {
        unsafe { self.0.to_array() }
    }

    #[inline(always)]
    unsafe fn xor(self, other: Self) -> Self {
        Self(unsafe { self.0.xor(other.0) })
    }

    #[inline(always)]
    unsafe fn rotate_left_1(self) -> Self {
        Self(Avx2(unsafe { _mm256_rol_epi32::<1>(self.0.0) }))
    }

    #[inline(always)]
    unsafe fn rotate_right_1(self) -> Self {
        Self(Avx2(unsafe { _mm256_ror_epi32::<1>(self.0.0) }))
    }

    #[inline(always)]
    unsafe fn packed_base_at(chunk: Self, offset: u32) -> Self {
        Self(unsafe { Avx2::packed_base_at(chunk.0, offset) })
    }

    #[inline(always)]
    unsafe fn lookup(table: Self::Table, codes: Self) -> Self {
        Self(unsafe { Avx2::lookup(table, codes.0) })
    }

    #[inline(always)]
    unsafe fn canonical(forward: Self, reverse: Self) -> Self {
        Self(unsafe { Avx2::canonical(forward.0, reverse.0) })
    }

    #[inline(always)]
    unsafe fn forward_mask(forward: Self, reverse: Self) -> u32 {
        unsafe { _mm256_cmplt_epu32_mask(forward.0.0, reverse.0.0) as u32 }
    }

    #[inline(always)]
    unsafe fn splat(value: u32) -> Self {
        Self(unsafe { Avx2::splat(value) })
    }

    #[inline(always)]
    unsafe fn equal(self, other: Self) -> Self::Mask {
        unsafe { _mm256_cmpeq_epi32_mask(self.0.0, other.0.0) }
    }

    #[inline(always)]
    unsafe fn less(self, other: Self) -> Self::Mask {
        unsafe { _mm256_cmplt_epu32_mask(self.0.0, other.0.0) }
    }

    #[inline(always)]
    unsafe fn select(mask: Self::Mask, chosen: Self, other: Self) -> Self {
        Self(Avx2(unsafe {
            _mm256_mask_blend_epi32(mask, other.0.0, chosen.0.0)
        }))
    }

    #[inline(always)]
    unsafe fn clear_flag(self, mask: Self::Mask) -> Self {
        let cleared =
            unsafe { _mm256_mask_and_epi32(self.0.0, mask, self.0.0, _mm256_set1_epi32(!1)) };
        Self(Avx2(cleared))
    }

    #[inline(always)]
    unsafe fn mask_bits(mask: Self::Mask) -> u32 {
        mask as u32
    }

    #[inline(always)]
    unsafe fn load_block(
        bases: &[u8],
        first_bytes: &[u32; SIMD_LANES],
        block: usize,
    ) -> [Self; BLOCK_CHUNKS] {
        let [c0, c1, c2, c3, c4, c5, c6, c7] =
            unsafe { Avx2::load_block(bases, first_bytes, block) };
        [
            Self(c0),
            Self(c1),
            Self(c2),
            Self(c3),
            Self(c4),
            Self(c5),
            Self(c6),
            Self(c7),
        ]
    }
}

/// SSSE3: the eight lanes in two registers, lanes 0-3 and 4-7, each worked on by [`ssse3`].
#[cfg(target_arch = "x86_64")]
#[derive(Copy, Clone)]
pub(super) struct Ssse3([__m128i; 2]);

#[cfg(target_arch = "x86_64")]
impl HashVector<SIMD_LANES> for Ssse3 {
    /// The four values' 16 bytes, for the byte shuffle.
    type Table = __m128i;
    /// A vector of all ones or zero per lane, for each register.
    type Mask = [__m128i; 2];

    #[inline(always)]
    unsafe fn table(values: [u32; 4]) -> Self::Table {
        let [a, c, t, g] = values.map(|value| value as i32);
        unsafe { _mm_setr_epi32(a, c, t, g) }
    }

    #[inline(always)]
    unsafe fn from_array(values: [u32; SIMD_LANES]) -> Self {
        let values = values.as_ptr();
        unsafe {
            Self([
                _mm_loadu_si128(values.cast()),
                _mm_loadu_si128(values.add(4).cast()),
            ])
        }
    }

    #[inline(always)]
    unsafe fn to_array(self) -> [u32; SIMD_LANES] {
        let mut values = [0; SIMD_LANES];
        let pointer = values.as_mut_ptr();
        unsafe {
            _mm_storeu_si128(pointer.cast(), self.0[0]);
            _mm_storeu_si128(pointer.add(4).cast(), self.0[1]);
        }
        values
    }

    #[inline(always)]
    unsafe fn xor(self, other: Self) -> Self {
        let [a, b] = self.0;
        let [c, d] = other.0;
        unsafe { Self([_mm_xor_si128(a, c), _mm_xor_si128(b, d)]) }
    }

    #[inline(always)]
    unsafe fn rotate_left_1(self) -> Self {
        let [a, b] = self.0;
        unsafe { Self([ssse3::rotate_left_1(a), ssse3::rotate_left_1(b)]) }
    }

    #[inline(always)]
    unsafe fn rotate_right_1(self) -> Self {
        let [a, b] = self.0;
        unsafe { Self([ssse3::rotate_right_1(a), ssse3::rotate_right_1(b)]) }
    }

    #[inline(always)]
    unsafe fn packed_base_at(chunk: Self, offset: u32) -> Self {
        let [a, b] = chunk.0;
        unsafe {
            let shift = _mm_cvtsi32_si128((2 * offset) as i32);
            Self([ssse3::code(a, shift), ssse3::code(b, shift)])
        }
    }

    #[inline(always)]
    unsafe fn lookup(table: Self::Table, codes: Self) -> Self {
        let [a, b] = codes.0;
        unsafe { Self([ssse3::lookup(table, a), ssse3::lookup(table, b)]) }
    }

    #[inline(always)]
    unsafe fn canonical(forward: Self, reverse: Self) -> Self {
        let ([a, b], [c, d]) = (forward.0, reverse.0);
        unsafe { Self([ssse3::canonical(a, c), ssse3::canonical(b, d)]) }
    }

    #[inline(always)]
    unsafe fn forward_mask(forward: Self, reverse: Self) -> u32 {
        let ([a, b], [c, d]) = (forward.0, reverse.0);
        unsafe { ssse3::forward_mask(a, c) | ssse3::forward_mask(b, d) << 4 }
    }

    #[inline(always)]
    unsafe fn splat(value: u32) -> Self {
        let a = unsafe { _mm_set1_epi32(value as i32) };
        Self([a, a])
    }

    #[inline(always)]
    unsafe fn equal(self, other: Self) -> Self::Mask {
        let ([a, b], [c, d]) = (self.0, other.0);
        unsafe { [_mm_cmpeq_epi32(a, c), _mm_cmpeq_epi32(b, d)] }
    }

    #[inline(always)]
    unsafe fn less(self, other: Self) -> Self::Mask {
        let ([a, b], [c, d]) = (self.0, other.0);
        unsafe { [ssse3::less(a, c), ssse3::less(b, d)] }
    }

    #[inline(always)]
    unsafe fn select(mask: Self::Mask, chosen: Self, other: Self) -> Self {
        let ([m, n], [a, b], [c, d]) = (mask, chosen.0, other.0);
        unsafe { Self([ssse3::select(m, a, c), ssse3::select(n, b, d)]) }
    }

    #[inline(always)]
    unsafe fn clear_flag(self, mask: Self::Mask) -> Self {
        let ([a, b], [m, n]) = (self.0, mask);
        unsafe { Self([ssse3::clear_flag(a, m), ssse3::clear_flag(b, n)]) }
    }

    #[inline(always)]
    unsafe fn mask_bits(mask: Self::Mask) -> u32 {
        let [m, n] = mask;
        unsafe { ssse3::mask_bits(m) | ssse3::mask_bits(n) << 4 }
    }

    #[inline(always)]
    unsafe fn load_block(
        bases: &[u8],
        first_bytes: &[u32; SIMD_LANES],
        block: usize,
    ) -> [Self; BLOCK_CHUNKS] {
        let skip = 4 * BLOCK_CHUNKS * block;
        // The last lane reads furthest: if its 32 bytes are in the buffer, all are.
        if first_bytes[SIMD_LANES - 1] as usize + skip + 4 * BLOCK_CHUNKS > bases.len() {
            let chunks = unsafe { Portable::load_block(bases, first_bytes, block) };
            let mut vectors = [unsafe { Self::from_array([0; SIMD_LANES]) }; BLOCK_CHUNKS];
            for index in 0..BLOCK_CHUNKS {
                vectors[index] = unsafe { Self::from_array(chunks[index].0) };
            }
            return vectors;
        }
        let [low, high] = [&first_bytes[..4], &first_bytes[4..]];
        unsafe {
            let [a0, a1, a2, a3] = ssse3::load_quarter(bases, low, skip);
            let [a4, a5, a6, a7] = ssse3::load_quarter(bases, low, skip + 16);
            let [b0, b1, b2, b3] = ssse3::load_quarter(bases, high, skip);
            let [b4, b5, b6, b7] = ssse3::load_quarter(bases, high, skip + 16);
            [
                Self([a0, b0]),
                Self([a1, b1]),
                Self([a2, b2]),
                Self([a3, b3]),
                Self([a4, b4]),
                Self([a5, b5]),
                Self([a6, b6]),
                Self([a7, b7]),
            ]
        }
    }
}

/// [`Ssse3`]'s operations on one of its registers, four lanes. Its byte shuffle looks the tables
/// up, and its signed compares, on flipped sign bits, compare unsigned.
#[cfg(target_arch = "x86_64")]
mod ssse3 {
    use std::arch::x86_64::*;

    #[inline(always)]
    pub(super) unsafe fn rotate_left_1(a: __m128i) -> __m128i {
        unsafe { _mm_or_si128(_mm_slli_epi32::<1>(a), _mm_srli_epi32::<31>(a)) }
    }

    #[inline(always)]
    pub(super) unsafe fn rotate_right_1(a: __m128i) -> __m128i {
        unsafe { _mm_or_si128(_mm_srli_epi32::<1>(a), _mm_slli_epi32::<31>(a)) }
    }

    /// `chunk`'s codes at `shift`, twice their base's offset.
    #[inline(always)]
    pub(super) unsafe fn code(chunk: __m128i, shift: __m128i) -> __m128i {
        unsafe { _mm_and_si128(_mm_srl_epi32(chunk, shift), _mm_set1_epi32(3)) }
    }

    #[inline(always)]
    pub(super) unsafe fn lookup(table: __m128i, codes: __m128i) -> __m128i {
        unsafe {
            // Each lane's code times 4 in its four bytes, plus 0-3: the bytes of its value.
            let first_bytes = _mm_shuffle_epi8(
                _mm_slli_epi32::<2>(codes),
                _mm_setr_epi8(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8, 8, 12, 12, 12, 12),
            );
            let bytes = _mm_add_epi8(first_bytes, _mm_set1_epi32(0x0302_0100));
            _mm_shuffle_epi8(table, bytes)
        }
    }

    /// All ones in the lanes where `forward` is below `reverse`, unsigned.
    #[inline(always)]
    pub(super) unsafe fn less(forward: __m128i, reverse: __m128i) -> __m128i {
        unsafe {
            let sign = _mm_set1_epi32(i32::MIN);
            _mm_cmpgt_epi32(_mm_xor_si128(reverse, sign), _mm_xor_si128(forward, sign))
        }
    }

    #[inline(always)]
    pub(super) unsafe fn canonical(forward: __m128i, reverse: __m128i) -> __m128i {
        unsafe {
            let less = less(forward, reverse);
            let swap = _mm_and_si128(_mm_xor_si128(forward, reverse), less);
            let smaller = _mm_xor_si128(reverse, swap);
            let symmetric = _mm_cmpeq_epi32(forward, reverse);
            let unique = _mm_andnot_si128(symmetric, _mm_set1_epi32(1));
            _mm_or_si128(_mm_slli_epi32::<1>(smaller), unique)
        }
    }

    #[inline(always)]
    pub(super) unsafe fn forward_mask(forward: __m128i, reverse: __m128i) -> u32 {
        unsafe { _mm_movemask_ps(_mm_castsi128_ps(less(forward, reverse))) as u32 }
    }

    /// `chosen` in the lanes of `mask`, `other` in the others: no blend before SSE4.1.
    #[inline(always)]
    pub(super) unsafe fn select(mask: __m128i, chosen: __m128i, other: __m128i) -> __m128i {
        unsafe { _mm_or_si128(_mm_and_si128(mask, chosen), _mm_andnot_si128(mask, other)) }
    }

    #[inline(always)]
    pub(super) unsafe fn clear_flag(a: __m128i, mask: __m128i) -> __m128i {
        unsafe { _mm_andnot_si128(_mm_and_si128(mask, _mm_set1_epi32(1)), a) }
    }

    #[inline(always)]
    pub(super) unsafe fn mask_bits(mask: __m128i) -> u32 {
        unsafe { _mm_movemask_ps(_mm_castsi128_ps(mask)) as u32 }
    }

    /// Chunks 0-3 of four lanes, from 16 bytes per lane at `skip` bytes from their
    /// `first_bytes`, a vector per chunk: four rows of four 32-bit values, as columns.
    #[inline(always)]
    pub(super) unsafe fn load_quarter(
        bases: &[u8],
        first_bytes: &[u32],
        skip: usize,
    ) -> [__m128i; 4] {
        unsafe {
            let row = |lane: usize| {
                let start = bases.as_ptr().add(first_bytes[lane] as usize + skip);
                _mm_loadu_si128(start.cast())
            };
            let (a, b, c, d) = (row(0), row(1), row(2), row(3));
            let (ab_low, ab_high) = (_mm_unpacklo_epi32(a, b), _mm_unpackhi_epi32(a, b));
            let (cd_low, cd_high) = (_mm_unpacklo_epi32(c, d), _mm_unpackhi_epi32(c, d));
            [
                _mm_unpacklo_epi64(ab_low, cd_low),
                _mm_unpackhi_epi64(ab_low, cd_low),
                _mm_unpacklo_epi64(ab_high, cd_high),
                _mm_unpackhi_epi64(ab_high, cd_high),
            ]
        }
    }
}

/// NEON: the eight lanes in two registers, lanes 0-3 and 4-7, each worked on by [`neon`].
#[cfg(target_arch = "aarch64")]
#[derive(Copy, Clone)]
pub(super) struct Neon([uint32x4_t; 2]);

#[cfg(target_arch = "aarch64")]
impl HashVector<SIMD_LANES> for Neon {
    /// The four values' 16 bytes, for the table lookup.
    type Table = uint8x16_t;
    /// A vector of all ones or zero per lane, for each register.
    type Mask = [uint32x4_t; 2];

    #[inline(always)]
    unsafe fn table(values: [u32; 4]) -> Self::Table {
        unsafe { vreinterpretq_u8_u32(vld1q_u32(values.as_ptr())) }
    }

    #[inline(always)]
    unsafe fn from_array(values: [u32; SIMD_LANES]) -> Self {
        let values = values.as_ptr();
        unsafe { Self([vld1q_u32(values), vld1q_u32(values.add(4))]) }
    }

    #[inline(always)]
    unsafe fn to_array(self) -> [u32; SIMD_LANES] {
        let mut values = [0; SIMD_LANES];
        let pointer = values.as_mut_ptr();
        unsafe {
            vst1q_u32(pointer, self.0[0]);
            vst1q_u32(pointer.add(4), self.0[1]);
        }
        values
    }

    #[inline(always)]
    unsafe fn xor(self, other: Self) -> Self {
        let [a, b] = self.0;
        let [c, d] = other.0;
        unsafe { Self([veorq_u32(a, c), veorq_u32(b, d)]) }
    }

    #[inline(always)]
    unsafe fn rotate_left_1(self) -> Self {
        let [a, b] = self.0;
        unsafe { Self([neon::rotate_left_1(a), neon::rotate_left_1(b)]) }
    }

    #[inline(always)]
    unsafe fn rotate_right_1(self) -> Self {
        let [a, b] = self.0;
        unsafe { Self([neon::rotate_right_1(a), neon::rotate_right_1(b)]) }
    }

    #[inline(always)]
    unsafe fn packed_base_at(chunk: Self, offset: u32) -> Self {
        let [a, b] = chunk.0;
        unsafe {
            // A negative shift shifts right.
            let shift = vdupq_n_s32(-2 * offset as i32);
            Self([neon::code(a, shift), neon::code(b, shift)])
        }
    }

    #[inline(always)]
    unsafe fn lookup(table: Self::Table, codes: Self) -> Self {
        let [a, b] = codes.0;
        unsafe { Self([neon::lookup(table, a), neon::lookup(table, b)]) }
    }

    #[inline(always)]
    unsafe fn canonical(forward: Self, reverse: Self) -> Self {
        let ([a, b], [c, d]) = (forward.0, reverse.0);
        unsafe { Self([neon::canonical(a, c), neon::canonical(b, d)]) }
    }

    #[inline(always)]
    unsafe fn forward_mask(forward: Self, reverse: Self) -> u32 {
        let ([a, b], [c, d]) = (forward.0, reverse.0);
        unsafe { neon::forward_mask(a, c) | neon::forward_mask(b, d) << 4 }
    }

    #[inline(always)]
    unsafe fn splat(value: u32) -> Self {
        let a = unsafe { vdupq_n_u32(value) };
        Self([a, a])
    }

    #[inline(always)]
    unsafe fn equal(self, other: Self) -> Self::Mask {
        let ([a, b], [c, d]) = (self.0, other.0);
        unsafe { [vceqq_u32(a, c), vceqq_u32(b, d)] }
    }

    #[inline(always)]
    unsafe fn less(self, other: Self) -> Self::Mask {
        let ([a, b], [c, d]) = (self.0, other.0);
        unsafe { [vcltq_u32(a, c), vcltq_u32(b, d)] }
    }

    #[inline(always)]
    unsafe fn select(mask: Self::Mask, chosen: Self, other: Self) -> Self {
        let ([m, n], [a, b], [c, d]) = (mask, chosen.0, other.0);
        unsafe { Self([vbslq_u32(m, a, c), vbslq_u32(n, b, d)]) }
    }

    #[inline(always)]
    unsafe fn clear_flag(self, mask: Self::Mask) -> Self {
        let ([a, b], [m, n]) = (self.0, mask);
        unsafe { Self([neon::clear_flag(a, m), neon::clear_flag(b, n)]) }
    }

    #[inline(always)]
    unsafe fn mask_bits(mask: Self::Mask) -> u32 {
        let [m, n] = mask;
        unsafe { neon::mask_bits(m) | neon::mask_bits(n) << 4 }
    }

    #[inline(always)]
    unsafe fn load_block(
        bases: &[u8],
        first_bytes: &[u32; SIMD_LANES],
        block: usize,
    ) -> [Self; BLOCK_CHUNKS] {
        let skip = 4 * BLOCK_CHUNKS * block;
        // The last lane reads furthest: if its 32 bytes are in the buffer, all are.
        if first_bytes[SIMD_LANES - 1] as usize + skip + 4 * BLOCK_CHUNKS > bases.len() {
            let chunks = unsafe { Portable::load_block(bases, first_bytes, block) };
            let mut vectors = [unsafe { Self::from_array([0; SIMD_LANES]) }; BLOCK_CHUNKS];
            for index in 0..BLOCK_CHUNKS {
                vectors[index] = unsafe { Self::from_array(chunks[index].0) };
            }
            return vectors;
        }
        let [low, high] = [&first_bytes[..4], &first_bytes[4..]];
        unsafe {
            let [a0, a1, a2, a3] = neon::load_quarter(bases, low, skip);
            let [a4, a5, a6, a7] = neon::load_quarter(bases, low, skip + 16);
            let [b0, b1, b2, b3] = neon::load_quarter(bases, high, skip);
            let [b4, b5, b6, b7] = neon::load_quarter(bases, high, skip + 16);
            [
                Self([a0, b0]),
                Self([a1, b1]),
                Self([a2, b2]),
                Self([a3, b3]),
                Self([a4, b4]),
                Self([a5, b5]),
                Self([a6, b6]),
                Self([a7, b7]),
            ]
        }
    }
}

/// [`Neon`]'s operations on one of its registers, four lanes. Its table lookup looks the tables
/// up.
#[cfg(target_arch = "aarch64")]
mod neon {
    use std::arch::aarch64::*;

    /// The top bit, with the others shifted in above it.
    #[inline(always)]
    pub(super) unsafe fn rotate_left_1(a: uint32x4_t) -> uint32x4_t {
        unsafe { vsliq_n_u32::<1>(vshrq_n_u32::<31>(a), a) }
    }

    /// The bottom bit, with the others shifted in below it.
    #[inline(always)]
    pub(super) unsafe fn rotate_right_1(a: uint32x4_t) -> uint32x4_t {
        unsafe { vsriq_n_u32::<1>(vshlq_n_u32::<31>(a), a) }
    }

    /// `chunk`'s codes at `shift`, minus twice their base's offset.
    #[inline(always)]
    pub(super) unsafe fn code(chunk: uint32x4_t, shift: int32x4_t) -> uint32x4_t {
        unsafe { vandq_u32(vshlq_u32(chunk, shift), vdupq_n_u32(3)) }
    }

    #[inline(always)]
    pub(super) unsafe fn lookup(table: uint8x16_t, codes: uint32x4_t) -> uint32x4_t {
        unsafe {
            // Each lane's code times 4 in its four bytes, plus 0-3: the bytes of its value.
            let bytes = vmlaq_n_u32(vdupq_n_u32(0x0302_0100), codes, 0x0404_0404);
            vreinterpretq_u32_u8(vqtbl1q_u8(table, vreinterpretq_u8_u32(bytes)))
        }
    }

    #[inline(always)]
    pub(super) unsafe fn canonical(forward: uint32x4_t, reverse: uint32x4_t) -> uint32x4_t {
        unsafe {
            let smaller = vshlq_n_u32::<1>(vminq_u32(forward, reverse));
            let unique = vbicq_u32(vdupq_n_u32(1), vceqq_u32(forward, reverse));
            vorrq_u32(smaller, unique)
        }
    }

    #[inline(always)]
    pub(super) unsafe fn forward_mask(forward: uint32x4_t, reverse: uint32x4_t) -> u32 {
        unsafe {
            let bits = [1, 2, 4, 8];
            let less = vcltq_u32(forward, reverse);
            vaddvq_u32(vandq_u32(less, vld1q_u32(bits.as_ptr())))
        }
    }

    #[inline(always)]
    pub(super) unsafe fn clear_flag(a: uint32x4_t, mask: uint32x4_t) -> uint32x4_t {
        unsafe { vbicq_u32(a, vandq_u32(mask, vdupq_n_u32(1))) }
    }

    #[inline(always)]
    pub(super) unsafe fn mask_bits(mask: uint32x4_t) -> u32 {
        unsafe {
            let bits = [1, 2, 4, 8];
            vaddvq_u32(vandq_u32(mask, vld1q_u32(bits.as_ptr())))
        }
    }

    /// Chunks 0-3 of four lanes, from 16 bytes per lane at `skip` bytes from their
    /// `first_bytes`, a vector per chunk: four rows of four 32-bit values, as columns. Loaded as
    /// bytes, which need no alignment, and read as little-endian words.
    #[inline(always)]
    pub(super) unsafe fn load_quarter(
        bases: &[u8],
        first_bytes: &[u32],
        skip: usize,
    ) -> [uint32x4_t; 4] {
        unsafe {
            let row = |lane: usize| {
                let start = bases.as_ptr().add(first_bytes[lane] as usize + skip);
                vreinterpretq_u32_u8(vld1q_u8(start))
            };
            let (a, b, c, d) = (row(0), row(1), row(2), row(3));
            let (ab_low, ab_high) = (vzip1q_u32(a, b), vzip2q_u32(a, b));
            let (cd_low, cd_high) = (vzip1q_u32(c, d), vzip2q_u32(c, d));
            let pairs = |first: uint32x4_t, second: uint32x4_t| {
                let (first, second) = (vreinterpretq_u64_u32(first), vreinterpretq_u64_u32(second));
                [
                    vreinterpretq_u32_u64(vzip1q_u64(first, second)),
                    vreinterpretq_u32_u64(vzip2q_u64(first, second)),
                ]
            };
            let ([column0, column1], [column2, column3]) =
                (pairs(ab_low, cd_low), pairs(ab_high, cd_high));
            [column0, column1, column2, column3]
        }
    }
}

/// The instructions [`CanonicalNtHashLanes::for_each`] compiles its loop for.
///
/// [`CanonicalNtHashLanes::for_each`]: super::nthash::CanonicalNtHashLanes::for_each
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HashKernel {
    #[cfg(target_arch = "x86_64")]
    Avx512,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Ssse3,
    #[cfg(target_arch = "aarch64")]
    Neon,
    /// Plain arrays, what the build targets with no more.
    Portable,
}

impl HashKernel {
    /// The best this CPU supports.
    pub fn detect() -> Self {
        Self::supported()[0]
    }

    pub fn is_supported(self) -> bool {
        match self {
            #[cfg(target_arch = "x86_64")]
            Self::Avx512 => {
                is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512vl")
            }
            #[cfg(target_arch = "x86_64")]
            Self::Avx2 => is_x86_feature_detected!("avx2"),
            #[cfg(target_arch = "x86_64")]
            Self::Ssse3 => is_x86_feature_detected!("ssse3"),
            #[cfg(target_arch = "aarch64")]
            Self::Neon => std::arch::is_aarch64_feature_detected!("neon"),
            Self::Portable => true,
        }
    }

    /// Every kernel this CPU supports, the best first.
    pub fn supported() -> Vec<Self> {
        let all = [
            #[cfg(target_arch = "x86_64")]
            Self::Avx512,
            #[cfg(target_arch = "x86_64")]
            Self::Avx2,
            #[cfg(target_arch = "x86_64")]
            Self::Ssse3,
            #[cfg(target_arch = "aarch64")]
            Self::Neon,
            Self::Portable,
        ];
        all.into_iter()
            .filter(|kernel| kernel.is_supported())
            .collect()
    }
}
