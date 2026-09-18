//! SIMD-accelerated canonical ntHash computation.

use wide::u32x8;

pub const SIMD_LANES: usize = 8;

/// Eight equally-sized, 2-bit packed DNA sequences in time-major layout.
///
/// Every group of eight `u32`s is one 16-base chunk: word `8 * chunk + lane`
/// belongs to `lane`. Within a word, the earliest base occupies the low two bits.
#[derive(Copy, Clone, Debug)]
pub struct PackedSimdSequence<'a, const ACTIVE_LANES: usize> {
    words: &'a [u32],
    bases_count: usize,
}

impl<'a, const ACTIVE_LANES: usize> PackedSimdSequence<'a, ACTIVE_LANES> {
    pub fn new(words: &'a [u32], bases_count: usize) -> Result<Self, &'static str> {
        if ACTIVE_LANES == 0 || ACTIVE_LANES > SIMD_LANES {
            return Err("active lane count must be in 1..=8");
        }
        let chunks = bases_count.div_ceil(16);
        if words.len() != chunks * SIMD_LANES {
            return Err("packed SIMD input has the wrong number of words");
        }
        Ok(Self { words, bases_count })
    }

    pub fn bases_count(&self) -> usize {
        self.bases_count
    }
    pub const fn active_lanes(&self) -> usize {
        ACTIVE_LANES
    }

    #[inline(always)]
    fn hashes(&self, index: usize) -> (u32x8, u32x8) {
        debug_assert!(index < self.bases_count);
        let start = (index / 16) * SIMD_LANES;
        let shift = (index % 16) * 2;
        // The constructor validated that every chunk contains eight words.
        // Load the complete time-major chunk as one vector, then decode the
        // requested base in all lanes with a packed shift and mask.
        let packed = unsafe {
            u32x8::new(
                self.words
                    .as_ptr()
                    .add(start)
                    .cast::<[u32; SIMD_LANES]>()
                    .read_unaligned(),
            )
        };
        let bases = (packed >> shift as u32) & u32x8::splat(3);
        let encoded = bases << 1;
        let multiplier = u32x8::splat(SIMD_MULTIPLIER);
        (
            (encoded + u32x8::splat(1)) * multiplier,
            ((encoded ^ u32x8::splat(4)) + u32x8::splat(1)) * multiplier,
        )
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ExtCanonicalNtHashSimd<const ACTIVE_LANES: usize> {
    forward: u32x8,
    reverse: u32x8,
}

impl<const ACTIVE_LANES: usize> ExtCanonicalNtHashSimd<ACTIVE_LANES> {
    pub fn forward(self) -> [u32; SIMD_LANES] {
        self.forward.to_array()
    }
    pub fn reverse(self) -> [u32; SIMD_LANES] {
        self.reverse.to_array()
    }
    pub fn to_unextendable(self) -> u32x8 {
        self.forward
            .cmp_lt(self.reverse)
            .blend(self.forward, self.reverse)
            << 1
    }
    /// Canonical hash with the low bit set for non-symmetric forward/reverse
    /// pairs, matching the duplicate-separation encoding used by minqueues.
    pub fn to_unextendable_separate_duplicates(self) -> u32x8 {
        self.to_unextendable() | (!self.forward.cmp_eq(self.reverse) & u32x8::splat(1))
    }
    pub fn to_unextendable_array(self) -> [u32; SIMD_LANES] {
        self.to_unextendable().to_array()
    }
    pub fn is_forward(self) -> [bool; SIMD_LANES] {
        let a = self.forward.to_array();
        let b = self.reverse.to_array();
        std::array::from_fn(|i| i < ACTIVE_LANES && a[i] < b[i])
    }
    pub fn is_rc_symmetric(self) -> [bool; SIMD_LANES] {
        let a = self.forward.to_array();
        let b = self.reverse.to_array();
        std::array::from_fn(|i| i < ACTIVE_LANES && a[i] == b[i])
    }
}

impl<const ACTIVE_LANES: usize> ExtCanonicalNtHashSimd<ACTIVE_LANES> {
    /// All-ones in every lane whose forward strand is the canonical one.
    #[inline(always)]
    pub fn is_forward_mask(self) -> u32x8 {
        self.forward.cmp_lt(self.reverse)
    }

    #[inline(always)]
    pub fn to_unextendable_dup<const SEPARATE_DUPLICATES: bool>(self) -> u32x8 {
        if SEPARATE_DUPLICATES {
            self.to_unextendable_separate_duplicates()
        } else {
            self.to_unextendable()
        }
    }
}

/// Hashes paired with the metadata the minimizer queue carries along.
///
/// The metadata packs the m-mer position and its strand, so the winning
/// minimizer of every window arrives with the position it was found at.
#[inline(always)]
pub fn canonical_minimizer_items<const SEPARATE_DUPLICATES: bool>(
    seq: PackedSimdSequence<'_, SIMD_LANES>,
    m: usize,
) -> Result<impl ExactSizeIterator<Item = (u32x8, u32x8)> + '_, &'static str> {
    let one = u32x8::splat(1);
    Ok(CanonicalNtHashSimdIterator::new(seq, m)?
        .enumerate()
        .map(move |(index, hash)| {
            (
                hash.to_unextendable_dup::<SEPARATE_DUPLICATES>(),
                u32x8::splat((index as u32) << 1) | (hash.is_forward_mask() & one),
            )
        }))
}

/// Hashes without metadata, for the consumers that do not need a position.
#[inline(always)]
pub fn canonical_hash_items<const SEPARATE_DUPLICATES: bool>(
    seq: PackedSimdSequence<'_, SIMD_LANES>,
    m: usize,
) -> Result<impl ExactSizeIterator<Item = (u32x8, ())> + '_, &'static str> {
    Ok(CanonicalNtHashSimdIterator::new(seq, m)?
        .map(|hash| (hash.to_unextendable_dup::<SEPARATE_DUPLICATES>(), ())))
}

#[derive(Clone, Debug)]
pub struct CanonicalNtHashSimdIterator<'a, const ACTIVE_LANES: usize> {
    seq: PackedSimdSequence<'a, ACTIVE_LANES>,
    k_minus1: usize,
    index: usize,
    fh: u32x8,
    rc: u32x8,
}

const SIMD_MULTIPLIER: u32 = 0x6ae330f9;
#[cfg(test)]
const SIMD_H_LOOKUP: [u32; 4] = [
    SIMD_MULTIPLIER,
    SIMD_MULTIPLIER.wrapping_mul(3),
    SIMD_MULTIPLIER.wrapping_mul(5),
    SIMD_MULTIPLIER.wrapping_mul(7),
];
#[cfg(test)]
const SIMD_RC_LOOKUP: [u32; 4] = [
    SIMD_MULTIPLIER.wrapping_mul(5),
    SIMD_MULTIPLIER.wrapping_mul(7),
    SIMD_MULTIPLIER,
    SIMD_MULTIPLIER.wrapping_mul(3),
];
#[inline(always)]
fn rotl(v: u32x8, n: usize) -> u32x8 {
    let n = (n & 31) as u32;
    if n == 0 {
        v
    } else {
        (v << n) | (v >> (32 - n))
    }
}
#[inline(always)]
fn rotr(v: u32x8, n: usize) -> u32x8 {
    let n = (n & 31) as u32;
    if n == 0 {
        v
    } else {
        (v >> n) | (v << (32 - n))
    }
}

impl<'a, const ACTIVE_LANES: usize> CanonicalNtHashSimdIterator<'a, ACTIVE_LANES> {
    pub fn new(seq: PackedSimdSequence<'a, ACTIVE_LANES>, k: usize) -> Result<Self, &'static str> {
        if k == 0 || k > seq.bases_count {
            return Err("K out of range!");
        }
        let mut fh = u32x8::splat(0);
        let mut rc_hash = u32x8::splat(0);
        for i in 0..k - 1 {
            let (base_h, base_rc) = seq.hashes(i);
            fh ^= rotl(base_h, k - i - 2);
            rc_hash ^= rotl(base_rc, i);
        }
        Ok(Self {
            seq,
            k_minus1: k - 1,
            index: 0,
            fh,
            rc: rc_hash,
        })
    }
}

impl<const ACTIVE_LANES: usize> Iterator for CanonicalNtHashSimdIterator<'_, ACTIVE_LANES> {
    type Item = ExtCanonicalNtHashSimd<ACTIVE_LANES>;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index + self.k_minus1 >= self.seq.bases_count {
            return None;
        }
        let (outgoing_h, outgoing_rc) = self.seq.hashes(self.index);
        let (incoming_h, incoming_rc) = self.seq.hashes(self.index + self.k_minus1);
        let res = rotl(self.fh, 1) ^ incoming_h;
        self.fh = res ^ rotl(outgoing_h, self.k_minus1);
        let res_rc = self.rc ^ rotl(incoming_rc, self.k_minus1);
        self.rc = rotr(res_rc ^ outgoing_rc, 1);
        self.index += 1;
        Some(ExtCanonicalNtHashSimd {
            forward: res,
            reverse: res_rc,
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.len();
        (n, Some(n))
    }
}
impl<const ACTIVE_LANES: usize> ExactSizeIterator
    for CanonicalNtHashSimdIterator<'_, ACTIVE_LANES>
{
    fn len(&self) -> usize {
        (self.seq.bases_count - self.k_minus1).saturating_sub(self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CanonicalNtHashSimdIterator, PackedSimdSequence, SIMD_H_LOOKUP, SIMD_LANES, SIMD_RC_LOOKUP,
    };

    fn scalar_32_hashes(seq: &[u8], k: usize) -> Vec<(u32, u32)> {
        let mut fh = 0u32;
        let mut rc = 0u32;
        for i in 0..k - 1 {
            fh ^= SIMD_H_LOOKUP[seq[i] as usize].rotate_left((k - i - 2) as u32);
            rc ^= SIMD_RC_LOOKUP[seq[i] as usize].rotate_left(i as u32);
        }
        (0..=seq.len() - k)
            .map(|i| {
                let outgoing = seq[i] as usize;
                let incoming = seq[i + k - 1] as usize;
                let forward = fh.rotate_left(1) ^ SIMD_H_LOOKUP[incoming];
                fh = forward ^ SIMD_H_LOOKUP[outgoing].rotate_left((k - 1) as u32);
                let reverse = rc ^ SIMD_RC_LOOKUP[incoming].rotate_left((k - 1) as u32);
                rc = (reverse ^ SIMD_RC_LOOKUP[outgoing]).rotate_right(1);
                (forward, reverse)
            })
            .collect()
    }

    fn pack_lanes(lanes: &[Vec<u8>; SIMD_LANES]) -> Vec<u32> {
        let mut words = vec![0; lanes[0].len().div_ceil(16) * SIMD_LANES];
        for lane in 0..SIMD_LANES {
            for (i, &base) in lanes[lane].iter().enumerate() {
                words[(i / 16) * SIMD_LANES + lane] |= (base as u32) << ((i % 16) * 2);
            }
        }
        words
    }

    #[test]
    fn simd_matches_scalar_for_packed_sequences() {
        for len in [17, 31, 32, 63, 80] {
            let lanes: [Vec<u8>; SIMD_LANES] = std::array::from_fn(|lane| {
                let mut state = 0x9e37_79b9u32 ^ lane as u32 ^ len as u32;
                (0..len)
                    .map(|i| {
                        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                        if lane == 0 {
                            (i & 3) as u8
                        } else {
                            (state >> 30) as u8
                        }
                    })
                    .collect()
            });
            let packed = pack_lanes(&lanes);
            for k in [1, 15, 16, 17, 31].into_iter().filter(|&k| k <= len) {
                let seq = PackedSimdSequence::<3>::new(&packed, len).unwrap();
                let simd: Vec<_> = CanonicalNtHashSimdIterator::new(seq, k).unwrap().collect();
                for lane in 0..3 {
                    let scalar = scalar_32_hashes(&lanes[lane], k);
                    assert_eq!(simd.len(), scalar.len());
                    for (s, expected) in simd.iter().zip(scalar) {
                        assert_eq!(s.forward()[lane], expected.0);
                        assert_eq!(s.reverse()[lane], expected.1);
                        assert_eq!(
                            s.to_unextendable_array()[lane],
                            expected.0.min(expected.1) << 1
                        );
                        assert_eq!(
                            s.to_unextendable_separate_duplicates().to_array()[lane],
                            (expected.0.min(expected.1) << 1) | u32::from(expected.0 != expected.1)
                        );
                        assert_eq!(s.is_forward()[lane], expected.0 < expected.1);
                        assert_eq!(s.is_rc_symmetric()[lane], expected.0 == expected.1);
                    }
                }
                assert!(
                    simd.iter()
                        .all(|h| !h.is_forward()[3] && !h.is_rc_symmetric()[3])
                );
            }
        }
    }

    #[test]
    fn simd_matches_the_scalar_minimizer_hash() {
        use hashes::cn_nthash32::CanonicalNtHash32Iterator;
        use hashes::{ExtendableHashTraitType, HashFunction};

        const BASES: [u8; 4] = [b'A', b'C', b'T', b'G'];
        for len in [31usize, 64, 97] {
            let lanes: [Vec<u8>; SIMD_LANES] = std::array::from_fn(|lane| {
                let mut state = 0x1234_5678u32 ^ (lane as u32) ^ (len as u32);
                (0..len)
                    .map(|_| {
                        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                        (state >> 29) as u8 & 3
                    })
                    .collect()
            });
            let packed = pack_lanes(&lanes);
            for m in [11usize, 12, 17, 31].into_iter().filter(|&m| m <= len) {
                let sequence = PackedSimdSequence::<SIMD_LANES>::new(&packed, len).unwrap();
                let simd: Vec<_> = CanonicalNtHashSimdIterator::new(sequence, m)
                    .unwrap()
                    .collect();
                for lane in 0..SIMD_LANES {
                    let ascii: Vec<u8> = lanes[lane].iter().map(|&c| BASES[c as usize]).collect();
                    let scalar: Vec<_> = CanonicalNtHash32Iterator::new(ascii.as_slice(), m)
                        .unwrap()
                        .iter()
                        .collect();
                    assert_eq!(simd.len(), scalar.len());
                    for (vector, expected) in simd.iter().zip(scalar) {
                        assert_eq!(vector.forward()[lane], expected.0, "m {m} lane {lane}");
                        assert_eq!(vector.reverse()[lane], expected.1);
                        assert_eq!(
                            vector.to_unextendable().to_array()[lane] as u64,
                            expected.to_unextendable()
                        );
                        assert_eq!(
                            vector.to_unextendable_separate_duplicates().to_array()[lane] as u64,
                            expected.to_unextendable()
                                | u64::from(!ExtendableHashTraitType::is_rc_symmetric(&expected))
                        );
                        assert_eq!(
                            vector.is_forward()[lane],
                            ExtendableHashTraitType::is_forward(&expected)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn packed_simd_sequence_rejects_invalid_shapes() {
        assert!(PackedSimdSequence::<0>::new(&[0; 16], 17).is_err());
        assert!(PackedSimdSequence::<8>::new(&[0; 8], 17).is_err());
        assert!(
            CanonicalNtHashSimdIterator::new(
                PackedSimdSequence::<8>::new(&[0; 16], 17).unwrap(),
                18,
            )
            .is_err()
        );
    }
}
