//! A canonical ntHash of 32 bits over [`BaseLanes`]: one lane for a scalar hash, eight for a
//! SIMD one, from the same rolling code. That code is written over [`HashVector`];
//! [`CanonicalNtHashLanes::for_each`] runs it on the best [`HashKernel`] this CPU has. The loop
//! hands each position's vectors to a consumer: `for_each`'s hands the canonical hashes out, and
//! [`super::minqueue`]'s queues them for their minimizers. The consumers, and the dispatch to a
//! kernel, are in `super::tasks`.
//!
//! Base `c`, A=0, C=1, T=2, G=3, hashes to `(2c + 1) * MULTIPLIER` and its complement to
//! `((2c ^ 4) + 1) * MULTIPLIER`: as the 64-bit ntHash of [`crate::cn_nthash`], on 32 bits.

use super::kernels::Portable;
use super::tasks::{HandOut, LanesConsumer, LanesTask, OnLanes};
use super::{
    BLOCK_BASES, BLOCK_CHUNKS, BaseLanes, CHUNK_BASES, HashKernel, HashVector, SIMD_LANES,
};

const MULTIPLIER: u32 = 0x6ae3_30f9;

/// The blocks the hash keeps: the bases leaving the m-mers are up to `m - 1` positions behind
/// those coming in, so `m` can be up to [`MAX_M`].
const RING_BLOCKS: usize = 4;
/// The longest m-mer hashed.
pub const MAX_M: usize = (RING_BLOCKS - 1) * BLOCK_BASES + 1;

/// The hashes of the m-mers at one position of every lane, of their forward strand and of
/// their reverse complement: what iterating gives.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NtHashPairs<const LANES: usize> {
    pub forward: [u32; LANES],
    pub reverse: [u32; LANES],
}

impl<const LANES: usize> NtHashPairs<LANES> {
    /// See [`NtHashLanes::canonical`].
    pub fn canonical(&self) -> [u32; LANES] {
        let mut canonical = [0; LANES];
        for lane in 0..LANES {
            let (forward, reverse) = (self.forward[lane], self.reverse[lane]);
            canonical[lane] = (forward.min(reverse) << 1) | (forward != reverse) as u32;
        }
        canonical
    }

    /// Whether the forward strand is the canonical one, in every lane.
    pub fn is_forward(&self) -> [bool; LANES] {
        let mut forward = [false; LANES];
        for lane in 0..LANES {
            forward[lane] = self.forward[lane] < self.reverse[lane];
        }
        forward
    }
}

/// The canonical hashes of the m-mers at one position of every lane, as a minimizer queue
/// takes them: what [`CanonicalNtHashLanes::for_each`] hands out, computed in the kernel's
/// vectors.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NtHashLanes<const LANES: usize> {
    /// The smaller of the two strands' hashes, shifted left by one, with the unique flag of a
    /// minimizer queue in bit 0: set for the m-mers that are not their own reverse complement.
    /// Without separating duplicates, `canonical & !1`.
    pub canonical: [u32; LANES],
    /// Bit `lane` set where the forward strand is the canonical one.
    pub forward: u32,
}

/// The chunk one of the two bases the hash rolls over is in, the one leaving the m-mers or the
/// one coming in, with its index among the lanes' chunks.
#[derive(Copy, Clone)]
struct Current<V> {
    chunk: V,
    index: usize,
}

/// What the hashes of every lane roll with, on the vectors of one kernel: the lanes, and the
/// hashes of the bases. What changes as they roll is a [`RollState`] and [`Chunks`], apart, so
/// that the loop keeps the state in registers.
#[derive(Clone)]
struct Roller<S, V: HashVector<LANES>, const LANES: usize> {
    seq: S,
    m_minus1: usize,
    /// Positions are counted from the start of the blocks, this many bases before the lanes.
    phase: usize,
    /// Each base's hash and its complement's, as they come into the m-mer, and rotated by
    /// `m - 1` as they leave it.
    base_hashes: V::Table,
    complement_hashes: V::Table,
    base_leaving: V::Table,
    complement_coming: V::Table,
}

/// The hashes as they roll, and where they are.
#[derive(Copy, Clone)]
struct RollState<V> {
    forward: V,
    reverse: V,
    index: usize,
    /// How many blocks have been read into the [`Chunks`].
    blocks: usize,
    leaving: Current<V>,
    coming: Current<V>,
}

/// The chunks of the last blocks read, chunk `i` at `i % (RING_BLOCKS * BLOCK_CHUNKS)`: both
/// bases the hash rolls over read them.
type Chunks<V> = [V; RING_BLOCKS * BLOCK_CHUNKS];

impl<S: BaseLanes<LANES>, V: HashVector<LANES>, const LANES: usize> Roller<S, V, LANES> {
    /// # Safety
    ///
    /// As [`HashVector`]'s; the lanes are `m` bases long at least, `m` is not zero and at most
    /// [`MAX_M`].
    #[inline(always)]
    unsafe fn new(seq: S, m: usize) -> (Self, RollState<V>, Chunks<V>) {
        let base_hashes: [u32; 4] =
            std::array::from_fn(|code| ((2 * code as u32) + 1).wrapping_mul(MULTIPLIER));
        let complement_hashes: [u32; 4] =
            std::array::from_fn(|code| (((2 * code as u32) ^ 4) + 1).wrapping_mul(MULTIPLIER));
        // The first m - 1 bases, each rotated by its place: once, so lane by lane.
        let (mut forward, mut reverse) = ([0u32; LANES], [0u32; LANES]);
        for i in 0..m - 1 {
            let codes = unsafe { seq.gather_at_pos(i) };
            for lane in 0..LANES {
                let code = codes[lane] as usize;
                forward[lane] ^= base_hashes[code].rotate_left((m - i - 2) as u32);
                reverse[lane] ^= complement_hashes[code].rotate_left(i as u32);
            }
        }
        unsafe {
            let empty = Current {
                chunk: V::from_array([0; LANES]),
                index: usize::MAX,
            };
            let state = RollState {
                forward: V::from_array(forward),
                reverse: V::from_array(reverse),
                index: 0,
                blocks: 0,
                leaving: empty,
                coming: empty,
            };
            let roller = Self {
                m_minus1: m - 1,
                phase: seq.phase(),
                base_hashes: V::table(base_hashes),
                complement_hashes: V::table(complement_hashes),
                base_leaving: V::table(base_hashes.map(|hash| hash.rotate_left((m - 1) as u32))),
                complement_coming: V::table(
                    complement_hashes.map(|hash| hash.rotate_left((m - 1) as u32)),
                ),
                seq,
            };
            (roller, state, [empty.chunk; RING_BLOCKS * BLOCK_CHUNKS])
        }
    }

    /// How many positions are left.
    #[inline(always)]
    fn remaining(&self, state: &RollState<V>) -> usize {
        (self.seq.lane_bases() - self.m_minus1).saturating_sub(state.index)
    }

    /// Reads the blocks up to the one chunk `index` is in, into `chunks`, those read so far
    /// counted by `blocks`.
    #[inline(always)]
    unsafe fn read_blocks_to(&self, blocks: &mut usize, chunks: &mut Chunks<V>, index: usize) {
        while *blocks * BLOCK_CHUNKS <= index {
            let block = unsafe { self.seq.gather_block::<V>(*blocks) };
            let first = (*blocks % RING_BLOCKS) * BLOCK_CHUNKS;
            for (offset, &chunk) in block.iter().enumerate() {
                // SAFETY: below the ring's length, a whole number of blocks.
                unsafe { *chunks.get_unchecked_mut(first + offset) = chunk };
            }
            *blocks += 1;
        }
    }

    /// One roll of the hashes `forward` and `reverse`, with the codes of the bases coming in
    /// and leaving: the hashes of the m-mers they end, of both strands, and the rolled ones.
    #[inline(always)]
    unsafe fn roll_once(&self, forward: V, reverse: V, coming: V, leaving: V) -> [V; 4] {
        unsafe {
            let hash = forward
                .rotate_left_1()
                .xor(V::lookup(self.base_hashes, coming));
            let complement = reverse.xor(V::lookup(self.complement_coming, coming));
            [
                hash,
                complement,
                hash.xor(V::lookup(self.base_leaving, leaving)),
                complement
                    .xor(V::lookup(self.complement_hashes, leaving))
                    .rotate_right_1(),
            ]
        }
    }

    /// The hashes of the next position, which there is, of the forward strands and of the
    /// reverse complements, and the state after it: a position at a time, for iterating.
    ///
    /// # Safety
    ///
    /// As [`HashVector`]'s; [`Self::remaining`] is not zero.
    #[inline(always)]
    unsafe fn step(&self, state: RollState<V>, chunks: &mut Chunks<V>) -> (RollState<V>, V, V) {
        let RollState {
            forward,
            reverse,
            index,
            mut blocks,
            mut leaving,
            mut coming,
        } = state;
        let position = index + self.phase;
        let coming_position = position + self.m_minus1;
        let coming_index = coming_position / CHUNK_BASES;
        if coming_index != coming.index {
            unsafe { self.read_blocks_to(&mut blocks, chunks, coming_index) };
            coming = Current {
                chunk: chunks[coming_index % (RING_BLOCKS * BLOCK_CHUNKS)],
                index: coming_index,
            };
        }
        let leaving_index = position / CHUNK_BASES;
        if leaving_index != leaving.index {
            leaving = Current {
                chunk: chunks[leaving_index % (RING_BLOCKS * BLOCK_CHUNKS)],
                index: leaving_index,
            };
        }
        unsafe {
            let coming_codes =
                V::packed_base_at(coming.chunk, (coming_position % CHUNK_BASES) as u32);
            let leaving_codes = V::packed_base_at(leaving.chunk, (position % CHUNK_BASES) as u32);
            let [hash, complement, forward, reverse] =
                self.roll_once(forward, reverse, coming_codes, leaving_codes);
            let next = RollState {
                forward,
                reverse,
                index: index + 1,
                blocks,
                leaving,
                coming,
            };
            (next, hash, complement)
        }
    }
}

/// The canonical ntHash of every m-mer of every lane, rolled a position at a time, all lanes at
/// once, m-mer `i` of a lane starting at its base `i`. Iterating gives the raw hashes of both
/// strands, [`NtHashPairs`], on plain arrays; [`Self::for_each`] runs on the vectors of a
/// [`HashKernel`] and hands out the canonical ones, [`NtHashLanes`].
#[derive(Clone)]
pub struct CanonicalNtHashLanes<S: BaseLanes<LANES>, const LANES: usize> {
    roller: Roller<S, Portable<LANES>, LANES>,
    state: RollState<Portable<LANES>>,
    chunks: Chunks<Portable<LANES>>,
    seq: S,
    m: usize,
}

impl<S: BaseLanes<LANES> + Copy, const LANES: usize> CanonicalNtHashLanes<S, LANES> {
    /// The hashes of the m-mers of `seq`'s lanes, each lane at least `m` bases long, `m` at most
    /// [`MAX_M`].
    pub fn new(seq: S, m: usize) -> Result<Self, &'static str> {
        if m == 0 || m > seq.lane_bases() || m > MAX_M {
            return Err("m out of range");
        }
        // SAFETY: plain arrays run anywhere; the lanes are long enough, just checked.
        let (roller, state, chunks) = unsafe { Roller::new(seq, m) };
        Ok(Self {
            roller,
            state,
            chunks,
            seq,
            m,
        })
    }
}

impl<S: BaseLanes<SIMD_LANES> + Copy> CanonicalNtHashLanes<S, SIMD_LANES> {
    /// Calls `callback` with each position and its hashes, as iterating would, in a loop
    /// compiled for `kernel`, which this CPU has to support, on its vectors.
    pub fn for_each(
        self,
        kernel: HashKernel,
        callback: impl FnMut(usize, &NtHashLanes<SIMD_LANES>),
    ) {
        self.run(kernel, HandOut(callback));
    }

    /// Runs `task` on the hashes, in a loop compiled for `kernel`, which this CPU has to
    /// support, on its vectors.
    pub(super) fn run(self, kernel: HashKernel, task: impl LanesTask<SIMD_LANES>) {
        // The lanes and m were checked when made.
        kernel.run(OnLanes {
            seq: self.seq,
            m: self.m,
            task,
        });
    }
}

impl<S: BaseLanes<1> + Copy> CanonicalNtHashLanes<S, 1> {
    /// As the eight lanes' [`CanonicalNtHashLanes::for_each`]: one lane is the scalar hash,
    /// the same on every kernel.
    pub fn for_each(self, _kernel: HashKernel, callback: impl FnMut(usize, &NtHashLanes<1>)) {
        // SAFETY: plain arrays run anywhere.
        unsafe { HandOut(callback).run::<_, Portable<1>>(self.seq, self.m) }
    }
}

impl<S: BaseLanes<LANES>, const LANES: usize> Iterator for CanonicalNtHashLanes<S, LANES> {
    type Item = NtHashPairs<LANES>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: plain arrays run anywhere; a position is left, just checked.
        (self.roller.remaining(&self.state) > 0).then(|| unsafe {
            let (next, forward, reverse) = self.roller.step(self.state, &mut self.chunks);
            self.state = next;
            NtHashPairs {
                forward: forward.to_array(),
                reverse: reverse.to_array(),
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.roller.remaining(&self.state);
        (remaining, Some(remaining))
    }
}

impl<S: BaseLanes<LANES>, const LANES: usize> ExactSizeIterator for CanonicalNtHashLanes<S, LANES> {}

/// The hashing loop, on the vectors `V`, handing every position's hashes to `consumer`.
///
/// # Safety
///
/// As [`HashVector`]'s; the lanes are `m` bases long at least, `m` is not zero.
#[inline(always)]
pub(super) unsafe fn roll<
    S: BaseLanes<LANES>,
    V: HashVector<LANES>,
    C: LanesConsumer<V, LANES>,
    const LANES: usize,
>(
    seq: S,
    m: usize,
    consumer: &mut C,
) {
    let (roller, state, mut chunks) = unsafe { Roller::<S, V, LANES>::new(seq, m) };
    let count = roller.remaining(&state);
    let (mut forward, mut reverse, mut blocks) = (state.forward, state.reverse, state.blocks);
    let ring = RING_BLOCKS * BLOCK_CHUNKS;
    let mut index = 0;
    // In runs over which neither base's chunk changes, so that the loop over a run has nothing
    // but the hashing in it, and keeps it in registers.
    while index < count {
        let position = index + roller.phase;
        let coming_position = position + roller.m_minus1;
        unsafe { roller.read_blocks_to(&mut blocks, &mut chunks, coming_position / CHUNK_BASES) };
        let coming = chunks[(coming_position / CHUNK_BASES) % ring];
        let leaving = chunks[(position / CHUNK_BASES) % ring];
        let (coming_offset, leaving_offset) =
            (coming_position % CHUNK_BASES, position % CHUNK_BASES);
        let run = (CHUNK_BASES - coming_offset.max(leaving_offset)).min(count - index);
        let run = unsafe { consumer.prepare(index, run) };
        debug_assert!(run > 0, "a run of no positions");
        for offset in 0..run {
            unsafe {
                let coming_codes = V::packed_base_at(coming, (coming_offset + offset) as u32);
                let leaving_codes = V::packed_base_at(leaving, (leaving_offset + offset) as u32);
                let [hash, complement, next_forward, next_reverse] =
                    roller.roll_once(forward, reverse, coming_codes, leaving_codes);
                forward = next_forward;
                reverse = next_reverse;
                consumer.consume(index + offset, hash, complement);
            }
        }
        index += run;
    }
    unsafe { consumer.finish() };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::{PackedLanes, SequenceLane};

    /// `count` random base codes, from `seed`.
    fn codes(count: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..count)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state % 4) as u8
            })
            .collect()
    }

    /// `codes` packed four to a byte, from base `first` of the first byte.
    fn packed(codes: &[u8], first: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; (first + codes.len()).div_ceil(4).max(1)];
        for (index, &code) in codes.iter().enumerate() {
            let base = first + index;
            bytes[base / 4] |= code << (2 * (base % 4));
        }
        bytes
    }

    /// The hashes of the m-mer of `codes` at `start`, from their definition: every base's hash
    /// rotated by its distance from the m-mer's end, and its complement's by its distance from
    /// the start.
    fn from_scratch(codes: &[u8], start: usize, m: usize) -> (u32, u32) {
        let (mut forward, mut reverse) = (0u32, 0u32);
        for (offset, &code) in codes[start..start + m].iter().enumerate() {
            let code = code as u32;
            let base = (2 * code + 1).wrapping_mul(MULTIPLIER);
            let complement = ((2 * code ^ 4) + 1).wrapping_mul(MULTIPLIER);
            forward ^= base.rotate_left((m - 1 - offset) as u32);
            reverse ^= complement.rotate_left(offset as u32);
        }
        (forward, reverse)
    }

    #[test]
    fn one_lane_rolls_the_hash_of_every_m_mer() {
        for m in [1, 2, 11, 31, 32, 33, 63] {
            let codes = codes(500, m as u64);
            let bytes = packed(&codes, 0);
            let lanes = PackedLanes::<1>::new(&bytes, 0, codes.len(), 0);
            let hashes: Vec<_> = CanonicalNtHashLanes::new(lanes, m).unwrap().collect();
            assert_eq!(hashes.len(), codes.len() - m + 1, "m {m}");
            for (start, hash) in hashes.iter().enumerate() {
                let (forward, reverse) = from_scratch(&codes, start, m);
                assert_eq!(
                    (hash.forward[0], hash.reverse[0]),
                    (forward, reverse),
                    "m {m}, start {start}"
                );
            }
        }
    }

    #[test]
    fn eight_lanes_hash_what_one_lane_hashes_at_their_offsets() {
        for m in [1, 11, 31] {
            for count in [m, m + 1, 100, 1000, 4099] {
                for first in [0, 1, 3] {
                    for overlap in [0, m - 1, 30] {
                        let codes = codes(count, (m * count + first) as u64);
                        let bytes = packed(&codes, first);
                        let one: Vec<_> = CanonicalNtHashLanes::new(
                            PackedLanes::<1>::new(&bytes, first, count, 0),
                            m,
                        )
                        .unwrap()
                        .collect();
                        let lanes = PackedLanes::<SIMD_LANES>::new(&bytes, first, count, overlap);
                        if lanes.lane_bases() < m {
                            continue;
                        }
                        // Every m-mer of the input in some lane, once lanes overlap by m - 1.
                        let mut covered = vec![false; one.len()];
                        for kernel in HashKernel::supported() {
                            let mut seen = 0;
                            CanonicalNtHashLanes::new(lanes, m)
                                .unwrap()
                                .for_each(kernel, |index, hash| {
                                    seen += 1;
                                    for lane in 0..SIMD_LANES {
                                        let start = lanes.offsets()[lane] - first + index;
                                        // Past the input's end: left out.
                                        if let Some(expected) = one.get(start) {
                                            let found = (hash.canonical[lane], hash.forward >> lane & 1 == 1);
                                            assert_eq!(
                                                found,
                                                (expected.canonical()[0], expected.is_forward()[0]),
                                                "{kernel:?}, m {m}, count {count}, first {first}, overlap {overlap}, lane {lane}"
                                            );
                                            covered[start] = true;
                                        }
                                    }
                                });
                            assert_eq!(seen, lanes.lane_bases() - m + 1, "{kernel:?}");
                        }
                        if overlap >= m - 1 {
                            assert!(
                                covered.iter().all(|&covered| covered),
                                "m {m}, count {count}, first {first}, overlap {overlap}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_sequence_hashes_as_its_packed_bases() {
        let codes = codes(300, 7);
        let ascii: Vec<u8> = codes.iter().map(|&code| b"ACTG"[code as usize]).collect();
        let bytes = packed(&codes, 0);
        for m in [1, 11, 31] {
            let from_ascii: Vec<_> = CanonicalNtHashLanes::new(SequenceLane(&ascii[..]), m)
                .unwrap()
                .collect();
            let from_packed: Vec<_> =
                CanonicalNtHashLanes::new(PackedLanes::<1>::new(&bytes, 0, codes.len(), 0), m)
                    .unwrap()
                    .collect();
            assert_eq!(from_ascii, from_packed, "m {m}");
        }
    }

    #[test]
    fn the_canonical_hash_is_the_smaller_strand_with_the_unique_flag() {
        // ACGT and GTAC are their own reverse complements: half the 4-mers of every lane.
        let codes: Vec<u8> = b"ACGT"
            .repeat(300)
            .iter()
            .map(|&base| (base >> 1) & 3)
            .collect();
        let bytes = packed(&codes, 0);
        let one: Vec<_> =
            CanonicalNtHashLanes::new(PackedLanes::<1>::new(&bytes, 0, codes.len(), 0), 4)
                .unwrap()
                .collect();
        let lanes = PackedLanes::<SIMD_LANES>::new(&bytes, 0, codes.len(), 3);
        for kernel in HashKernel::supported() {
            let mut symmetric = 0;
            CanonicalNtHashLanes::new(lanes, 4)
                .unwrap()
                .for_each(kernel, |index, hash| {
                    for lane in 0..SIMD_LANES {
                        let Some(pair) = one.get(lanes.offsets()[lane] + index) else {
                            continue;
                        };
                        let (forward, reverse) = (pair.forward[0], pair.reverse[0]);
                        let expected = (forward.min(reverse) << 1) | (forward != reverse) as u32;
                        assert_eq!(hash.canonical[lane], expected, "{kernel:?}, lane {lane}");
                        assert_eq!(hash.forward >> lane & 1 == 1, forward < reverse);
                        symmetric += (hash.canonical[lane] & 1 == 0) as usize;
                    }
                });
            assert!(symmetric > 500, "{kernel:?}: {symmetric} symmetric 4-mers");
        }
    }
}
