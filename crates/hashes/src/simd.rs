//! SIMD over lanes of bases: a packed buffer, or any sequence, read as [`SIMD_LANES`] lanes (or
//! one) a block of [`BLOCK_BASES`] at a time, by [`BaseLanes`]; the vector operations on them,
//! [`HashVector`], implemented for each [`HashKernel`] the CPU may have; and what runs on both:
//! the ntHash of [`nthash`], and the minimizer queue of [`minqueue`] fed by it.

use crate::HashableSequence;

mod kernels;
pub mod minqueue;
pub mod nthash;
mod tasks;

pub use kernels::{HashKernel, HashVector};

/// The lanes of a SIMD hash: eight hashes of 32 bits fill an AVX2 register.
pub const SIMD_LANES: usize = 8;

/// The bases of a chunk, two bits each in 32: a lane's chunks are its 32-bit words.
pub const CHUNK_BASES: usize = 16;
/// The chunks of a block: the 32 bytes of one load per lane.
pub const BLOCK_CHUNKS: usize = 8;
/// The bases of a block.
pub const BLOCK_BASES: usize = CHUNK_BASES * BLOCK_CHUNKS;

/// Bases in `LANES` lanes of [`Self::lane_bases`] bases each: what
/// [`nthash::CanonicalNtHashLanes`] hashes, a position of every lane at a time, read a block of
/// [`BLOCK_BASES`] at a time.
pub trait BaseLanes<const LANES: usize> {
    /// How many bases each lane has.
    fn lane_bases(&self) -> usize;

    /// How many bases come before each lane's first in its first chunk, the same for every
    /// lane: the blocks start that much before the lanes.
    fn phase(&self) -> usize;

    /// The chunks of block `block` of every lane, in vectors of the kernel running: chunk `j`
    /// holds the codes, A=0, C=1, T=2, G=3, of the [`CHUNK_BASES`] bases from position
    /// `BLOCK_BASES * block + CHUNK_BASES * j - phase` of every lane, base `i` of it in bits
    /// `2i`; those outside a lane are any.
    ///
    /// # Safety
    ///
    /// The CPU has `V`'s instructions.
    unsafe fn gather_block<V: HashVector<LANES>>(&self, block: usize) -> [V; BLOCK_CHUNKS];

    /// The code of base `pos` of every lane.
    ///
    /// # Safety
    ///
    /// `pos` is below [`Self::lane_bases`].
    unsafe fn gather_at_pos(&self, pos: usize) -> [u32; LANES];
}

/// Bases packed four to a byte, base `i` in bits `2 * (i % 4)` of byte `i / 4`, as the SIMD
/// parser's buffers and io's `CompressedRead` hold them, split into `LANES` lanes of equal
/// length, from equally spaced offsets. Each lane goes `overlap` bases into the next, so that
/// whatever spans the boundary of two lanes is whole in one: `m - 1` bases of overlap for
/// m-mers, `k - 1` for k-mers.
///
/// The lanes are spaced by a whole number of bytes, so that they share their place in their
/// first byte and a block is one load per lane. The last lane can end past the input, by a few
/// bases per lane: those read as A, and what they hash is to be left out.
#[derive(Copy, Clone, Debug)]
pub struct PackedLanes<'a, const LANES: usize> {
    bases: &'a [u8],
    /// Where each lane starts, in bases from the start of `bases`.
    offsets: [u32; LANES],
    /// The byte holding each lane's first base.
    first_bytes: [u32; LANES],
    lane_bases: usize,
}

impl<'a, const LANES: usize> PackedLanes<'a, LANES> {
    /// The `bases_count` bases of `bases` from base `first`, in lanes going `overlap` bases
    /// into each other. With one lane, all of them.
    ///
    /// # Panics
    ///
    /// If `bases` is shorter than the bases asked for, or of 1 GiB or more: the lanes' offsets,
    /// in bases, are kept in 32 bits.
    pub fn new(bases: &'a [u8], first: usize, bases_count: usize, overlap: usize) -> Self {
        assert!(LANES > 0, "at least one lane");
        assert!(
            (first + bases_count).div_ceil(4) <= bases.len(),
            "{bases_count} bases from {first} do not fit in {} bytes",
            bases.len()
        );
        assert!(bases.len() < 1 << 30, "{} bytes are too many", bases.len());
        let stride = bases_count
            .saturating_sub(overlap)
            .div_ceil(LANES)
            .next_multiple_of(4);
        let (mut offsets, mut first_bytes) = ([0; LANES], [0; LANES]);
        for lane in 0..LANES {
            offsets[lane] = (first + lane * stride) as u32;
            first_bytes[lane] = offsets[lane] / 4;
        }
        Self {
            bases,
            offsets,
            first_bytes,
            lane_bases: (stride + overlap).min(bases_count),
        }
    }

    /// Where each lane starts, in bases from the start of the buffer.
    pub fn offsets(&self) -> [usize; LANES] {
        self.offsets.map(|offset| offset as usize)
    }
}

impl<const LANES: usize> BaseLanes<LANES> for PackedLanes<'_, LANES> {
    #[inline(always)]
    fn lane_bases(&self) -> usize {
        self.lane_bases
    }

    #[inline(always)]
    fn phase(&self) -> usize {
        self.offsets[0] as usize % 4
    }

    #[inline(always)]
    unsafe fn gather_block<V: HashVector<LANES>>(&self, block: usize) -> [V; BLOCK_CHUNKS] {
        // SAFETY: the caller's; the first bytes go up with the lanes.
        unsafe { V::load_block(self.bases, &self.first_bytes, block) }
    }

    #[inline(always)]
    unsafe fn gather_at_pos(&self, pos: usize) -> [u32; LANES] {
        let mut codes = [0; LANES];
        for lane in 0..LANES {
            codes[lane] = packed_chunk(self.bases, self.offsets[lane] as usize + pos) & 3;
        }
        codes
    }
}

/// The 32 bits of packed `bases` from byte `byte`; bytes past the end of the buffer read as A.
#[inline(always)]
fn packed_word(bases: &[u8], byte: usize) -> u32 {
    if byte + 4 <= bases.len() {
        // SAFETY: the 4 bytes from `byte` are in the buffer, just checked.
        u32::from_le(unsafe { bases.as_ptr().add(byte).cast::<u32>().read_unaligned() })
    } else {
        // Byte by byte: no call in the hashing loop, which keeps its vectors in registers.
        let mut word = 0;
        for (index, &value) in bases.iter().skip(byte).take(4).enumerate() {
            word |= (value as u32) << (8 * index);
        }
        word
    }
}

/// The bases of packed `bases` from base `base`: 13 at least.
#[inline(always)]
fn packed_chunk(bases: &[u8], base: usize) -> u32 {
    packed_word(bases, base / 4) >> (2 * (base % 4))
}

/// Any sequence the other hashes take, as one lane: the scalar hash.
#[derive(Copy, Clone, Debug)]
pub struct SequenceLane<N: HashableSequence>(pub N);

impl<N: HashableSequence> BaseLanes<1> for SequenceLane<N> {
    #[inline(always)]
    fn lane_bases(&self) -> usize {
        self.0.bases_count()
    }

    #[inline(always)]
    fn phase(&self) -> usize {
        0
    }

    #[inline(always)]
    unsafe fn gather_block<V: HashVector<1>>(&self, block: usize) -> [V; BLOCK_CHUNKS] {
        let mut chunks = [0u32; BLOCK_CHUNKS];
        for (index, chunk) in chunks.iter_mut().enumerate() {
            let start = BLOCK_BASES * block + CHUNK_BASES * index;
            let end = (start + CHUNK_BASES).min(self.0.bases_count());
            for pos in start..end {
                // SAFETY: below the sequence's length.
                *chunk |= unsafe { self.gather_at_pos(pos)[0] } << (2 * (pos - start));
            }
        }
        let mut vectors = [unsafe { V::from_array([0]) }; BLOCK_CHUNKS];
        for index in 0..BLOCK_CHUNKS {
            // SAFETY: the caller's.
            vectors[index] = unsafe { V::from_array([chunks[index]]) };
        }
        vectors
    }

    #[inline(always)]
    unsafe fn gather_at_pos(&self, pos: usize) -> [u32; 1] {
        // SAFETY: the caller's.
        let base = unsafe { self.0.get_unchecked_cbase(pos) };
        [if N::IS_COMPRESSED {
            base
        } else {
            (base >> 1) & 3
        } as u32]
    }
}
