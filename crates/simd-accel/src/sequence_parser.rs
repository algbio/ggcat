//! Parsing FASTA input a block of 64 bytes at a time, from the masks of [`crate::masks`].

use crate::compressed_read::SimdCompressedRead;
#[cfg(target_arch = "aarch64")]
use crate::masks::Neon;
#[cfg(target_arch = "x86_64")]
use crate::masks::{Avx2, Ssse3};
use crate::masks::{Kernel, MaskKernel, Portable};
use std::hint::black_box;
use utils::dna_sequence::DnaSequence;

/// Bytes the mask kernels classify at once.
pub const BLOCK_SIZE: usize = 64;

/// What the parser needs to know of one block: one bit per byte, two for the bases.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct BlockMasks {
    /// Bit `i` is set when byte `i` is a line feed.
    line_feeds: u64,
    /// Bit `i` is set when byte `i` is not one of `ACGTacgt`.
    non_acgt: u64,
    /// Bits `2i` and `2i + 1` hold bits 1 and 2 of byte `i`, which are A=0, C=1, T=2, G=3 for
    /// the bases in either case. Meaningful only where `non_acgt` is clear.
    two_bits: u128,
}

impl BlockMasks {
    /// The masks of `block`, at most [`BLOCK_SIZE`] bytes, by kernel `K`. A shorter block, the
    /// end of the input, is copied into a zeroed buffer first so the kernel never reads past it:
    /// its bits past the end describe that zero padding.
    ///
    /// # Safety
    ///
    /// The CPU must support `K`.
    #[inline(always)]
    unsafe fn of<K: Kernel>(block: &[u8]) -> Self {
        // SAFETY: the caller's.
        let masks = unsafe {
            match <&[u8; BLOCK_SIZE]>::try_from(block) {
                Ok(block) => K::extract(block),
                Err(_) => {
                    let mut padded = [0u8; BLOCK_SIZE];
                    padded[..block.len()].copy_from_slice(block);
                    K::extract(&padded)
                }
            }
        };
        // The kernels also find `>` and `\r`, which are not kept: they are inlined, so what
        // is not used here is not computed.
        Self {
            line_feeds: masks.line_feeds,
            non_acgt: masks.mask_non_acgt,
            two_bits: masks.two_bits,
        }
    }
}

/// Parses FASTA input into sequences, a block of [`BLOCK_SIZE`] bytes at a time.
///
/// For now it only classifies each block: no sequence is built yet.
pub struct SequenceParser {
    /// Fixed for the parser's life, and supported by this CPU.
    kernel: MaskKernel,
}

impl Default for SequenceParser {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceParser {
    /// A parser on the fastest kernel this CPU runs.
    pub fn new() -> Self {
        Self {
            kernel: MaskKernel::detect(),
        }
    }

    /// A parser on `kernel`, which this CPU has to support.
    pub fn with_kernel(kernel: MaskKernel) -> Self {
        assert!(kernel.is_supported(), "{kernel:?} is not supported here");
        Self { kernel }
    }

    pub fn kernel(&self) -> MaskKernel {
        self.kernel
    }

    /// Takes the next bytes of the input: a whole number of [`BLOCK_SIZE`] blocks, as io's
    /// `GenericBinaryReader` with a buffer size that is a multiple of it hands them out, with
    /// only the last slice of the input shorter. `callback` is to get every sequence these bytes
    /// complete, its bases already packed two bits each, which `.into()` turns into io's
    /// `CompressedRead`.
    ///
    /// The kernel costs one branch per call, always taken the same way: the blocks themselves
    /// run in a loop compiled for it, which inlines it. Larger slices make that branch rarer.
    pub fn parse_data(
        &mut self,
        data: &[u8],
        callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>),
    ) {
        match self.kernel {
            // SAFETY: `new` and `with_kernel` keep only a kernel this CPU supports.
            #[cfg(target_arch = "x86_64")]
            MaskKernel::Avx2 => unsafe { self.parse_data_avx2(data, callback) },
            #[cfg(target_arch = "x86_64")]
            MaskKernel::Ssse3 => unsafe { self.parse_data_ssse3(data, callback) },
            #[cfg(target_arch = "aarch64")]
            MaskKernel::Neon => unsafe { self.parse_data_neon(data, callback) },
            // SAFETY: the portable kernel runs anywhere.
            MaskKernel::Portable => unsafe { self.parse_blocks::<Portable>(data, callback) },
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    fn parse_data_avx2(
        &mut self,
        data: &[u8],
        callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>),
    ) {
        // SAFETY: this function only runs where AVX2 is enabled.
        unsafe { self.parse_blocks::<Avx2>(data, callback) }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "ssse3")]
    fn parse_data_ssse3(
        &mut self,
        data: &[u8],
        callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>),
    ) {
        // SAFETY: this function only runs where SSSE3 is enabled.
        unsafe { self.parse_blocks::<Ssse3>(data, callback) }
    }

    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "neon")]
    fn parse_data_neon(
        &mut self,
        data: &[u8],
        callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>),
    ) {
        // SAFETY: this function only runs where NEON is enabled.
        unsafe { self.parse_blocks::<Neon>(data, callback) }
    }

    /// The block loop, compiled into each of the functions above for its kernel.
    ///
    /// # Safety
    ///
    /// The CPU must support `K`.
    #[inline(always)]
    unsafe fn parse_blocks<K: Kernel>(
        &mut self,
        data: &[u8],
        callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>),
    ) {
        // TODO: build the sequences from the masks and hand them to `callback`.
        let _ = callback;
        for block in data.chunks(BLOCK_SIZE) {
            // SAFETY: the caller's.
            black_box(unsafe { BlockMasks::of::<K>(block) });
        }
    }

    /// Ends the input, handing `callback` the sequence still open, if any. The blocks alone
    /// cannot tell: an input that is a whole number of blocks long has no shorter last one.
    pub fn finish(&mut self, callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>)) {
        // TODO: hand the last sequence to `callback`.
        let _ = callback;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::masks::tests::supported_kernels;

    const INPUT: &[u8] = b">seq1 description\nACGTacgtNNRY\nGGCCAATT\n>seq2\nacgt\r\n";

    /// The masks of `block` computed a byte at a time, zero padding included.
    fn reference(block: &[u8]) -> BlockMasks {
        let mut masks = BlockMasks {
            line_feeds: 0,
            non_acgt: 0,
            two_bits: 0,
        };
        let padded = block.iter().chain(std::iter::repeat(&0)).take(BLOCK_SIZE);
        for (index, &byte) in padded.enumerate() {
            masks.line_feeds |= ((byte == b'\n') as u64) << index;
            masks.non_acgt |=
                (!matches!(byte & 0b1101_1111, b'A' | b'C' | b'G' | b'T') as u64) << index;
            masks.two_bits |= (((byte >> 1) & 3) as u128) << (2 * index);
        }
        masks
    }

    /// The masks of `block` by `kernel`, which this CPU supports.
    fn masks_with(kernel: MaskKernel, block: &[u8]) -> BlockMasks {
        assert!(kernel.is_supported());
        // SAFETY: just checked.
        unsafe {
            match kernel {
                #[cfg(target_arch = "x86_64")]
                MaskKernel::Avx2 => BlockMasks::of::<Avx2>(block),
                #[cfg(target_arch = "x86_64")]
                MaskKernel::Ssse3 => BlockMasks::of::<Ssse3>(block),
                #[cfg(target_arch = "aarch64")]
                MaskKernel::Neon => BlockMasks::of::<Neon>(block),
                MaskKernel::Portable => BlockMasks::of::<Portable>(block),
            }
        }
    }

    #[test]
    fn blocks_of_every_length_are_classified() {
        let input = INPUT.repeat(3);
        for kernel in supported_kernels() {
            for len in 1..=BLOCK_SIZE {
                let block = &input[..len];
                assert_eq!(
                    masks_with(kernel, block),
                    reference(block),
                    "{kernel:?}, length {len}"
                );
            }
        }
    }

    #[test]
    fn parse_data_takes_blocks_and_builds_no_sequence_yet() {
        let input = INPUT.repeat(3);
        for kernel in supported_kernels() {
            let mut parser = SequenceParser::with_kernel(kernel);
            let mut sequences = 0;
            // One block at a time, then all of them at once.
            for block in input.chunks(BLOCK_SIZE) {
                parser.parse_data(block, |_| sequences += 1);
            }
            parser.parse_data(&input, |_| sequences += 1);
            parser.finish(|_| sequences += 1);
            assert_eq!(sequences, 0, "{kernel:?}");
        }
    }

    #[test]
    fn a_new_parser_takes_the_detected_kernel() {
        assert_eq!(SequenceParser::new().kernel(), MaskKernel::detect());
    }
}
