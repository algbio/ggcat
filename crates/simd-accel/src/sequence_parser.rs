//! Parsing FASTA input a block of 64 bytes at a time, from the masks of [`crate::masks`].

use crate::compressed_read::SimdCompressedRead;
use crate::masks::extract_fasta_masks;
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
    /// The masks of `block`, at most [`BLOCK_SIZE`] bytes. A shorter block, the end of the
    /// input, is copied into a zeroed buffer first so the kernel never reads past it: its bits
    /// past the end describe that zero padding.
    #[inline(always)]
    fn of(block: &[u8]) -> Self {
        let masks = match <&[u8; BLOCK_SIZE]>::try_from(block) {
            Ok(block) => extract_fasta_masks(block),
            Err(_) => {
                let mut padded = [0u8; BLOCK_SIZE];
                padded[..block.len()].copy_from_slice(block);
                extract_fasta_masks(&padded)
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
#[derive(Default)]
pub struct SequenceParser {}

impl SequenceParser {
    pub fn new() -> Self {
        Self {}
    }

    /// Takes the next bytes of the input: one block of [`BLOCK_SIZE`] bytes, as io's
    /// `GenericBinaryReader` with that buffer size hands them out, with only the last one of
    /// the input shorter. `callback` is to get every sequence these bytes complete, its bases
    /// already packed two bits each, which `.into()` turns into io's `CompressedRead`.
    pub fn parse_data(
        &mut self,
        data: &[u8],
        callback: impl FnMut(DnaSequence<'_, SimdCompressedRead<'_>>),
    ) {
        // TODO: build the sequences from the masks and hand them to `callback`.
        let _ = callback;
        for block in data.chunks(BLOCK_SIZE) {
            black_box(BlockMasks::of(block));
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

    #[test]
    fn blocks_of_every_length_are_classified() {
        let input = INPUT.repeat(3);
        for len in 1..=BLOCK_SIZE {
            let block = &input[..len];
            assert_eq!(BlockMasks::of(block), reference(block), "length {len}");
        }
    }

    #[test]
    fn parse_data_takes_blocks_and_builds_no_sequence_yet() {
        let input = INPUT.repeat(3);
        let mut parser = SequenceParser::new();
        let mut sequences = 0;
        for block in input.chunks(BLOCK_SIZE) {
            parser.parse_data(block, |_| sequences += 1);
        }
        assert_eq!(sequences, 0);
    }
}
