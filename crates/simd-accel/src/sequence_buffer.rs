//! The sequences the parser hands over, many at a time: their headers and packed bases in two
//! vectors, and where each sequence is in them.

use crate::compressed_read::SimdCompressedRead;
use utils::dna_sequence::{DnaSequence, DnaSequencesFileType};

/// Where one sequence of a [`SequenceBuffer`] is.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SequenceDescription {
    /// Its header: `ident_len` bytes of [`SequenceBuffer::ident`] from `ident_start`, with its
    /// `>` or `@` but without its line ending; none when the headers are not kept.
    pub ident_start: usize,
    pub ident_len: usize,
    /// Its first base in [`SequenceBuffer::bases`], counted in bases from its start.
    pub bases_start: usize,
    /// How many bases it has.
    pub bases_len: usize,
}

/// A batch of sequences of one input, as the parser hands them over.
///
/// The bases are packed four per byte, base `i` in bits `2 * (i % 4)` of byte `i / 4`, A=0, C=1,
/// T=2, G=3, as io's `CompressedRead` holds them. Each sequence starts in a byte of its own, not
/// at the next free base: the bits before its first base in that byte, and up to 15 bytes before
/// it, may be unused, which is what keeps writing FASTQ sequences free of shifts. A sequence too
/// long for the buffer is handed over in parts, each starting with the last bases of the one
/// before (the parser's `copyback_size`), all with the same header.
///
/// Sequences hold only ACGT bases: a record is split at its other bytes, such as `N`, into
/// several sequences with its header, those shorter than the parser's `min_seqlen` left out. The
/// bytes split at stay in [`Self::bases`], between the sequences, as the bases their bits make.
///
/// The parser hands each buffer to its callback by value, to keep or send away, and goes on in
/// another one, which the callback gives back: a new one from [`Self::new`], which holds no
/// memory, or one it is done with. The parser clears it and makes the room it needs in it. A
/// buffer handed over ends at its last sequence, its bases and its headers alike.
pub struct SequenceBuffer {
    pub(crate) ident: Vec<u8>,
    pub(crate) bases: Vec<u8>,
    pub(crate) sequences: Vec<SequenceDescription>,
    /// The index of each sequence's record, from a parser `with_record_indices`; else empty.
    pub(crate) record_indices: Vec<u64>,
    pub(crate) format: DnaSequencesFileType,
}

impl SequenceBuffer {
    /// An empty buffer holding no memory, for a callback to give the parser: the parser makes
    /// the room it needs in it, and sets its format.
    pub fn new() -> Self {
        Self {
            ident: Vec::new(),
            bases: Vec::new(),
            sequences: Vec::new(),
            record_indices: Vec::new(),
            format: DnaSequencesFileType::FASTA,
        }
    }

    /// How many sequences it holds.
    pub fn len(&self) -> usize {
        self.sequences.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sequences.is_empty()
    }

    /// The format of the input the sequences come from.
    pub fn format(&self) -> DnaSequencesFileType {
        self.format
    }

    /// Where each sequence is, in the order of the input.
    pub fn descriptions(&self) -> &[SequenceDescription] {
        &self.sequences
    }

    /// The headers of all the sequences, one after the other.
    pub fn ident(&self) -> &[u8] {
        &self.ident
    }

    /// The bases of all the sequences, packed.
    pub fn bases(&self) -> &[u8] {
        &self.bases
    }

    /// The index of each sequence's record, in the order of [`Self::descriptions`], from a parser
    /// made `with_record_indices`; empty from any other.
    pub fn record_indices(&self) -> &[u64] {
        &self.record_indices
    }

    /// The sequence `description` describes, which has to be one of this buffer's.
    pub fn sequence(
        &self,
        description: &SequenceDescription,
    ) -> DnaSequence<'_, SimdCompressedRead<'_>> {
        let start = description.bases_start;
        DnaSequence {
            ident_data: &self.ident
                [description.ident_start..description.ident_start + description.ident_len],
            seq: SimdCompressedRead::new(
                &self.bases[start / 4..],
                (start % 4) as u8,
                description.bases_len,
            ),
            format: self.format,
        }
    }

    /// Every sequence, in the order of the input.
    pub fn sequences(&self) -> impl Iterator<Item = DnaSequence<'_, SimdCompressedRead<'_>>> {
        self.sequences
            .iter()
            .map(|description| self.sequence(description))
    }

    /// Empties it, keeping its memory.
    pub fn clear(&mut self) {
        self.ident.clear();
        self.bases.clear();
        self.sequences.clear();
        self.record_indices.clear();
    }

    /// Empties it for an input of `format`, with room for `bases` bytes of bases and `ident` of
    /// headers: exactly that much if it has less, nothing allocated if it has as much already.
    pub(crate) fn prepare(&mut self, format: DnaSequencesFileType, bases: usize, ident: usize) {
        self.clear();
        self.format = format;
        self.bases.reserve_exact(bases);
        self.ident.reserve_exact(ident);
    }
}

impl Default for SequenceBuffer {
    fn default() -> Self {
        Self::new()
    }
}
