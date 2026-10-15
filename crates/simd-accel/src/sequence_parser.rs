//! Parsing FASTA or FASTQ input a block of 64 bytes at a time, from the masks of
//! [`crate::masks`].

#[cfg(target_arch = "aarch64")]
use crate::masks::Neon;
#[cfg(target_arch = "x86_64")]
use crate::masks::{Avx2, Ssse3};
use crate::masks::{Kernel, MaskKernel, Portable};
use crate::sequence_buffer::{SequenceBuffer, SequenceDescription};
use utils::dna_sequence::DnaSequencesFileType;

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

    /// The first line feed of the block at or after byte `from`, which is in the block.
    #[inline(always)]
    fn line_feed_from(&self, from: usize) -> Option<usize> {
        // Bits of a shorter last block past its end are zero padding, never a line feed.
        let line_feeds = self.line_feeds & (u64::MAX << from);
        (line_feeds != 0).then(|| line_feeds.trailing_zeros() as usize)
    }
}

/// Where the parser is in its input: one set of states for FASTA and one for FASTQ, as
/// [`SequenceParser::reset`] picks the format.
///
/// A state that parses a line ends past its line feed: the next state starts on the first byte
/// of its own line.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ParserState {
    /// In a header line, from its `>` to its line feed. Then [`Self::FastaSequence`].
    FastaHeader,
    /// In the sequence lines of a record, up to the next line that starts with `>`, or the end
    /// of the input. Then [`Self::FastaHeader`].
    FastaSequence,
    /// In a header line, from its `@` to its line feed. Then [`Self::FastqSequence`].
    FastqHeader,
    /// In the sequence line of a record, to its line feed: one line, as the line reader also
    /// reads FASTQ. The line feed ends what is kept of the record, which is handed over there.
    /// Then [`Self::FastqPlus`].
    FastqSequence,
    /// In the separator line, to its line feed: skipped whole, unchecked, so its `+` and the
    /// header it can repeat are not read. Then [`Self::FastqQuality`].
    FastqPlus,
    /// In the quality line, to its line feed: one line, as long as the sequence line, skipped
    /// whole. Then [`Self::FastqHeader`].
    FastqQuality,
}

impl ParserState {
    /// The state an input of `format` starts in.
    fn start(format: DnaSequencesFileType) -> Self {
        match format {
            DnaSequencesFileType::FASTA => Self::FastaHeader,
            DnaSequencesFileType::FASTQ => Self::FastqHeader,
            DnaSequencesFileType::GFA => panic!("the SIMD parser reads FASTA and FASTQ only"),
        }
    }
}

/// What [`SequenceParser::new`] gives a buffer: room for this many bytes of packed bases, 4 M bases.
pub const DEFAULT_BASES_CAPACITY: usize = 1 << 20;
/// What [`SequenceParser::new`] gives a buffer: room for this many bytes of headers.
pub const DEFAULT_IDENT_CAPACITY: usize = 256 << 10;

/// Parses FASTA or FASTQ input into sequences, a block of [`BLOCK_SIZE`] bytes at a time, and
/// hands them over a [`SequenceBuffer`] at a time.
///
/// The buffer is handed to the callback when it is nearly full at the end of a sequence (less
/// than 1/16 of either capacity left), when it is full in the middle of one (with a
/// `copyback_size`), or at the end of the input; then it is cleared and filled again.
///
/// The format is chosen by [`Self::reset`], before each input: FASTA until the first one.
/// For now FASTA sequences are a `todo!`.
pub struct SequenceParser {
    /// Fixed for the parser's life, and supported by this CPU.
    kernel: MaskKernel,
    /// Whether each sequence gets its header line, or an empty one: as `copy_ident` of the
    /// line reader.
    keep_ident: bool,
    /// How many bases a sequence split in the middle, as the buffer is full, repeats at the
    /// start of its next part: as `line_split_copyback` of the line reader. `None` splits none,
    /// growing the buffer past its capacity for a sequence that needs it.
    copyback_size: Option<usize>,
    /// When the buffer counts as full: bytes of packed bases, bytes of headers.
    bases_capacity: usize,
    ident_capacity: usize,
    /// The sequences parsed and not handed over yet, the current record's header and bases
    /// included.
    buffer: SequenceBuffer,
    /// Where the current record's header starts in the buffer's `ident`: it is written there
    /// as it is parsed.
    record_ident_start: usize,
    /// Where the current record's first base is in the buffer's `bases`, in bases, once it has
    /// one.
    ///
    /// A FASTQ sequence, one contiguous line, goes in a whole block at a time, as the masks'
    /// `two_bits` hold them, from the end of the buffer: it starts as many bases into its first
    /// block as the line does, so no bits are shifted. Lines that wrap are to go through
    /// [`Self::push_bases`] instead, from a byte of their own: whole 64-bit words only, the
    /// bases past them waiting in `sequence_word` until [`Self::flush_sequence`].
    record_bases_start: usize,
    /// How many bases the current record has so far.
    record_bases: usize,
    /// The unfinished last word of the sequence: its low `sequence_word_bits` bits, the rest
    /// zero.
    sequence_word: u64,
    /// The bits of `sequence_word` in use, below 64.
    sequence_word_bits: usize,
    state: ParserState,
    /// Whether the last slice [`Self::parse_data`] took was not a whole number of blocks: it
    /// must have been the input's last, as the block-wise copy of FASTQ sequences relies on.
    ended_short: bool,
}

impl SequenceParser {
    /// A parser on the fastest kernel this CPU runs, keeping the headers or not, splitting the
    /// sequences too long for its buffer with `copyback_size` bases repeated, or not at all; its
    /// buffer is [`DEFAULT_BASES_CAPACITY`] and [`DEFAULT_IDENT_CAPACITY`] bytes.
    pub fn new(keep_ident: bool, copyback_size: Option<usize>) -> Self {
        Self::with_kernel(
            MaskKernel::detect(),
            keep_ident,
            copyback_size,
            DEFAULT_BASES_CAPACITY,
            DEFAULT_IDENT_CAPACITY,
        )
    }

    /// A parser on `kernel`, which this CPU has to support, with a buffer of `bases_capacity`
    /// bytes of packed bases and `ident_capacity` bytes of headers; otherwise as [`Self::new`].
    ///
    /// # Panics
    ///
    /// If the bases carried over at a split, and two blocks, would take more than half the
    /// buffer: it could then never empty.
    pub fn with_kernel(
        kernel: MaskKernel,
        keep_ident: bool,
        copyback_size: Option<usize>,
        bases_capacity: usize,
        ident_capacity: usize,
    ) -> Self {
        assert!(kernel.is_supported(), "{kernel:?} is not supported here");
        if let Some(copyback) = copyback_size {
            assert!(
                copyback.div_ceil(4) + 2 * BLOCK_SIZE / 4 <= bases_capacity / 2,
                "a copyback of {copyback} bases does not fit twice in {bases_capacity} bytes"
            );
        }
        let format = DnaSequencesFileType::FASTA;
        Self {
            kernel,
            keep_ident,
            copyback_size,
            bases_capacity,
            ident_capacity,
            buffer: SequenceBuffer::new(format),
            record_ident_start: 0,
            record_bases_start: 0,
            record_bases: 0,
            sequence_word: 0,
            sequence_word_bits: 0,
            state: ParserState::start(format),
            ended_short: false,
        }
    }

    pub fn kernel(&self) -> MaskKernel {
        self.kernel
    }

    /// Forgets the input parsed so far, what the buffer holds included, to start a new one of
    /// `format`, FASTA or FASTQ.
    pub fn reset(&mut self, format: DnaSequencesFileType) {
        self.buffer.clear();
        self.buffer.format = format;
        self.start_record();
        self.state = ParserState::start(format);
        self.ended_short = false;
    }

    /// Starts a record at the end of the buffer.
    fn start_record(&mut self) {
        self.record_ident_start = self.buffer.ident.len();
        self.record_bases_start = 0;
        self.record_bases = 0;
        self.sequence_word = 0;
        self.sequence_word_bits = 0;
    }

    /// Takes the next bytes of the input: a whole number of [`BLOCK_SIZE`] blocks, as io's
    /// `GenericBinaryReader` with a buffer size that is a multiple of it hands them out, with
    /// only the last slice of the input shorter. `callback` gets the buffer whenever it is
    /// handed over, its sequences' bases already packed two bits each, which `.into()` turns
    /// into io's `CompressedRead`.
    ///
    /// The kernel costs one branch per call, always taken the same way: the blocks themselves
    /// run in a loop compiled for it, which inlines it. Larger slices make that branch rarer.
    ///
    /// # Panics
    ///
    /// If the previous slice since [`Self::reset`] was not a whole number of blocks.
    pub fn parse_data(&mut self, data: &[u8], callback: impl FnMut(&SequenceBuffer)) {
        assert!(
            !self.ended_short,
            "only the last slice of an input can be shorter than a whole number of blocks"
        );
        self.ended_short = !data.len().is_multiple_of(BLOCK_SIZE);
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
    fn parse_data_avx2(&mut self, data: &[u8], callback: impl FnMut(&SequenceBuffer)) {
        // SAFETY: this function only runs where AVX2 is enabled.
        unsafe { self.parse_blocks::<Avx2>(data, callback) }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "ssse3")]
    fn parse_data_ssse3(&mut self, data: &[u8], callback: impl FnMut(&SequenceBuffer)) {
        // SAFETY: this function only runs where SSSE3 is enabled.
        unsafe { self.parse_blocks::<Ssse3>(data, callback) }
    }

    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "neon")]
    fn parse_data_neon(&mut self, data: &[u8], callback: impl FnMut(&SequenceBuffer)) {
        // SAFETY: this function only runs where NEON is enabled.
        unsafe { self.parse_blocks::<Neon>(data, callback) }
    }

    /// The block loop, compiled into each of the functions above for its kernel.
    ///
    /// Each block is parsed by the state the parser is in, from `processed_index`, where the
    /// previous state stopped, up to where its own part ends, which sets the next state: a block
    /// can hold several, such as the end of a header and the start of its sequence. The loop
    /// ends at the end of the block, [`BLOCK_SIZE`] bytes but for the input's last one. Each
    /// state returns where it stopped: past its line feed, or the end of the block.
    ///
    /// # Safety
    ///
    /// The CPU must support `K`.
    #[inline(always)]
    unsafe fn parse_blocks<K: Kernel>(
        &mut self,
        data: &[u8],
        mut callback: impl FnMut(&SequenceBuffer),
    ) {
        for block in data.chunks(BLOCK_SIZE) {
            // SAFETY: the caller's.
            let masks = unsafe { BlockMasks::of::<K>(block) };
            let mut processed_index = 0;
            while processed_index < block.len() {
                match self.state {
                    ParserState::FastaHeader => {
                        processed_index = self.parse_header(
                            block,
                            &masks,
                            processed_index,
                            ParserState::FastaSequence,
                        );
                    }
                    ParserState::FastqHeader => {
                        processed_index = self.parse_header(
                            block,
                            &masks,
                            processed_index,
                            ParserState::FastqSequence,
                        );
                    }
                    ParserState::FastaSequence => todo!("FASTA sequence"),
                    ParserState::FastqSequence => {
                        processed_index = self.parse_fastq_sequence(
                            block,
                            &masks,
                            processed_index,
                            &mut callback,
                        );
                    }
                    ParserState::FastqPlus => {
                        processed_index = match masks.line_feed_from(processed_index) {
                            Some(line_feed) => {
                                self.state = ParserState::FastqQuality;
                                line_feed + 1
                            }
                            None => block.len(),
                        };
                    }
                    ParserState::FastqQuality => {
                        processed_index = self.parse_fastq_quality(block, &masks, processed_index);
                    }
                }
            }
        }
    }

    /// Parses the header line the parser is in, from `processed_index` to its line feed or, if
    /// the block has none past it, to the end of the block; writes that part to the buffer's
    /// `ident` if asked. Returns where it stopped.
    ///
    /// At the line feed the header is done: a `\r` before it is trimmed, even if it ended the
    /// previous block, and the parser moves past the line feed to `sequence`, the state of its
    /// format that follows.
    #[inline(always)]
    fn parse_header(
        &mut self,
        block: &[u8],
        masks: &BlockMasks,
        processed_index: usize,
        sequence: ParserState,
    ) -> usize {
        debug_assert!(processed_index < block.len());
        let line_feed = masks.line_feed_from(processed_index);
        if self.keep_ident {
            let ident = &mut self.buffer.ident;
            let end = line_feed.unwrap_or(block.len());
            ident.extend_from_slice(&block[processed_index..end]);
            if line_feed.is_some()
                && ident.len() > self.record_ident_start
                && ident.last() == Some(&b'\r')
            {
                ident.pop();
            }
        }
        match line_feed {
            Some(line_feed) => {
                self.state = sequence;
                line_feed + 1
            }
            None => block.len(),
        }
    }

    /// Parses the FASTQ sequence line the parser is in, from `processed_index` to its line feed
    /// or, if the block has none past it, to the end of the block: the block's `two_bits` go
    /// into the buffer whole, with no shifting, as the line is contiguous and its blocks are
    /// consecutive. Returns where it stopped. 20% faster than appending only the line's bases:
    /// `experiments-logs/2026-10-15_fastq-simd-slowdown`.
    ///
    /// At the line feed the record ends, and the parser moves past it to the separator line.
    /// Before it, a buffer full with a `copyback_size` splits the record.
    ///
    /// A `\r` is a line ending, never a base: one before the line feed, or one ending the block
    /// before a line feed in the next, is left out.
    // TODO: the bytes that are not ACGT, such as `N`, are kept as the two bits they happen to
    // have: the sequence is to be split at them (`masks.non_acgt`), as `SimdCompressedRead`
    // cannot hold them.
    #[inline(always)]
    fn parse_fastq_sequence(
        &mut self,
        block: &[u8],
        masks: &BlockMasks,
        processed_index: usize,
        callback: &mut impl FnMut(&SequenceBuffer),
    ) -> usize {
        debug_assert!(processed_index < block.len());
        let line_feed = masks.line_feed_from(processed_index);
        let mut end = line_feed.unwrap_or(block.len());
        if end > processed_index && block[end - 1] == b'\r' {
            end -= 1;
        }
        if end > processed_index {
            let bases = &mut self.buffer.bases;
            if self.record_bases == 0 {
                self.record_bases_start = 4 * bases.len() + processed_index;
            }
            bases.extend_from_slice(&masks.two_bits.to_le_bytes());
            self.record_bases += end - processed_index;
        }
        match line_feed {
            Some(line_feed) => {
                self.end_record(callback);
                self.state = ParserState::FastqPlus;
                line_feed + 1
            }
            None => {
                if let Some(copyback) = self.copyback_size
                    && self.buffer.bases.len() >= self.bases_capacity
                {
                    self.split_record(copyback, callback);
                }
                block.len()
            }
        }
    }

    /// Ends the current record, all its bases in: adds it to the buffer, unless it has none,
    /// and then its header goes too; trims the buffer's bases to the byte of its last one; hands
    /// the buffer over if less than 1/16 of either capacity is left. Then starts the next
    /// record.
    #[inline(always)]
    fn end_record(&mut self, callback: &mut impl FnMut(&SequenceBuffer)) {
        if self.record_bases > 0 {
            self.flush_sequence();
            let buffer = &mut self.buffer;
            let end = self.record_bases_start + self.record_bases;
            buffer.sequences.push(SequenceDescription {
                ident_start: self.record_ident_start,
                ident_len: buffer.ident.len() - self.record_ident_start,
                bases_start: self.record_bases_start,
                bases_len: self.record_bases,
            });
            buffer.bases.truncate(end.div_ceil(4));
        } else {
            self.buffer.ident.truncate(self.record_ident_start);
        }
        let (bases, ident) = (self.buffer.bases.len(), self.buffer.ident.len());
        if bases >= self.bases_capacity - self.bases_capacity / 16
            || ident >= self.ident_capacity - self.ident_capacity / 16
        {
            self.hand_over(callback);
        }
        self.start_record();
    }

    /// Hands the buffer over, if it holds any sequence, and clears it.
    #[inline(always)]
    fn hand_over(&mut self, callback: &mut impl FnMut(&SequenceBuffer)) {
        if !self.buffer.is_empty() {
            callback(&self.buffer);
        }
        self.buffer.clear();
    }

    /// Splits the current record, in the middle of its sequence line as the buffer is full:
    /// the part so far is added and the buffer handed over; then the record goes on in the
    /// cleared buffer from its header and its last `copyback` bases. A record of `copyback`
    /// bases or fewer is only moved, unsplit.
    ///
    /// The record's bases run to the end of the buffer, as a FASTQ line written a block at a
    /// time does between its blocks: so do the ones carried over, from the same place in their
    /// byte, and the next block follows them with no shifting.
    #[cold]
    fn split_record(&mut self, copyback: usize, callback: &mut impl FnMut(&SequenceBuffer)) {
        let end = self.record_bases_start + self.record_bases;
        debug_assert_eq!(end.div_ceil(4), self.buffer.bases.len());
        let keep_from = match self.record_bases > copyback {
            true => end - copyback,
            false => self.record_bases_start,
        };
        if keep_from > self.record_bases_start {
            self.buffer.sequences.push(SequenceDescription {
                ident_start: self.record_ident_start,
                ident_len: self.buffer.ident.len() - self.record_ident_start,
                bases_start: self.record_bases_start,
                bases_len: self.record_bases,
            });
        }
        if !self.buffer.is_empty() {
            callback(&self.buffer);
        }
        let buffer = &mut self.buffer;
        buffer.sequences.clear();
        buffer.ident.drain(..self.record_ident_start);
        buffer.bases.drain(..keep_from / 4);
        self.record_ident_start = 0;
        self.record_bases_start = keep_from % 4;
        self.record_bases = end - keep_from;
    }

    /// Appends to the sequence the bases of bytes `from..to` of a block, from its masks'
    /// `two_bits`: in two halves of up to 32 bases, as one 64-bit word can take them, through
    /// `sequence_word`, which only whole words leave. Nothing is read back from the buffer.
    /// Faster than writing into it directly: `experiments-logs/2026-10-15_push-bases-variants`.
    /// The record is to start at a byte of its own: `record_bases_start` four times the
    /// buffer's length, before the first call.
    #[expect(
        dead_code,
        reason = "for the FASTA sequence lines, which can wrap, still todo"
    )]
    #[inline(always)]
    fn push_bases(&mut self, two_bits: u128, from: usize, to: usize) {
        let count = to - from;
        if count == 0 {
            return;
        }
        let bits = (two_bits >> (2 * from)) & (u128::MAX >> (128 - 2 * count));
        self.push_sequence_bits(bits as u64, 2 * count.min(32));
        if count > 32 {
            self.push_sequence_bits((bits >> 64) as u64, 2 * (count - 32));
        }
        self.record_bases += count;
    }

    /// Appends the `count` low bits of `value`, whose other bits are zero; `count` in 1..=64.
    #[inline(always)]
    fn push_sequence_bits(&mut self, value: u64, count: usize) {
        self.sequence_word |= value << self.sequence_word_bits;
        let total = self.sequence_word_bits + count;
        if total < 64 {
            self.sequence_word_bits = total;
            return;
        }
        self.buffer
            .bases
            .extend_from_slice(&self.sequence_word.to_le_bytes());
        // The bits of `value` that did not fit: none if the word was empty.
        self.sequence_word = match self.sequence_word_bits {
            0 => 0,
            used => value >> (64 - used),
        };
        self.sequence_word_bits = total - 64;
    }

    /// Moves the unfinished last word of the sequence into the buffer: once all its bases are
    /// in. Nothing to move for a FASTQ sequence, written a block at a time.
    #[inline(always)]
    fn flush_sequence(&mut self) {
        let bytes = self.sequence_word_bits.div_ceil(8);
        self.buffer
            .bases
            .extend_from_slice(&self.sequence_word.to_le_bytes()[..bytes]);
        self.sequence_word = 0;
        self.sequence_word_bits = 0;
    }

    /// Skips the FASTQ quality line the parser is in, from `processed_index` to its line feed
    /// or, if the block has none past it, to the end of the block. Returns where it stopped;
    /// past the line feed, the parser moves to the next header.
    #[inline(always)]
    fn parse_fastq_quality(
        &mut self,
        block: &[u8],
        masks: &BlockMasks,
        processed_index: usize,
    ) -> usize {
        debug_assert!(processed_index < block.len());
        match masks.line_feed_from(processed_index) {
            Some(line_feed) => {
                self.state = ParserState::FastqHeader;
                line_feed + 1
            }
            None => block.len(),
        }
    }

    /// Ends the input: ends the record still open, if any, and hands the buffer over to
    /// `callback`, unless it holds no sequence. The blocks alone cannot tell where the input
    /// ends: one a whole number of blocks long has no shorter last one.
    ///
    /// Only a sequence state can end with a record to add: a FASTA record's last line, or a
    /// FASTQ sequence line cut by the end of the input. Elsewhere the record either was added
    /// already or has no bases yet, and only its header is dropped.
    pub fn finish(&mut self, mut callback: impl FnMut(&SequenceBuffer)) {
        match self.state {
            ParserState::FastaSequence | ParserState::FastqSequence => {
                self.end_record(&mut callback)
            }
            ParserState::FastaHeader
            | ParserState::FastqHeader
            | ParserState::FastqPlus
            | ParserState::FastqQuality => {
                self.buffer.ident.truncate(self.record_ident_start);
                self.start_record();
            }
        }
        self.hand_over(&mut callback);
        self.start_record();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compressed_read::SimdCompressedRead;
    use crate::masks::tests::supported_kernels;
    use utils::dna_sequence::DnaSequence;

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

    /// A FASTQ parser on `kernel` with the default buffer.
    fn fastq_parser(kernel: MaskKernel, keep_ident: bool) -> SequenceParser {
        let mut parser = SequenceParser::with_kernel(
            kernel,
            keep_ident,
            None,
            DEFAULT_BASES_CAPACITY,
            DEFAULT_IDENT_CAPACITY,
        );
        parser.reset(DnaSequencesFileType::FASTQ);
        parser
    }

    /// The header of the current record, as written so far.
    fn ident_of(parser: &SequenceParser) -> &[u8] {
        &parser.buffer.ident[parser.record_ident_start..]
    }

    /// A sequence as a buffer hands it over: its header, and its bases' codes, one per base.
    #[derive(Debug, PartialEq)]
    struct Record {
        ident: Vec<u8>,
        codes: Vec<u8>,
    }

    impl Record {
        fn of(sequence: DnaSequence<'_, SimdCompressedRead<'_>>) -> Self {
            assert!(matches!(sequence.format, DnaSequencesFileType::FASTQ));
            let read = sequence.seq;
            let start = read.start as usize;
            assert!(start < 4, "start {start} is not within the first byte");
            // SAFETY: the read borrows the bytes of its `size` bases, from base `start`.
            let bytes =
                unsafe { std::slice::from_raw_parts(read.data, (start + read.size).div_ceil(4)) };
            let codes = (start..start + read.size)
                .map(|base| (bytes[base / 4] >> (2 * (base % 4))) & 3)
                .collect();
            Self {
                ident: sequence.ident_data.to_vec(),
                codes,
            }
        }

        /// The record of header `ident` and bases `bases`, as letters.
        fn expected(ident: &[u8], bases: &[u8]) -> Self {
            Self {
                ident: ident.to_vec(),
                codes: bases.iter().map(|base| (base >> 1) & 3).collect(),
            }
        }
    }

    /// A callback adding the sequences of every buffer it gets to `records`.
    fn collect(records: &mut Vec<Record>) -> impl FnMut(&SequenceBuffer) + '_ {
        |buffer| records.extend(buffer.sequences().map(Record::of))
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
    fn a_new_parser_takes_the_detected_kernel_and_reads_fasta() {
        let parser = SequenceParser::new(false, None);
        assert_eq!(parser.kernel(), MaskKernel::detect());
        assert_eq!(parser.state, ParserState::FastaHeader);
    }

    #[test]
    fn reset_starts_each_format_at_its_header() {
        let mut parser = SequenceParser::new(false, None);
        parser.reset(DnaSequencesFileType::FASTQ);
        assert_eq!(parser.state, ParserState::FastqHeader);
        assert!(matches!(
            parser.buffer.format(),
            DnaSequencesFileType::FASTQ
        ));
        parser.reset(DnaSequencesFileType::FASTA);
        assert_eq!(parser.state, ParserState::FastaHeader);
    }

    #[test]
    #[should_panic(expected = "FASTA and FASTQ only")]
    fn reset_rejects_gfa() {
        SequenceParser::new(false, None).reset(DnaSequencesFileType::GFA);
    }

    #[test]
    fn reset_forgets_the_buffer_and_the_record() {
        let mut parser = SequenceParser::new(true, None);
        parser.reset(DnaSequencesFileType::FASTQ);
        // A whole record, then one cut inside its sequence line: nothing handed over yet.
        parser.parse_data(b"@read1\nACGT\n+\nIIII\n@read2\nACGT", |_| {
            panic!("not full, not finished")
        });
        assert_eq!(parser.buffer.len(), 1);
        assert_eq!(parser.record_bases, 4);
        parser.reset(DnaSequencesFileType::FASTA);
        assert!(parser.buffer.is_empty());
        assert!(parser.buffer.ident().is_empty());
        assert!(parser.buffer.bases().is_empty());
        assert_eq!(parser.record_bases, 0);
        parser.finish(|_| panic!("forgotten"));
    }

    #[test]
    fn a_header_is_kept_up_to_its_line_feed_without_its_carriage_return() {
        let block = b">seq1 description\r\nACGT\n";
        for kernel in supported_kernels() {
            let mut parser = fastq_parser(kernel, true);
            let masks = masks_with(kernel, block);
            let end = parser.parse_header(block, &masks, 0, ParserState::FastaSequence);
            assert_eq!(end, 19, "{kernel:?}: past the line feed");
            assert_eq!(ident_of(&parser), b">seq1 description", "{kernel:?}");
            assert_eq!(parser.state, ParserState::FastaSequence, "{kernel:?}");
        }
    }

    #[test]
    fn a_header_not_kept_is_skipped_from_where_it_starts() {
        let block = b"ACGT\n@read1\nACGT\n+\nIIII\n";
        for kernel in supported_kernels() {
            let mut parser = fastq_parser(kernel, false);
            let masks = masks_with(kernel, block);
            let end = parser.parse_header(block, &masks, 5, ParserState::FastqSequence);
            assert_eq!(end, 12, "{kernel:?}: past the line feed");
            assert!(ident_of(&parser).is_empty(), "{kernel:?}");
            assert_eq!(parser.state, ParserState::FastqSequence, "{kernel:?}");
        }
    }

    #[test]
    fn a_header_spans_blocks_and_loses_a_carriage_return_that_ended_one() {
        // `@`, 62 bytes and a `\r` fill the first block; the line feed starts the second.
        let mut first = vec![b'@'];
        first.extend(std::iter::repeat_n(b'x', BLOCK_SIZE - 2));
        first.push(b'\r');
        let second = b"\nACGT\n";
        for kernel in supported_kernels() {
            let mut parser = fastq_parser(kernel, true);
            // A whole block with no line feed: the loop takes it all, in the header.
            parser.parse_data(&first, |_| panic!("no sequence yet"));
            assert_eq!(ident_of(&parser), first, "{kernel:?}");
            assert_eq!(parser.state, ParserState::FastqHeader, "{kernel:?}");

            let masks = masks_with(kernel, second);
            let end = parser.parse_header(second, &masks, 0, ParserState::FastqSequence);
            assert_eq!(end, 1, "{kernel:?}: past the line feed");
            assert_eq!(ident_of(&parser), &first[..BLOCK_SIZE - 1], "{kernel:?}");
            assert_eq!(parser.state, ParserState::FastqSequence, "{kernel:?}");
        }
    }

    #[test]
    fn a_header_cut_by_the_end_of_a_short_block_is_kept_whole() {
        for kernel in supported_kernels() {
            let mut parser = fastq_parser(kernel, true);
            parser.parse_data(b"@seq1 description", |_| panic!("no sequence yet"));
            assert_eq!(ident_of(&parser), b"@seq1 description", "{kernel:?}");
            assert_eq!(parser.state, ParserState::FastqHeader, "{kernel:?}");
        }
    }

    #[test]
    fn a_separator_line_is_skipped_whole_unchecked() {
        // Not even a `+`, and longer than two blocks.
        let line: Vec<u8> = std::iter::repeat_n(b'x', 2 * BLOCK_SIZE)
            .chain(*b"\r\n")
            .collect();
        for kernel in supported_kernels() {
            let mut parser = fastq_parser(kernel, false);
            parser.state = ParserState::FastqPlus;
            parser.parse_data(&line, |_| panic!("no sequence"));
            assert_eq!(parser.state, ParserState::FastqQuality, "{kernel:?}");
        }
    }

    #[test]
    fn a_fastq_record_is_added_at_the_end_of_its_sequence_and_handed_over_at_the_end() {
        for kernel in supported_kernels() {
            for keep_ident in [true, false] {
                let mut parser = fastq_parser(kernel, keep_ident);
                // The first block ends inside the quality line, past the sequence's.
                let mut input = b"@read1 x\r\nACGTNacgt\r\n+\r\n".to_vec();
                input.extend(std::iter::repeat_n(b'I', 60));
                input.extend_from_slice(b"\r\n");
                let (first, rest) = input.split_at(BLOCK_SIZE);
                parser.parse_data(first, |_| panic!("not full"));
                assert_eq!(parser.buffer.len(), 1, "{kernel:?}");
                assert_eq!(parser.state, ParserState::FastqQuality, "{kernel:?}");
                assert_eq!(parser.record_bases, 0, "{kernel:?}");
                // Trimmed to the byte of its last base: 9 bases from base 10 of the block.
                assert_eq!(parser.buffer.bases().len(), 5, "{kernel:?}");

                let mut records = Vec::new();
                parser.parse_data(rest, |_| panic!("not full"));
                parser.finish(collect(&mut records));
                let ident: &[u8] = if keep_ident { b"@read1 x" } else { b"" };
                let expected = Record::expected(ident, b"ACGTNacgt");
                assert_eq!(records, [expected], "{kernel:?}, keep_ident {keep_ident}");
                assert!(parser.buffer.is_empty(), "{kernel:?}");
            }
        }
    }

    #[test]
    fn a_fastq_record_with_no_bases_is_dropped_with_its_header() {
        for kernel in supported_kernels() {
            let mut parser = fastq_parser(kernel, true);
            let mut records = Vec::new();
            parser.parse_data(b"@empty\n\n+\n\n@read\nAC\n+\nII\n", |_| panic!("not full"));
            assert_eq!(parser.buffer.ident(), b"@read", "{kernel:?}");
            parser.finish(collect(&mut records));
            assert_eq!(records, [Record::expected(b"@read", b"AC")], "{kernel:?}");
        }
    }

    #[test]
    fn fastq_parsing_does_not_depend_on_where_the_input_is_cut() {
        // Two records, the first header longer by 0 to 63 bytes, so that every line ending lands
        // at every place in a block, cut into two calls at every block boundary. The second
        // header, and the separator repeating it, are longer than a block; its sequence spans
        // three.
        let header = b"@read2 a description long enough to fill a whole block on its own";
        let bases: Vec<u8> = b"ACGTNacgtn".iter().copied().cycle().take(150).collect();
        for shift in 0..BLOCK_SIZE {
            let first_header: Vec<u8> = b"@read1"
                .iter()
                .copied()
                .chain(std::iter::repeat_n(b'x', shift))
                .collect();
            fastq_parsing_does_not_depend_on_the_cut(&first_header, header, &bases);
        }
    }

    fn fastq_parsing_does_not_depend_on_the_cut(first_header: &[u8], header: &[u8], bases: &[u8]) {
        let mut input = first_header.to_vec();
        input.extend_from_slice(b"\r\nACGT\r\n+\r\nIIII\r\n");
        input.extend_from_slice(header);
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(bases);
        input.extend_from_slice(b"\r\n+");
        input.extend_from_slice(&header[1..]);
        input.extend_from_slice(b"\r\n");
        input.extend(std::iter::repeat_n(b'I', bases.len()));
        input.extend_from_slice(b"\r\n");
        let expected = [
            Record::expected(first_header, b"ACGT"),
            Record::expected(header, bases),
        ];
        let shift = first_header.len() - 6;
        for kernel in supported_kernels() {
            for cut in (0..=input.len()).step_by(BLOCK_SIZE) {
                let mut parser = fastq_parser(kernel, true);
                let mut records = Vec::new();
                parser.parse_data(&input[..cut], |_| panic!("not full"));
                parser.parse_data(&input[cut..], |_| panic!("not full"));
                assert_eq!(
                    parser.state,
                    ParserState::FastqHeader,
                    "{kernel:?}, shift {shift}, cut {cut}"
                );
                parser.finish(collect(&mut records));
                assert_eq!(records, expected, "{kernel:?}, shift {shift}, cut {cut}");
            }
        }
    }

    #[test]
    #[should_panic(expected = "only the last slice of an input")]
    fn only_the_last_slice_of_an_input_can_end_a_block_short() {
        let mut parser = SequenceParser::new(false, None);
        parser.reset(DnaSequencesFileType::FASTQ);
        parser.parse_data(b"@read1\nAC", |_| {});
        parser.parse_data(b"GT\n", |_| {});
    }

    #[test]
    fn finish_hands_over_a_sequence_cut_by_the_end_of_the_input() {
        // Without a line feed, with a line ending cut after its `\r`, and a whole block long.
        let long: Vec<u8> = std::iter::repeat_n(b'T', BLOCK_SIZE - 3).collect();
        for bases in [&b"ACGT"[..], b"ACGT\r", &long] {
            for kernel in supported_kernels() {
                let mut parser = fastq_parser(kernel, true);
                let mut records = Vec::new();
                let mut input = b"@r\n".to_vec();
                input.extend_from_slice(bases);
                parser.parse_data(&input, |_| panic!("not full"));
                parser.finish(collect(&mut records));
                let bases = bases.strip_suffix(b"\r").unwrap_or(bases);
                assert_eq!(records, [Record::expected(b"@r", bases)], "{kernel:?}");
                assert!(parser.buffer.is_empty(), "{kernel:?}");
                assert!(parser.buffer.ident().is_empty(), "{kernel:?}");
            }
        }
    }

    #[test]
    fn finish_hands_over_nothing_but_whole_records() {
        let mut parser = SequenceParser::new(true, None);
        parser.reset(DnaSequencesFileType::FASTQ);
        let mut buffers = 0;
        parser.parse_data(b"@r\nACGT\n+\nIIII", |_| buffers += 1);
        parser.finish(|buffer| {
            assert_eq!(buffer.len(), 1);
            assert_eq!(buffer.ident(), b"@r");
            buffers += 1;
        });
        assert_eq!(buffers, 1);
        // A header with no sequence is only forgotten; an empty input hands nothing over.
        parser.reset(DnaSequencesFileType::FASTQ);
        parser.parse_data(b"@cut", |_| buffers += 1);
        parser.finish(|_| buffers += 1);
        parser.reset(DnaSequencesFileType::FASTQ);
        parser.finish(|_| buffers += 1);
        assert_eq!(buffers, 1);
        assert!(parser.buffer.ident().is_empty());
    }

    /// `count` FASTQ reads, of 1 to `max` bases, from a fixed seed: their headers and bases.
    fn reads(count: usize, max: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..count)
            .map(|index| {
                let length = 1 + (next() as usize) % max;
                let bases = (0..length)
                    .map(|_| b"ACGTacgt"[(next() % 8) as usize])
                    .collect();
                (format!("@read{index}").into_bytes(), bases)
            })
            .collect()
    }

    fn fastq_of(reads: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
        let mut input = Vec::new();
        for (header, bases) in reads {
            input.extend_from_slice(header);
            input.push(b'\n');
            input.extend_from_slice(bases);
            input.extend_from_slice(b"\n+\n");
            input.extend(std::iter::repeat_n(b'I', bases.len()));
            input.push(b'\n');
        }
        input
    }

    #[test]
    fn a_buffer_is_handed_over_when_nearly_full_of_bases_at_the_end_of_a_sequence() {
        let reads = reads(100, 60);
        let input = fastq_of(&reads);
        let capacity = 128;
        for kernel in supported_kernels() {
            let mut parser = SequenceParser::with_kernel(kernel, true, None, capacity, 1 << 20);
            parser.reset(DnaSequencesFileType::FASTQ);
            let mut records = Vec::new();
            let mut sizes = Vec::new();
            let mut callback = |buffer: &SequenceBuffer| {
                sizes.push(buffer.bases().len());
                records.extend(buffer.sequences().map(Record::of));
            };
            parser.parse_data(&input, &mut callback);
            parser.finish(&mut callback);
            let expected: Vec<Record> = reads
                .iter()
                .map(|(header, bases)| Record::expected(header, bases))
                .collect();
            assert_eq!(records, expected, "{kernel:?}");
            assert!(sizes.len() > 5, "{kernel:?}: {sizes:?}");
            // Every buffer but the last went once nearly full.
            for &size in &sizes[..sizes.len() - 1] {
                assert!(size >= capacity - capacity / 16, "{kernel:?}: {sizes:?}");
            }
        }
    }

    #[test]
    fn a_buffer_is_handed_over_when_nearly_full_of_headers() {
        let reads = reads(20, 10);
        let input = fastq_of(&reads);
        for kernel in supported_kernels() {
            let mut parser = SequenceParser::with_kernel(kernel, true, None, 1 << 20, 32);
            parser.reset(DnaSequencesFileType::FASTQ);
            let mut lengths = Vec::new();
            parser.parse_data(&input, |buffer| lengths.push(buffer.len()));
            parser.finish(|buffer| lengths.push(buffer.len()));
            // "@readN" headers of 6 or 7 bytes: 30 bytes of them fill 32 nearly.
            assert_eq!(lengths.iter().sum::<usize>(), 20, "{kernel:?}");
            assert!(
                lengths.iter().all(|&length| length <= 5),
                "{kernel:?}: {lengths:?}"
            );
        }
    }

    #[test]
    fn without_a_copyback_a_long_sequence_grows_the_buffer() {
        let reads = reads(1, 1)
            .into_iter()
            .map(|(header, _)| (header, b"ACGT".repeat(500)));
        let reads: Vec<_> = reads.collect();
        let input = fastq_of(&reads);
        for kernel in supported_kernels() {
            let mut parser = SequenceParser::with_kernel(kernel, false, None, 128, 1 << 20);
            parser.reset(DnaSequencesFileType::FASTQ);
            let mut records = Vec::new();
            parser.parse_data(&input, collect(&mut records));
            parser.finish(collect(&mut records));
            assert_eq!(records, [Record::expected(b"", &reads[0].1)], "{kernel:?}");
        }
    }

    #[test]
    fn long_sequences_are_split_with_their_last_bases_repeated_and_short_ones_moved() {
        // Reads up to 600 bases in buffers of 256 bytes, 1024 bases, with a copyback of 100.
        let copyback = 100;
        let reads = reads(200, 600);
        let input = fastq_of(&reads);
        for kernel in supported_kernels() {
            let mut parser =
                SequenceParser::with_kernel(kernel, true, Some(copyback), 256, 1 << 20);
            parser.reset(DnaSequencesFileType::FASTQ);
            let mut records = Vec::new();
            let mut callback = |buffer: &SequenceBuffer| {
                // No sequence starts a whole block of bases past where the one before ended.
                for pair in buffer.descriptions().windows(2) {
                    let end = (pair[0].bases_start + pair[0].bases_len).div_ceil(4) * 4;
                    let gap = pair[1].bases_start - end;
                    assert!(gap < BLOCK_SIZE, "{kernel:?}: {gap} bases unused");
                }
                records.extend(buffer.sequences().map(Record::of));
            };
            for chunk in input.chunks(16 * BLOCK_SIZE) {
                parser.parse_data(chunk, &mut callback);
            }
            parser.finish(&mut callback);

            let mut parts = records.iter().peekable();
            let mut splits = 0;
            for (header, bases) in &reads {
                let expected = Record::expected(header, bases).codes;
                let first = parts.next().unwrap();
                assert_eq!(first.ident, *header, "{kernel:?}");
                let mut joined = first.codes.clone();
                while let Some(part) = parts.next_if(|part| part.ident == *header) {
                    splits += 1;
                    assert!(
                        joined.len() > copyback,
                        "{kernel:?}: a short part was split"
                    );
                    assert_eq!(
                        part.codes[..copyback],
                        joined[joined.len() - copyback..],
                        "{kernel:?}: the part does not start with the last bases"
                    );
                    joined.extend_from_slice(&part.codes[copyback..]);
                }
                assert_eq!(
                    joined,
                    expected,
                    "{kernel:?}, {}",
                    String::from_utf8_lossy(header)
                );
            }
            assert!(parts.next().is_none(), "{kernel:?}");
            assert!(splits > 10, "{kernel:?}: only {splits} splits");
        }
    }

    #[test]
    #[should_panic(expected = "does not fit twice")]
    fn a_copyback_has_to_fit_twice_in_the_buffer() {
        SequenceParser::with_kernel(MaskKernel::Portable, false, Some(200), 64, 64);
    }
}
