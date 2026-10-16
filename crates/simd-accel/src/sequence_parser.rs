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
    /// In a header line, from its `>` to its line feed. Then [`Self::FastaLineStart`].
    FastaHeader,
    /// At the first byte of a line, which it leaves to the next state: a `>` ends the record,
    /// then [`Self::FastaHeader`]; a `;` starts a comment, [`Self::FastaComment`]; anything else
    /// starts a line of the record's sequence, [`Self::FastaSequence`] while the record has no
    /// bases yet, else [`Self::FastaWrapped`]. `wrapped` once the record went over to that. An
    /// input starts here too: sequence lines before any header make a record with no header,
    /// as the line reader also reads them.
    FastaLineStart { wrapped: bool },
    /// In a comment line, from its `;` to its line feed: skipped whole, wherever a line can
    /// start. Then [`Self::FastaLineStart`], with the same `wrapped`.
    FastaComment { wrapped: bool },
    /// In the first sequence line of a record, to its line feed: written a whole block at a time
    /// with no shifting, as a FASTQ sequence is. A record on one line, as assemblies often are,
    /// needs nothing else. Then [`Self::FastaLineStart`].
    FastaSequence,
    /// In a later sequence line of a record, to its line feed: each piece of it between line
    /// feeds appended shifted after the bases before. Then [`Self::FastaLineStart`].
    FastaWrapped,
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
    /// At the start of a block, in a sequence line the previous block ended with a `\r`, left out
    /// as a line ending: a line feed now makes it one. Otherwise the line goes on, so the `\r` was
    /// a byte of it, added back as one that is not ACGT. Either way the block goes to the line's
    /// own state, kept in [`SequenceParser::after_carriage_return`].
    CarriageReturn,
}

impl ParserState {
    /// The state an input of `format` starts in.
    fn start(format: DnaSequencesFileType) -> Self {
        match format {
            DnaSequencesFileType::FASTA => Self::FastaLineStart { wrapped: false },
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
    /// The shortest sequence handed over, at least 1: shorter pieces of a record, between its
    /// bytes that are not ACGT, are left out.
    min_length: usize,
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
    /// block as the line does, so no bits are shifted. So does a FASTA record's first line; its
    /// later lines go through [`Self::push_bases`] instead, after [`Self::start_shifting`]:
    /// whole 64-bit words only, the bases past them waiting in `sequence_word` until
    /// [`Self::flush_sequence`].
    record_bases_start: usize,
    /// How many bases the current record has so far, its bytes that are not ACGT included: they
    /// are written as the bases their two bits make, and only the sequences skip them.
    record_bases: usize,
    /// Where the current record's open run of ACGT bases starts in the buffer's `bases`, in
    /// bases: each byte that is not ACGT ends one, which becomes a sequence if it is long enough.
    run_start: usize,
    /// How many sequences the buffer held when the current record started: if it holds no more
    /// at its end, the record kept none, and its header goes.
    record_sequences: usize,
    /// How many bytes of the buffer's `bases` to keep at the end of the current record: up to its
    /// last sequence kept, or none of it.
    kept_end: usize,
    /// The unfinished last word of the sequence: its low `sequence_word_bits` bits, the rest
    /// zero.
    sequence_word: u64,
    /// The bits of `sequence_word` in use, below 64.
    sequence_word_bits: usize,
    state: ParserState,
    /// The sequence state [`ParserState::CarriageReturn`] goes back to.
    after_carriage_return: ParserState,
    /// Whether the last slice [`Self::parse_data`] took was not a whole number of blocks: it
    /// must have been the input's last, as the block-wise copy of FASTQ sequences relies on.
    ended_short: bool,
}

impl SequenceParser {
    /// A parser on the fastest kernel this CPU runs, keeping the headers or not, splitting the
    /// sequences too long for its buffer with `copyback_size` bases repeated, or not at all, and
    /// dropping those shorter than `min_seqlen`; its buffer is [`DEFAULT_BASES_CAPACITY`] and
    /// [`DEFAULT_IDENT_CAPACITY`] bytes.
    pub fn new(keep_ident: bool, copyback_size: Option<usize>, min_seqlen: usize) -> Self {
        Self::with_kernel(
            MaskKernel::detect(),
            keep_ident,
            copyback_size,
            min_seqlen,
            DEFAULT_BASES_CAPACITY,
            DEFAULT_IDENT_CAPACITY,
        )
    }

    /// A parser on `kernel`, which this CPU has to support, with a buffer of `bases_capacity`
    /// bytes of packed bases and `ident_capacity` bytes of headers; otherwise as [`Self::new`].
    ///
    /// # Panics
    ///
    /// If the bases carried over at a split (the copyback, or a sequence too short to be split),
    /// and two blocks, would take more than half the buffer: it could then never empty.
    pub fn with_kernel(
        kernel: MaskKernel,
        keep_ident: bool,
        copyback_size: Option<usize>,
        min_seqlen: usize,
        bases_capacity: usize,
        ident_capacity: usize,
    ) -> Self {
        assert!(kernel.is_supported(), "{kernel:?} is not supported here");
        if let Some(copyback) = copyback_size {
            let carried = copyback.max(min_seqlen);
            assert!(
                carried.div_ceil(4) + 2 * BLOCK_SIZE / 4 <= bases_capacity / 2,
                "a copyback of {copyback} bases does not fit twice in {bases_capacity} bytes"
            );
        }
        let format = DnaSequencesFileType::FASTA;
        Self {
            kernel,
            keep_ident,
            copyback_size,
            min_length: min_seqlen.max(1),
            bases_capacity,
            ident_capacity,
            buffer: SequenceBuffer::new(format),
            record_ident_start: 0,
            record_bases_start: 0,
            record_bases: 0,
            run_start: 0,
            record_sequences: 0,
            kept_end: 0,
            sequence_word: 0,
            sequence_word_bits: 0,
            state: ParserState::start(format),
            after_carriage_return: ParserState::start(format),
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
        self.run_start = 0;
        self.record_sequences = self.buffer.sequences.len();
        self.kept_end = self.buffer.bases.len();
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
                            ParserState::FastaLineStart { wrapped: false },
                        );
                    }
                    ParserState::FastaLineStart { wrapped } => {
                        self.parse_fasta_line_start(block, processed_index, wrapped, &mut callback);
                    }
                    ParserState::FastaComment { wrapped } => {
                        processed_index = match masks.line_feed_from(processed_index) {
                            Some(line_feed) => {
                                self.state = ParserState::FastaLineStart { wrapped };
                                line_feed + 1
                            }
                            None => block.len(),
                        };
                    }
                    ParserState::FastaSequence => {
                        processed_index = match self.copy_line_blocks(
                            block,
                            &masks,
                            processed_index,
                            &mut callback,
                        ) {
                            Some(line_feed) => {
                                self.state = ParserState::FastaLineStart { wrapped: false };
                                line_feed + 1
                            }
                            None => block.len(),
                        };
                    }
                    ParserState::FastaWrapped => {
                        processed_index =
                            self.parse_fasta_wrapped(block, &masks, processed_index, &mut callback);
                    }
                    ParserState::FastqHeader => {
                        processed_index = self.parse_header(
                            block,
                            &masks,
                            processed_index,
                            ParserState::FastqSequence,
                        );
                    }
                    ParserState::FastqSequence => {
                        processed_index = match self.copy_line_blocks(
                            block,
                            &masks,
                            processed_index,
                            &mut callback,
                        ) {
                            // The line feed ends what is kept of the record.
                            Some(line_feed) => {
                                self.end_record(&mut callback);
                                self.state = ParserState::FastqPlus;
                                line_feed + 1
                            }
                            None => block.len(),
                        };
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
                    ParserState::CarriageReturn => {
                        debug_assert_eq!(processed_index, 0);
                        self.state = self.after_carriage_return;
                        if block[0] != b'\n' {
                            self.add_carriage_return();
                        }
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

    /// Copies the sequence line the parser is in, from `processed_index` to its line feed or, if
    /// the block has none past it, to the end of the block: the block's `two_bits` go into the
    /// buffer whole, with no shifting, as the line is contiguous and its blocks are consecutive.
    /// Returns the line feed, if the block has it. Copying whole blocks spares the shifts and
    /// masks of appending only the line's bases, for a few unused bits before the record.
    ///
    /// With no line feed the line goes on in the next block: a buffer full then, with a
    /// `copyback_size`, splits the record.
    #[inline(always)]
    fn copy_line_blocks(
        &mut self,
        block: &[u8],
        masks: &BlockMasks,
        processed_index: usize,
        callback: &mut impl FnMut(&SequenceBuffer),
    ) -> Option<usize> {
        debug_assert!(processed_index < block.len());
        let line_feed = masks.line_feed_from(processed_index);
        let end = self.bases_end(block, processed_index, line_feed);
        if end > processed_index {
            let first_base = 4 * self.buffer.bases.len() + processed_index;
            if self.record_bases == 0 {
                self.record_bases_start = first_base;
                self.run_start = first_base;
            }
            self.buffer
                .bases
                .extend_from_slice(&masks.two_bits.to_le_bytes());
            self.record_bases += end - processed_index;
            self.split_at_non_acgt(masks.non_acgt, processed_index, end, first_base);
        }
        if line_feed.is_none()
            && let Some(copyback) = self.copyback_size
            && self.buffer.bases.len() >= self.bases_capacity
        {
            self.split_record(copyback, false, callback);
        }
        line_feed
    }

    /// Picks the state of the FASTA line starting at `processed_index`, inside a record, from its
    /// first byte, which it leaves to that state: see [`ParserState::FastaLineStart`].
    #[inline(always)]
    fn parse_fasta_line_start(
        &mut self,
        block: &[u8],
        processed_index: usize,
        wrapped: bool,
        callback: &mut impl FnMut(&SequenceBuffer),
    ) {
        let first = block[processed_index];
        self.state = if first == b'>' {
            self.end_record(callback);
            ParserState::FastaHeader
        } else if first == b';' {
            ParserState::FastaComment { wrapped }
        } else if self.record_bases == 0 {
            ParserState::FastaSequence
        } else {
            if !wrapped {
                self.start_shifting();
            }
            ParserState::FastaWrapped
        };
    }

    /// Appends the piece of the later FASTA sequence line the parser is in, from
    /// `processed_index` to its line feed or, if the block has none past it, to the end of the
    /// block, shifted after the bases before. Returns where it stopped; past the line feed, the
    /// parser moves to the start of the next line. A buffer full, with a `copyback_size`,
    /// splits the record.
    #[inline(always)]
    fn parse_fasta_wrapped(
        &mut self,
        block: &[u8],
        masks: &BlockMasks,
        processed_index: usize,
        callback: &mut impl FnMut(&SequenceBuffer),
    ) -> usize {
        debug_assert!(processed_index < block.len());
        let line_feed = masks.line_feed_from(processed_index);
        let end = self.bases_end(block, processed_index, line_feed);
        let first_base = self.record_bases_start + self.record_bases;
        if end > processed_index {
            self.push_bases(masks.two_bits, processed_index, end);
            self.split_at_non_acgt(masks.non_acgt, processed_index, end, first_base);
        }
        if let Some(copyback) = self.copyback_size
            && self.buffer.bases.len() >= self.bases_capacity
        {
            self.split_record(copyback, true, callback);
        }
        match line_feed {
            Some(line_feed) => {
                self.state = ParserState::FastaLineStart { wrapped: true };
                line_feed + 1
            }
            None => block.len(),
        }
    }

    /// Where the bases of the sequence line the parser is in run to in a block, from `from`: its
    /// line feed, or the end of the block, less a `\r` before it. A `\r` ending a line is not a
    /// base: one before the line feed is left out; so is one ending the block, the line going on
    /// in the next, but only for now: [`ParserState::CarriageReturn`] adds it back unless the
    /// next block starts with the line feed.
    #[inline(always)]
    fn bases_end(&mut self, block: &[u8], from: usize, line_feed: Option<usize>) -> usize {
        let end = line_feed.unwrap_or(block.len());
        if end > from && block[end - 1] == b'\r' {
            if line_feed.is_none() {
                self.after_carriage_return = self.state;
                self.state = ParserState::CarriageReturn;
            }
            end - 1
        } else {
            end
        }
    }

    /// Adds the `\r` that ended the previous block, left out by [`Self::bases_end`], back to the
    /// record, the line having gone on past it: a byte that is not ACGT like any other, it ends
    /// the open run. Before the record's first base it would split nothing off, so it is not
    /// added then.
    #[cold]
    fn add_carriage_return(&mut self) {
        if self.record_bases == 0 {
            return;
        }
        let base = self.record_bases_start + self.record_bases;
        if self.state == ParserState::FastaWrapped {
            self.push_bases(0, 0, 1);
        } else {
            // Copied a block at a time: its two bits are in the buffer already, at the end of the
            // previous block.
            self.record_bases += 1;
        }
        self.close_run(base);
        self.run_start = base + 1;
    }

    /// Goes over from copying the record a block at a time to appending it shifted, at its
    /// second line: the rest of the last block copied, past the record's end, is dropped, and
    /// the bases of the record's last byte move into `sequence_word`, which the appends go on
    /// from.
    fn start_shifting(&mut self) {
        let end = self.record_bases_start + self.record_bases;
        let bases = &mut self.buffer.bases;
        let partial = end % 4;
        self.sequence_word = match partial {
            0 => 0,
            _ => u64::from(bases[end / 4]) & ((1 << (2 * partial)) - 1),
        };
        self.sequence_word_bits = 2 * partial;
        bases.truncate(end / 4);
    }

    /// Ends the open run of ACGT bases at the byte that is not ACGT, or the record's end, at base
    /// `end` of the buffer: adds it as a sequence, with the record's header, if it is long
    /// enough.
    #[inline(always)]
    fn close_run(&mut self, end: usize) {
        let length = end - self.run_start;
        if length >= self.min_length {
            let buffer = &mut self.buffer;
            buffer.sequences.push(SequenceDescription {
                ident_start: self.record_ident_start,
                ident_len: buffer.ident.len() - self.record_ident_start,
                bases_start: self.run_start,
                bases_len: length,
            });
            self.kept_end = end.div_ceil(4);
        }
    }

    /// Splits the record's sequence at the bytes `from..to` of a block that are not ACGT, now
    /// bases of the buffer from `first_base` on: each ends the open run and starts the next past
    /// it. One test when there are none, as in nearly every block.
    #[inline(always)]
    fn split_at_non_acgt(&mut self, non_acgt: u64, from: usize, to: usize, first_base: usize) {
        debug_assert!(from < to && to <= BLOCK_SIZE);
        let mut bytes = non_acgt & (u64::MAX << from) & (u64::MAX >> (BLOCK_SIZE - to));
        while bytes != 0 {
            let base = first_base + bytes.trailing_zeros() as usize - from;
            self.close_run(base);
            self.run_start = base + 1;
            bytes &= bytes - 1;
        }
    }

    /// Ends the current record, all its bases in: its last run of ACGT bases becomes a sequence
    /// if it is long enough; the buffer's bases are trimmed to the byte of its last sequence
    /// kept, or to before the record if it kept none, and then its header goes too; the buffer
    /// is handed over if less than 1/16 of either capacity is left. Then starts the next record.
    #[inline(always)]
    fn end_record(&mut self, callback: &mut impl FnMut(&SequenceBuffer)) {
        if self.record_bases > 0 {
            self.flush_sequence();
            self.close_run(self.record_bases_start + self.record_bases);
        }
        self.buffer.bases.truncate(self.kept_end);
        if self.buffer.sequences.len() == self.record_sequences {
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

    /// Splits the current record, in the middle of its sequence as the buffer is full: its open
    /// run of ACGT bases so far is added and the buffer handed over; then the record goes on in
    /// the cleared buffer from its header and that run's last `copyback` bases. A run of
    /// `copyback` bases or fewer, or too short to be kept, is only moved, unsplit.
    ///
    /// Copied a block at a time, the record's bases run to the end of the buffer between its
    /// blocks: so do the ones carried over, from the same place in their byte, and the next
    /// block follows them with no shifting. `shifted`, appended after [`Self::start_shifting`],
    /// they are first moved out of `sequence_word`, and the carried ones' last byte back in.
    #[cold]
    fn split_record(
        &mut self,
        copyback: usize,
        shifted: bool,
        callback: &mut impl FnMut(&SequenceBuffer),
    ) {
        if shifted {
            self.flush_sequence();
        }
        let end = self.record_bases_start + self.record_bases;
        // Copied a block at a time, the input's last block, shorter, can leave bytes past it.
        debug_assert!(end.div_ceil(4) <= self.buffer.bases.len());
        self.buffer.bases.truncate(end.div_ceil(4));
        let open = end - self.run_start;
        let keep_from = if open > copyback && open >= self.min_length {
            end - copyback
        } else {
            self.run_start
        };
        if keep_from > self.run_start {
            self.close_run(end);
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
        self.run_start = keep_from % 4;
        self.record_sequences = 0;
        self.kept_end = 0;
        if shifted {
            self.start_shifting();
        }
    }

    /// Appends to the sequence the bases of bytes `from..to` of a block, from its masks'
    /// `two_bits`: in two halves of up to 32 bases, as one 64-bit word can take them, through
    /// `sequence_word`, which only whole words leave, so nothing is read back from the buffer.
    /// The record's bases so far are to end at the end of the buffer and `sequence_word`, as
    /// [`Self::start_shifting`] leaves them.
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
    /// FASTQ sequence line cut by the end of the input, a `\r` there ending it. Elsewhere the record either was added
    /// already or has no bases yet, and only its header is dropped.
    pub fn finish(&mut self, mut callback: impl FnMut(&SequenceBuffer)) {
        match self.state {
            ParserState::FastaLineStart { .. }
            | ParserState::FastaComment { .. }
            | ParserState::FastaSequence
            | ParserState::FastaWrapped
            | ParserState::FastqSequence
            | ParserState::CarriageReturn => self.end_record(&mut callback),
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
mod tests;