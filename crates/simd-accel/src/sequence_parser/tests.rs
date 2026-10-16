use super::*;
use crate::compressed_read::SimdCompressedRead;
use crate::masks::tests::supported_kernels;
use std::collections::BTreeSet;
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
        0,
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

/// A sequence as a buffer hands it over: its header, its bases' codes, one per base, and its
/// record's index, if the parser gives them.
#[derive(Debug, PartialEq)]
struct Record {
    ident: Vec<u8>,
    codes: Vec<u8>,
    record: Option<u64>,
}

impl Record {
    fn of(sequence: DnaSequence<'_, SimdCompressedRead<'_>>) -> Self {
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
            record: None,
        }
    }

    /// The sequences of `buffer`, with their records' indices if it has them.
    fn all_of(buffer: &SequenceBuffer) -> impl Iterator<Item = Self> + '_ {
        let indices = buffer.record_indices();
        buffer
            .sequences()
            .enumerate()
            .map(|(index, sequence)| Self {
                record: indices.get(index).copied(),
                ..Self::of(sequence)
            })
    }

    /// The sequences of a record of header `ident` and bytes `bases`: its runs of ACGT bases,
    /// split at every other byte, those shorter than `min_length` left out.
    fn runs(ident: &[u8], bases: &[u8], min_length: usize) -> Vec<Self> {
        bases
            .split(|byte| !matches!(byte & 0b1101_1111, b'A' | b'C' | b'G' | b'T'))
            .filter(|run| run.len() >= min_length.max(1))
            .map(|run| Self::expected(ident, run))
            .collect()
    }

    /// The record of header `ident` and bases `bases`, as letters.
    fn expected(ident: &[u8], bases: &[u8]) -> Self {
        Self {
            ident: ident.to_vec(),
            codes: bases.iter().map(|base| (base >> 1) & 3).collect(),
            record: None,
        }
    }
}

/// A callback adding the sequences of every buffer it gets to `records`, then giving it back.
fn collect(records: &mut Vec<Record>) -> impl FnMut(SequenceBuffer) -> SequenceBuffer + '_ {
    |buffer| {
        records.extend(Record::all_of(&buffer));
        buffer
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
fn a_new_parser_takes_the_detected_kernel_and_reads_fasta() {
    let parser = SequenceParser::new(false, None, 0);
    assert_eq!(parser.kernel(), MaskKernel::detect());
    assert_eq!(parser.state, ParserState::FastaLineStart { wrapped: false });
}

#[test]
fn reset_starts_each_format_at_its_first_line() {
    let mut parser = SequenceParser::new(false, None, 0);
    parser.reset(DnaSequencesFileType::FASTQ);
    assert_eq!(parser.state, ParserState::FastqHeader);
    assert!(matches!(
        parser.buffer.format(),
        DnaSequencesFileType::FASTQ
    ));
    parser.reset(DnaSequencesFileType::FASTA);
    assert_eq!(parser.state, ParserState::FastaLineStart { wrapped: false });
}

#[test]
#[should_panic(expected = "FASTA and FASTQ only")]
fn reset_rejects_gfa() {
    SequenceParser::new(false, None, 0).reset(DnaSequencesFileType::GFA);
}

#[test]
fn reset_forgets_the_buffer_and_the_record() {
    let mut parser = SequenceParser::new(true, None, 0);
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
            // Split at its `N`.
            assert_eq!(parser.buffer.len(), 2, "{kernel:?}");
            assert_eq!(parser.state, ParserState::FastqQuality, "{kernel:?}");
            assert_eq!(parser.record_bases, 0, "{kernel:?}");
            // Trimmed to the byte of its last base: 9 bases from base 10 of the block.
            assert_eq!(parser.buffer.bases().len(), 5, "{kernel:?}");

            let mut records = Vec::new();
            parser.parse_data(rest, |_| panic!("not full"));
            parser.finish(collect(&mut records));
            let ident: &[u8] = if keep_ident { b"@read1 x" } else { b"" };
            let expected = Record::runs(ident, b"ACGTNacgt", 0);
            assert_eq!(records, expected, "{kernel:?}, keep_ident {keep_ident}");
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
    let mut expected = vec![Record::expected(first_header, b"ACGT")];
    expected.extend(Record::runs(header, bases, 0));
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
    let mut parser = SequenceParser::new(false, None, 0);
    parser.reset(DnaSequencesFileType::FASTQ);
    parser.parse_data(b"@read1\nAC", |buffer| buffer);
    parser.parse_data(b"GT\n", |buffer| buffer);
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
    let mut parser = SequenceParser::new(true, None, 0);
    parser.reset(DnaSequencesFileType::FASTQ);
    let mut buffers = 0;
    parser.parse_data(b"@r\nACGT\n+\nIIII", |buffer| {
        buffers += 1;
        buffer
    });
    parser.finish(|buffer| {
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.ident(), b"@r");
        buffers += 1;
        buffer
    });
    assert_eq!(buffers, 1);
    // A header with no sequence is only forgotten; an empty input hands nothing over.
    parser.reset(DnaSequencesFileType::FASTQ);
    parser.parse_data(b"@cut", |buffer| {
        buffers += 1;
        buffer
    });
    parser.finish(|buffer| {
        buffers += 1;
        buffer
    });
    parser.reset(DnaSequencesFileType::FASTQ);
    parser.finish(|buffer| {
        buffers += 1;
        buffer
    });
    assert_eq!(buffers, 1);
    assert!(parser.buffer.ident().is_empty());
}

/// `count` FASTQ reads, of 1 to `max` bases, from a fixed seed: their headers and bases.
fn reads(count: usize, max: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    reads_with_n(count, max, 0)
}

/// As [`reads`], with each byte an `N` or `n` with a probability of `n_per_mille` / 1000.
fn reads_with_n(count: usize, max: usize, n_per_mille: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
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
                .map(|_| {
                    if next() % 1000 < n_per_mille {
                        b"Nn"[(next() % 2) as usize]
                    } else {
                        b"ACGTacgt"[(next() % 8) as usize]
                    }
                })
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

/// `reads` as FASTA, the sequence of read `i` in lines of `width(i)` bases (all on one line
/// for `None`), each line ended by `ending`.
fn fasta_of(
    reads: &[(Vec<u8>, Vec<u8>)],
    width: impl Fn(usize) -> Option<usize>,
    ending: &[u8],
) -> Vec<u8> {
    let mut input = Vec::new();
    for (index, (header, bases)) in reads.iter().enumerate() {
        input.push(b'>');
        input.extend_from_slice(&header[1..]);
        input.extend_from_slice(ending);
        let width = width(index).unwrap_or(bases.len().max(1));
        for line in bases.chunks(width) {
            input.extend_from_slice(line);
            input.extend_from_slice(ending);
        }
    }
    input
}

/// `reads` as their records: the FASTA headers start with `>`.
fn fasta_records(reads: &[(Vec<u8>, Vec<u8>)]) -> Vec<Record> {
    reads
        .iter()
        .map(|(header, bases)| {
            let mut ident = b">".to_vec();
            ident.extend_from_slice(&header[1..]);
            Record::expected(&ident, bases)
        })
        .collect()
}

/// All of `input` through a FASTA parser on `kernel`, in calls of `slice` bytes, a whole
/// number of blocks; the records handed over.
fn parse_fasta(kernel: MaskKernel, input: &[u8], slice: usize) -> Vec<Record> {
    let mut parser = fastq_parser(kernel, true);
    parser.reset(DnaSequencesFileType::FASTA);
    let mut records = Vec::new();
    for chunk in input.chunks(slice) {
        parser.parse_data(chunk, collect(&mut records));
    }
    parser.finish(collect(&mut records));
    records
}

#[test]
fn fasta_lines_of_any_width_and_ending_are_joined() {
    let reads = reads(30, 300);
    let widths: Vec<Option<usize>> = (1..=80).map(Some).chain([None]).collect();
    for kernel in supported_kernels() {
        for &width in &widths {
            for ending in [&b"\n"[..], b"\r\n"] {
                let input = fasta_of(&reads, |_| width, ending);
                assert_eq!(
                    parse_fasta(kernel, &input, 16 * BLOCK_SIZE),
                    fasta_records(&reads),
                    "{kernel:?}, width {width:?}, ending {ending:?}"
                );
            }
        }
        // Wrapped and unwrapped records mixed.
        let input = fasta_of(
            &reads,
            |index| (index % 3 > 0).then_some(7 * index + 1),
            b"\n",
        );
        assert_eq!(
            parse_fasta(kernel, &input, BLOCK_SIZE),
            fasta_records(&reads),
            "{kernel:?}"
        );
    }
}

#[test]
fn a_fasta_record_on_one_line_keeps_its_place_in_its_first_block() {
    for kernel in supported_kernels() {
        let mut parser = fastq_parser(kernel, true);
        parser.reset(DnaSequencesFileType::FASTA);
        let mut starts = Vec::new();
        parser.parse_data(b">r1 x\nACGTACGT\n>r2\nGG\nCC\n", |_| panic!("not full"));
        parser.finish(|buffer| {
            starts.extend(
                buffer
                    .descriptions()
                    .iter()
                    .map(|description| description.bases_start),
            );
            buffer
        });
        // r1 starts at byte 6 of the block, copied whole from the start of the buffer, so at
        // base 6; r2, wrapped, is copied likewise from the byte after r1's last.
        assert_eq!(starts, [6, 4 * 4 + 19], "{kernel:?}");
    }
}

#[test]
fn fasta_records_without_bases_are_dropped_and_empty_lines_add_nothing() {
    let input = b">empty\n>also empty\n\n\r\n>r\n\nAC\n\r\nGT\n\n>last empty";
    for kernel in supported_kernels() {
        assert_eq!(
            parse_fasta(kernel, input, BLOCK_SIZE),
            [Record::expected(b">r", b"ACGT")],
            "{kernel:?}"
        );
    }
}

#[test]
fn a_fasta_input_can_end_without_a_line_feed() {
    for (input, bases) in [
        (&b">r\nACGT"[..], &b"ACGT"[..]),
        (b">r\nACGT\r", b"ACGT"),
        (b">r\nAC\nGT", b"ACGT"),
        (b">r\nAC\r\nGT\r", b"ACGT"),
        (b">r\nACGT\n", b"ACGT"),
    ] {
        for kernel in supported_kernels() {
            assert_eq!(
                parse_fasta(kernel, input, BLOCK_SIZE),
                [Record::expected(b">r", bases)],
                "{kernel:?}, {}",
                String::from_utf8_lossy(input)
            );
        }
    }
}

#[test]
fn fasta_comment_lines_are_skipped_wherever_a_line_starts() {
    // Before the first header, after a header, between the lines of a record copied a block
    // at a time and of one already shifted, between records, at the end without a line
    // feed; one longer than a block; with Windows line endings in the second input.
    let long = format!(";{}", "x".repeat(2 * BLOCK_SIZE));
    let input = format!(
        ";first\n>r1 a\n;after the header\nACGT\n;inside\nGGCC\n;again\nTT\n{long}\n\
             >r2\nAAAA\n;between records\n>r3\nCC\n;last"
    );
    let expected = [
        Record::expected(b">r1 a", b"ACGTGGCCTT"),
        Record::expected(b">r2", b"AAAA"),
        Record::expected(b">r3", b"CC"),
    ];
    for kernel in supported_kernels() {
        for input in [input.clone(), input.replace('\n', "\r\n")] {
            assert_eq!(
                parse_fasta(kernel, input.as_bytes(), BLOCK_SIZE),
                expected,
                "{kernel:?}, {input:?}"
            );
        }
    }
}

#[test]
fn fasta_lines_before_any_header_make_a_record_without_one() {
    for kernel in supported_kernels() {
        assert_eq!(
            parse_fasta(kernel, b"ACGT\nGG\n>r\nTT\n", BLOCK_SIZE),
            [
                Record::expected(b"", b"ACGTGG"),
                Record::expected(b">r", b"TT")
            ],
            "{kernel:?}"
        );
    }
}

#[test]
fn fasta_parsing_does_not_depend_on_where_the_input_is_cut() {
    // The first header longer by 0 to 63 bytes, so that every line ending lands at every
    // place in a block; the input cut into two calls at every block boundary.
    let reads = reads(6, 200);
    for shift in 0..BLOCK_SIZE {
        let mut reads = reads.clone();
        reads[0].0.extend(std::iter::repeat_n(b'x', shift));
        let input = fasta_of(&reads, |index| (index % 2 == 0).then_some(61), b"\r\n");
        for kernel in supported_kernels() {
            for cut in (0..=input.len()).step_by(BLOCK_SIZE) {
                let mut parser = fastq_parser(kernel, true);
                parser.reset(DnaSequencesFileType::FASTA);
                let mut records = Vec::new();
                parser.parse_data(&input[..cut], |_| panic!("not full"));
                parser.parse_data(&input[cut..], |_| panic!("not full"));
                parser.finish(collect(&mut records));
                assert_eq!(
                    records,
                    fasta_records(&reads),
                    "{kernel:?}, shift {shift}, cut {cut}"
                );
            }
        }
    }
}

#[test]
fn sequences_are_split_at_every_byte_that_is_not_acgt() {
    // At the start and the end, in a row, lowercase and IUPAC codes.
    let bases = b"NACGTNNacgtRYnACGTACGTKMn".repeat(5);
    let reads = vec![
        (b"@r1".to_vec(), bases.clone()),
        (b"@r2".to_vec(), b"NNNN".to_vec()),
    ];
    let expected = Record::runs(b"@r1", &bases, 0);
    for kernel in supported_kernels() {
        let mut parser = fastq_parser(kernel, true);
        let mut records = Vec::new();
        parser.parse_data(&fastq_of(&reads), collect(&mut records));
        parser.finish(collect(&mut records));
        assert_eq!(records, expected, "{kernel:?}, FASTQ");
        // FASTA on one line, then wrapped, across the switch to shifted appends.
        let expected: Vec<_> = Record::runs(b">r1", &bases, 0);
        for width in [None, Some(1), Some(7), Some(60)] {
            let input = fasta_of(&reads, |_| width, b"\r\n");
            assert_eq!(
                parse_fasta(kernel, &input, BLOCK_SIZE),
                expected,
                "{kernel:?}, FASTA, width {width:?}"
            );
        }
    }
}

#[test]
fn sequences_shorter_than_min_seqlen_are_left_out() {
    // Runs of 1 to 6 bases between N; then a record with none of 4 or more.
    let input = b">r1\nANACNACGNACGTNACGTANACGTAC\n>r2\nACGNACNA\n>r3\nGGGG\n";
    for kernel in supported_kernels() {
        let mut parser = SequenceParser::with_kernel(kernel, true, None, 4, 1 << 20, 1 << 20);
        parser.reset(DnaSequencesFileType::FASTA);
        let mut records = Vec::new();
        parser.parse_data(input, |_| panic!("not full"));
        parser.finish(|buffer| {
            // r2 kept nothing: neither its header nor its bases are left.
            assert_eq!(buffer.ident(), b">r1>r3", "{kernel:?}");
            let last = buffer.descriptions().last().unwrap();
            assert_eq!(
                buffer.bases().len(),
                (last.bases_start + last.bases_len).div_ceil(4)
            );
            records.extend(buffer.sequences().map(Record::of));
            buffer
        });
        let mut expected = Record::runs(b">r1", b"ANACNACGNACGTNACGTANACGTAC", 4);
        expected.push(Record::expected(b">r3", b"GGGG"));
        assert_eq!(records, expected, "{kernel:?}");
    }
}

#[test]
fn a_buffer_is_handed_over_when_nearly_full_of_bases_at_the_end_of_a_sequence() {
    let reads = reads(100, 60);
    let input = fastq_of(&reads);
    let capacity = 128;
    for kernel in supported_kernels() {
        let mut parser = SequenceParser::with_kernel(kernel, true, None, 0, capacity, 1 << 20);
        parser.reset(DnaSequencesFileType::FASTQ);
        let mut records = Vec::new();
        let mut sizes = Vec::new();
        let mut callback = |buffer: SequenceBuffer| {
            sizes.push(buffer.bases().len());
            records.extend(buffer.sequences().map(Record::of));
            buffer
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
        let mut parser = SequenceParser::with_kernel(kernel, true, None, 0, 1 << 20, 32);
        parser.reset(DnaSequencesFileType::FASTQ);
        let mut lengths = Vec::new();
        let mut count = |buffer: SequenceBuffer| {
            lengths.push(buffer.len());
            buffer
        };
        parser.parse_data(&input, &mut count);
        parser.finish(&mut count);
        // "@readN" headers of 6 or 7 bytes: 30 bytes of them fill 32 nearly.
        assert_eq!(lengths.iter().sum::<usize>(), 20, "{kernel:?}");
        assert!(
            lengths.iter().all(|&length| length <= 5),
            "{kernel:?}: {lengths:?}"
        );
    }
}

#[test]
fn the_callback_owns_each_buffer_and_any_it_gives_back_is_cleared() {
    let reads = reads(100, 60);
    let input = fastq_of(&reads);
    let expected: Vec<Record> = reads
        .iter()
        .map(|(header, bases)| Record::expected(header, bases))
        .collect();
    for kernel in supported_kernels() {
        // Buffers of 128 bytes of bases, several for these reads: each kept, a new one given
        // back.
        let mut parser = SequenceParser::with_kernel(kernel, true, None, 0, 128, 1 << 20);
        parser.reset(DnaSequencesFileType::FASTQ);
        let mut kept = Vec::new();
        let mut keep = |buffer: SequenceBuffer| {
            kept.push(buffer);
            SequenceBuffer::new()
        };
        parser.parse_data(&input, &mut keep);
        parser.finish(&mut keep);
        assert!(kept.len() > 5, "{kernel:?}: {} buffers", kept.len());
        let records: Vec<Record> = kept
            .iter()
            .flat_map(|buffer| buffer.sequences().map(Record::of))
            .collect();
        assert_eq!(records, expected, "{kernel:?}");

        // Again, given back those full buffers, one each time.
        parser.reset(DnaSequencesFileType::FASTQ);
        let (mut full, mut records) = (kept.into_iter(), Vec::new());
        let mut refill = |buffer: SequenceBuffer| {
            records.extend(buffer.sequences().map(Record::of));
            full.next().unwrap_or_default()
        };
        parser.parse_data(&input, &mut refill);
        parser.finish(&mut refill);
        assert_eq!(records, expected, "{kernel:?}");
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
        let mut parser = SequenceParser::with_kernel(kernel, false, None, 0, 128, 1 << 20);
        parser.reset(DnaSequencesFileType::FASTQ);
        let mut records = Vec::new();
        parser.parse_data(&input, collect(&mut records));
        parser.finish(collect(&mut records));
        assert_eq!(records, [Record::expected(b"", &reads[0].1)], "{kernel:?}");
    }
}

#[test]
fn long_sequences_are_split_with_their_last_bases_repeated_and_short_ones_moved() {
    // Reads up to 600 bases in buffers of 256 bytes, 1024 bases: as FASTQ, and as FASTA with
    // every third record on one line and the others wrapped. Without N and every sequence
    // kept, with a copyback of 100; then with N, as is, and dropping those under 31 with the
    // copyback of 30 that k = 31 asks for.
    for (n_per_mille, copyback, min_seqlen) in [(0, 100, 0), (3, 100, 0), (3, 30, 31)] {
        let reads = reads_with_n(200, 600, n_per_mille);
        let fastq = fastq_of(&reads);
        let split = |reads: &[(Vec<u8>, Vec<u8>)], format, input: &[u8]| {
            long_sequences_are_split(reads, format, input, copyback, min_seqlen, n_per_mille == 0)
        };
        split(&reads, DnaSequencesFileType::FASTQ, &fastq);
        let fasta = fasta_of(
            &reads,
            |index| (index % 3 > 0).then_some(13 * index % 97 + 1),
            b"\r\n",
        );
        let reads: Vec<_> = reads
            .iter()
            .map(|(header, bases)| {
                let mut ident = b">".to_vec();
                ident.extend_from_slice(&header[1..]);
                (ident, bases.clone())
            })
            .collect();
        split(&reads, DnaSequencesFileType::FASTA, &fasta);
    }
}

fn long_sequences_are_split(
    reads: &[(Vec<u8>, Vec<u8>)],
    format: DnaSequencesFileType,
    input: &[u8],
    copyback: usize,
    min_seqlen: usize,
    one_sequence_per_record: bool,
) {
    for kernel in supported_kernels() {
        let mut parser =
            SequenceParser::with_kernel(kernel, true, Some(copyback), min_seqlen, 256, 1 << 20);
        parser.reset(format);
        let mut records = Vec::new();
        let mut callback = |buffer: SequenceBuffer| {
            // No record starts a whole block of bases past where the one before ended. The
            // runs of one record are apart by the bytes split at, which stay in the buffer.
            for pair in buffer.descriptions().windows(2) {
                let end = (pair[0].bases_start + pair[0].bases_len).div_ceil(4) * 4;
                if one_sequence_per_record {
                    let gap = pair[1].bases_start - end;
                    assert!(gap < BLOCK_SIZE, "{kernel:?}: {gap} bases unused");
                }
            }
            records.extend(buffer.sequences().map(Record::of));
            buffer
        };
        for chunk in input.chunks(16 * BLOCK_SIZE) {
            parser.parse_data(chunk, &mut callback);
        }
        parser.finish(&mut callback);

        // Each run of each read, rebuilt from its parts.
        let mut parts = records.iter().peekable();
        let mut splits = 0;
        for (header, bases) in reads {
            for run in Record::runs(header, bases, min_seqlen) {
                let first = parts.next().unwrap();
                assert_eq!(first.ident, *header, "{kernel:?}");
                assert!(
                    run.codes.starts_with(&first.codes),
                    "{kernel:?}: not the run"
                );
                let mut joined = first.codes.clone();
                while joined.len() < run.codes.len() {
                    let part = parts.next().unwrap();
                    splits += 1;
                    assert_eq!(part.ident, *header, "{kernel:?}");
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
                    run.codes,
                    "{kernel:?}, {}",
                    String::from_utf8_lossy(header)
                );
                // A split just before the run's end leaves a last part of its copyback
                // only, kept if it is long enough, as the line reader keeps it.
                let tail = &joined[joined.len().saturating_sub(copyback)..];
                parts.next_if(|part| part.ident == *header && part.codes == tail);
            }
        }
        assert!(parts.next().is_none(), "{kernel:?}");
        assert!(splits > 10, "{kernel:?}: only {splits} splits");
    }
}

#[test]
#[should_panic(expected = "does not fit twice")]
fn a_copyback_has_to_fit_twice_in_the_buffer() {
    SequenceParser::with_kernel(MaskKernel::Portable, false, Some(200), 0, 64, 64);
}

#[test]
fn a_carriage_return_ending_a_block_ends_a_line_only_before_a_line_feed() {
    // The `\r` is the last byte of the first block: in a FASTQ sequence; in the first line of
    // a FASTA record, copied a block at a time; in a later one, appended shifted; first in
    // its line. The line then goes on, split there as at any byte that is not ACGT, or ends
    // with a line feed, the FASTA record going on in the next line.
    let starts = [
        (
            DnaSequencesFileType::FASTQ,
            [&b"@r\n"[..], &[b'C'; 60]].concat(),
        ),
        (
            DnaSequencesFileType::FASTA,
            [&b">r\n"[..], &[b'C'; 60]].concat(),
        ),
        (
            DnaSequencesFileType::FASTA,
            [&b">r\nAA\n"[..], &[b'C'; 57]].concat(),
        ),
        (
            DnaSequencesFileType::FASTA,
            [&b">"[..], &[b'x'; 61], b"\n"].concat(),
        ),
    ];
    for (case, (format, start)) in starts.into_iter().enumerate() {
        assert_eq!(start.len(), BLOCK_SIZE - 1);
        // The runs each makes: the `\r` splits off the bases before it, if any.
        for (end, runs) in [
            (&b"\rGGTT\n+\nII\n"[..], if case == 3 { 1 } else { 2 }),
            (b"\r\nGGTT\n+\nII\n", 1),
        ] {
            let input = [&start[..], end].concat();
            let (expected, _) = reference_sequences(&input, format, true, 0, None);
            assert_eq!(
                expected.len(),
                runs,
                "case {case}, {}",
                input.escape_ascii()
            );
            for kernel in supported_kernels() {
                let mut parser = fastq_parser(kernel, true);
                parser.reset(format);
                let mut records = Vec::new();
                for block in input.chunks(BLOCK_SIZE) {
                    parser.parse_data(block, collect(&mut records));
                }
                parser.finish(collect(&mut records));
                assert_eq!(records, expected, "{kernel:?}, {}", input.escape_ascii());
            }
        }
    }
}

#[test]
fn a_buffer_full_in_the_last_block_of_the_input_is_split_there() {
    // A FASTQ sequence with no line feed: four whole blocks, then 10 bytes; the buffer of 66
    // bytes, the least a copyback of 3 takes, is full only once the short last block is in.
    let mut input = b"@r\n".to_vec();
    input.extend(b"ACGT".iter().cycle().take(4 * BLOCK_SIZE + 10 - 3));
    let (expected, _) = reference_sequences(&input, DnaSequencesFileType::FASTQ, true, 0, None);
    for kernel in supported_kernels() {
        let mut parser = SequenceParser::with_kernel(kernel, true, Some(3), 0, 66, 1 << 20);
        parser.reset(DnaSequencesFileType::FASTQ);
        let mut parts = Vec::new();
        parser.parse_data(&input, collect(&mut parts));
        parser.finish(collect(&mut parts));
        assert_eq!(parts.len(), 2, "{kernel:?}: split once");
        assert_eq!(rebuild(&expected, &parts, 3), Ok(()), "{kernel:?}");
    }
}

#[test]
fn every_sequence_has_its_records_index_and_every_record_counts() {
    // Lines before the first header; a record split at an N; one with no bases; one with no run
    // long enough; a header cut by the end of the input, which does not count.
    let fasta = b"ACGT\n>r1\nACNGT\n>r2\n>r3\nNNN\n>r4\nGG\nCC\n>cut";
    // A sequence split at an N; an empty sequence; a record cut in its header.
    let fastq = b"@q0\nACNGT\n+\nIIIII\n@q1\n\n+\n\n@q2\nAC\n+\nII\n@cut";
    let run = |ident: &[u8], bases: &[u8], record| Record {
        record: Some(record),
        ..Record::expected(ident, bases)
    };
    for kernel in supported_kernels() {
        for (format, input, expected, records) in [
            (
                DnaSequencesFileType::FASTA,
                &fasta[..],
                vec![
                    run(b"", b"ACGT", 0),
                    run(b">r1", b"AC", 1),
                    run(b">r1", b"GT", 1),
                    run(b">r4", b"GGCC", 4),
                ],
                5,
            ),
            (
                DnaSequencesFileType::FASTQ,
                fastq,
                vec![
                    run(b"@q0", b"AC", 0),
                    run(b"@q0", b"GT", 0),
                    run(b"@q2", b"AC", 2),
                ],
                3,
            ),
        ] {
            let mut parser = SequenceParser::with_kernel(kernel, true, None, 2, 1 << 20, 1 << 20)
                .with_record_indices();
            parser.reset(format);
            let mut found = Vec::new();
            parser.parse_data(input, collect(&mut found));
            parser.finish(collect(&mut found));
            assert_eq!(found, expected, "{kernel:?}");
            assert_eq!(parser.records(), records, "{kernel:?}");
            // An archive's next file goes on from there.
            parser.reset(format);
            parser.parse_data(input, collect(&mut found));
            parser.finish(collect(&mut found));
            assert_eq!(parser.records(), 2 * records, "{kernel:?}");
        }
    }
}

#[test]
fn a_record_pushed_whole_is_read_as_a_fasta_record_on_one_line() {
    // Records with N, empty, under min_seqlen, and longer than a buffer of 128 bytes.
    let reads: Vec<_> = reads_with_n(60, 1500, 5)
        .into_iter()
        .map(|(header, bases)| ([&b">"[..], &header[1..]].concat(), bases))
        .collect();
    let mut fasta = Vec::new();
    for (header, bases) in &reads {
        fasta.extend_from_slice(header);
        fasta.push(b'\n');
        fasta.extend_from_slice(bases);
        fasta.push(b'\n');
    }
    let (expected, records) =
        reference_sequences(&fasta, DnaSequencesFileType::FASTA, true, 31, Some(0));
    assert_eq!(records, reads.len() as u64);
    for kernel in supported_kernels() {
        // Without a copyback the same sequences as the file's; with one, split where the buffer
        // fills, which a record in a file, starting inside a block, does elsewhere.
        for copyback in [None, Some(30)] {
            let parser = || {
                let mut parser =
                    SequenceParser::with_kernel(kernel, true, copyback, 31, 128, 1 << 20)
                        .with_record_indices();
                parser.reset(DnaSequencesFileType::FASTA);
                parser
            };
            let (mut parsed, mut pushed) = (Vec::new(), Vec::new());
            let mut from_file = parser();
            from_file.parse_data(&fasta, collect(&mut parsed));
            from_file.finish(collect(&mut parsed));
            let mut from_memory = parser();
            for (header, bases) in &reads {
                from_memory.push_record(header, bases, collect(&mut pushed));
            }
            from_memory.finish(collect(&mut pushed));
            assert_eq!(from_memory.records(), records, "{kernel:?}, {copyback:?}");
            match copyback {
                None => assert_eq!(pushed, parsed, "{kernel:?}"),
                Some(copyback) => {
                    assert!(pushed.len() > expected.len(), "{kernel:?}: none split");
                    assert_eq!(rebuild(&expected, &pushed, copyback), Ok(()), "{kernel:?}");
                }
            }
        }
    }
}

/// Xorshift, for the fuzz tests: the same seed makes the same cases.
struct Random(u64);

impl Random {
    /// A generator from any `seed`, mixed first by splitmix64, as xorshift needs bits set.
    fn new(seed: u64) -> Self {
        let mut mixed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        Self((mixed ^ (mixed >> 31)) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number in `0..bound`.
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    /// True with a probability of `per_mille` / 1000.
    fn chance(&mut self, per_mille: u64) -> bool {
        self.next() % 1000 < per_mille
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

#[test]
fn random_blocks_are_classified() {
    let mut random = Random::new(1);
    for _ in 0..20_000 {
        let length = 1 + random.below(BLOCK_SIZE);
        let block: Vec<u8> = (0..length)
            .map(|_| {
                if random.chance(500) {
                    random.next() as u8
                } else {
                    random.pick(b"ACGTacgtNn\r\n>@")
                }
            })
            .collect();
        for kernel in supported_kernels() {
            assert_eq!(
                masks_with(kernel, &block),
                reference(&block),
                "{kernel:?}, {}",
                block.escape_ascii()
            );
        }
    }
}

/// The bytes of a fuzzed sequence that are not ACGT, mostly: N and the IUPAC codes, in either
/// case, and bytes no sequence should hold, such as a `\r` that ends no line or those that
/// start lines.
const OTHER_BYTES: &[u8] =
    b"NnNnRYKMSWBDHVrykmswbdhvUuX-*. \t\r\r>;@+\x00\x01\x80\xc1\xc3\xc7\xd4\xe1\xff";

/// What the lines of one fuzzed input look like.
struct Shape {
    /// Per mille of the sequences' bytes that are not ACGT.
    other: u64,
    /// Per mille of the line endings that are `\r\n`, the others `\n`.
    crlf: u64,
}

impl Shape {
    fn new(random: &mut Random) -> Self {
        Self {
            other: random.pick(&[0, 0, 1, 10, 100, 700]),
            crlf: random.pick(&[0, 0, 1000, 1000, 500]),
        }
    }

    fn end_line(&self, random: &mut Random, input: &mut Vec<u8>) {
        if random.chance(self.crlf) {
            input.push(b'\r');
        }
        input.push(b'\n');
    }

    /// `length` bytes of a sequence.
    fn bases(&self, random: &mut Random, length: usize) -> Vec<u8> {
        (0..length)
            .map(|_| {
                if !random.chance(self.other) {
                    random.pick(b"ACGTacgt")
                } else if random.chance(900) {
                    random.pick(OTHER_BYTES)
                } else {
                    // Any byte but a line feed.
                    match random.next() as u8 {
                        b'\n' => b'N',
                        byte => byte,
                    }
                }
            })
            .collect()
    }

    /// The sequence of a FASTA record: on one line, wrapped at one width, or at random
    /// widths; comment and empty lines among its lines, before and after them.
    fn sequence_lines(&self, random: &mut Random, input: &mut Vec<u8>) {
        let length = sequence_length(random);
        let bases = self.bases(random, length);
        let width = random.below(121);
        let mode = random.below(3);
        let mut rest = &bases[..];
        loop {
            match random.below(100) {
                0..=2 => {
                    input.push(b';');
                    input.extend(text(random));
                    self.end_line(random, input);
                }
                3..=4 => self.end_line(random, input),
                _ => {}
            }
            if rest.is_empty() {
                break;
            }
            let length = match mode {
                0 => rest.len(),
                1 => width + 1,
                _ => 1 + random.below(200),
            };
            let (line, next) = rest.split_at(length.min(rest.len()));
            input.extend_from_slice(line);
            self.end_line(random, input);
            rest = next;
        }
    }

    /// Sometimes takes the last line feed of `input` away, or its whole line ending.
    fn end_input(&self, random: &mut Random, input: &mut Vec<u8>) {
        let strip = random.below(10);
        if strip < 3 && input.last() == Some(&b'\n') {
            input.pop();
        }
        if strip == 2 && input.last() == Some(&b'\r') {
            input.pop();
        }
    }
}

/// The length of a fuzzed sequence: mostly within a block or a few, sometimes over many, so
/// that small buffers fill in the middle of one.
fn sequence_length(random: &mut Random) -> usize {
    match random.below(20) {
        0..=9 => random.below(81),
        10..=15 => random.below(300),
        16..=18 => 300 + random.below(2000),
        _ => 2000 + random.below(20000),
    }
}

/// The bytes of a fuzzed header or comment past its first: mostly a few, sometimes over
/// blocks; the bytes that start lines, and a `\r` that ends none, among them.
fn text(random: &mut Random) -> Vec<u8> {
    let length = if random.chance(100) {
        50 + random.below(200)
    } else {
        random.below(40)
    };
    (0..length)
        .map(|_| random.pick(b"abcdxyz0123456789 _-|:.=\t>;@+\r\x80\xff"))
        .collect()
}

/// A FASTA input as any can be: records of every shape, with no bases too; lines before any
/// header; headers over several blocks; the last line with or without its line ending.
fn fuzzed_fasta(random: &mut Random) -> Vec<u8> {
    let shape = Shape::new(random);
    let mut input = Vec::new();
    if random.chance(150) {
        shape.sequence_lines(random, &mut input);
    }
    for _ in 0..random.below(12) {
        input.push(b'>');
        input.extend(text(random));
        shape.end_line(random, &mut input);
        shape.sequence_lines(random, &mut input);
    }
    shape.end_input(random, &mut input);
    input
}

/// A FASTQ input as any can be: sequences of every length, with no bases too; separators
/// with or without the header again; quality bytes that start lines; sometimes cut anywhere
/// in its last record.
fn fuzzed_fastq(random: &mut Random) -> Vec<u8> {
    let shape = Shape::new(random);
    let mut input = Vec::new();
    let mut last_record = 0;
    for _ in 0..random.below(12) {
        last_record = input.len();
        let header = text(random);
        input.push(b'@');
        input.extend_from_slice(&header);
        shape.end_line(random, &mut input);
        let length = sequence_length(random);
        let bases = shape.bases(random, length);
        input.extend_from_slice(&bases);
        shape.end_line(random, &mut input);
        input.push(b'+');
        if random.chance(300) {
            input.extend_from_slice(&header);
        }
        shape.end_line(random, &mut input);
        input.extend((0..bases.len()).map(|_| random.pick(b"!#+5?@ABCDEFGHIJ~")));
        shape.end_line(random, &mut input);
    }
    if input.len() > last_record && random.chance(300) {
        input.truncate(last_record + random.below(input.len() - last_record));
    } else {
        shape.end_input(random, &mut input);
    }
    input
}

/// The sequences of `input` read a line at a time, as the line reader reads it: a line ends
/// at a line feed or at the end of the input, less one `\r` before it; a `>` line starts a
/// FASTA record, a `;` line is skipped, any other adds its bytes to the record, lines before
/// any header making one without; a FASTQ record is four lines, its second the sequence, and
/// one cut before that has none. Each record is split at the bytes that are not ACGT.
///
/// Also how many records there are, as [`SequenceParser::records`] counts them: those whose
/// header line ends with a line feed, and the lines before any FASTA header if they have bytes.
/// With `first_record`, the sequences have the indices of their records, counted from it.
fn reference_sequences(
    input: &[u8],
    format: DnaSequencesFileType,
    keep_ident: bool,
    min_length: usize,
    first_record: Option<u64>,
) -> (Vec<Record>, u64) {
    // Each line, and whether a line feed ends it.
    let mut lines: Vec<(&[u8], bool)> = Vec::new();
    let mut rest = input;
    while !rest.is_empty() {
        let (line, ended) = match rest.iter().position(|&byte| byte == b'\n') {
            Some(end) => (&rest[..end], true),
            None => (rest, false),
        };
        rest = &rest[(line.len() + ended as usize)..];
        lines.push((line.strip_suffix(b"\r").unwrap_or(line), ended));
    }
    let ident = |line: &[u8]| {
        if keep_ident {
            line.to_vec()
        } else {
            Vec::new()
        }
    };
    // Each record's header and bases, and whether it counts.
    let mut records: Vec<(Vec<u8>, Vec<u8>, bool)> = Vec::new();
    match format {
        DnaSequencesFileType::FASTA => {
            records.push((Vec::new(), Vec::new(), false));
            for (line, ended) in lines {
                match line.first() {
                    Some(b'>') => records.push((ident(line), Vec::new(), ended)),
                    Some(b';') => {}
                    _ => records.last_mut().unwrap().1.extend_from_slice(line),
                }
            }
            records[0].2 = !records[0].1.is_empty();
        }
        DnaSequencesFileType::FASTQ => {
            for record in lines.chunks(4) {
                let bases = record.get(1).map_or(&[][..], |line| line.0);
                records.push((ident(record[0].0), bases.to_vec(), record[0].1));
            }
        }
        DnaSequencesFileType::GFA => unreachable!(),
    }
    let mut sequences = Vec::new();
    let mut counted = 0;
    for (ident, bases, counts) in &records {
        let index = first_record.map(|first| first + counted);
        counted += *counts as u64;
        sequences.extend(
            Record::runs(ident, bases, min_length)
                .into_iter()
                .map(|run| Record {
                    record: index,
                    ..run
                }),
        );
    }
    (sequences, counted)
}

/// A parser's settings in the fuzz test.
#[derive(Clone, Copy, Debug)]
struct Config {
    keep_ident: bool,
    record_indices: bool,
    copyback: Option<usize>,
    min_seqlen: usize,
    bases_capacity: usize,
    ident_capacity: usize,
}

impl Config {
    /// Any settings, buffers so small that every record fills one included; with a copyback,
    /// `min_seqlen` is at most one more, so that no part of a run split is shorter.
    fn fuzzed(random: &mut Random) -> Self {
        let copyback = random
            .chance(500)
            .then(|| random.pick(&[0, 1, 2, 3, 30, 62, 100]));
        let min_seqlen = match copyback {
            Some(copyback) => random.below(copyback + 2),
            None => random.pick(&[0, 1, 2, 5, 31, 100]),
        };
        let carried = copyback.map_or(0, |copyback| copyback.max(min_seqlen));
        let smallest = match copyback {
            Some(_) => 2 * (carried.div_ceil(4) + 2 * BLOCK_SIZE / 4),
            None => 0,
        };
        let bases_capacity = match random.below(4) {
            0 => smallest + random.below(16),
            1 => smallest + random.below(1000),
            2 => smallest.max(random.pick(&[0, 1, 16, 64])),
            _ => DEFAULT_BASES_CAPACITY,
        };
        Self {
            keep_ident: random.chance(700),
            record_indices: random.chance(500),
            copyback,
            min_seqlen,
            bases_capacity,
            ident_capacity: random.pick(&[0, 1, 16, 100, DEFAULT_IDENT_CAPACITY]),
        }
    }
}

/// What is wrong with `buffer` as a parser of `config` hands it over, if anything.
fn check_buffer(
    buffer: &SequenceBuffer,
    format: DnaSequencesFileType,
    config: &Config,
) -> Option<String> {
    if buffer.is_empty() {
        return Some("an empty buffer was handed over".into());
    }
    if std::mem::discriminant(&buffer.format()) != std::mem::discriminant(&format) {
        return Some("a buffer of the wrong format was handed over".into());
    }
    for description in buffer.descriptions() {
        if description.bases_len < config.min_seqlen.max(1)
            || (description.bases_start + description.bases_len).div_ceil(4) > buffer.bases().len()
            || description.ident_start + description.ident_len > buffer.ident().len()
            || (!config.keep_ident && description.ident_len > 0)
        {
            return Some(format!(
                "{description:?} in a buffer of {} bytes of bases and {} of headers",
                buffer.bases().len(),
                buffer.ident().len()
            ));
        }
    }
    let indices = if config.record_indices {
        buffer.len()
    } else {
        0
    };
    if buffer.record_indices().len() != indices {
        return Some(format!(
            "{} record indices for {} sequences",
            buffer.record_indices().len(),
            buffer.len()
        ));
    }
    let last = buffer.descriptions().last().unwrap();
    if buffer.bases().len() != (last.bases_start + last.bases_len).div_ceil(4)
        || buffer.ident().len() != last.ident_start + last.ident_len
    {
        return Some(format!(
            "a buffer of {} bytes of bases and {} of headers, past its last sequence {last:?}",
            buffer.bases().len(),
            buffer.ident().len()
        ));
    }
    // Every buffer, given back or new, gets the room the parser makes in it: split when full in
    // the middle of a sequence, it never needs more for its bases, and with no headers kept it
    // needs none for them.
    let room = config.bases_capacity + 2 * (BLOCK_SIZE / 4);
    if (config.copyback.is_some() && buffer.bases.capacity() != room)
        || (!config.keep_ident && buffer.ident.capacity() != 0)
    {
        return Some(format!(
            "a buffer of {} bytes of bases, room for {} of a room of {room}, and room for {} \
             bytes of headers",
            buffer.bases().len(),
            buffer.bases.capacity(),
            buffer.ident.capacity()
        ));
    }
    None
}

/// The sequences of `input` from `parser` of `config`, reset to `format` and fed slices of
/// whole blocks of any number, none too; the problems of the buffers handed over. The callback
/// gives back, as `random` picks for the whole input, the buffer it got; a new one, keeping
/// each it gets; or the one it got two hand-overs before, not cleared. A buffer kept has to
/// hold at the end what it held when handed over.
fn parse_in_slices(
    parser: &mut SequenceParser,
    config: &Config,
    format: DnaSequencesFileType,
    input: &[u8],
    random: &mut Random,
) -> (Vec<Record>, Vec<String>) {
    parser.reset(format);
    let (mut records, mut problems) = (Vec::new(), Vec::new());
    // The buffers kept, with where their sequences start in `records`.
    let mut kept: Vec<(SequenceBuffer, usize)> = Vec::new();
    let give_back = random.below(3);
    let mut callback = |buffer: SequenceBuffer| {
        problems.extend(check_buffer(&buffer, format, config));
        let first = records.len();
        records.extend(Record::all_of(&buffer));
        match give_back {
            0 => buffer,
            1 => {
                kept.push((buffer, first));
                SequenceBuffer::new()
            }
            _ => {
                kept.push((buffer, first));
                if kept.len() > 2 {
                    kept.remove(0).0
                } else {
                    SequenceBuffer::new()
                }
            }
        }
    };
    let mut rest = input;
    while !rest.is_empty() {
        let blocks = match random.below(8) {
            0 => 0,
            1..=5 => 1 + random.below(4),
            6 => random.below(64),
            _ => rest.len().div_ceil(BLOCK_SIZE),
        };
        let (slice, next) = rest.split_at((blocks * BLOCK_SIZE).min(rest.len()));
        parser.parse_data(slice, &mut callback);
        rest = next;
    }
    parser.finish(&mut callback);
    for (buffer, first) in &kept {
        let again: Vec<Record> = Record::all_of(buffer).collect();
        if records.get(*first..*first + again.len()) != Some(&again[..]) {
            problems.push("a buffer kept by the callback changed after it was handed over".into());
        }
    }
    (records, problems)
}

/// A sequence for a failure message: its header and its bases as letters.
fn describe(record: Option<&Record>) -> String {
    match record {
        None => "none".into(),
        Some(record) => {
            let letters: String = record
                .codes
                .iter()
                .map(|&code| b"ACTG"[code as usize] as char)
                .collect();
            format!(
                "{} {letters} {:?}",
                record.ident.escape_ascii(),
                record.record
            )
        }
    }
}

/// How many of `parts` make `run`, split as a buffer full in its middle splits it: each part
/// past the first starting with the last `copyback` bases of the one before, which was
/// longer.
fn rebuild_run(run: &Record, parts: &[Record], copyback: usize) -> Option<usize> {
    let first = parts.first()?;
    if first.ident != run.ident
        || first.record != run.record
        || !run.codes.starts_with(&first.codes)
    {
        return None;
    }
    let (mut length, mut used) = (first.codes.len(), 1);
    while length < run.codes.len() {
        let part = parts.get(used)?;
        used += 1;
        if length <= copyback {
            return None;
        }
        let from = length - copyback;
        if part.ident != run.ident
            || part.record != run.record
            || part.codes.len() <= copyback
            || run.codes.get(from..from + part.codes.len()) != Some(&part.codes[..])
        {
            return None;
        }
        length = from + part.codes.len();
    }
    Some(used)
}

/// Whether `parts` are `runs`, each rebuilt by [`rebuild_run`] and maybe followed by a part
/// of its last `copyback` bases alone, from a split at its very end; else the first run no
/// reading of `parts` gets past. A part of the copyback alone can also be the next run.
fn rebuild(runs: &[Record], parts: &[Record], copyback: usize) -> Result<(), usize> {
    let mut starts = BTreeSet::from([0]);
    for (index, run) in runs.iter().enumerate() {
        let mut next = BTreeSet::new();
        for &start in &starts {
            let Some(used) = rebuild_run(run, &parts[start..], copyback) else {
                continue;
            };
            let end = start + used;
            next.insert(end);
            let tail = &run.codes[run.codes.len().saturating_sub(copyback)..];
            if run.codes.len() > copyback
                && parts.get(end).is_some_and(|part| {
                    part.ident == run.ident && part.record == run.record && part.codes == tail
                })
            {
                next.insert(end + 1);
            }
        }
        if next.is_empty() {
            return Err(index);
        }
        starts = next;
    }
    if starts.contains(&parts.len()) {
        Ok(())
    } else {
        Err(runs.len())
    }
}

/// `input` of `format` through a parser of `config` on `kernel`, fed slices drawn from
/// `seed`, against [`reference_sequences`]: what differs, if anything. Sometimes the parser
/// is left first in the middle of the same input read in the other format, for its reset to
/// forget.
fn check_input(
    format: DnaSequencesFileType,
    input: &[u8],
    config: &Config,
    kernel: MaskKernel,
    seed: u64,
) -> Result<(), String> {
    let mut random = Random::new(seed);
    let mut parser = ParserSettings {
        keep_ident: config.keep_ident,
        copyback_size: config.copyback,
        min_seqlen: config.min_seqlen,
        record_indices: config.record_indices,
        bases_capacity: config.bases_capacity,
        ident_capacity: config.ident_capacity,
    }
    .parser(kernel);
    if random.chance(250) {
        parser.reset(match format {
            DnaSequencesFileType::FASTA => DnaSequencesFileType::FASTQ,
            _ => DnaSequencesFileType::FASTA,
        });
        let blocks = random.below(input.len() / BLOCK_SIZE + 1);
        parser.parse_data(&input[..blocks * BLOCK_SIZE], |buffer| buffer);
    }
    // The records go on being counted across a reset.
    let first_record = parser.records();
    let (found, problems) = parse_in_slices(&mut parser, config, format, input, &mut random);
    if let Some(problem) = problems.first() {
        return Err(problem.clone());
    }
    let (expected, records) = reference_sequences(
        input,
        format,
        config.keep_ident,
        config.min_seqlen,
        config.record_indices.then_some(first_record),
    );
    if parser.records() != first_record + records {
        return Err(format!(
            "{} records counted, {records} expected",
            parser.records() - first_record
        ));
    }
    let first_difference = match config.copyback {
        None => (0..expected.len().max(found.len()))
            .find(|&index| expected.get(index) != found.get(index)),
        Some(copyback) => rebuild(&expected, &found, copyback).err(),
    };
    match first_difference {
        None => Ok(()),
        Some(index) => Err(format!(
            "{} sequences expected, {} found; at {index}, expected {}, found {}",
            expected.len(),
            found.len(),
            describe(expected.get(index)),
            describe(found.get(index))
        )),
    }
}

/// [`check_input`], a panic of the parser's included.
fn check_input_unwinding(
    format: DnaSequencesFileType,
    input: &[u8],
    config: &Config,
    kernel: MaskKernel,
    seed: u64,
) -> Result<(), String> {
    std::panic::catch_unwind(|| check_input(format, input, config, kernel, seed)).unwrap_or_else(
        |payload| {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()));
            Err(format!("panicked: {}", message.unwrap_or_default()))
        },
    )
}

/// A shorter input `fails` still holds for, by taking pieces of `input` out, of a power of two
/// bytes, from the largest: so whole blocks too, which keep the rest where it was in its
/// block. At most `tries` calls.
fn minimize(input: &[u8], mut fails: impl FnMut(&[u8]) -> bool, mut tries: usize) -> Vec<u8> {
    let mut input = input.to_vec();
    let mut size = input.len().next_power_of_two() / 2;
    while size > 0 && tries > 0 {
        let mut start = 0;
        while start < input.len() && tries > 0 {
            tries -= 1;
            let mut shorter = input.clone();
            shorter.drain(start..(start + size).min(input.len()));
            if fails(&shorter) {
                input = shorter;
            } else {
                start += size;
            }
        }
        size /= 2;
    }
    input
}

/// Random inputs of either format, of every shape [`fuzzed_fasta`] and [`fuzzed_fastq`]
/// make, through parsers of random settings on every kernel, against a parse a line at a
/// time. `GGCAT_PARSER_FUZZ_CASES` sets how many (300 by default) and
/// `GGCAT_PARSER_FUZZ_SEED` where they start: a failure names the seed of its case, and
/// shows its input shortened as far as it still fails.
#[test]
fn parsing_matches_a_parse_a_line_at_a_time() {
    let variable = |name: &str, default: u64| {
        std::env::var(name).map_or(default, |value| value.parse().expect(name))
    };
    let cases = variable("GGCAT_PARSER_FUZZ_CASES", 300);
    let first = variable("GGCAT_PARSER_FUZZ_SEED", 0);
    for seed in first..first + cases {
        let mut random = Random::new(seed);
        let (format, input) = if random.chance(500) {
            (DnaSequencesFileType::FASTA, fuzzed_fasta(&mut random))
        } else {
            (DnaSequencesFileType::FASTQ, fuzzed_fastq(&mut random))
        };
        let config = Config::fuzzed(&mut random);
        let slices = random.next();
        for kernel in supported_kernels() {
            let Err(error) = check_input_unwinding(format, &input, &config, kernel, slices) else {
                continue;
            };
            let hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let fails = |input: &[u8]| {
                check_input_unwinding(format, input, &config, kernel, slices).is_err()
            };
            let short = minimize(&input, fails, 20_000);
            let short_error = check_input_unwinding(format, &short, &config, kernel, slices);
            std::panic::set_hook(hook);
            panic!(
                "seed {seed}, {}, {kernel:?}, {config:?}, input of {} bytes: {error}\n\
                     shortened to {} bytes: {}\n{:?}",
                if matches!(format, DnaSequencesFileType::FASTA) {
                    "FASTA"
                } else {
                    "FASTQ"
                },
                input.len(),
                short.len(),
                short.escape_ascii(),
                short_error.err()
            );
        }
    }
}
