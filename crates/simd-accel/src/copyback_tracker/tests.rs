use super::*;
use crate::masks::MaskKernel;
use crate::sequence_parser::{BLOCK_SIZE, SequenceParser};
use utils::dna_sequence::DnaSequencesFileType;

/// A few files of one stream, at their offsets, with gaps between them as the headers of a tar
/// archive leave.
struct Stream {
    files: Vec<(u64, Vec<u8>)>,
}

/// What a byte of a FASTA file is, as the parser reads it: by the line it is in, or a line end.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum Byte {
    /// A `\r`, a `\n`, or the `\n` of a `\r\n`.
    #[default]
    LineEnd,
    Header,
    Comment,
    /// A byte of a sequence line.
    Base,
}

/// The bytes of a FASTA file classified line by line, independently of the parser: a line ends
/// at a `\r`, at a `\n` not right after one, or at the end of the file, the `\n` of a `\r\n`
/// belonging to no line; a line starting with `>` is a header, one with `;` a comment, any other
/// a sequence line, whose bytes are all bases.
fn classify(data: &[u8]) -> Vec<Byte> {
    let mut bytes = vec![Byte::LineEnd; data.len()];
    let mut start = 0;
    while start < data.len() {
        let end = data[start..]
            .iter()
            .position(|&byte| matches!(byte, b'\r' | b'\n'))
            .map_or(data.len(), |at| start + at);
        let kind = match data[start..end].first() {
            Some(b'>') => Byte::Header,
            Some(b';') => Byte::Comment,
            _ => Byte::Base,
        };
        bytes[start..end].fill(kind);
        start = match data[end..] {
            [b'\r', b'\n', ..] => end + 2,
            _ => end + 1,
        };
    }
    bytes
}

/// How many bases `bytes` holds.
fn bases_in(bytes: &[Byte]) -> u64 {
    bytes.iter().filter(|&&byte| byte == Byte::Base).count() as u64
}

/// A line ending of a stream whose line endings are of kind `kind`, the one picked out of them
/// by `pick`, any number: always `\n`; always `\r\n`; or any of `\n`, `\r\n`, a lone `\r` and
/// `\n\r`, which is two line ends.
fn line_ending(kind: u64, pick: u64) -> &'static [u8] {
    const ENDINGS: [&[u8]; 4] = [b"\n", b"\r\n", b"\r", b"\n\r"];
    match kind {
        0 => ENDINGS[0],
        1 => ENDINGS[1],
        _ => ENDINGS[(pick % 4) as usize],
    }
}

/// Random FASTA: records with headers, sequence lines of all lengths (none, unwrapped, wrapped),
/// bases and other bytes, empty lines, comments, line endings of every kind, lines before any
/// header.
fn fasta(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let endings = next(3);
    let mut data = Vec::new();
    if next(2) == 0 {
        data.extend_from_slice(b"ACGTNACGT");
        data.extend_from_slice(line_ending(endings, next(4)));
    }
    let mut record = 0;
    while data.len() < len {
        match next(12) {
            0 => data.extend_from_slice(b";a comment"),
            1 => {}
            2..=4 => {
                data.extend_from_slice(format!(">record_{record} some description").as_bytes());
                record += 1;
            }
            _ => {
                let line = match next(4) {
                    0 => next(8),
                    1 => 60 + next(30),
                    _ => next(300),
                };
                for _ in 0..line {
                    data.push(b"ACGTACGTACGTNa"[next(14) as usize]);
                }
            }
        }
        data.extend_from_slice(line_ending(endings, next(4)));
    }
    data
}

/// Copies with destinations in stream order, not overlapping, and sources anywhere before them
/// in the files, each ending before its destination starts, as lz_copyback cuts the matches.
/// None continues the one before it, which the tracker would merge into it.
fn copies(seed: u64, stream: &Stream, count: usize) -> Vec<LzCopy> {
    let mut state = seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound.max(1)
    };
    let (last_offset, last) = stream.files.last().unwrap();
    let stream_end = last_offset + last.len() as u64;
    let mut copies = Vec::new();
    let mut destination = 1 + next(200);
    while copies.len() < count && destination < stream_end {
        let longest = match next(4) {
            0 => 8,
            1 => 64,
            2 => 400,
            _ => 3000,
        };
        let mut offset = 1 + next(destination);
        if copies.last().is_some_and(|last: &LzCopy| {
            last.destination + last.len as u64 == destination && last.offset as u64 == offset
        }) {
            // Right after the one before, so 2 at least.
            offset = if offset < destination {
                offset + 1
            } else {
                offset - 1
            };
        }
        let len = (1 + next(longest))
            .min(stream_end - destination)
            .min(offset);
        copies.push(LzCopy {
            destination,
            offset: offset as u32,
            len: len as u32,
        });
        destination += len + next(40);
    }
    copies
}

/// What the tracker should end with for a copy: its status, the copy trimmed, its start and end
/// bases, and whether it opened.
type Model = (CopyStatus, LzCopy, u64, u64, bool);

/// What the tracker should end with for each copy, with `margin`: a model of the module's rules
/// over the stream's classified bytes. A source starts in the file that holds its start, or in
/// the next one; its start cut is its base `margin` bases past the base index there, if that
/// base is in the source and in the file; its end, the end of the file if it ends first, and
/// its end cut is past the base `margin` bases before its last one in the file, if that comes
/// after the start cut.
fn model_copies(stream: &Stream, copies: &[LzCopy], margin: u64) -> Vec<Model> {
    // Per file: its range, its bytes, and the base index before each byte and past the last.
    let mut files = Vec::new();
    let mut base_index = 0;
    for (offset, data) in &stream.files {
        let bytes = classify(data);
        let mut before = Vec::with_capacity(bytes.len() + 1);
        for &byte in &bytes {
            before.push(base_index);
            base_index += (byte == Byte::Base) as u64;
        }
        before.push(base_index);
        files.push((*offset, *offset + data.len() as u64, bytes, before));
    }
    copies
        .iter()
        .map(|copy| {
            let (source, source_end) = (copy.source_start(), copy.source_end());
            let Some((file_start, file_end, bytes, before)) =
                files.iter().find(|(_, end, ..)| source < *end)
            else {
                return (CopyStatus::Waiting, *copy, 0, 0, false);
            };
            let at = |position: u64| (position - file_start) as usize;
            let (from, to) = (source.max(*file_start), source_end.min(*file_end));
            let start_cut = before[at(from)] + margin;
            let Some(start) = (from..to).find(|&position| {
                bytes[at(position)] == Byte::Base && before[at(position)] == start_cut
            }) else {
                return (CopyStatus::Dropped, *copy, 0, 0, false);
            };
            let end_base = before[at(to.max(from))] - margin;
            if end_base <= start_cut {
                return (CopyStatus::Dropped, *copy, 0, 0, true);
            }
            let last = (start..to)
                .rev()
                .find(|&position| {
                    bytes[at(position)] == Byte::Base && before[at(position)] == end_base - 1
                })
                .unwrap();
            let trimmed = LzCopy {
                destination: copy.destination + (start - source),
                offset: copy.offset,
                len: (last + 1 - start) as u32,
            };
            (CopyStatus::Closed, trimmed, start_cut, end_base, true)
        })
        .collect()
}

/// Parses the stream's files in slices of `chunk` bytes (a whole number of blocks), the copies
/// all received before, with `margin`; returns the tracker, and the most blocks it remembered
/// for its end cuts after a slice.
fn track(
    stream: &Stream,
    copies: &[LzCopy],
    chunk: usize,
    kernel: MaskKernel,
    margin: u64,
) -> (CopybackTracker, usize) {
    let mut tracker = CopybackTracker::new();
    tracker.start_stream(margin as u32);
    for &copy in copies {
        tracker.add_copy(copy);
    }
    let mut parser = SequenceParser::with_kernel(kernel, false, None, 1, 1 << 16, 1 << 12);
    let mut remembered = 0;
    for (offset, data) in &stream.files {
        parser.reset(DnaSequencesFileType::FASTA);
        for (index, slice) in data.chunks(chunk).enumerate() {
            parser.parse_data_tracked(
                slice,
                offset + (index * chunk) as u64,
                &mut tracker,
                |buffer| buffer,
            );
            remembered = remembered.max(tracker.recent_base_blocks.len());
        }
        parser.finish(|buffer| buffer);
        tracker.end_file();
    }
    (tracker, remembered)
}

/// Checks the tracker against the model with each of `margins`, on two kernels and in slices of
/// several sizes: the bases counted, each copy, the stats, and that it remembers `margin + 2`
/// blocks at most for its end cuts. Returns how many copies closed with each margin.
fn check_against_model(
    stream: &Stream,
    copies: &[LzCopy],
    margins: &[u64],
    what: &str,
) -> Vec<u64> {
    let total: u64 = (stream.files.iter())
        .map(|(_, data)| bases_in(&classify(data)))
        .sum();
    let mut closed_counts = Vec::new();
    for &margin in margins {
        let model = model_copies(stream, copies, margin);
        let closed: Vec<&Model> = (model.iter())
            .filter(|model| model.0 == CopyStatus::Closed)
            .collect();
        let opened = model.iter().filter(|model| model.4).count() as u64;
        let skippable: u64 = closed.iter().map(|model| model.3 - model.2).sum();
        for kernel in [MaskKernel::detect(), MaskKernel::Portable] {
            for chunk in [BLOCK_SIZE, 3 * BLOCK_SIZE, 4096, 1 << 20] {
                let (tracker, remembered) = track(stream, copies, chunk, kernel, margin);
                let at = format!("{what}, margin {margin}, {kernel:?}, chunk {chunk}");
                assert_eq!(tracker.base_index(), total, "{at}: the bases counted");
                assert!(
                    remembered <= margin as usize + 2,
                    "{at}: {remembered} blocks remembered"
                );
                for (index, (tracked, model)) in tracker.copies().zip(&model).enumerate() {
                    let (status, copy, start_base, end_base, _) = *model;
                    let at = format!("{at}, copy {index} {:?}", copies[index]);
                    assert_eq!(tracked.status, status, "{at}");
                    if status == CopyStatus::Closed {
                        assert_eq!(
                            (tracked.copy, tracked.start_base, tracked.end_base),
                            (copy, start_base, end_base),
                            "{at}: trimmed"
                        );
                    }
                }
                let stats = tracker.stats();
                assert_eq!(
                    (stats.opened, stats.closed, stats.dropped_too_short),
                    (
                        opened,
                        closed.len() as u64,
                        (copies.len() - closed.len()) as u64
                    ),
                    "{at}"
                );
                assert_eq!(stats.skippable_bases, skippable, "{at}: skippable bases");
            }
        }
        closed_counts.push(closed.len() as u64);
    }
    closed_counts
}

/// The margins the tests follow copies with: none, the least, `k − 1` for a usual `k`, and more
/// than many lines' worth.
const MARGINS: [u64; 4] = [0, 1, 30, 100];

#[test]
fn copies_are_trimmed_by_their_margins_and_counted_in_bases() {
    for seed in 0..40 {
        let stream = Stream {
            files: vec![(0, fasta(seed, 60_000))],
        };
        let copies = copies(seed, &stream, 1000);
        let closed = check_against_model(&stream, &copies, &MARGINS, &format!("seed {seed}"));
        assert!(
            closed[0] > copies.len() as u64 / 2 && closed[3] > 0,
            "seed {seed}: {closed:?} of {} copies closed",
            copies.len()
        );
    }
}

#[test]
fn sources_do_not_run_across_files() {
    for seed in 0..30 {
        // Three files of one stream, 512 bytes of tar header between them, base indices going
        // on across them.
        let mut files = Vec::new();
        let mut offset = 1024;
        for file in 0..3 {
            let data = fasta(seed * 3 + file, 6_000 + 1_000 * file as usize);
            let len = data.len() as u64;
            files.push((offset, data));
            offset += len.next_multiple_of(512) + 512;
        }
        let stream = Stream { files };
        let copies = copies(seed, &stream, 300);
        let closed = check_against_model(
            &stream,
            &copies,
            &MARGINS,
            &format!("seed {seed}, three files"),
        );
        assert!(
            closed[0] > copies.len() as u64 / 2,
            "seed {seed}: {closed:?}"
        );
    }
}

/// FASTA of short lines, of 1 to 3 bases, with line ends of every kind, and headers, comments
/// and empty lines among them: margins span many lines, and the lines that are not sequence
/// fall inside margins and inside the bases kept.
fn short_lines(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut data = Vec::new();
    while data.len() < len {
        match next(10) {
            0 => data.extend_from_slice(b">header"),
            1 => data.extend_from_slice(b";comment"),
            2 => {}
            _ => {
                for _ in 0..1 + next(3) {
                    data.push(b"ACGTN"[next(5) as usize]);
                }
            }
        }
        data.extend_from_slice(line_ending(2, next(4)));
    }
    data
}

#[test]
fn margins_count_bases_across_line_ends_headers_and_comments() {
    for seed in 0..10 {
        let stream = Stream {
            files: vec![(0, short_lines(seed, 20_000))],
        };
        let copies = copies(seed, &stream, 300);
        let closed =
            check_against_model(&stream, &copies, &[1, 2, 30, 100], &format!("seed {seed}"));
        assert!(
            closed.iter().all(|&closed| closed > 0),
            "seed {seed}: {closed:?}"
        );
    }
}

/// A copy keeps its bases but `margin` at each end: one with `2 · margin` bases keeps none, and
/// is dropped; one with a base more keeps that one.
#[test]
fn a_copy_too_short_for_its_margins_is_dropped() {
    let mut data = b">r\n".to_vec();
    data.extend(b"ACGT".iter().cycle().take(1000));
    data.push(b'\n');
    let stream = Stream {
        files: vec![(0, data)],
    };
    for margin in [1, 30, 100] {
        // Sources at bytes 100 and 400, bases 97 and 397.
        let copies = [
            LzCopy {
                destination: 600,
                offset: 500,
                len: 2 * margin as u32,
            },
            LzCopy {
                destination: 900,
                offset: 500,
                len: 2 * margin as u32 + 1,
            },
        ];
        check_against_model(&stream, &copies, &[margin], &format!("margin {margin}"));
        let (tracker, _) = track(&stream, &copies, BLOCK_SIZE, MaskKernel::detect(), margin);
        let tracked: Vec<_> = tracker.copies().collect();
        assert_eq!(tracked[0].status, CopyStatus::Dropped, "margin {margin}");
        assert_eq!(tracked[1].status, CopyStatus::Closed, "margin {margin}");
        assert_eq!(
            (
                tracked[1].copy,
                tracked[1].start_base,
                tracked[1].base_count()
            ),
            (
                LzCopy {
                    destination: 900 + margin,
                    offset: 500,
                    len: 1
                },
                397 + margin,
                1
            ),
            "margin {margin}"
        );
    }
}

/// The blocks remembered for the end cuts are those with bases: a header of many blocks inside
/// a margin, or between the bases kept, adds none.
#[test]
fn a_long_header_adds_no_block_to_remember() {
    let mut data = b">a\n".to_vec();
    data.extend(b"ACGT".iter().cycle().take(300));
    data.extend_from_slice(b"\n>");
    data.extend(std::iter::repeat_n(b'x', 8000));
    data.push(b'\n');
    data.extend(b"TTGCA".iter().cycle().take(3000));
    data.push(b'\n');
    let stream = Stream {
        files: vec![(0, data)],
    };
    // Random copies, then two whose source runs across the header, from 103 bases before it to
    // 149 and 249 past it.
    let mut copies = copies(3, &stream, 100);
    for (destination, len) in [(20_000, 8255), (30_000, 8355)] {
        copies.push(LzCopy {
            destination,
            offset: destination as u32 - 200,
            len,
        });
    }
    let closed = check_against_model(&stream, &copies, &[1, 30, 100], "a long header");
    assert!(closed.iter().all(|&closed| closed >= 2), "{closed:?}");
}

/// Lines of a base each: a margin of 100 runs across 100 lines and more.
#[test]
fn margins_run_across_lines_of_one_base() {
    let mut data = b">r\n".to_vec();
    for index in 0..4000u64 {
        data.push(b"ACGT"[(index % 4) as usize]);
        data.extend_from_slice(line_ending(2, index * 7 / 3));
    }
    let stream = Stream {
        files: vec![(0, data)],
    };
    let copies = copies(5, &stream, 200);
    let closed = check_against_model(&stream, &copies, &[1, 30, 100], "one base per line");
    assert!(closed.iter().all(|&closed| closed > 0), "{closed:?}");
}

/// One base per block, in a line of its own, the rest of the block a comment: the end cuts can
/// be `margin + 1` blocks back, which are all remembered, and no more.
#[test]
fn one_base_per_block_remembers_margin_plus_2_blocks() {
    let mut block = b"A\n;".to_vec();
    block.extend(std::iter::repeat_n(b'x', BLOCK_SIZE - 4));
    block.push(b'\n');
    assert_eq!(block.len(), BLOCK_SIZE);
    let stream = Stream {
        files: vec![(0, block.repeat(400))],
    };
    // Random copies, then two long enough to keep bases.
    let mut copies = copies(9, &stream, 100);
    for (destination, len) in [(26_000, 12_000), (40_000, 9_000)] {
        copies.push(LzCopy {
            destination,
            offset: destination as u32 - 1000,
            len,
        });
    }
    let closed = check_against_model(&stream, &copies, &[30], "one base per block");
    assert!(closed[0] > 0);
    let (_, remembered) = track(&stream, &copies, BLOCK_SIZE, MaskKernel::detect(), 30);
    assert_eq!(remembered, 32);
}

/// `fasta(seed)` with `count` copies planted: each destination made a byte-for-byte repeat of
/// its source, in order, so that the stream holds what the copies say.
fn planted(seed: u64, len: usize, count: usize) -> (Stream, Vec<LzCopy>) {
    let mut data = fasta(seed, len);
    let copies = copies(
        seed,
        &Stream {
            files: vec![(0, data.clone())],
        },
        count,
    );
    for copy in &copies {
        let source = copy.source_start() as usize;
        data.copy_within(
            source..source + copy.len as usize,
            copy.destination as usize,
        );
    }
    (
        Stream {
            files: vec![(0, data)],
        },
        copies,
    )
}

/// Checks the contract of [`TrackedCopy`] on a stream of one file whose copies repeat their
/// sources byte for byte: a closed copy passes rule 1 when, by the classifier, the byte before
/// its destination is in a sequence line, or with a margin, a line end; and rule 2 when its
/// destination ends in the file. Then the destination holds `end_base − start_base` bases.
/// Returns each closed copy's index, and whether it passed.
fn check_contract(
    stream: &Stream,
    copies: &[LzCopy],
    margin: u64,
    what: &str,
) -> Vec<(usize, bool)> {
    let (tracker, _) = track(stream, copies, 4096, MaskKernel::detect(), margin);
    let (file_start, data) = &stream.files[0];
    let bytes = classify(data);
    (tracker.copies().enumerate())
        .filter(|(_, tracked)| tracked.status == CopyStatus::Closed)
        .map(|(index, tracked)| {
            let destination = (tracked.copy.destination - file_start) as usize;
            let end = destination + tracked.copy.len as usize;
            let rule_1 = destination > 0
                && match bytes[destination - 1] {
                    Byte::Base => true,
                    Byte::LineEnd => margin >= 1,
                    Byte::Header | Byte::Comment => false,
                };
            let passes = rule_1 && end <= data.len();
            if passes {
                assert_eq!(
                    bases_in(&bytes[destination..end]),
                    tracked.base_count(),
                    "{what}, margin {margin}, copy {index} {tracked:?}"
                );
            }
            (index, passes)
        })
        .collect()
}

/// Wherever a consumer may skip a closed copy by the contract, its destination holds the bases
/// counted: on random FASTA with random copies planted, line starts, headers and comments
/// running into destinations included.
#[test]
fn closed_copies_hold_the_bases_counted_where_they_may_be_skipped() {
    let margins = [0, 1, 2, 30, 100];
    // Per margin, how many closed copies passed the rules, of how many.
    let mut passed = [(0, 0); 5];
    for seed in 0..20 {
        let (stream, copies) = planted(seed, 60_000, 1000);
        for (margin, passed) in margins.iter().zip(&mut passed) {
            let checked = check_contract(&stream, &copies, *margin, &format!("seed {seed}"));
            passed.0 += checked.iter().filter(|(_, passes)| *passes).count();
            passed.1 += checked.len();
        }
    }
    assert!(
        passed.iter().all(|&(passed, closed)| passed * 10 > closed),
        "passed of closed per margin {margins:?}: {passed:?}"
    );
}

/// Where a destination starts, as the contract tests craft it.
#[derive(Copy, Clone, Debug)]
enum Destination {
    /// In a header that runs on into the copy, which repeats a source in a sequence line.
    InHeader,
    /// The same, in a comment.
    InComment,
    /// At a line start, the source starting on a `>` in the middle of a sequence line: the
    /// destination is a header.
    LineStartOnMidLineByte(u8),
    /// The start cut on the first base of a line, after this line end, which is inside the copy:
    /// with no margin, its first bytes.
    LineStartAfter(&'static [u8]),
    /// In the middle of a sequence line.
    MidLine,
}

impl Destination {
    /// Whether the contract lets a consumer skip it with `margin`.
    fn may_skip(self, margin: u64) -> bool {
        match self {
            Self::InHeader | Self::InComment | Self::LineStartOnMidLineByte(_) => false,
            Self::LineStartAfter(_) => margin >= 1,
            Self::MidLine => true,
        }
    }
}

/// The contract rejects exactly the destinations a skip would count wrong, with each margin: one
/// in a header or a comment that runs past its start cut, one at a line start whose first byte
/// starts a header or a comment there, and, with no margin, any at a line start. Its line starts
/// with a margin, and those in a sequence line, hold the bases counted.
#[test]
fn the_skip_contract_rejects_exactly_the_unsafe_destinations() {
    let cases = [
        Destination::InHeader,
        Destination::InComment,
        Destination::LineStartOnMidLineByte(b'>'),
        Destination::LineStartOnMidLineByte(b';'),
        Destination::LineStartAfter(b"\n"),
        Destination::LineStartAfter(b"\r"),
        Destination::LineStartAfter(b"\r\n"),
        Destination::MidLine,
    ];
    let mut state = 0x1234_5678_9abc_def1u64;
    let mut bases = move |count: usize| -> Vec<u8> {
        (0..count)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                b"ACGT"[(state % 4) as usize]
            })
            .collect()
    };
    for margin in [0, 1, 2, 30, 100] {
        // The sources, one record of them, then the destinations, each the bytes of its source
        // after a crafted start: `(destination kind, source, len)`.
        let mut data = b">sources\n".to_vec();
        let mut sources = Vec::new();
        for case in cases {
            data.extend(bases(20));
            let source = data.len();
            match case {
                Destination::LineStartOnMidLineByte(byte) => data.push(byte),
                Destination::LineStartAfter(line_end) => {
                    // The start cut on the first base after the line end, which is in the copy
                    // with no margin too.
                    data.extend(bases(margin as usize));
                    data.extend_from_slice(line_end);
                }
                _ => {}
            }
            data.extend(bases(300));
            data.push(b'\n');
            data.extend(bases(200));
            let len = data.len() - source - 100;
            data.push(b'\n');
            sources.push((case, source, len));
        }
        data.extend_from_slice(b">destinations\n");
        let mut copies = Vec::new();
        for &(case, source, len) in &sources {
            let start: &[u8] = match case {
                Destination::InHeader => b"\n>header ",
                Destination::InComment => b"\n;comment ",
                Destination::LineStartOnMidLineByte(_) => b"\n",
                Destination::LineStartAfter(_) | Destination::MidLine => b"\nACGT",
            };
            data.extend_from_slice(start);
            copies.push(LzCopy {
                destination: data.len() as u64,
                offset: (data.len() - source) as u32,
                len: len as u32,
            });
            data.extend_from_within(source..source + len);
            data.extend_from_slice(b"ACGT\n");
        }
        let stream = Stream {
            files: vec![(0, data)],
        };
        check_against_model(
            &stream,
            &copies,
            &[margin],
            &format!("crafted, margin {margin}"),
        );
        let checked = check_contract(&stream, &copies, margin, "crafted");
        assert_eq!(
            checked.len(),
            cases.len(),
            "margin {margin}: not all closed"
        );
        for (index, passes) in checked {
            assert_eq!(
                passes,
                cases[index].may_skip(margin),
                "margin {margin}, {:?}",
                cases[index]
            );
        }
    }
}

#[test]
fn copies_whose_destination_was_passed_are_dropped_from_the_front() {
    let stream = Stream {
        files: vec![(0, fasta(7, 50_000))],
    };
    let copies = copies(7, &stream, 1000);
    let mut tracker = CopybackTracker::new();
    let mut parser = SequenceParser::new(false, None, 1);
    parser.reset(DnaSequencesFileType::FASTA);
    let data = &stream.files[0].1;
    let mut added = 0;
    // As the binary reader hands a slice out: once the decoder is its window past it, the
    // farthest a copy reaches back, so that every copy of the slice's bytes is known.
    let window = copies.iter().map(|copy| copy.offset as u64).max().unwrap();
    for (index, slice) in data.chunks(4096).enumerate() {
        let (start, end) = ((index * 4096) as u64, (index * 4096 + slice.len()) as u64);
        while added < copies.len() && copies[added].destination < end + window {
            tracker.add_copy(copies[added]);
            added += 1;
        }
        parser.parse_data_tracked(slice, start, &mut tracker, |buffer| buffer);
        tracker.retire_before(end);
        // Those left are the last ones pushed, all of those not passed.
        let left = (copies[..added].iter())
            .filter(|copy| copy.destination >= end)
            .count();
        assert_eq!(tracker.live_copies(), left);
        assert!(
            tracker
                .copies()
                .all(|source| source.copy.destination >= end)
        );
    }
    parser.finish(|buffer| buffer);
    tracker.end_file();
    assert_eq!(tracker.stats().dropped_late, 0);
}

/// A copy that comes after the parser read its source whole is dropped; one it read in part,
/// trimmed to the rest.
#[test]
fn late_copies_keep_only_what_is_left_to_parse() {
    let data =
        b">a\nACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT\nACGTACGTACGTACGT\n"
            .repeat(4);
    let mut tracker = CopybackTracker::new();
    let mut parser = SequenceParser::new(false, None, 1);
    parser.reset(DnaSequencesFileType::FASTA);
    parser.parse_data_tracked(&data[..128], 0, &mut tracker, |buffer| buffer);
    tracker.add_copy(LzCopy {
        destination: 300,
        offset: 290,
        len: 50,
    });
    tracker.add_copy(LzCopy {
        destination: 320,
        offset: 220,
        len: 40,
    });
    parser.parse_data_tracked(&data[128..], 128, &mut tracker, |buffer| buffer);
    parser.finish(|buffer| buffer);
    tracker.end_file();
    let tracked: Vec<_> = tracker.copies().collect();
    assert_eq!(tracked[0].status, CopyStatus::Dropped);
    assert_eq!(tracked[1].status, CopyStatus::Closed);
    assert_eq!(tracked[1].copy.source_start(), 128);
    assert_eq!(tracked[1].copy.destination, 348);
    assert_eq!(tracker.stats().dropped_late, 1);
}

/// A copy min-queue pops its entries by key, as a sorted list of them would: keys pushed behind,
/// at and ahead of the last one popped, as the tracker pushes them at each block, popped up to the
/// end of the next block; `retain`, `drain` and `clear` at times in between.
#[test]
fn the_copy_min_queue_pops_in_key_order() {
    use std::collections::BTreeSet;
    let mut state = 0x1234_5678_9abc_def1u64;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound.max(1)
    };
    let mut queue = CopyMinQueue::new();
    let mut sorted = BTreeSet::new();
    let (mut last, mut end) = (0u64, 64u64);
    let mut copy = 0u64;
    let mut popped = 0;
    for _ in 0..200_000 {
        for _ in 0..next(4) {
            let key = match next(10) {
                0 => last.saturating_sub(next(5_000)),
                1 => last,
                2 => end + next(1 << 26),
                _ => end + next(1 << 20),
            };
            queue.push(key, copy);
            sorted.insert((key, copy));
            copy += 1;
        }
        end += match next(50) {
            0 => next(1 << 28),
            _ => 64 * (1 + next(4)),
        };
        while let Some((key, copy)) = queue.peek() {
            if key >= end {
                break;
            }
            queue.pop();
            // The smallest key, any copy of those that have it.
            assert_eq!(sorted.first().map(|first| first.0), Some(key));
            assert!(sorted.remove(&(key, copy)), "{copy} popped twice");
            last = key;
            popped += 1;
        }
        match next(1000) {
            0..=9 => {
                let modulo = 2 + next(5);
                queue.retain(|copy| copy % modulo != 0);
                sorted.retain(|(_, copy)| copy % modulo != 0);
            }
            10 => {
                let mut drained: Vec<_> = queue.drain().collect();
                drained.sort_unstable();
                assert!(drained.iter().eq(sorted.iter()), "not the entries left");
                sorted.clear();
            }
            11 => {
                queue.clear();
                sorted.clear();
            }
            _ => {}
        }
        assert_eq!(
            queue.next_key(),
            sorted.first().map_or(u64::MAX, |first| first.0)
        );
        assert_eq!(queue.len(), sorted.len());
        assert_eq!(queue.is_empty(), sorted.is_empty());
    }
    assert!(popped > 100_000, "{popped} popped");
}

/// A match continuing the last copy, which comes on its own once the copy left lz_copyback's
/// queue, makes the copy grow to its end while the parser has not passed its source, as if it
/// had come whole; once the copy is closed or dropped, it is a copy of its own, trimmed to what
/// is left to parse.
#[test]
fn a_copy_continued_is_followed_to_its_new_end() {
    let data = fasta(5, 4_000);
    let stream = Stream {
        files: vec![(0, data.clone())],
    };
    // Its source is bytes 100..200, then 100..500.
    let copy = LzCopy {
        destination: 3000,
        offset: 2900,
        len: 100,
    };
    let continuation = LzCopy {
        destination: 3100,
        offset: 2900,
        len: 300,
    };
    let grown = LzCopy { len: 400, ..copy };
    // Continued before the parser gets to the source, in it, and past it.
    let rest = continuation.with_source_start(256);
    for (parsed, whole, margin) in [
        (0, vec![grown], 0),
        (128, vec![grown], 0),
        (256, vec![copy, rest], 0),
        (0, vec![grown], 30),
        (128, vec![grown], 30),
        (256, vec![copy, rest], 30),
    ] {
        let mut tracker = CopybackTracker::new();
        tracker.start_stream(margin as u32);
        tracker.add_copy(copy);
        let mut parser = SequenceParser::new(false, None, 1);
        parser.reset(DnaSequencesFileType::FASTA);
        parser.parse_data_tracked(&data[..parsed], 0, &mut tracker, |buffer| buffer);
        tracker.add_copy(continuation);
        parser.parse_data_tracked(&data[parsed..], parsed as u64, &mut tracker, |buffer| {
            buffer
        });
        parser.finish(|buffer| buffer);
        tracker.end_file();
        assert_eq!(
            tracker.stats().received,
            whole.len() as u64,
            "after {parsed} bytes"
        );
        let expected = model_copies(&stream, &whole, margin);
        for (tracked, expected) in tracker.copies().zip(&expected) {
            let (status, trimmed, start_base, end_base, _) = *expected;
            let at = format!("continued after {parsed} bytes, margin {margin}");
            assert_eq!(tracked.status, status, "{at}");
            assert_eq!(
                (tracked.copy, tracked.start_base, tracked.end_base),
                (trimmed, start_base, end_base),
                "{at}"
            );
        }
    }
}

/// A copy that continues the last one makes it grow only up to their offset in all: the pieces
/// lz_copyback cuts a periodic repeat into stay copies of their own.
#[test]
fn adjacent_full_pieces_are_not_merged() {
    let mut tracker = CopybackTracker::new();
    let piece = LzCopy {
        destination: 1000,
        offset: 300,
        len: 300,
    };
    tracker.add_copy(piece);
    tracker.add_copy(LzCopy {
        destination: 1300,
        ..piece
    });
    tracker.add_copy(LzCopy {
        destination: 1600,
        len: 100,
        ..piece
    });
    assert_eq!(tracker.live_copies(), 3);
    // Within the offset, a continuation is merged.
    tracker.add_copy(LzCopy {
        destination: 1700,
        len: 200,
        ..piece
    });
    assert_eq!(tracker.live_copies(), 3);
    assert_eq!(
        tracker.copies().last().unwrap().copy,
        LzCopy {
            destination: 1600,
            ..piece
        }
    );
}
