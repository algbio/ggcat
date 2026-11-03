use super::*;
use crate::masks::MaskKernel;
use crate::sequence_buffer::SequenceBuffer;
use crate::sequence_parser::{BLOCK_SIZE, SequenceParser};
use std::collections::BTreeSet;
use utils::dna_sequence::DnaSequencesFileType;

/// A few files of one stream, at their offsets, with gaps between them as the headers of a tar
/// archive leave.
pub(super) struct Stream {
    pub(super) files: Vec<(u64, Vec<u8>)>,
}

/// What a byte of a FASTA file is, as the parser reads it: by the line it is in, or a line end.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum Byte {
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
pub(super) fn classify(data: &[u8]) -> Vec<Byte> {
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
pub(super) fn line_ending(kind: u64, pick: u64) -> &'static [u8] {
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
pub(super) fn fasta(seed: u64, len: usize) -> Vec<u8> {
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
/// in the files, up to about `max_offset` bytes back, each ending before its destination
/// starts, as lz_copyback cuts the matches. None continues the one before it, which the
/// tracker would merge into it.
pub(super) fn copies(seed: u64, stream: &Stream, count: usize, max_offset: u64) -> Vec<LzCopy> {
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
        let mut offset = 1 + next(destination.min(max_offset));
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

/// Parses the stream's files in slices of `chunk` bytes (a whole number of blocks), with
/// `margin`, the copies received as the binary reader hands them out: before a slice, those
/// whose destination lies before its end plus the window, the largest offset, so that every
/// copy of the slice's bytes is known. Retires the copies the parser passed after each slice,
/// checking the forget rule of the history then, and the rest at the end; returns the tracker
/// and every copy just before it was retired, in order. The parser skips the copies if
/// `skipping`, which only planted streams allow, else the tracker only follows them.
fn track(
    stream: &Stream,
    copies: &[LzCopy],
    chunk: usize,
    kernel: MaskKernel,
    margin: u64,
    skipping: bool,
) -> (CopybackTracker, Vec<TrackedCopy>) {
    let mut parser = SequenceParser::with_kernel(kernel, false, None, 1, 1 << 16, 1 << 12);
    track_with(
        &mut parser,
        stream,
        copies,
        chunk,
        margin,
        skipping,
        false,
        |buffer| buffer,
    )
}

/// As [`track`], with `parser`, whose buffers go to `callback`, and with header marks if
/// `header_marks`.
#[allow(clippy::too_many_arguments)]
fn track_with(
    parser: &mut SequenceParser,
    stream: &Stream,
    copies: &[LzCopy],
    chunk: usize,
    margin: u64,
    skipping: bool,
    header_marks: bool,
    mut callback: impl FnMut(SequenceBuffer) -> SequenceBuffer,
) -> (CopybackTracker, Vec<TrackedCopy>) {
    let mut tracker = if skipping {
        CopybackTracker::new()
    } else {
        CopybackTracker::following_only()
    };
    tracker.start_stream(margin as u32, header_marks);
    let window = copies
        .iter()
        .map(|copy| copy.offset as u64)
        .max()
        .unwrap_or(0);
    let mut added = 0;
    let mut retired = Vec::new();
    for (offset, data) in &stream.files {
        parser.reset(DnaSequencesFileType::FASTA);
        tracker.start_file(Some(offset + data.len() as u64));
        for (index, slice) in data.chunks(chunk).enumerate() {
            let start = offset + (index * chunk) as u64;
            let end = start + slice.len() as u64;
            while added < copies.len() && copies[added].destination < end + window {
                tracker.add_copy(copies[added]);
                added += 1;
            }
            parser.parse_data_tracked(slice, start, &mut tracker, &mut callback);
            retired.extend(
                (tracker.copies())
                    .take_while(|tracked| tracked.copy.destination < end)
                    .cloned(),
            );
            tracker.retire_before(end);
            check_forget_rule(&tracker);
        }
        parser.finish(&mut callback);
        tracker.end_file();
    }
    retired.extend(tracker.copies().cloned());
    tracker.retire_before(u64::MAX);
    (tracker, retired)
}

/// Checks that the history kept no group it could forget: none but the last has both the group
/// after it starting at or before the offset bound, and its bases before the base bound.
fn check_forget_rule(tracker: &CopybackTracker) {
    let offset_bound = tracker
        .parsed_to
        .saturating_sub(tracker.largest_copy_offset);
    let base_bound = tracker.base_index().saturating_sub(tracker.margin + 1);
    for pair in tracker.base_run_history.groups().windows(2) {
        let ((_, end), (next_start, _)) = (pair[0], pair[1]);
        assert!(
            !(next_start <= offset_bound && end.unwrap() <= base_bound),
            "a group left to forget: {pair:?}, bounds {offset_bound} and {base_bound}"
        );
    }
}

/// Checks the tracker against the model with each of `margins`, on two kernels and in slices of
/// several sizes: the bases counted, each copy, and the stats. The parser skips the copies if
/// `skipping`, on a planted stream only. Returns how many copies closed with each margin.
fn check_against_model(
    stream: &Stream,
    copies: &[LzCopy],
    margins: &[u64],
    what: &str,
    skipping: bool,
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
                let (tracker, retired) = track(stream, copies, chunk, kernel, margin, skipping);
                let at = format!("{what}, margin {margin}, {kernel:?}, chunk {chunk}");
                assert_eq!(tracker.base_index(), total, "{at}: the bases counted");
                assert_eq!(retired.len(), copies.len(), "{at}: the copies retired");
                for (index, (tracked, model)) in retired.iter().zip(&model).enumerate() {
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
        let copies = copies(seed, &stream, 1000, u64::MAX);
        let closed =
            check_against_model(&stream, &copies, &MARGINS, &format!("seed {seed}"), false);
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
        let copies = copies(seed, &stream, 300, u64::MAX);
        let closed = check_against_model(
            &stream,
            &copies,
            &MARGINS,
            &format!("seed {seed}, three files"),
            false,
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
        let copies = copies(seed, &stream, 300, u64::MAX);
        let closed = check_against_model(
            &stream,
            &copies,
            &[1, 2, 30, 100],
            &format!("seed {seed}"),
            false,
        );
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
        check_against_model(
            &stream,
            &copies,
            &[margin],
            &format!("margin {margin}"),
            false,
        );
        let (_, tracked) = track(
            &stream,
            &copies,
            BLOCK_SIZE,
            MaskKernel::detect(),
            margin,
            false,
        );
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

/// The history holds a group per run of bases: a header of many blocks inside a margin, or
/// between the bases kept, adds none.
#[test]
fn a_long_header_adds_no_group() {
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
    let mut copies = copies(3, &stream, 100, u64::MAX);
    for (destination, len) in [(20_000, 8255), (30_000, 8355)] {
        copies.push(LzCopy {
            destination,
            offset: destination as u32 - 200,
            len,
        });
    }
    let closed = check_against_model(&stream, &copies, &[1, 30, 100], "a long header", false);
    assert!(closed.iter().all(|&closed| closed >= 2), "{closed:?}");
    // The copies reach back to the stream's start, so nothing is forgotten.
    let (tracker, _) = track(
        &stream,
        &copies,
        BLOCK_SIZE,
        MaskKernel::detect(),
        30,
        false,
    );
    assert_eq!(
        tracker.base_run_history.groups(),
        [(3, Some(300)), (8306, Some(3300))]
    );
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
    let copies = copies(5, &stream, 200, u64::MAX);
    let closed = check_against_model(&stream, &copies, &[1, 30, 100], "one base per line", false);
    assert!(closed.iter().all(|&closed| closed > 0), "{closed:?}");
}

/// One base per block, in a line of its own, the rest of the block a comment: the end cuts can
/// be `margin + 1` blocks back, and the runs of all the blocks make one group.
#[test]
fn one_base_per_block_makes_one_group() {
    let mut block = b"A\n;".to_vec();
    block.extend(std::iter::repeat_n(b'x', BLOCK_SIZE - 4));
    block.push(b'\n');
    assert_eq!(block.len(), BLOCK_SIZE);
    let stream = Stream {
        files: vec![(0, block.repeat(400))],
    };
    // Random copies, then two long enough to keep bases.
    let mut copies = copies(9, &stream, 100, u64::MAX);
    for (destination, len) in [(26_000, 12_000), (40_000, 9_000)] {
        copies.push(LzCopy {
            destination,
            offset: destination as u32 - 1000,
            len,
        });
    }
    let closed = check_against_model(&stream, &copies, &[30], "one base per block", false);
    assert!(closed[0] > 0);
    let (tracker, _) = track(
        &stream,
        &copies,
        BLOCK_SIZE,
        MaskKernel::detect(),
        30,
        false,
    );
    assert_eq!(tracker.base_run_history.groups(), [(0, Some(400))]);
}

/// `fasta(seed)` with `count` copies planted, of offsets up to about `max_offset`: each
/// destination made a byte-for-byte repeat of its source, in order, so that the stream holds
/// what the copies say.
fn planted(seed: u64, len: usize, count: usize, max_offset: u64) -> (Stream, Vec<LzCopy>) {
    plant(seed, fasta(seed, len), count, max_offset)
}

/// `data`, a stream of one file, with `count` copies planted as [`planted`] does.
pub(super) fn plant(
    seed: u64,
    mut data: Vec<u8>,
    count: usize,
    max_offset: u64,
) -> (Stream, Vec<LzCopy>) {
    let copies = copies(
        seed,
        &Stream {
            files: vec![(0, data.clone())],
        },
        count,
        max_offset,
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

/// Random numbers below a bound, as the generators here draw them.
fn random(seed: u64) -> impl FnMut(u64) -> u64 {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound.max(1)
    }
}

/// Records of up to 3000 bases, wrapped at `width` columns with `\n`, or on one line, up to
/// 20000 bases, for a `width` of 0: regular, so that their runs make groups of many.
pub(super) fn wrapped_records(seed: u64, len: usize, width: usize) -> Vec<u8> {
    let mut next = random(seed);
    let mut data = Vec::new();
    let mut record = 0;
    while data.len() < len {
        data.extend_from_slice(format!(">record_{record}\n").as_bytes());
        record += 1;
        let bases = if width == 0 {
            next(20_000)
        } else {
            next(3_000)
        };
        for base in 0..bases {
            data.push(b"ACGT"[next(4) as usize]);
            if width != 0 && (base + 1) % width as u64 == 0 {
                data.push(b'\n');
            }
        }
        data.push(b'\n');
    }
    data
}

/// `file_count` files of `fasta`, of `len` bytes each, one after the other, with a gap of 512
/// to 2047 bytes before each, as tar headers leave, and `count` copies planted in them, of
/// offsets up to about `max_offset`: those of the copies of [`copies`] whose every destination
/// byte in a file has its source byte, `offset` before, in a file too, planted byte by byte in
/// order. So a copy can start in a gap, and run past its file's end.
fn planted_files(
    seed: u64,
    file_count: u64,
    len: usize,
    count: usize,
    max_offset: u64,
) -> (Stream, Vec<LzCopy>) {
    let mut next = random(seed);
    let mut files = Vec::new();
    let mut offset = 0;
    for file in 0..file_count {
        offset += 512 + next(1536);
        let data = fasta(seed + file, len);
        let file_len = data.len() as u64;
        files.push((offset, data));
        offset += file_len;
    }
    let mut stream = Stream { files };
    let locate = |files: &[(u64, Vec<u8>)], position: u64| {
        (files.iter().enumerate())
            .find(|(_, (start, data))| (*start..*start + data.len() as u64).contains(&position))
            .map(|(file, (start, _))| (file, (position - start) as usize))
    };
    let kept: Vec<LzCopy> = (copies(seed, &stream, count, max_offset).into_iter())
        .filter(|copy| {
            (copy.destination..copy.destination + copy.len as u64).all(|destination| {
                locate(&stream.files, destination).is_none()
                    || locate(&stream.files, destination - copy.offset as u64).is_some()
            })
        })
        .collect();
    for copy in &kept {
        for destination in copy.destination..copy.destination + copy.len as u64 {
            if let Some((file, at)) = locate(&stream.files, destination) {
                let (source_file, source_at) =
                    locate(&stream.files, destination - copy.offset as u64).unwrap();
                stream.files[file].1[at] = stream.files[source_file].1[source_at];
            }
        }
    }
    (stream, kept)
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
    let (_, retired) = track(stream, copies, 4096, MaskKernel::detect(), margin, false);
    let (file_start, data) = &stream.files[0];
    let bytes = classify(data);
    (retired.iter().enumerate())
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
        let (stream, copies) = planted(seed, 60_000, 1000, u64::MAX);
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
            false,
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
    let copies = copies(7, &stream, 1000, u64::MAX);
    // The copies do not repeat their sources, so the parser cannot skip them.
    let mut tracker = CopybackTracker::following_only();
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
    let mut tracker = CopybackTracker::following_only();
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
        let mut tracker = CopybackTracker::following_only();
        tracker.start_stream(margin as u32, false);
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

/// The sequences a parser hands over, each as its bases' codes, one per base.
fn collect_codes(
    sequences: &mut Vec<Vec<u8>>,
) -> impl FnMut(SequenceBuffer) -> SequenceBuffer + '_ {
    |buffer| {
        for sequence in buffer.sequences() {
            let read = sequence.seq;
            let start = read.start as usize;
            // SAFETY: the read borrows the bytes of its `size` bases, from base `start`.
            let bytes =
                unsafe { std::slice::from_raw_parts(read.data, (start + read.size).div_ceil(4)) };
            sequences.push(
                (start..start + read.size)
                    .map(|base| (bytes[base / 4] >> (2 * (base % 4))) & 3)
                    .collect(),
            );
        }
        buffer
    }
}

/// The sequences of the stream as the parser would hand them over with the bytes of `skipped`
/// left out, from the classifier: a header byte, a skipped byte, a base that is not ACGT and a
/// file's end end a sequence; an ACGT base extends it; line ends and comments change nothing.
/// Those shorter than `min_length` are left out.
fn sequences_without(stream: &Stream, skipped: &[(u64, u64)], min_length: usize) -> Vec<Vec<u8>> {
    let mut sequences = Vec::new();
    let mut ranges = skipped.iter().peekable();
    for (file_start, data) in &stream.files {
        let mut sequence = Vec::new();
        let mut end = |sequence: &mut Vec<u8>| {
            if sequence.len() >= min_length {
                sequences.push(std::mem::take(sequence));
            }
            sequence.clear();
        };
        for (index, (&byte, kind)) in data.iter().zip(classify(data)).enumerate() {
            let position = file_start + index as u64;
            while ranges.next_if(|range| range.1 <= position).is_some() {}
            if ranges.peek().is_some_and(|range| range.0 <= position) {
                end(&mut sequence);
                continue;
            }
            match kind {
                Byte::Header => end(&mut sequence),
                Byte::Base if matches!(byte & 0b1101_1111, b'A' | b'C' | b'G' | b'T') => {
                    sequence.push((byte >> 1) & 3)
                }
                Byte::Base => end(&mut sequence),
                Byte::LineEnd | Byte::Comment => {}
            }
        }
        end(&mut sequence);
    }
    sequences
}

/// The k-mers of `k` bases of `sequences`.
fn kmers(sequences: &[Vec<u8>], k: usize) -> BTreeSet<&[u8]> {
    (sequences.iter())
        .flat_map(|sequence| sequence.windows(k))
        .collect()
}

/// The streams the skipping tests run on, all planted: random FASTA, with copies reaching
/// anywhere back or a few kilobytes only, so that the history forgets; wrapped and one-line
/// records; and several files with gaps between them.
fn planted_streams(seed: u64) -> Vec<(&'static str, Stream, Vec<LzCopy>)> {
    let (one_line, one_line_copies) = plant(seed, wrapped_records(seed, 60_000, 0), 1500, 4000);
    let (wrapped, wrapped_copies) = plant(seed, wrapped_records(seed, 60_000, 60), 1500, 4000);
    let (near, near_copies) = planted(seed, 60_000, 1500, 3000);
    let (far, far_copies) = planted(seed, 60_000, 1000, u64::MAX);
    let (files, files_copies) = planted_files(seed, 4, 15_000, 1500, 6000);
    vec![
        ("random FASTA", far, far_copies),
        ("random FASTA, near copies", near, near_copies),
        ("one-line records", one_line, one_line_copies),
        ("wrapped records", wrapped, wrapped_copies),
        ("four files", files, files_copies),
    ]
}

/// Skipping changes none of the tracker's records: over planted streams, the copies a skipping
/// tracker retires are those of one that only follows them, with the same cuts and base
/// indices, and so are the bases counted and the following side of the stats; and both match
/// the model. Copies of skipped bytes, at any depth, are followed and skipped in turn: the
/// replays leave exact runs. The streams hold what the skips must get through: chains of
/// copies, chunk ends on a line end in a skip, skips ending on a chunk's end, repeats inside
/// one line, stops on the `\n` of a `\r\n`, forgotten groups, and gaps between files.
#[test]
fn skipping_changes_no_record() {
    // Skips; groups forgotten; chunk ends on a line end inside a skip; skips ending on a chunk's
    // end; the deepest chain of skipped copies; skipped copies whose source is in their line;
    // copies starting on the `\n` of a `\r\n`.
    let mut seen = [0u64; 7];
    for seed in 0..12 {
        for (what, stream, copies) in planted_streams(seed) {
            let what = format!("{what}, seed {seed}");
            check_against_model(&stream, &copies, &[1, 30], &what, true);
            for margin in [0, 1, 30, 100] {
                let kernel = if seed % 4 == 3 {
                    MaskKernel::Portable
                } else {
                    MaskKernel::detect()
                };
                for chunk in [BLOCK_SIZE, 3 * BLOCK_SIZE, 4096, 1 << 16] {
                    let at = format!("{what}, margin {margin}, chunk {chunk}");
                    let (following, expected) =
                        track(&stream, &copies, chunk, kernel, margin, false);
                    let (skipping, retired) = track(&stream, &copies, chunk, kernel, margin, true);
                    assert_eq!(retired.len(), expected.len(), "{at}");
                    for (index, (tracked, expected)) in retired.iter().zip(&expected).enumerate() {
                        assert_eq!(tracked, expected, "{at}, copy {index}");
                    }
                    assert_eq!(skipping.base_index(), following.base_index(), "{at}");
                    let (a, b) = (skipping.stats(), following.stats());
                    assert_eq!(
                        (
                            a.received,
                            a.opened,
                            a.closed,
                            a.dropped_too_short,
                            a.dropped_late
                        ),
                        (
                            b.received,
                            b.opened,
                            b.closed,
                            b.dropped_too_short,
                            b.dropped_late
                        ),
                        "{at}"
                    );
                    assert_eq!(a.skippable_bases, b.skippable_bases, "{at}");
                    seen[0] += a.skips;
                    seen[1] += skipping.base_run_history.forgotten();
                    count_skip_shapes(&stream, &copies, &retired, &skipping, chunk, &mut seen);
                }
            }
        }
    }
    let [
        skips,
        forgotten,
        cut_on_line_ends,
        ending_on_chunk_ends,
        depth,
        same_line,
        on_crlf,
    ] = seen;
    assert!(
        skips > 10_000
            && forgotten > 1000
            && cut_on_line_ends > 100
            && ending_on_chunk_ends > 10
            && depth >= 3
            && same_line > 100
            && on_crlf > 10,
        "{seen:?}"
    );
}

/// Counts the shapes [`skipping_changes_no_record`] wants its streams to hold, into `seen`.
fn count_skip_shapes(
    stream: &Stream,
    copies: &[LzCopy],
    retired: &[TrackedCopy],
    skipping: &CopybackTracker,
    chunk: usize,
    seen: &mut [u64; 7],
) {
    let ranges = &skipping.skipped_ranges;
    let skipped_in = |start: u64, end: u64| {
        let index = ranges.partition_point(|range| range.1 <= start);
        ranges
            .get(index)
            .is_some_and(|range| range.0 <= start && end <= range.1)
    };
    for (file_start, data) in &stream.files {
        let bytes = classify(data);
        let file_end = file_start + data.len() as u64;
        for &(start, end) in ranges {
            if start < *file_start || end > file_end {
                continue;
            }
            // Chunk ends inside the skip, on a line end.
            let first = (start - file_start).div_ceil(chunk as u64) * chunk as u64;
            for cut in (first..end - file_start).step_by(chunk) {
                if cut > start - file_start && bytes[cut as usize] == Byte::LineEnd {
                    seen[2] += 1;
                }
            }
            seen[3] += ((end - file_start) % chunk as u64 == 0) as u64;
        }
        for copy in copies {
            let at = copy.destination.wrapping_sub(*file_start) as usize;
            if at >= 1 && at < data.len() && data[at] == b'\n' && data[at - 1] == b'\r' {
                seen[6] += 1;
            }
        }
    }
    // The chains of skipped copies: each one's depth is one more than the deepest skipped copy
    // its source overlaps.
    let mut depths: Vec<(u64, u64, u64)> = Vec::new();
    for tracked in retired {
        let (start, end) = (
            tracked.copy.destination,
            tracked.copy.destination + tracked.copy.len as u64,
        );
        if tracked.status != CopyStatus::Closed || !skipped_in(start, end) {
            continue;
        }
        let (source_start, source_end) = (tracked.copy.source_start(), tracked.copy.source_end());
        let depth = 1
            + (depths.iter())
                .filter(|&&(start, end, _)| start < source_end && source_start < end)
                .map(|&(_, _, depth)| depth)
                .max()
                .unwrap_or(0);
        seen[4] = seen[4].max(depth);
        depths.push((start, end, depth));
        // A source in the line of its destination.
        if let Some((file_start, data)) = (stream.files.iter()).find(|(file_start, data)| {
            (*file_start..file_start + data.len() as u64).contains(&start)
        }) {
            let from = source_start.max(*file_start) - file_start;
            let line = &data[from as usize..(end - file_start) as usize];
            seen[5] += !line.iter().any(|&byte| matches!(byte, b'\r' | b'\n')) as u64;
        }
    }
}

/// The parser's sequences with skipping are those of the stream with the skipped ranges cut
/// out, exactly; so the k-mers of `margin + 1` bases are those of a full parse.
#[test]
fn skipping_leaves_out_exactly_the_skipped_ranges() {
    for seed in 0..8 {
        for (what, stream, copies) in planted_streams(seed) {
            for margin in [0, 1, 30] {
                for chunk in [BLOCK_SIZE, 4096] {
                    let at = format!("{what}, seed {seed}, margin {margin}, chunk {chunk}");
                    let kernel = MaskKernel::detect();
                    let (mut skipped, mut full) = (Vec::new(), Vec::new());
                    let mut parser =
                        SequenceParser::with_kernel(kernel, false, None, 1, 4096, 1024);
                    let (tracker, _) = track_with(
                        &mut parser,
                        &stream,
                        &copies,
                        chunk,
                        margin,
                        true,
                        false,
                        collect_codes(&mut skipped),
                    );
                    let mut parser =
                        SequenceParser::with_kernel(kernel, false, None, 1, 4096, 1024);
                    track_with(
                        &mut parser,
                        &stream,
                        &copies,
                        chunk,
                        margin,
                        false,
                        false,
                        collect_codes(&mut full),
                    );
                    let expected = sequences_without(&stream, &tracker.skipped_ranges, 1);
                    assert_eq!(skipped.len(), expected.len(), "{at}: the sequences");
                    for (index, (sequence, expected)) in skipped.iter().zip(&expected).enumerate() {
                        assert!(sequence == expected, "{at}: sequence {index}");
                    }
                    assert!(
                        full == sequences_without(&stream, &[], 1),
                        "{at}: the full parse"
                    );
                    let k = margin as usize + 1;
                    assert!(kmers(&skipped, k) == kmers(&full, k), "{at}: the {k}-mers");
                }
            }
        }
    }
}

/// Random bases, as the crafted tests need them.
fn random_bases(next: &mut impl FnMut(u64) -> u64, count: usize) -> Vec<u8> {
    (0..count).map(|_| b"ACGT"[next(4) as usize]).collect()
}

/// A destination starting in a header that runs into it skips from the source's next sequence
/// line, shifted, even when a chunk ends in between; with no sequence line left in the copy
/// after the header, it is not skipped.
#[test]
fn a_destination_starting_in_a_header_skips_from_the_next_sequence_line() {
    let mut next = random(11);
    for margin in [1, 30] {
        for chunk in [BLOCK_SIZE, 4096] {
            for next_line_kept in [true, false] {
                let mut data = b">sources\n".to_vec();
                data.extend(random_bases(&mut next, 200));
                // The last 100 bases of the line, then the next one, up to 50 bases before its
                // end, or only `margin` bases of it, which the end cut leaves out.
                let source = data.len() - 100;
                data.push(b'\n');
                let next_line = data.len();
                data.extend(random_bases(&mut next, 300));
                data.push(b'\n');
                let len = if next_line_kept {
                    next_line + 250 - source
                } else {
                    next_line + margin as usize - source
                };
                data.extend_from_slice(b">destination ");
                let destination = data.len();
                data.extend_from_within(source..source + len);
                data.extend_from_slice(b"ACGT\n");
                let offset = (destination - source) as u64;
                let copy = LzCopy {
                    destination: destination as u64,
                    offset: offset as u32,
                    len: len as u32,
                };
                let stream = Stream {
                    files: vec![(0, data)],
                };
                let at = format!("margin {margin}, chunk {chunk}, next line {next_line_kept}");
                let kernel = MaskKernel::detect();
                let (tracker, retired) = track(&stream, &[copy], chunk, kernel, margin, true);
                assert_eq!(retired[0].status, CopyStatus::Closed, "{at}");
                let end = retired[0].copy.destination + retired[0].copy.len as u64;
                let stats = tracker.stats();
                if next_line_kept {
                    assert_eq!((stats.deferred_skip_starts, stats.skips), (1, 1), "{at}");
                    assert_eq!(
                        tracker.skipped_ranges,
                        [(next_line as u64 + offset, end)],
                        "{at}"
                    );
                } else {
                    assert_eq!(stats.skips_refused_without_start, 1, "{at}");
                    assert_eq!(stats.skips, 0, "{at}");
                }
            }
        }
    }
}

/// A comment line fewer than `margin` bases before a skip start cancels the skip, as a k-mer
/// could run across it; one at `margin` bases does not, nor one inside a skipped range, which
/// the parser never enters.
#[test]
fn a_comment_within_the_margin_before_a_start_cancels_the_skip() {
    let mut next = random(13);
    let margin = 30;
    // A source of a short line, then a long one: copied after a `;`, the short line is a
    // comment, and the start cut falls `margin − 20` bases into the long line.
    let mut data = b">sources\n".to_vec();
    let short_source = data.len();
    data.extend(random_bases(&mut next, 20));
    data.push(b'\n');
    data.extend(random_bases(&mut next, 300));
    data.push(b'\n');
    // A long line, a comment, a long line: copied from inside the first to inside the last.
    let inner_source = data.len() + 100;
    data.extend(random_bases(&mut next, 200));
    data.extend_from_slice(b"\n;inner comment\n");
    data.extend(random_bases(&mut next, 200));
    data.push(b'\n');
    let mut copies = Vec::new();
    let mut plant = |data: &mut Vec<u8>, prefix: &[u8], source: usize, len: usize| {
        data.extend_from_slice(prefix);
        copies.push(LzCopy {
            destination: data.len() as u64,
            offset: (data.len() - source) as u32,
            len: len as u32,
        });
        data.extend_from_within(source..source + len);
        data.extend(random_bases(&mut next, 100));
        data.push(b'\n');
    };
    // Refused: the comment is 10 bases before the start cut.
    plant(&mut data, b">near\n;", short_source, 21 + 200);
    // Skipped: the comment is `margin` bases before it.
    plant(&mut data, b">far\n;comment\n", inner_source + 116 + 50, 150);
    // Skipped: the comment inside, never entered; then the next copy, right after, too.
    plant(&mut data, b">inner\n", inner_source, 116 + 150);
    let stream = Stream {
        files: vec![(0, data)],
    };
    for chunk in [BLOCK_SIZE, 4096] {
        let (tracker, retired) = track(&stream, &copies, chunk, MaskKernel::detect(), margin, true);
        assert!(
            retired
                .iter()
                .all(|tracked| tracked.status == CopyStatus::Closed)
        );
        let stats = tracker.stats();
        assert_eq!(
            (stats.skips, stats.skips_refused_near_comment),
            (2, 1),
            "chunk {chunk}: {stats:?}"
        );
    }
}

/// A copy whose end is past its file's end is not skipped, the bytes past it not being the
/// file's; one starting in the gap before a file, its destination as far before a later one,
/// is skipped there, past the file's first byte, with a margin; and a stop in a gap, whose copy
/// cannot be skipped there, holds up no skip after it.
#[test]
fn skips_stay_in_their_file() {
    let mut next = random(17);
    let first_line = random_bases(&mut next, 500);
    let mut first = first_line.clone();
    first.extend_from_slice(b"\n>a\n");
    first.extend(random_bases(&mut next, 1000));
    first.push(b'\n');
    let (first_start, gap) = (700u64, 700u64);
    let second_start = first_start + first.len() as u64 + gap;
    // The second file starts as the first, and ends 300 bytes into a copy of 500.
    let mut second = first_line[..400].to_vec();
    second.extend_from_slice(b"\n>b\n");
    second.extend(random_bases(&mut next, 300));
    let past_end_source = first_start + 600;
    let past_end = second_start + second.len() as u64;
    second.extend_from_slice(&first[600..900]);
    let copies = [
        // From the gap before the first file to the gap before the second.
        LzCopy {
            destination: second_start - 100,
            offset: (second_start - first_start) as u32,
            len: 500,
        },
        // Ending past the second file.
        LzCopy {
            destination: past_end,
            offset: (past_end - past_end_source) as u32,
            len: 500,
        },
    ];
    let stream = Stream {
        files: vec![(first_start, first), (second_start, second)],
    };
    for margin in [0, 1, 30] {
        let (tracker, retired) = track(
            &stream,
            &copies,
            BLOCK_SIZE,
            MaskKernel::detect(),
            margin,
            true,
        );
        let stats = tracker.stats();
        let at = format!("margin {margin}: {stats:?}");
        assert!(
            retired
                .iter()
                .all(|tracked| tracked.status == CopyStatus::Closed),
            "{at}"
        );
        assert_eq!(stats.skips_refused_past_file_end, 1, "{at}");
        if margin == 0 {
            assert_eq!(
                (stats.skips, stats.skips_refused_at_file_start),
                (0, 1),
                "{at}"
            );
        } else {
            let trimmed = retired[0].copy;
            let skipped = (
                trimmed.destination,
                trimmed.destination + trimmed.len as u64,
            );
            assert_eq!(trimmed.len as u64, 400 - 2 * margin, "{at}");
            assert_eq!(tracker.skipped_ranges, [skipped], "{at}");
        }
    }
    // A stop in the gap: its copy refused there, as never reached, and the copy after it, in
    // the second file, skipped.
    let mut stream = stream;
    stream.files[1].1.truncate(400);
    let source = first_start + 100;
    let in_second = second_start + 50;
    stream.files[1].1[50..300].copy_from_slice(&first_line[100..350]);
    let copies = [
        LzCopy {
            destination: second_start - 400,
            offset: (second_start - 400 - source) as u32,
            len: 300,
        },
        LzCopy {
            destination: in_second,
            offset: (in_second - source) as u32,
            len: 250,
        },
    ];
    let (tracker, retired) = track(&stream, &copies, BLOCK_SIZE, MaskKernel::detect(), 30, true);
    let stats = tracker.stats();
    assert!(
        retired
            .iter()
            .all(|tracked| tracked.status == CopyStatus::Closed)
    );
    assert_eq!(
        (stats.skips, stats.skips_refused_unreached),
        (1, 1),
        "{stats:?}"
    );
    assert_eq!(tracker.skipped_ranges, [(in_second + 30, in_second + 220)]);
}

/// A source starting on a `>` in the middle of a sequence line, whose destination starts a
/// line, where the `>` starts a header: with a first line of `margin` bases or fewer, the
/// header ends before the start cut, which is skipped from; with more, the start falls in the
/// header and is deferred to the next line, or refused without one; with no margin, a line
/// start is refused.
#[test]
fn a_source_starting_on_a_mid_line_header_byte() {
    let mut next = random(19);
    for (margin, first_line, next_line_kept) in [
        (30, 20, true),
        (30, 29, true),
        (30, 60, true),
        (30, 60, false),
        (1, 0, true),
        (0, 10, true),
    ] {
        let mut data = b">sources\n".to_vec();
        data.extend(random_bases(&mut next, 50));
        let source = data.len();
        data.push(b'>');
        data.extend(random_bases(&mut next, first_line));
        data.push(b'\n');
        let next_line = data.len();
        data.extend(random_bases(&mut next, 300));
        data.push(b'\n');
        // The `>` and the first line, then the next one up to 50 bases before its end, or no
        // base of it past the end cut.
        let len = if next_line_kept {
            next_line + 250 - source
        } else {
            next_line + margin - source
        };
        data.extend_from_slice(b">destinations\n");
        let destination = data.len();
        data.extend_from_within(source..source + len);
        data.extend_from_slice(b"ACGT\n");
        let offset = (destination - source) as u64;
        let copy = LzCopy {
            destination: destination as u64,
            offset: offset as u32,
            len: len as u32,
        };
        let stream = Stream {
            files: vec![(0, data)],
        };
        let at = format!("margin {margin}, first line {first_line}, next line {next_line_kept}");
        let (tracker, retired) = track(
            &stream,
            &[copy],
            BLOCK_SIZE,
            MaskKernel::detect(),
            margin as u64,
            true,
        );
        let stats = tracker.stats();
        assert_eq!(retired[0].status, CopyStatus::Closed, "{at}");
        let (start, end) = (
            retired[0].copy.destination,
            retired[0].copy.destination + retired[0].copy.len as u64,
        );
        if margin == 0 {
            assert_eq!(
                (stats.skips, stats.skips_refused_without_margin),
                (0, 1),
                "{at}"
            );
        } else if first_line < margin {
            assert_eq!(tracker.skipped_ranges, [(start, end)], "{at}");
        } else if next_line_kept {
            assert_eq!(stats.deferred_skip_starts, 1, "{at}");
            assert_eq!(
                tracker.skipped_ranges,
                [(next_line as u64 + offset, end)],
                "{at}"
            );
        } else {
            assert_eq!(
                (stats.skips, stats.skips_refused_without_start),
                (0, 1),
                "{at}"
            );
        }
    }
}

/// A sequence as the parser hands it over: its header, its record's index, and its bases'
/// codes.
type Sequence = (Vec<u8>, u64, Vec<u8>);

/// The sequences of every buffer a parser with record indices hands over.
fn collect_sequences(
    sequences: &mut Vec<Sequence>,
) -> impl FnMut(SequenceBuffer) -> SequenceBuffer + '_ {
    |buffer| {
        let mut codes = Vec::new();
        let buffer = collect_codes(&mut codes)(buffer);
        let headers = buffer
            .sequences()
            .map(|sequence| sequence.ident_data.to_vec());
        for ((header, &record), codes) in headers.zip(buffer.record_indices()).zip(codes) {
            sequences.push((header, record, codes));
        }
        buffer
    }
}

/// The lines of a FASTA file, cut as `classify` cuts them: each one's start and end, its line
/// end, or the file's end for a line cut by it.
fn line_spans(data: &[u8]) -> Vec<(usize, usize)> {
    let mut lines = Vec::new();
    let mut start = 0;
    while start < data.len() {
        let end = (data[start..].iter())
            .position(|&byte| matches!(byte, b'\r' | b'\n'))
            .map_or(data.len(), |at| start + at);
        lines.push((start, end));
        start = match data[end..] {
            [b'\r', b'\n', ..] => end + 2,
            _ => end + 1,
        };
    }
    lines
}

/// As [`sequences_without`], each sequence with the header and the index of its record as a
/// full parse has them, and the records counted in all: every header line that ends, and in
/// each file the lines before its first header, if one has a byte.
fn records_without(
    stream: &Stream,
    skipped: &[(u64, u64)],
    min_length: usize,
) -> (Vec<Sequence>, u64) {
    let mut sequences = Vec::new();
    let mut records = 0u64;
    let mut ranges = skipped.iter().peekable();
    for (file_start, data) in &stream.files {
        let input_records = records;
        let mut header = Vec::new();
        let mut codes = Vec::new();
        let mut end = |header: &[u8], records: u64, codes: &mut Vec<u8>| {
            if codes.len() >= min_length {
                sequences.push((header.to_vec(), records - 1, std::mem::take(codes)));
            }
            codes.clear();
        };
        for (start, line_end) in line_spans(data) {
            let line = &data[start..line_end];
            match line.first() {
                Some(b'>') => {
                    end(&header, records, &mut codes);
                    if line_end < data.len() {
                        records += 1;
                        header = line.to_vec();
                    }
                }
                Some(b';') => {}
                _ => {
                    if !line.is_empty() && records == input_records {
                        records += 1;
                        header.clear();
                    }
                    for (index, &byte) in line.iter().enumerate() {
                        let position = file_start + (start + index) as u64;
                        while ranges.next_if(|range| range.1 <= position).is_some() {}
                        if ranges.peek().is_some_and(|range| range.0 <= position)
                            || !matches!(byte & 0b1101_1111, b'A' | b'C' | b'G' | b'T')
                        {
                            end(&header, records, &mut codes);
                        } else {
                            codes.push((byte >> 1) & 3);
                        }
                    }
                }
            }
        }
        end(&header, records, &mut codes);
    }
    (sequences, records)
}

/// With header marks, the skips keep the headers and record indices exact: every sequence
/// after a skip has the header and the index of its record in a full parse, the records the
/// skipped bytes hold counted, and so does the parser's count of records.
#[test]
fn header_marks_keep_headers_and_record_indices_exact() {
    let mut headers_skipped = 0;
    for seed in 0..6 {
        for (what, stream, copies) in planted_streams(seed) {
            for margin in [1, 30] {
                for chunk in [BLOCK_SIZE, 4096] {
                    let at = format!("{what}, seed {seed}, margin {margin}, chunk {chunk}");
                    let kernel = MaskKernel::detect();
                    let mut parser = SequenceParser::with_kernel(kernel, true, None, 1, 4096, 1024)
                        .with_record_indices();
                    let mut sequences = Vec::new();
                    let (tracker, _) = track_with(
                        &mut parser,
                        &stream,
                        &copies,
                        chunk,
                        margin,
                        true,
                        true,
                        collect_sequences(&mut sequences),
                    );
                    let skipped = &tracker.skipped_ranges;
                    let (expected, records) = records_without(&stream, skipped, 1);
                    assert_eq!(sequences.len(), expected.len(), "{at}: the sequences");
                    for (index, (sequence, expected)) in sequences.iter().zip(&expected).enumerate()
                    {
                        assert!(sequence == expected, "{at}: sequence {index}");
                    }
                    assert_eq!(parser.records(), records, "{at}: the records");
                    // The header lines inside the skipped ranges.
                    for (file_start, data) in &stream.files {
                        for (start, end) in line_spans(data) {
                            let range = (file_start + start as u64, file_start + end as u64);
                            headers_skipped += (data[start] == b'>'
                                && (skipped.iter())
                                    .any(|skipped| skipped.0 <= range.0 && range.1 < skipped.1))
                                as u64;
                        }
                    }
                }
            }
        }
    }
    assert!(headers_skipped > 1000, "{headers_skipped} headers skipped");
}
