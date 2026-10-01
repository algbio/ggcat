//! Using the LZ copies of the input to skip DNA the parser has already seen.
//!
//! An LZ decoder reports, for a compressed input, that the bytes at `dst..dst + len`
//! repeat the bytes at `src..src + len`. Everything here works in **absolute base
//! coordinates**: the index of a base in the concatenation of every ACGT base the SIMD
//! lexer parses from one tracked input (an archive, or a loose file), in stream order.
//! Newlines, carriage returns, headers, comments, non-ACGT characters and opaque bytes
//! hold no coordinate, and need none.
//!
//! # Tags
//!
//! Whatever the lexer parses is *tagged* in [`ParseTags`], kept as far back as a copy can
//! reach: every run of consecutive bases on contiguous bytes with its byte position and
//! coordinate, and the few things between runs that the packer needs -- where a record
//! starts and its header ends, and runs of characters that are not ACGT. Newlines,
//! carriage returns and comments need no tag. A byte range stands for exactly the tagged
//! bases inside it.
//!
//! # Skipping a copy
//!
//! When the lexer reaches a copy's destination ([`CopybackTracker::push_span`]), the
//! source's tags say where its bases are. The destination is lexed up to the byte of the
//! source's base `2k - m` (the front margin), and the lexer checks its own state there:
//! in sequence, inside a record. The source's byte is a base, so its state is the same;
//! from equal states the same bytes parse the same way, so the rest of the copy is known
//! without looking at it. The lexer then jumps: the source's tags up to the last `2k - m`
//! bases (the back margin) are *replayed* -- their bases dropped from the packer as a
//! read break, exactly as a run of `N` would be, but keeping their coordinates; their
//! record starts and ambiguity characters handed over as the lexer would have; the tags
//! themselves copied to the destination, so that it can serve as a later copy's source.
//! Lexing resumes at the back margin. A header inside the skipped stretch costs nothing
//! extra, and newlines never matter: the stretch is a range of bases.
//!
//! Only the copy's first partial line can parse differently at the two ends (a header at
//! one, sequence at the other), which the state check catches. An opaque stretch at the
//! source (a tar member boundary) resets the lexer there, so a skip stops before it and
//! the rest of the copy is planned again as a copy of its own.
//!
//! # Why the margins
//!
//! Dropping the interior of a repeated range is only sound if every k-mer it removes was
//! already emitted at the source. That holds for the k-mers lying entirely inside the
//! copy; the ones straddling its boundary reach into bytes the copy does not cover. So
//! `2k - m` bases -- the longest a super-k-mer can be -- are parsed at each end, and a
//! copy of at most `4k - 2m` bases yields no hole at all.
//!
//! The k-mer **set** is then exact. Their **counts** are not, and further tracking is
//! needed to recover the exact counts.

use std::collections::VecDeque;
use std::ops::Range;

use crate::batch::{BatchSink, NO_ABS_BASE};
use crate::fasta_lexer::{FastaSimdLexer, LexOutput};
use crate::masks::{RawFastaMasks, low_mask_u64};
use crate::packer::LanePacker;
use crate::stats::CopybackStats;

/// One LZ copy in absolute decompressed-stream byte coordinates: the `len` bytes at
/// `dst..dst + len` repeat the ones at `src..src + len`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CopySpan {
    pub src: u64,
    pub dst: u64,
    pub len: u64,
}

impl CopySpan {
    pub fn src_end(&self) -> u64 {
        self.src + self.len
    }
    pub fn dst_end(&self) -> u64 {
        self.dst + self.len
    }
}

/// A half-open range of absolute base coordinates.
pub type BaseRange = Range<u64>;

/// `len` consecutive bases parsed from the contiguous bytes `[pos, pos + len)`, the
/// first of which has absolute base coordinate `abs`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BaseRun {
    pub pos: u64,
    pub abs: u64,
    pub len: u64,
}

impl BaseRun {
    pub fn end(&self) -> u64 {
        self.pos + self.len
    }
}

/// What lies between runs of bases that the packer still needs to hear about.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Mark {
    /// A record starts at `pos`, with the header `[pos, header_end)`: its `>` and
    /// everything up to the line terminator, carriage returns inside included.
    Record { pos: u64, header_end: u64 },
    /// `count` contiguous characters of a sequence line that are not ACGT.
    Invalid { pos: u64, count: u64 },
}

impl Mark {
    pub fn pos(&self) -> u64 {
        match *self {
            Mark::Record { pos, .. } | Mark::Invalid { pos, .. } => pos,
        }
    }

    fn end(&self) -> u64 {
        match *self {
            Mark::Record { header_end, .. } => header_end,
            Mark::Invalid { pos, count } => pos + count,
        }
    }

    fn shifted(&self, shift: u64) -> Self {
        match *self {
            Mark::Record { pos, header_end } => Mark::Record {
                pos: pos + shift,
                header_end: header_end + shift,
            },
            Mark::Invalid { pos, count } => Mark::Invalid {
                pos: pos + shift,
                count,
            },
        }
    }
}

/// Tags kept allocated across tracked inputs; a large input must not pin its peak.
const RETAINED_TAGS: usize = 1 << 16;

/// What the lexer found in the bytes parsed or skipped so far, kept as far back as a
/// copy can reach: runs of bases with their coordinates, and [`Mark`]s. This holds the
/// only count of absolute base coordinates.
#[derive(Default)]
pub struct ParseTags {
    runs: VecDeque<BaseRun>,
    marks: VecDeque<Mark>,
    /// Merged byte ranges that are not part of any FASTA stream, kept as far back.
    opaque: VecDeque<Range<u64>>,
    /// Absolute base coordinate of the next base.
    next_abs: u64,
    /// Bytes kept behind the frontier.
    retain: u64,
    /// Everything below this byte has been forgotten.
    forgotten: u64,
}

impl ParseTags {
    /// Starts a tracked input from coordinate zero, keeping `retain` bytes behind the
    /// frontier: at least the input's largest LZ distance, so that every copy's source is
    /// still known when its destination arrives.
    pub fn reset(&mut self, retain: u64) {
        self.runs.clear();
        self.runs.shrink_to(RETAINED_TAGS);
        self.marks.clear();
        self.marks.shrink_to(RETAINED_TAGS);
        self.opaque.clear();
        self.opaque.shrink_to(64);
        self.next_abs = 0;
        self.retain = retain;
        self.forgotten = 0;
    }

    /// Absolute base coordinate of the next base: the bases tagged so far.
    pub fn next_abs(&self) -> u64 {
        self.next_abs
    }

    pub fn runs_len(&self) -> usize {
        self.runs.len()
    }

    pub fn marks_len(&self) -> usize {
        self.marks.len()
    }

    /// The runs of bases, in order.
    pub fn runs(&self) -> impl Iterator<Item = &BaseRun> {
        self.runs.iter()
    }

    /// The marks, in order.
    pub fn marks(&self) -> impl Iterator<Item = &Mark> {
        self.marks.iter()
    }

    /// Tags `len` bases parsed from the bytes `[pos, pos + len)`, and returns the
    /// coordinate of the first.
    #[inline(always)]
    pub fn push_run(&mut self, pos: u64, len: u64) -> u64 {
        let abs = self.next_abs;
        match self.runs.back_mut() {
            Some(last) if last.end() == pos => last.len += len,
            _ => {
                debug_assert!(self.runs.back().is_none_or(|last| last.end() < pos));
                self.runs.push_back(BaseRun { pos, abs, len });
            }
        }
        self.next_abs += len;
        abs
    }

    /// Tags a record starting at `pos`; [`Self::extend_header`] then follows its header.
    pub fn push_record(&mut self, pos: u64) {
        self.marks.push_back(Mark::Record {
            pos,
            header_end: pos,
        });
    }

    /// The header of the last record reaches `end`.
    pub fn extend_header(&mut self, end: u64) {
        if let Some(Mark::Record { header_end, .. }) = self.marks.back_mut() {
            *header_end = end;
        }
    }

    /// Tags `count` contiguous non-ACGT characters of a sequence line at `pos`.
    pub fn push_invalid(&mut self, pos: u64, count: u64) {
        match self.marks.back_mut() {
            Some(Mark::Invalid {
                pos: last,
                count: n,
            }) if *last + *n == pos => *n += count,
            _ => self.marks.push_back(Mark::Invalid { pos, count }),
        }
    }

    fn push_mark(&mut self, mark: Mark) {
        match mark {
            Mark::Record { pos, header_end } => {
                self.push_record(pos);
                self.extend_header(header_end);
            }
            Mark::Invalid { pos, count } => self.push_invalid(pos, count),
        }
    }

    /// The bytes `[pos, pos + len)` are not part of any FASTA stream.
    pub fn mark_opaque(&mut self, pos: u64, len: u64) {
        if len == 0 {
            return;
        }
        match self.opaque.back_mut() {
            Some(last) if last.end == pos => last.end = pos + len,
            _ => self.opaque.push_back(pos..pos + len),
        }
    }

    /// Forgets what lies more than `retain` bytes behind `frontier`.
    pub fn trim(&mut self, frontier: u64) {
        let limit = frontier.saturating_sub(self.retain);
        if limit <= self.forgotten {
            return;
        }
        self.forgotten = limit;
        while self.runs.front().is_some_and(|run| run.end() <= limit) {
            self.runs.pop_front();
        }
        while self.marks.front().is_some_and(|mark| mark.end() <= limit) {
            self.marks.pop_front();
        }
        while self.opaque.front().is_some_and(|range| range.end <= limit) {
            self.opaque.pop_front();
        }
    }

    /// Whether the byte at `pos` is still remembered.
    pub fn holds(&self, pos: u64) -> bool {
        pos >= self.forgotten
    }

    /// Appends the runs overlapping `range` to `out`, clipped to it: exactly the tagged
    /// bases inside the range, with their coordinates.
    pub fn clipped(&self, range: Range<u64>, out: &mut Vec<BaseRun>) {
        if range.is_empty() {
            return;
        }
        let first = self.runs.partition_point(|run| run.end() <= range.start);
        for run in self.runs.range(first..) {
            if run.pos >= range.end {
                break;
            }
            let (start, end) = (run.pos.max(range.start), run.end().min(range.end));
            out.push(BaseRun {
                pos: start,
                abs: run.abs + (start - run.pos),
                len: end - start,
            });
        }
    }

    /// Appends the marks starting inside `range` to `out`.
    pub fn marks_in(&self, range: Range<u64>, out: &mut Vec<Mark>) {
        let first = self.marks.partition_point(|mark| mark.pos() < range.start);
        for mark in self.marks.range(first..) {
            if mark.pos() >= range.end {
                break;
            }
            out.push(*mark);
        }
    }

    /// The coordinates of the tagged bases inside the byte range, or `None` when it
    /// holds none. They are consecutive, since the bytes are.
    pub fn bases_in(&self, range: Range<u64>) -> Option<BaseRange> {
        if range.is_empty() {
            return None;
        }
        let first = self.runs.partition_point(|run| run.end() <= range.start);
        let head = self.runs.get(first).filter(|run| run.pos < range.end)?;
        let last = self.runs.partition_point(|run| run.pos < range.end) - 1;
        let tail = self.runs[last];
        let start = head.abs + range.start.saturating_sub(head.pos);
        let end = tail.abs + (tail.end().min(range.end) - tail.pos);
        Some(start..end)
    }

    /// Calls `each` with the byte of every base in `abs`, which must have been tagged in
    /// `bytes`, whose first byte is at absolute stream offset `base`.
    pub fn debug_for_each_base(
        &self,
        abs: BaseRange,
        base: u64,
        bytes: &[u8],
        mut each: impl FnMut(u8),
    ) {
        if abs.is_empty() {
            return;
        }
        let mut index = self
            .runs
            .partition_point(|run| run.abs + run.len <= abs.start);
        let mut at = abs.start;
        while at < abs.end {
            let run = self.runs[index];
            let end = abs.end.min(run.abs + run.len);
            let from = (run.pos + (at - run.abs) - base) as usize;
            for &byte in &bytes[from..from + (end - at) as usize] {
                each(byte);
            }
            at = end;
            index += 1;
        }
    }

    /// The first opaque byte in `range`, if any, and where its stretch ends.
    fn first_opaque(&self, range: Range<u64>) -> Option<Range<u64>> {
        let first = self.opaque.partition_point(|o| o.end <= range.start);
        self.opaque
            .get(first)
            .filter(|o| o.start < range.end)
            .map(|o| o.start.max(range.start)..o.end)
    }
}

/// The lexer's output while a tracked span is parsed: everything goes to the packer and
/// is tagged on the way.
struct TrackedOutput<'a, X: Clone, S: BatchSink<X>> {
    packer: &'a mut LanePacker<X>,
    sink: &'a mut S,
    tags: &'a mut ParseTags,
}

impl<X: Clone, S: BatchSink<X>> LexOutput<X> for TrackedOutput<'_, X, S> {
    fn begin_record(&mut self, extra: X, pos: u64) {
        self.tags.push_record(pos);
        self.packer.begin_record(extra);
    }

    fn push_header_bytes(&mut self, bytes: &[u8], pos: u64) {
        self.tags.extend_header(pos + bytes.len() as u64);
        self.packer.push_header_bytes(bytes);
    }

    #[inline(always)]
    fn push_bases(&mut self, masks: &RawFastaMasks, first: usize, len: usize, block_pos: u64) {
        let end = first + len;
        let mut index = first;
        while index < end {
            let remaining = end - index;
            let invalid = (masks.mask_non_acgt >> index) & low_mask_u64(remaining);
            let run = if invalid == 0 {
                remaining
            } else {
                invalid.trailing_zeros() as usize
            };
            if run != 0 {
                self.tags.push_run(block_pos + index as u64, run as u64);
                index += run;
            }
            if index < end {
                let valid = !(masks.mask_non_acgt >> index) & low_mask_u64(end - index);
                let skipped = if valid == 0 {
                    end - index
                } else {
                    valid.trailing_zeros() as usize
                };
                self.tags
                    .push_invalid(block_pos + index as u64, skipped as u64);
                index += skipped;
            }
        }
        self.packer
            .push_bases(self.sink, masks.two_bits, masks.mask_non_acgt, first, len);
    }

    fn end_record(&mut self) {
        self.packer.end_record(self.sink, false);
    }
}

/// Bases kept at each end of a copy: `2k - m`, the longest a super-k-mer can be, so any
/// super-k-mer straddling the boundary survives whole.
pub fn keep_bases(k: usize, m: usize) -> u64 {
    (2 * k).saturating_sub(m) as u64
}

/// The shortest copy that can yield a hole: `4k - 2m`, twice what is kept. A copy of
/// that many bytes holds at most that many bases, so shorter ones are not worth
/// reporting.
pub fn min_range_bases(k: usize, m: usize) -> u64 {
    2 * keep_bases(k, m)
}

/// What the superkmers range manager learns from one window. `id`s are unique within a
/// tracked input, shared by sources and copybacks, and follow the input order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkmEvent {
    /// A new merged copy-source range, as it stands at the end of the window.
    Source { id: u32, bases: BaseRange },
    /// A source range from an earlier window grew: a later piece overlapped or touched it.
    ExtendSource { id: u32, end: u64 },
    /// A copy that was dropped as a hole, with the `2k - m` bases kept at each end; its
    /// bases repeat `src`, which lies inside one source range.
    Copyback {
        id: u32,
        dst: BaseRange,
        src: BaseRange,
    },
}

/// A merged copy-source range, in absolute bases.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SourceSpan {
    start: u64,
    end: u64,
    /// [`PENDING_ID`] until the window that created it hands out its ids.
    id: u32,
}

const PENDING_ID: u32 = u32::MAX;

/// The superkmers range manager side of the tracker: merged sources and the ids.
#[derive(Default)]
struct SkmTracking {
    /// Sorted, disjoint; only the last one can still grow.
    sources: Vec<SourceSpan>,
    /// Sources created by the current window: `(byte position, index into sources)`.
    window_sources: Vec<(u64, usize)>,
    /// Holes made by the current window: `(byte position, dst, src)`.
    window_copybacks: Vec<(u64, BaseRange, BaseRange)>,
    /// A source from an earlier window that the current one extended.
    extended: Option<usize>,
    next_id: u32,
    /// Whether [`Self::events`] is filled. Off unless a consumer drains it with
    /// [`CopybackTracker::take_skm_events`]: undrained, it grows for a whole archive.
    record_events: bool,
    events: Vec<SkmEvent>,
}

impl SkmTracking {
    fn reset(&mut self) {
        self.sources.clear();
        self.window_sources.clear();
        self.window_copybacks.clear();
        self.extended = None;
        self.next_id = 0;
        self.events.clear();
    }

    /// Whether `src` lies inside one merged source range.
    fn contains(&self, src: &BaseRange) -> bool {
        let index = self.sources.partition_point(|s| s.start <= src.start);
        index > 0 && self.sources[index - 1].end >= src.end
    }

    /// Hands out the window's ids in input order and turns the window into events.
    fn close_window(&mut self) {
        if !self.record_events {
            // Only the ids are needed, to tell a source of an earlier window apart.
            for (_, index) in self.window_sources.drain(..) {
                self.sources[index].id = self.next_id;
                self.next_id += 1;
            }
            self.next_id += self.window_copybacks.len() as u32;
            self.window_copybacks.clear();
            self.extended = None;
            return;
        }
        if let Some(index) = self.extended.take() {
            let s = self.sources[index];
            self.events.push(SkmEvent::ExtendSource {
                id: s.id,
                end: s.end,
            });
        }
        let mut sources = std::mem::take(&mut self.window_sources)
            .into_iter()
            .peekable();
        let mut copybacks = std::mem::take(&mut self.window_copybacks)
            .into_iter()
            .peekable();
        loop {
            let take_source = match (sources.peek(), copybacks.peek()) {
                (Some(s), Some(c)) => s.0 <= c.0,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let id = self.next_id;
            self.next_id += 1;
            if take_source {
                let (_, index) = sources.next().unwrap();
                let s = &mut self.sources[index];
                s.id = id;
                self.events.push(SkmEvent::Source {
                    id,
                    bases: s.start..s.end,
                });
            } else {
                let (_, dst, src) = copybacks.next().unwrap();
                self.events.push(SkmEvent::Copyback { id, dst, src });
            }
        }
    }
}

/// A hole as it was made, kept for verification (see [`CopybackTracker::with_recording`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedHole {
    /// The dropped bases, at the destination.
    pub hole: BaseRange,
    /// Where the same bases start at the source.
    pub src_start: u64,
}

pub struct CopybackTracker {
    tags: ParseTags,
    /// The holes of the last span, in order.
    holes: Vec<BaseRange>,
    /// Bases kept at each end of a copy, `2k - m`.
    keep: u64,
    enable_skip: bool,
    forward: Vec<(u64, u64)>,
    /// The copies landing in the span being parsed, by destination.
    pending: VecDeque<CopySpan>,
    runs_scratch: Vec<BaseRun>,
    marks_scratch: Vec<Mark>,
    header_scratch: Vec<u8>,
    /// Whether the last span's holes and forward sources are kept, for verification.
    record: bool,
    planned: Vec<PlannedHole>,
    sources: Vec<BaseRange>,
    pub stats: CopybackStats,
}

/// Where a copy's skip goes, as its source's tags say.
struct Skip {
    /// Destination byte of the source's base `2k - m`: the first byte not lexed.
    from: u64,
    /// Destination byte where lexing resumes, the first of the back margin.
    to: u64,
    /// `to - from` at the source is `[from - shift, to - shift)`.
    shift: u64,
    /// The dropped bases' coordinates at the source.
    src_bases: BaseRange,
    /// The rest of the copy, past an opaque stretch at the source, to plan again.
    rest: Option<CopySpan>,
}

impl CopybackTracker {
    /// `k` is the k-mer length and `m` the minimizer length.
    pub fn new(enable_skip: bool, k: usize, m: usize) -> Self {
        Self {
            tags: ParseTags::default(),
            holes: Vec::new(),
            keep: keep_bases(k, m),
            enable_skip,
            forward: Vec::new(),
            pending: VecDeque::new(),
            runs_scratch: Vec::new(),
            marks_scratch: Vec::new(),
            header_scratch: Vec::new(),
            record: false,
            planned: Vec::new(),
            sources: Vec::new(),
            stats: CopybackStats::default(),
        }
    }
    /// Keeps the holes and forward sources of the last span, for [`Self::planned`] and
    /// [`Self::sources`]. Off by default.
    pub fn with_recording(mut self, enable: bool) -> Self {
        self.record = enable;
        self
    }

    /// The holes of the last span with their sources, when recording.
    pub fn planned(&self) -> &[PlannedHole] {
        &self.planned
    }

    /// The bases of the forward copies' sources in the last span, when recording.
    pub fn sources(&self) -> &[BaseRange] {
        &self.sources
    }

    /// The holes of the last span, in order.
    pub fn holes(&self) -> &[BaseRange] {
        &self.holes
    }

    /// What was parsed so far, with its coordinates.
    pub fn tags(&self) -> &ParseTags {
        &self.tags
    }

    /// Bases kept at each end of a copy.
    pub fn keep(&self) -> u64 {
        self.keep
    }

    /// Start a new tracked input: one archive, or one loose file. Not one tar member --
    /// coordinates run through the whole archive, because most copies cross members.
    /// `retain` is how many bytes back a copy's source can lie.
    pub fn begin_input(&mut self, retain: u64) {
        self.tags.reset(retain);
        self.holes.clear();
    }

    /// Bytes that are not part of any FASTA stream.
    pub fn mark_opaque(&mut self, base: u64, len: u64) {
        self.tags.mark_opaque(base, len);
        self.stats.opaque_bytes += len;
    }

    /// Parses one span of a tracked FASTA stream into the packer, whose first byte is at
    /// absolute stream offset `base`, skipping the copies it can.
    ///
    /// `forward` are the *source ranges* of the copies sourced in this span, `backward`
    /// the copies landing in it, whose destinations must be inside it. Both are in byte
    /// coordinates; the sources of the backward ones may lie as far back as the `retain`
    /// of [`Self::begin_input`].
    #[allow(clippy::too_many_arguments)]
    pub fn push_span<X: Clone>(
        &mut self,
        lexer: &mut FastaSimdLexer<X>,
        packer: &mut LanePacker<X>,
        sink: &mut impl BatchSink<X>,
        base: u64,
        bytes: &[u8],
        forward: impl IntoIterator<Item = (u64, u64)>,
        backward: impl IntoIterator<Item = CopySpan>,
    ) {
        let end = base + bytes.len() as u64;
        self.stats.bytes_seen += bytes.len() as u64;
        let bases_before = self.tags.next_abs();
        self.holes.clear();
        self.planned.clear();
        self.sources.clear();
        self.forward.clear();
        self.forward.extend(forward);
        // Sources are recorded as soon as their bytes are tagged, in order.
        self.forward.sort_unstable();
        let mut forward_done = 0usize;
        self.pending.clear();
        for c in backward {
            // Counted even when skipping is off, so that the report says how much the
            // backward direction offers before anything acts on it.
            self.stats.back_copies += 1;
            self.stats.back_bytes += c.len;
            if self.enable_skip {
                self.pending.push_back(c);
            }
        }
        self.pending
            .make_contiguous()
            .sort_unstable_by_key(|c| (c.dst, c.src));

        packer.set_abs(self.tags.next_abs());
        let mut at = base;
        while let Some(c) = self.pending.pop_front() {
            if c.dst < at {
                // Its start was already lexed, or skipped for an earlier copy.
                self.stats.back_passed += 1;
                continue;
            }
            self.lex(lexer, packer, sink, base, bytes, &mut at, c.dst);
            self.record_forward(&mut forward_done, at);
            let Some(from) = self.skip_start(&c) else {
                continue;
            };
            self.lex(lexer, packer, sink, base, bytes, &mut at, from);
            if !lexer.in_sequence_record() {
                // The copy's first line is not sequence here, although it is at the source.
                self.stats.back_state_mismatch += 1;
                continue;
            }
            self.record_forward(&mut forward_done, at);
            let Some(skip) = self.plan_skip(&c, from) else {
                continue;
            };
            let extra = lexer.stream_extra();
            self.replay(&skip, base, bytes, packer, sink, extra);
            self.stats.bytes_skipped += skip.to - skip.from;
            at = skip.to;
            lexer.resume_in_sequence();
            if let Some(rest) = skip.rest {
                // Past an opaque stretch at the source: the rest is a copy of its own.
                self.stats.back_opaque_cut += 1;
                let index = self
                    .pending
                    .partition_point(|p| (p.dst, p.src) <= (rest.dst, rest.src));
                self.pending.insert(index, rest);
            }
        }
        self.lex(lexer, packer, sink, base, bytes, &mut at, end);
        self.record_forward(&mut forward_done, end);
        debug_assert_eq!(packer.abs(), self.tags.next_abs());
        packer.set_abs(NO_ABS_BASE);

        self.stats.bases_seen += self.tags.next_abs() - bases_before;
        self.tags.trim(end);
        self.stats.runs_peak.max_with(self.tags.runs_len() as u64);
        self.stats.marks_peak.max_with(self.tags.marks_len() as u64);
    }

    /// Lexes the span's bytes from `*at` up to `to`.
    #[allow(clippy::too_many_arguments)]
    fn lex<X: Clone>(
        &mut self,
        lexer: &mut FastaSimdLexer<X>,
        packer: &mut LanePacker<X>,
        sink: &mut impl BatchSink<X>,
        base: u64,
        bytes: &[u8],
        at: &mut u64,
        to: u64,
    ) {
        if to <= *at {
            return;
        }
        let mut out = TrackedOutput {
            packer,
            sink,
            tags: &mut self.tags,
        };
        lexer.parse_span(
            &mut out,
            *at,
            &bytes[(*at - base) as usize..(to - base) as usize],
        );
        self.stats.bytes_lexed += to - *at;
        *at = to;
    }

    /// Reports the forward copies whose sources are tagged by now, below `at`.
    fn record_forward(&mut self, done: &mut usize, at: u64) {
        while let Some(&(src, len)) = self.forward.get(*done) {
            if src + len > at {
                break;
            }
            *done += 1;
            self.report_forward(src, len);
        }
    }

    /// The forward half: the bases of this copy's source.
    fn report_forward(&mut self, src: u64, len: u64) {
        self.stats.fwd_copies += 1;
        self.stats.fwd_bytes += len;
        let Some(bases) = self.tags.bases_in(src..src + len) else {
            self.stats.fwd_no_bases += 1;
            return;
        };
        self.stats.fwd_bases += bases.end - bases.start;
        if self.record {
            self.sources.push(bases.clone());
        }
    }

    /// Where the lexer should stop in the copy's destination: the byte of the source's
    /// base `2k - m`, found through the source's tags, all of which lie before the
    /// destination by now.
    fn skip_start(&mut self, c: &CopySpan) -> Option<u64> {
        if !self.tags.holds(c.src) {
            self.stats.back_unknown_source += 1;
            return None;
        }
        let mut runs = std::mem::take(&mut self.runs_scratch);
        runs.clear();
        self.tags.clipped(c.src..c.src_end().min(c.dst), &mut runs);
        let mut before = 0u64;
        let mut start = None;
        for run in &runs {
            if before + run.len > self.keep {
                start = Some(run.pos + (self.keep - before));
                break;
            }
            before += run.len;
        }
        self.runs_scratch = runs;
        match start {
            Some(src_pos) => Some(src_pos + (c.dst - c.src)),
            None => {
                self.stats.back_too_short += 1;
                None
            }
        }
    }

    /// Plans the skip of a copy whose destination was lexed up to `from`, where the
    /// lexer's state matched the source's. `None` when the copy is too short to drop
    /// anything, or its source was not recorded.
    fn plan_skip(&mut self, c: &CopySpan, from: u64) -> Option<Skip> {
        let shift = c.dst - c.src;
        let src_from = from - shift;
        // Only what is tagged counts: an overlapping copy's source may reach past the
        // lexer, which stands at `from`. An opaque stretch ends the skip; the rest of the
        // copy is planned again beyond it.
        let mut limit = c.src_end().min(from);
        let mut rest = None;
        if let Some(opaque) = self.tags.first_opaque(src_from..limit) {
            limit = opaque.start;
            if opaque.end < c.src_end() {
                rest = Some(CopySpan {
                    src: opaque.end,
                    dst: opaque.end + shift,
                    len: c.src_end() - opaque.end,
                });
            }
        }
        let mut runs = std::mem::take(&mut self.runs_scratch);
        runs.clear();
        self.tags.clipped(src_from..limit, &mut runs);
        let bases: u64 = runs.iter().map(|run| run.len).sum();
        let result = if bases <= self.keep {
            self.stats.back_too_short += 1;
            None
        } else {
            // The dropped bases, then where the back margin starts.
            let dropped = bases - self.keep;
            let mut left = dropped;
            let mut to = limit;
            for run in &runs {
                if left < run.len {
                    to = run.pos + left;
                    break;
                }
                left -= run.len;
            }
            let src_start = runs[0].abs;
            Some(Skip {
                from,
                to: to + shift,
                shift,
                src_bases: src_start..src_start + dropped,
                rest,
            })
        };
        self.runs_scratch = runs;
        let skip = result?;
        Some(skip)
    }

    /// Skips `[skip.from, skip.to)` of the destination: its source's tags go to the packer
    /// as the lexer would have handed them over, bases dropped as a hole, and to the tags,
    /// shifted to the destination.
    fn replay<X: Clone>(
        &mut self,
        skip: &Skip,
        base: u64,
        bytes: &[u8],
        packer: &mut LanePacker<X>,
        sink: &mut impl BatchSink<X>,
        extra: X,
    ) {
        let src = skip.from - skip.shift..skip.to - skip.shift;
        let mut runs = std::mem::take(&mut self.runs_scratch);
        let mut marks = std::mem::take(&mut self.marks_scratch);
        runs.clear();
        marks.clear();
        self.tags.clipped(src.clone(), &mut runs);
        self.tags.marks_in(src, &mut marks);

        let hole_start = self.tags.next_abs();
        let (mut r, mut m) = (0usize, 0usize);
        let mut pending = 0u64;
        while r < runs.len() || m < marks.len() {
            let take_run = match (runs.get(r), marks.get(m)) {
                (Some(run), Some(mark)) => run.pos < mark.pos(),
                (Some(_), None) => true,
                _ => false,
            };
            if take_run {
                let run = runs[r];
                r += 1;
                self.tags.push_run(run.pos + skip.shift, run.len);
                pending += run.len;
                continue;
            }
            let mark = marks[m];
            m += 1;
            if pending != 0 {
                packer.push_hole(sink, pending);
                pending = 0;
            }
            match mark {
                Mark::Invalid { count, .. } => packer.push_invalid(sink, count),
                Mark::Record { pos, header_end } => {
                    packer.end_record(sink, false);
                    packer.begin_record(extra.clone());
                    // The header is read from the destination, where it is the same.
                    let (from, to) = (
                        (pos + skip.shift - base) as usize,
                        (header_end + skip.shift - base) as usize,
                    );
                    self.header_scratch.clear();
                    self.header_scratch
                        .extend(bytes[from..to].iter().copied().filter(|&b| b != b'\r'));
                    packer.push_header_bytes(&self.header_scratch);
                }
            }
            self.tags.push_mark(mark.shifted(skip.shift));
        }
        if pending != 0 {
            packer.push_hole(sink, pending);
        }
        self.runs_scratch = runs;
        self.marks_scratch = marks;

        let hole = hole_start..self.tags.next_abs();
        debug_assert_eq!(
            hole.end - hole.start,
            skip.src_bases.end - skip.src_bases.start
        );
        debug_assert_eq!(packer.abs(), hole.end);
        if self.record {
            self.planned.push(PlannedHole {
                hole: hole.clone(),
                src_start: skip.src_bases.start,
            });
        }
        self.stats.back_skipped += 1;
        self.stats.holes += 1;
        self.stats.hole_bases += hole.end - hole.start;
        self.holes.push(hole);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::testing::{Reconstructed, check_padding_is_zero, reconstruct};
    use crate::batch::{LaneBatch, VecBatchSink};
    use crate::hashing::SIMD_LANES;
    use rand::{Rng, SeedableRng};

    // Small k and m keep the fixtures readable: 2k - m = 8 bases are kept at each end
    // of a segment and a segment of at most 4k - 2m = 16 bases yields no hole.
    const TK: usize = 5;
    const TM: usize = 2;
    const KEEP: u64 = 8;
    /// Short lanes, so that records keep continuing in the next lane.
    const LANE: usize = 24;
    const CODES: [u8; 4] = [b'A', b'C', b'T', b'G'];

    fn is_acgt(byte: u8) -> bool {
        matches!(byte & 0xdf, b'A' | b'C' | b'G' | b'T')
    }

    /// The byte position of every base, found one byte at a time the way the lexer
    /// reads them: a line whose first byte other than `\r` is `>` or `;` holds none,
    /// `\r` and `\n` never are bases, and in any other line the ACGT bytes are. Each
    /// stream starts at a line start; bytes outside the streams are opaque.
    fn oracle_positions(bytes: &[u8], streams: &[Range<usize>]) -> Vec<u64> {
        let mut out = Vec::new();
        for stream in streams {
            let mut sequence: Option<bool> = None;
            for at in stream.clone() {
                let byte = bytes[at];
                if byte == b'\n' {
                    sequence = None;
                    continue;
                }
                if byte == b'\r' {
                    continue;
                }
                let is_sequence = *sequence.get_or_insert(byte != b'>' && byte != b';');
                if is_sequence && is_acgt(byte) {
                    out.push(at as u64);
                }
            }
        }
        out
    }

    /// The absolute base string: every base, upper case, in stream order.
    fn oracle_text(bytes: &[u8], streams: &[Range<usize>]) -> Vec<u8> {
        oracle_positions(bytes, streams)
            .iter()
            .map(|&at| bytes[at as usize] & 0xdf)
            .collect()
    }

    /// The coordinates of the bases inside a byte range, by the oracle.
    fn oracle_bases_in(positions: &[u64], range: Range<u64>) -> Range<u64> {
        let start = positions.partition_point(|&p| p < range.start) as u64;
        let end = positions.partition_point(|&p| p < range.end) as u64;
        start..end
    }

    struct Run {
        tracker: CopybackTracker,
        batches: Vec<LaneBatch<()>>,
        /// The holes of every span, in order.
        holes: Vec<BaseRange>,
        events: Vec<SkmEvent>,
    }

    /// Drives the whole pipeline over `bytes`: `streams` are the FASTA streams, and the
    /// bytes between them are opaque. Each stream is cut into spans of the lengths
    /// `cut` returns; `forward` copies are reported in the spans holding their sources
    /// and `backward` ones in the spans holding their destinations, clipped to them.
    fn run(
        bytes: &[u8],
        streams: &[Range<usize>],
        forward: &[CopySpan],
        backward: &[CopySpan],
        cut: &mut dyn FnMut() -> usize,
        mut tracker: CopybackTracker,
    ) -> Run {
        let mut lexer = FastaSimdLexer::new();
        let mut packer = LanePacker::new(TK, LANE, 0, true, 1 << 20).unwrap();
        let mut sink = VecBatchSink::new(LANE);
        tracker.begin_input(u64::MAX);
        let (mut holes, mut events) = (Vec::new(), Vec::new());
        let mut bounds = vec![0, bytes.len()];
        for stream in streams {
            bounds.extend([stream.start, stream.end]);
        }
        bounds.sort_unstable();
        bounds.dedup();
        for pair in bounds.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let Some(stream) = streams.iter().find(|s| s.start <= a && a < s.end) else {
                tracker.mark_opaque(a as u64, (b - a) as u64);
                continue;
            };
            if stream.start == a {
                lexer.begin_stream(());
            }
            let mut x = a;
            while x < b {
                let y = x.saturating_add(cut().max(1)).min(b);
                let (lo, hi) = (x as u64, y as u64);
                let fwd: Vec<(u64, u64)> = forward
                    .iter()
                    .filter_map(|c| {
                        let (s, e) = (c.src.max(lo), c.src_end().min(hi));
                        (s < e).then(|| (s, e - s))
                    })
                    .collect();
                let back: Vec<CopySpan> = backward
                    .iter()
                    .filter_map(|c| {
                        let (s, e) = (c.dst.max(lo), c.dst_end().min(hi));
                        (s < e).then(|| CopySpan {
                            src: c.src + (s - c.dst),
                            dst: s,
                            len: e - s,
                        })
                    })
                    .collect();
                tracker.push_span(
                    &mut lexer,
                    &mut packer,
                    &mut sink,
                    lo,
                    &bytes[x..y],
                    fwd,
                    back,
                );
                holes.extend_from_slice(tracker.holes());
                x = y;
            }
            if stream.end == b {
                lexer.end_stream(&mut packer, &mut sink);
            }
        }
        packer.finish(&mut sink);
        Run {
            tracker,
            batches: sink.batches,
            holes,
            events,
        }
    }

    fn whole() -> impl FnMut() -> usize {
        || usize::MAX
    }

    /// The pieces of `bytes` a stream is read in by the plain lexer, for comparison.
    fn plain_parse(bytes: &[u8], streams: &[Range<usize>], chunk: usize) -> Vec<Reconstructed> {
        let mut lexer = FastaSimdLexer::new();
        let mut packer = LanePacker::new(TK, LANE, 0, true, 1 << 20).unwrap();
        let mut sink = VecBatchSink::new(LANE);
        for stream in streams {
            lexer.begin_stream(());
            for piece in bytes[stream.clone()].chunks(chunk) {
                lexer.push(&mut packer, &mut sink, piece);
            }
            lexer.end_stream(&mut packer, &mut sink);
        }
        packer.finish(&mut sink);
        reconstruct(&sink.batches)
    }

    /// `bytes` with the bases at the coordinates of `holes` turned into `N`.
    fn with_breaks(bytes: &[u8], streams: &[Range<usize>], holes: &[BaseRange]) -> Vec<u8> {
        let positions = oracle_positions(bytes, streams);
        let mut out = bytes.to_vec();
        for hole in holes {
            for abs in hole.clone() {
                out[positions[abs as usize] as usize] = b'N';
            }
        }
        out
    }

    /// Every fragment's bases are the text at its coordinate, and none reaches into a
    /// hole; the coordinates handed out are the text's.
    fn check_coordinates(run: &Run, bytes: &[u8], streams: &[Range<usize>]) {
        let text = oracle_text(bytes, streams);
        assert_eq!(
            run.tracker.tags().next_abs(),
            text.len() as u64,
            "bases parsed"
        );
        let st = &run.tracker.stats;
        assert_eq!(
            st.bytes_lexed.get() + st.bytes_skipped.get(),
            st.bytes_seen.get()
        );
        for batch in &run.batches {
            batch.debug_check(TK);
            check_padding_is_zero(batch);
            for lane in 0..SIMD_LANES {
                for pair in batch.fragments(lane).windows(2) {
                    let fragment = pair[0];
                    assert_ne!(fragment.abs_start, NO_ABS_BASE);
                    let lanes: Vec<u8> = (fragment.lane_start..pair[1].lane_start)
                        .map(|p| CODES[batch.base(lane, p as usize) as usize])
                        .collect();
                    let at = fragment.abs_start as usize;
                    assert_eq!(
                        String::from_utf8_lossy(&lanes),
                        String::from_utf8_lossy(&text[at..at + lanes.len()]),
                        "fragment at {at}"
                    );
                    let end = (at + lanes.len()) as u64;
                    assert!(
                        !run.holes.iter().any(|h| h.start < end && h.end > at as u64),
                        "fragment {at}..{end} reaches into a hole"
                    );
                }
            }
        }
    }

    /// A random FASTA stream: `wrap` bases per line (0: unwrapped), optionally ragged
    /// lines and CRLF, with lower case, ambiguity characters, comments, empty lines, a
    /// carriage return in the middle of a line now and then, and sometimes bases before
    /// the first header.
    fn random_fasta(
        rng: &mut impl Rng,
        records: usize,
        wrap: usize,
        ragged: bool,
        crlf: bool,
    ) -> Vec<u8> {
        let eol: &[u8] = if crlf { b"\r\n" } else { b"\n" };
        let mut out = Vec::new();
        for r in 0..records {
            if r > 0 || rng.gen_bool(0.8) {
                if rng.gen_bool(0.1) {
                    out.extend_from_slice(b";a comment ACGT");
                    out.extend_from_slice(eol);
                }
                out.extend_from_slice(format!(">rec{r} ACGT").as_bytes());
                out.extend_from_slice(eol);
            }
            let len = rng.gen_range(0..300);
            let (mut in_line, mut width) = (0usize, wrap);
            for _ in 0..len {
                out.push(b"ACGTACGTACGTacgtNn"[rng.gen_range(0..18)]);
                if rng.gen_bool(0.005) {
                    out.push(b'\r');
                }
                in_line += 1;
                if width != 0 && in_line == width {
                    out.extend_from_slice(eol);
                    if rng.gen_bool(0.02) {
                        out.extend_from_slice(eol);
                    }
                    in_line = 0;
                    if ragged {
                        width = rng.gen_range(1..2 * wrap);
                    }
                }
            }
            if in_line != 0 || len == 0 {
                out.extend_from_slice(eol);
            }
        }
        out
    }

    /// One long unwrapped sequence line, the case that matters for assembly FASTA. The
    /// header holds no bases, so byte `3 + b` is base `b`.
    fn one_line(bases: usize) -> Vec<u8> {
        let mut out = b">h\n".to_vec();
        out.extend((0..bases).map(|i| b"ACGT"[i % 4]));
        out.push(b'\n');
        out
    }

    fn all(bytes: &[u8]) -> Vec<Range<usize>> {
        vec![0..bytes.len()]
    }

    fn plan_one(bytes: &[u8], copies: &[CopySpan]) -> Run {
        run(
            bytes,
            &all(bytes),
            copies,
            copies,
            &mut whole(),
            CopybackTracker::new(true, TK, TM),
        )
    }

    // ------------------------------------------------------------- base runs

    #[test]
    fn keep_and_minimum_follow_k_and_m() {
        assert_eq!(keep_bases(TK, TM), KEEP);
        assert_eq!(min_range_bases(TK, TM), 2 * KEEP);
        assert_eq!(keep_bases(31, 12), 50);
        assert_eq!(min_range_bases(31, 12), 100);
    }

    /// Streams of random FASTA separated by opaque stretches, as in an archive.
    fn random_layout(rng: &mut impl Rng, round: usize) -> (Vec<u8>, Vec<Range<usize>>) {
        let wrap = [0usize, 7, 60][round % 3];
        let ragged = round % 5 == 4 && wrap != 0;
        let crlf = round % 4 == 3;
        let mut bytes = Vec::new();
        let mut streams = Vec::new();
        for _ in 0..rng.gen_range(1..4) {
            if rng.gen_bool(0.5) {
                bytes.extend((0..rng.gen_range(1..40)).map(|_| b"ACGT\n>x"[rng.gen_range(0..7)]));
            }
            let start = bytes.len();
            let records = rng.gen_range(1..5);
            bytes.extend(random_fasta(rng, records, wrap, ragged, crlf));
            streams.push(start..bytes.len());
        }
        (bytes, streams)
    }

    #[test]
    fn a_byte_range_converts_to_exactly_the_bases_parsed_inside_it() {
        let mut rng = pcg_rand::Pcg64::seed_from_u64(7);
        for round in 0..200 {
            let (bytes, streams) = random_layout(&mut rng, round);
            let max = rng.gen_range(1..200);
            let result = run(
                &bytes,
                &streams,
                &[],
                &[],
                &mut || rng.gen_range(1..=max),
                CopybackTracker::new(true, TK, TM),
            );
            check_coordinates(&result, &bytes, &streams);
            let positions = oracle_positions(&bytes, &streams);
            let runs = result.tracker.tags();
            for _ in 0..200 {
                let x = rng.gen_range(0..=bytes.len()) as u64;
                let y = rng.gen_range(x..=bytes.len() as u64);
                let expected = oracle_bases_in(&positions, x..y);
                let mut clipped = Vec::new();
                runs.clipped(x..y, &mut clipped);
                let got: Vec<(u64, u64)> = clipped
                    .iter()
                    .flat_map(|r| (0..r.len).map(move |i| (r.pos + i, r.abs + i)))
                    .collect();
                let want: Vec<(u64, u64)> = expected
                    .clone()
                    .map(|abs| (positions[abs as usize], abs))
                    .collect();
                assert_eq!(got, want, "round {round}: bytes {x}..{y}");
                assert_eq!(
                    runs.bases_in(x..y),
                    (!expected.is_empty()).then_some(expected),
                    "round {round}: bytes {x}..{y}"
                );
            }
        }
    }

    #[test]
    fn a_tracked_parse_without_holes_is_a_plain_parse() {
        let mut rng = pcg_rand::Pcg64::seed_from_u64(3);
        for round in 0..100 {
            let (bytes, streams) = random_layout(&mut rng, round);
            for chunk in [1usize, 3, 63, 64, 65, 127, 4096] {
                let result = run(
                    &bytes,
                    &streams,
                    &[],
                    &[],
                    &mut || chunk,
                    CopybackTracker::new(true, TK, TM),
                );
                check_coordinates(&result, &bytes, &streams);
                assert_eq!(
                    reconstruct(&result.batches),
                    plain_parse(&bytes, &streams, chunk),
                    "round {round}, chunk {chunk}"
                );
            }
        }
    }

    #[test]
    fn runs_behind_the_retained_bytes_are_forgotten() {
        let bytes = one_line(1000);
        let mut runs = ParseTags::default();
        runs.reset(100);
        runs.push_run(3, 1000);
        runs.trim(503);
        assert!(!runs.holds(402) && runs.holds(403));
        // The run straddles the limit, so it is kept whole.
        assert_eq!(runs.bases_in(3..13), Some(0..10));
        runs.trim(bytes.len() as u64 + 2000);
        assert_eq!(runs.runs_len(), 0);
    }

    // ------------------------------------------------------------- the tracker

    #[test]
    fn a_segment_keeps_2k_minus_m_bases_at_each_end() {
        let bytes = one_line(200);
        // A 40 base copy inside the line: 8 kept at each end, 24 dropped.
        let result = plan_one(
            &bytes,
            &[CopySpan {
                src: 10,
                dst: 100,
                len: 40,
            }],
        );
        assert_eq!(result.holes, vec![97 + KEEP..137 - KEEP]);
        check_coordinates(&result, &bytes, &all(&bytes));
        let st = &result.tracker.stats;
        assert_eq!(st.hole_bases.get(), 40 - 2 * KEEP);
        assert_eq!(st.back_skipped.get(), 1);
        // Neither the header nor a newline sits in the hole, so bytes are bases here.
        assert_eq!(st.bytes_skipped.get(), 40 - 2 * KEEP);
    }

    #[test]
    fn a_segment_of_at_most_4k_minus_2m_bases_yields_no_hole() {
        let bytes = one_line(200);
        for len in [1u64, KEEP, 2 * KEEP - 1, 2 * KEEP] {
            let result = plan_one(
                &bytes,
                &[CopySpan {
                    src: 10,
                    dst: 100,
                    len,
                }],
            );
            assert!(result.holes.is_empty(), "len {len}: {:?}", result.holes);
            assert_eq!(result.tracker.stats.back_too_short.get(), 1);
        }
        // One base more and a single-base hole appears.
        let result = plan_one(
            &bytes,
            &[CopySpan {
                src: 10,
                dst: 100,
                len: 2 * KEEP + 1,
            }],
        );
        assert_eq!(result.holes, vec![97 + KEEP..97 + KEEP + 1]);
    }

    /// Newlines hold no coordinate, so the margins are counted in bases wherever the
    /// copy starts and ends, line starts and line ends included.
    #[test]
    fn the_margins_are_bases_whatever_the_newlines() {
        // Eight lines of ten bases; the second four repeat the first four.
        let mut bytes = b">h\n".to_vec();
        for _ in 0..8 {
            bytes.extend_from_slice(b"ACGTACGTAC\n");
        }
        let positions = oracle_positions(&bytes, &all(&bytes));
        for (src, len) in [(3u64, 44u64), (8, 30), (13, 22), (14, 21), (12, 3 * 11)] {
            let dst = src + 44;
            assert_eq!(
                bytes[src as usize..(src + len) as usize],
                bytes[dst as usize..(dst + len) as usize]
            );
            let result = plan_one(&bytes, &[CopySpan { src, dst, len }]);
            let d = oracle_bases_in(&positions, dst..dst + len);
            let expected = if d.end - d.start > 2 * KEEP {
                vec![d.start + KEEP..d.end - KEEP]
            } else {
                vec![]
            };
            assert_eq!(result.holes, expected, "copy {src} -> {dst} of {len}");
            check_coordinates(&result, &bytes, &all(&bytes));
        }
    }

    #[test]
    fn a_copied_header_yields_nothing() {
        let ident = b">dupdupdupdupdupdupdupdupdup\n";
        let mut bytes = ident.to_vec();
        bytes.extend((0..200).map(|i| b"ACGT"[i % 4]));
        bytes.extend_from_slice(b"\n>x\n");
        let dst = bytes.len() as u64;
        bytes.extend_from_slice(ident);
        let copy = CopySpan {
            src: 0,
            dst,
            len: ident.len() as u64,
        };
        let result = plan_one(&bytes, &[copy]);
        assert!(result.holes.is_empty());
        assert_eq!(result.tracker.stats.fwd_no_bases.get(), 1);
        assert_eq!(result.tracker.stats.back_too_short.get(), 1);
    }

    #[test]
    fn a_sequence_source_landing_in_a_header_is_not_dropped() {
        let unit: Vec<u8> = (0..40).map(|i| b"ACGT"[i % 4]).collect();
        let mut bytes = b">h\n".to_vec();
        bytes.extend_from_slice(&unit);
        bytes.extend_from_slice(b"\n>");
        let dst = bytes.len() as u64;
        bytes.extend_from_slice(&unit); // the same bases, now inside an identifier
        bytes.extend_from_slice(b"\nGGGG\n");
        let result = plan_one(
            &bytes,
            &[CopySpan {
                src: 3,
                dst,
                len: 40,
            }],
        );
        assert!(
            result.holes.is_empty(),
            "the destination is a header: {:?}",
            result.holes
        );
        assert_eq!(result.tracker.stats.back_state_mismatch.get(), 1);
    }

    /// A header inside a copy is a header at both ends, so the copy is skipped as one
    /// hole across the record boundary, the record start replayed from the source's tags
    /// and its header read from the destination.
    #[test]
    fn a_hole_runs_across_a_header_inside_the_copy() {
        let a: Vec<u8> = (0..60).map(|i| b"ACGTTGCA"[i % 8]).collect();
        let b: Vec<u8> = (0..60).map(|i| b"GGATCCAT"[i % 8]).collect();
        let mut block = b">a\n".to_vec();
        block.extend_from_slice(&a);
        block.extend_from_slice(b"\n>b\n");
        block.extend_from_slice(&b);
        block.push(b'\n');
        let mut bytes = block.clone();
        bytes.extend_from_slice(&block);
        let len = block.len() as u64;
        let result = plan_one(
            &bytes,
            &[CopySpan {
                src: 0,
                dst: len,
                len,
            }],
        );
        assert_eq!(result.holes, vec![120 + KEEP..240 - KEEP]);
        assert_eq!(result.tracker.stats.back_skipped.get(), 1);
        check_coordinates(&result, &bytes, &all(&bytes));
        let replaced = with_breaks(&bytes, &all(&bytes), &result.holes);
        assert_eq!(
            reconstruct(&result.batches),
            plain_parse(&replaced, &all(&replaced), 4096)
        );
    }

    /// An opaque stretch at the source is a member boundary: the reads break there but
    /// not at the destination, where the same bytes are only a comment line. So the skip
    /// stops before it, and the rest of the copy is skipped as a copy of its own, each
    /// keeping its own margins.
    #[test]
    fn an_opaque_stretch_at_the_source_cuts_the_segment() {
        let x: Vec<u8> = (0..60).map(|i| b"ACGTTGCA"[i % 8]).collect();
        let y: Vec<u8> = (0..60).map(|i| b"GGATCCAT"[i % 8]).collect();
        let gap = b"\n;xx\n";
        let mut bytes = b">a\n".to_vec();
        bytes.extend_from_slice(&x);
        bytes.push(b'\n');
        let first = 0..bytes.len();
        // The opaque stretch holds the gap's bytes but the newline that ends `x`.
        bytes.extend_from_slice(&gap[1..]);
        let second_start = bytes.len();
        bytes.extend_from_slice(&y);
        bytes.extend_from_slice(b"\n>c\n");
        let dst = bytes.len() as u64;
        bytes.extend_from_slice(&x);
        bytes.extend_from_slice(gap);
        bytes.extend_from_slice(&y);
        bytes.push(b'\n');
        let streams = vec![first, second_start..bytes.len()];
        let copy = CopySpan {
            src: 3,
            dst,
            len: (x.len() + gap.len() + y.len()) as u64,
        };
        assert_eq!(
            bytes[copy.src as usize..copy.src_end() as usize],
            bytes[copy.dst as usize..copy.dst_end() as usize]
        );
        let result = run(
            &bytes,
            &streams,
            &[copy],
            &[copy],
            &mut whole(),
            CopybackTracker::new(true, TK, TM),
        );
        assert_eq!(
            result.holes,
            vec![120 + KEEP..180 - KEEP, 180 + KEEP..240 - KEEP]
        );
        assert_eq!(result.tracker.stats.back_opaque_cut.get(), 1);
        check_coordinates(&result, &bytes, &streams);
        let replaced = with_breaks(&bytes, &streams, &result.holes);
        assert_eq!(
            reconstruct(&result.batches),
            plain_parse(&replaced, &streams, 4096)
        );
    }

    #[test]
    fn a_copy_of_a_copy_is_answerable_at_every_generation() {
        let unit: Vec<u8> = (0..60).map(|i| b"ACGTTGCA"[i % 8]).collect();
        let mut bytes = b">h\n".to_vec();
        let mut at = Vec::new();
        for _ in 0..4 {
            at.push(bytes.len() as u64);
            bytes.extend_from_slice(&unit);
        }
        bytes.push(b'\n');
        let len = unit.len() as u64;
        let copies: Vec<CopySpan> = (1..4)
            .map(|g| CopySpan {
                src: at[g - 1],
                dst: at[g],
                len,
            })
            .collect();
        let result = plan_one(&bytes, &copies);
        let base = |g: usize| at[g] - 3;
        assert_eq!(
            result.holes,
            (1..4)
                .map(|g| base(g) + KEEP..base(g) + len - KEEP)
                .collect::<Vec<_>>()
        );
        assert_eq!(result.tracker.stats.back_skipped.get(), 3);
    }

    #[test]
    fn skipping_off_plans_nothing_but_still_reports_forward() {
        let bytes = one_line(200);
        let copy = CopySpan {
            src: 10,
            dst: 100,
            len: 40,
        };
        let result = run(
            &bytes,
            &all(&bytes),
            &[copy],
            &[copy],
            &mut whole(),
            CopybackTracker::new(false, TK, TM),
        );
        assert!(result.holes.is_empty());
        assert_eq!(result.tracker.stats.fwd_copies.get(), 1);
        assert_eq!(result.tracker.stats.fwd_bases.get(), 40);
    }

    /// The whole thing end to end, on random archives: streams separated by opaque
    /// stretches, records repeating earlier ones byte for byte (sometimes from another
    /// stream), random span cuts. Every fragment must sit at its coordinate in the
    /// absolute base string, every copyback must repeat its source there, and the result
    /// must be exactly a plain parse with the holes' bases turned into `N`.
    #[test]
    fn absolute_coordinates_line_up_end_to_end() {
        let mut rng = pcg_rand::Pcg64::seed_from_u64(11);
        let mut copybacks = 0usize;
        let mut skipped_bytes = 0u64;
        for round in 0..80 {
            let wrap = [0usize, 13, 70][round % 3];
            let eol: &[u8] = if round % 2 == 1 { b"\r\n" } else { b"\n" };
            let mut bytes = Vec::new();
            let mut streams = Vec::new();
            let mut blocks: Vec<(usize, usize)> = Vec::new();
            let mut copies = Vec::new();
            for _ in 0..rng.gen_range(1..4) {
                if !streams.is_empty() || rng.gen_bool(0.3) {
                    bytes.extend(
                        (0..rng.gen_range(1..30)).map(|_| b"ACGT\n\0"[rng.gen_range(0..6)]),
                    );
                }
                let stream_start = bytes.len();
                for r in 0..rng.gen_range(1..8) {
                    bytes.extend_from_slice(format!(">r{r}").as_bytes());
                    bytes.extend_from_slice(eol);
                    let start = bytes.len();
                    if !blocks.is_empty() && rng.gen_bool(0.6) {
                        let (s0, s1) = blocks[rng.gen_range(0..blocks.len())];
                        let block = bytes[s0..s1].to_vec();
                        bytes.extend_from_slice(&block);
                        let a = rng.gen_range(0..block.len());
                        let b = rng.gen_range(a..=block.len());
                        if b > a {
                            copies.push(CopySpan {
                                src: (s0 + a) as u64,
                                dst: (start + a) as u64,
                                len: (b - a) as u64,
                            });
                        }
                    } else {
                        let len = rng.gen_range(1..400);
                        for i in 0..len {
                            bytes.push(if rng.gen_bool(0.01) {
                                b'N'
                            } else {
                                b"ACGTacgt"[rng.gen_range(0..8)]
                            });
                            if wrap != 0 && (i + 1) % wrap == 0 && i + 1 != len {
                                bytes.extend_from_slice(eol);
                            }
                        }
                        bytes.extend_from_slice(eol);
                    }
                    blocks.push((start, bytes.len()));
                }
                streams.push(stream_start..bytes.len());
            }
            let max = rng.gen_range(20..700);
            let mut cuts: Vec<usize> = Vec::new();
            let result = run(
                &bytes,
                &streams,
                &copies,
                &copies,
                &mut || {
                    let cut = rng.gen_range(1..=max);
                    cuts.push(cut);
                    cut
                },
                CopybackTracker::new(true, TK, TM),
            );
            check_coordinates(&result, &bytes, &streams);
            skipped_bytes += result.tracker.stats.bytes_skipped.get();
            // The tags of the skipped bytes were copied from their sources, and must be
            // exactly what parsing them would have produced.
            let mut replay = cuts.into_iter();
            let parsed = run(
                &bytes,
                &streams,
                &[],
                &[],
                &mut || replay.next().unwrap_or(usize::MAX),
                CopybackTracker::new(true, TK, TM),
            );
            let (a, b) = (result.tracker.tags(), parsed.tracker.tags());
            assert!(a.runs().eq(b.runs()), "round {round}: runs differ");
            assert!(a.marks().eq(b.marks()), "round {round}: marks differ");
            let text = oracle_text(&bytes, &streams);
            let mut holes = result.holes.iter();
            for event in &result.events {
                if let SkmEvent::Copyback { dst, src, .. } = event {
                    copybacks += 1;
                    assert_eq!(dst.end - dst.start, src.end - src.start);
                    assert_eq!(
                        text[dst.start as usize..dst.end as usize],
                        text[src.start as usize..src.end as usize],
                        "round {round}: a copyback repeats its source"
                    );
                    // Its hole keeps the margins, possibly merged with a neighbour's.
                    let hole = holes
                        .find(|h| h.end > dst.start)
                        .expect("a copyback without a hole");
                    assert!(hole.start <= dst.start + KEEP && hole.end >= dst.end - KEEP);
                }
            }
            let replaced = with_breaks(&bytes, &streams, &result.holes);
            let mut expected = plain_parse(&replaced, &streams, 4096);
            let mut actual = reconstruct(&result.batches);
            // The read index restarts with every stream, so compare in order.
            expected.iter_mut().for_each(|r| r.read_index = 0);
            actual.iter_mut().for_each(|r| r.read_index = 0);
            assert_eq!(actual, expected, "round {round}");
        }
        assert!(copybacks > 20, "only {copybacks} copybacks exercised");
        assert!(skipped_bytes > 10_000, "only {skipped_bytes} bytes skipped");
    }
}
