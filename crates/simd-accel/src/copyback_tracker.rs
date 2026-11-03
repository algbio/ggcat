//! The LZ copies of a FASTA stream, followed as the parser reads their sources, so that each
//! comes out trimmed to what a consumer may skip at its destination, its bases counted in the
//! coordinates of the bases read: see [`TrackedCopy`]. Unless the tracker only follows them,
//! the parser then skips those destinations, see [`CopybackTracker::decide_skip_at`].
//!
//! The copies come in the order lz_copyback reports them, by destination, each before any byte
//! of its source is parsed (see `ChunkWithCopybacks` of io's binary reader), and each with its
//! source ending before its destination starts. They are taken out of its queue as they come: a
//! match that continues the last copy, which lz_copyback would have merged into it if it were
//! still there, comes as a copy of its own, and makes the last copy grow instead, up to its
//! offset, while its source is not parsed whole.
//!
//! A copy loses `margin` bases at each end of its source, which its destination reads as any
//! other bytes: only bases count, the line ends, headers and comments among them passed over. It
//! waits in three queues, each named after the point of the source it waits for, its key:
//! `awaiting_source_start`, by the stream offset where its source starts, whose block fixes the
//! base index of its start cut, `margin` bases on; `awaiting_start_cut`, by that base index,
//! whose block trims the copy to start on it; and `awaiting_source_end`, by the stream offset of
//! the source's last byte, which it enters along with the second, so that a source ending before
//! its start cut is dropped. The block holding that byte trims the copy to end past the base
//! `margin` bases before the source's last one. A source ends with its file at the latest. Once
//! the parser is past a copy's destination, the copy is done and dropped, whatever its state.
//!
//! The parser tells the tracker where each sequence line starts and ends, which
//! [`BaseRunHistory`] keeps for the last window, and where each block ends: only a block
//! holding a key does more than a compare. The cuts are converted between stream offsets and
//! base indices in the history.
//!
//! The parser stops where the next closed copy's destination starts, at its start cut, and
//! the tracker decides whether it skips from there to the copy's end cut. Most sources lie in
//! earlier destinations, which nobody parses once skipped, yet the copies of those bytes need
//! their runs: the skipped bytes repeat their source, so the history replays the source's runs
//! in their place, shifted by the copy's offset, and the copies whose keys lie there move on as
//! if the bytes had been parsed. The replay goes chunk by chunk, as the parse would have, so
//! that the copies of a chunk are known when its bytes are replayed: a skip running past the
//! chunk's end stays active for the next. A copy of skipped bytes then finds their runs as
//! exact as parsed ones, at any depth of copies of copies.

mod base_run_history;
mod copy_min_queue;

use base_run_history::BaseRunHistory;
pub(crate) use base_run_history::ReplayedHeaderMarks;
use copy_min_queue::CopyMinQueue;
use std::collections::VecDeque;

/// An LZ copy in a stream: `len` bytes at `destination` repeat those `offset` bytes before, as
/// lz_copyback's `Copyback`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LzCopy {
    pub destination: u64,
    pub offset: u32,
    pub len: u32,
}

impl LzCopy {
    /// Where the bytes it repeats start.
    pub fn source_start(&self) -> u64 {
        self.destination - self.offset as u64
    }

    /// Where the bytes it repeats end, exclusive.
    pub fn source_end(&self) -> u64 {
        self.source_start() + self.len as u64
    }

    /// The copy with its source starting at `start`, in it, its destination moved alike.
    fn with_source_start(self, start: u64) -> Self {
        let skip = start - self.source_start();
        Self {
            destination: self.destination + skip,
            offset: self.offset,
            len: self.len - skip as u32,
        }
    }
}

/// How far the parser got with a copy's source.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CopyStatus {
    /// The parser has not reached the source.
    Waiting,
    /// The parser is in the source's first `margin` bases: its start cut is known by its base
    /// index only.
    InStartMargin,
    /// Trimmed to start on its start cut, the parser past it.
    Open,
    /// Trimmed to end at its end cut too: what a consumer may skip, see [`TrackedCopy`].
    Closed,
    /// Its source holds `2 · margin` bases or fewer in the file it starts in, nothing would be
    /// left, or the parser was past it when the copy came.
    Dropped,
}

/// A copy, and what the parser found of its source: trimmed by `margin` bases at each end,
/// counting only bases, a closed copy is exactly what a consumer may skip.
///
/// A consumer may skip a closed copy's destination `[d, d + len)` only if both rules hold:
/// 1. Every byte before `d` was parsed, or skipped under this contract, and at `d` the parser is
///    inside a FASTA sequence line (`FastaSequence` or `FastaWrapped`), not in a header or a
///    comment; or, with a margin of at least 1, at the start of a line: `d − 1` is a line end of
///    the same file, so `d` is not a file's first byte.
/// 2. `d + len` is within the current file.
///
/// Then the skipped bytes hold exactly `end_base − start_base` bases, which the consumer adds to
/// its base index. At `d + len` the parser is again inside a FASTA sequence line, right after a
/// base, as at the source's end cut: it resumes there with a new sequence, as after a byte that
/// is not ACGT. The k-mers the skip loses, inside the skipped bytes and straddling their ends,
/// lie in the original copy with a margin of at least `k − 1`, so its source has them too. The
/// records inside the skipped bytes are lost, as copies can span headers: their count, their
/// names, and which record the parser resumes in. Only a consumer that reads no headers or
/// record counts may skip. The `margin` bases on each side of a closed copy are read normally at
/// its destination.
///
/// Why: a byte is a base if and only if it is in a sequence line (one that starts with neither
/// `>` nor `;`) and is not a line end, which depends on its own line only. The destination's
/// bytes are the source's, and every byte past the copy's first line end has its line start in
/// the copy, so it is the same at both. Only the bytes before that first line end can differ:
/// they are all bases at the source, which starts on its start cut; at the destination, rule 1
/// makes them bases too. Inside a sequence line, the parser is in a line that runs through all
/// of them. At a line start, the margin puts at least one base of the copy before the start
/// cut, so `d − 1` (and `d − 2`, for a `\r\n`) is in the copy: `d` starts a line at the source
/// too, a sequence line as its first byte is a base there, so that byte is neither `>` nor `;`,
/// and the line is a sequence line at the destination too. With no margin, a line start can
/// differ: a `>` in the middle of a sequence line is a base at the source and starts a header
/// after a line end at the destination. By induction over the earlier skips, the parser's state
/// at `d` is the one a full parse has, so it can check rule 1. Rule 1 rejects a header or a
/// comment running into the copy, whose text can equal the source's bases; rule 2, a
/// destination running across the end of an archive's member.
///
/// Two more rules concern the k-mers, for a consumer of k-mers of `margin + 1` bases or fewer:
/// - A comment does not end a record, so a k-mer can run across it. A comment line fewer than
///   `margin` bases before `d` can leave a k-mer straddling `d` that reaches bases outside the
///   copy, which the source does not repeat: such a copy is not skipped.
/// - A destination starting in a header line that runs into the copy can be skipped from
///   `r + offset` instead, where `r` starts the first run of bases of the source after the
///   one holding its start cut, if that is before `d + len`. The header ends where that run
///   ends, shifted by `offset`: a line end after a base, so not the `\n` of a `\r\n`. The
///   lines between hold no base at the source, so none at the destination; `r − 1` is in the
///   copy, so `r + offset` starts a sequence line at both ends, and every byte from there on
///   is the same at both. No k-mer straddles `r + offset`, the header having ended the record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackedCopy {
    /// The copy, trimmed to its cuts: from [`CopyStatus::Open`] on, its source starts on its
    /// start cut, and from [`CopyStatus::Closed`] on, ends at its end cut. Its offset is kept,
    /// and its destination moves with its source.
    pub copy: LzCopy,
    pub status: CopyStatus,
    /// From [`CopyStatus::Open`] on, the base index of the first base kept, the start cut: how
    /// many bases the stream had before it, every byte of its FASTA sequence lines counted,
    /// those that are not ACGT included.
    pub start_base: u64,
    /// From [`CopyStatus::Closed`] on, the base index past the last base kept.
    pub end_base: u64,
}

impl TrackedCopy {
    /// How many bases a closed copy keeps.
    pub fn base_count(&self) -> u64 {
        self.end_base - self.start_base
    }
}

/// Counts of what happened to the copies, for tests and statistics.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct TrackerStats {
    pub received: u64,
    pub opened: u64,
    pub closed: u64,
    /// Dropped as their source holds `2 · margin` bases or fewer, in the file it starts in.
    pub dropped_too_short: u64,
    /// Dropped as the parser was past their whole source when they came.
    pub dropped_late: u64,
    /// The bases the closed copies keep, `end_base − start_base` summed: those a consumer could
    /// skip.
    pub skippable_bases: u64,
    /// The skips the parser took, and the bytes and bases they left out, `skippable_bases` and
    /// more with the copies whose start was deferred past a header.
    pub skips: u64,
    pub skipped_bytes: u64,
    pub skipped_bases: u64,
    /// The closed copies starting in a header that runs into them, whose skip was deferred to
    /// the source's next sequence line.
    pub deferred_skip_starts: u64,
    /// The closed copies not skipped: a comment line fewer than `margin` bases before;
    /// their end past their file's; their start in a header with no sequence line left after
    /// it in the copy; their start in bytes the parser never reached, as a tar header, or
    /// passed; their start in a comment line; at their file's first byte; at a line start with
    /// no margin.
    pub skips_refused_near_comment: u64,
    pub skips_refused_past_file_end: u64,
    pub skips_refused_without_start: u64,
    pub skips_refused_unreached: u64,
    pub skips_refused_in_comment: u64,
    pub skips_refused_at_file_start: u64,
    pub skips_refused_without_margin: u64,
}

/// Where the parser is when it stops at a skip start, for [`CopybackTracker::decide_skip_at`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ParserLineState {
    /// In a FASTA sequence line, past its first byte.
    InSequenceLine,
    /// At a line's first byte, its state not picked yet.
    AtLineStart,
    InHeaderLine,
    InCommentLine,
}

/// What the parser does at a skip start.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum SkipDecision {
    /// Parse on, as if there were no stop.
    ParseOn,
    /// Skip from the stop: nothing of the skipped bytes reaches the output.
    Skip(SkipProgress),
}

/// How far a skip got in the chunk being parsed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct SkipProgress {
    /// Where the parser resumes, in a FASTA sequence line right after a base, as after a byte
    /// that is not ACGT: `Some` when that is in the chunk or at its end, `None` when the skip
    /// goes on in the next chunk.
    pub(crate) resume_offset: Option<u64>,
    /// The header lines the skip passed in the chunk, with header marks only.
    pub(crate) replayed_headers: ReplayedHeaderMarks,
}

/// A skip whose end, `resume_offset`, is past the chunk parsed: its copy can be retired before
/// the parser gets there, so it keeps what its replay needs.
#[derive(Copy, Clone, Debug)]
struct ActiveSkip {
    resume_offset: u64,
    copy_offset: u64,
    base_shift: u64,
}

/// A skip start deferred past a header, to `start`, which can be in a later chunk than its
/// copy's destination: kept apart from the copy, which is retired with that chunk.
#[derive(Copy, Clone, Debug)]
struct DeferredSkip {
    start: u64,
    copy_offset: u64,
    end: u64,
}

/// The copies of one stream, see the module's documentation.
pub struct CopybackTracker {
    /// How many bases a copy loses at each end.
    margin: u64,
    /// The copies received and not retired, in the order they came, from the one numbered
    /// `front_index` on.
    copies: VecDeque<TrackedCopy>,
    front_index: u64,
    /// The [`CopyStatus::Waiting`] copies, by the stream offset where their source starts, or
    /// where the parser was when they came, if past it.
    awaiting_source_start: CopyMinQueue,
    /// The [`CopyStatus::InStartMargin`] copies, by the base index of their start cut, which
    /// they come in the order of: the front is the least.
    awaiting_start_cut: VecDeque<(u64, u64)>,
    /// The [`CopyStatus::InStartMargin`] and [`CopyStatus::Open`] copies, by the stream offset
    /// of their source's last byte.
    awaiting_source_end: CopyMinQueue,
    /// Where the bases of the last window are: every base and offset a lookup can reach, see
    /// [`Self::retire_before`].
    base_run_history: BaseRunHistory,
    /// The farthest a copy received reaches back from its destination.
    largest_copy_offset: u64,
    /// No block ending at or before this stream offset holds a key of `awaiting_source_start`
    /// or `awaiting_source_end`.
    next_due_offset: u64,
    /// The key at the front of `awaiting_start_cut`, `u64::MAX` if none.
    next_due_start_cut: u64,
    /// Where the bytes parsed, or skipped, so far end.
    parsed_to: u64,
    /// Whether the parser is offered skips: not by a tracker that only follows the copies.
    offers_skips: bool,
    /// Whether the parser tells the header lines, so that the records it skips are counted and
    /// the sequence it resumes in named: see [`Self::start_stream`].
    keeps_header_marks: bool,
    /// The copy whose destination the parser stops at next, the first by destination it has
    /// not passed, unless a deferred skip comes first.
    skip_candidate: u64,
    /// Where the parser stops next, `u64::MAX` for nowhere: see
    /// [`Self::refresh_next_skip_start`].
    next_skip_start: u64,
    deferred_skip: Option<DeferredSkip>,
    active_skip: Option<ActiveSkip>,
    /// Where the file being parsed starts, once its first chunk came, and where it ends,
    /// `u64::MAX` for a file that is its own stream.
    file_start_offset: Option<u64>,
    file_end_offset: u64,
    /// The base index where the last comment line the parser entered in the file starts.
    last_comment_base: Option<u64>,
    /// The ranges skipped, one merged into the last when it goes on from it.
    #[cfg(test)]
    skipped_ranges: Vec<(u64, u64)>,
    stats: TrackerStats,
}

impl Default for CopybackTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CopybackTracker {
    /// A tracker with no margin, for a stream that starts at offset 0, that offers the parser
    /// to skip the copies.
    pub fn new() -> Self {
        Self {
            margin: 0,
            copies: VecDeque::new(),
            front_index: 0,
            awaiting_source_start: CopyMinQueue::new(),
            awaiting_start_cut: VecDeque::new(),
            awaiting_source_end: CopyMinQueue::new(),
            base_run_history: BaseRunHistory::new(),
            largest_copy_offset: 0,
            next_due_offset: u64::MAX,
            next_due_start_cut: u64::MAX,
            parsed_to: 0,
            offers_skips: true,
            keeps_header_marks: false,
            skip_candidate: 0,
            next_skip_start: u64::MAX,
            deferred_skip: None,
            active_skip: None,
            file_start_offset: None,
            file_end_offset: u64::MAX,
            last_comment_base: None,
            #[cfg(test)]
            skipped_ranges: Vec::new(),
            stats: TrackerStats::default(),
        }
    }

    /// As [`Self::new`], for a tracker that only follows the copies, and never offers a skip.
    pub fn following_only() -> Self {
        Self {
            offers_skips: false,
            ..Self::new()
        }
    }

    /// Forgets everything but the stats, for a new stream, whose offsets and base indices start
    /// at 0, its copies losing `margin` bases at each end. With `keep_header_marks`, the parser
    /// tells the tracker where each header line is, and a skip replays them, so that the
    /// parser's record count and record indices stay exact, and the sequence it resumes in has
    /// the header of its record; without, the records in a skip are lost, and that sequence has
    /// no header.
    pub fn start_stream(&mut self, margin: u32, keep_header_marks: bool) {
        self.margin = margin as u64;
        self.keeps_header_marks = keep_header_marks;
        self.copies.clear();
        self.front_index = 0;
        self.awaiting_source_start.clear();
        self.awaiting_start_cut.clear();
        self.awaiting_source_end.clear();
        self.base_run_history.clear();
        self.largest_copy_offset = 0;
        self.next_due_offset = u64::MAX;
        self.next_due_start_cut = u64::MAX;
        self.parsed_to = 0;
        self.skip_candidate = 0;
        self.deferred_skip = None;
        self.active_skip = None;
        self.file_start_offset = None;
        self.file_end_offset = u64::MAX;
        self.last_comment_base = None;
        #[cfg(test)]
        self.skipped_ranges.clear();
        self.refresh_next_skip_start();
    }

    /// Starts a file of the stream, which ends at stream offset `end_offset`, `None` for a file
    /// that is its own stream. Where it starts comes with its first chunk.
    pub fn start_file(&mut self, end_offset: Option<u64>) {
        self.file_start_offset = None;
        self.file_end_offset = end_offset.unwrap_or(u64::MAX);
        self.last_comment_base = None;
    }

    pub fn stats(&self) -> TrackerStats {
        self.stats
    }

    /// The bases parsed, or skipped, in the stream so far.
    pub fn base_index(&self) -> u64 {
        self.base_run_history.base_index_at(self.parsed_to)
    }

    /// How many copies are received and not retired yet.
    pub fn live_copies(&self) -> usize {
        self.copies.len()
    }

    /// The copies received and not retired yet, in the order they came.
    pub fn copies(&self) -> impl Iterator<Item = &TrackedCopy> {
        self.copies.iter()
    }

    /// The copy numbered `index`, the count of copies before it since the stream started, if it
    /// is not retired yet.
    pub fn copy(&self, index: u64) -> Option<&TrackedCopy> {
        self.copies
            .get(index.checked_sub(self.front_index)? as usize)
    }

    /// The copy numbered `index` in `copies`, whose front is numbered `front_index`, if it is
    /// not retired yet: on the fields, for a loop that borrows another one.
    fn copy_in(
        copies: &mut VecDeque<TrackedCopy>,
        front_index: u64,
        index: u64,
    ) -> Option<&mut TrackedCopy> {
        copies.get_mut(index.checked_sub(front_index)? as usize)
    }

    /// Adds the next copy of the stream, before the parser reaches its source, which ends
    /// before its destination starts (`len <= offset`): one whose source starts before the bytes
    /// parsed already is trimmed to what is left of it. One that continues the last copy, at the
    /// same offset from where its destination ends, makes it grow instead, if its source is not
    /// parsed whole and the two are at most `offset` bytes long in all: the start cut moves the
    /// destination's start only. A closed or dropped copy stays as it was, and the continuation
    /// is a copy of its own.
    pub fn add_copy(&mut self, copy: LzCopy) {
        debug_assert!(copy.len <= copy.offset, "{copy:?} overlaps its source");
        self.largest_copy_offset = self.largest_copy_offset.max(copy.offset as u64);
        if let Some(last) = self.copies.back_mut()
            && matches!(
                last.status,
                CopyStatus::Waiting | CopyStatus::InStartMargin | CopyStatus::Open
            )
            && last.copy.offset == copy.offset
            && last.copy.destination + last.copy.len as u64 == copy.destination
            && last.copy.len as u64 + copy.len as u64 <= copy.offset as u64
        {
            last.copy.len += copy.len;
            return;
        }
        let index = self.front_index + self.copies.len() as u64;
        self.stats.received += 1;
        let mut status = CopyStatus::Waiting;
        if copy.source_end() <= self.parsed_to {
            self.stats.dropped_late += 1;
            status = CopyStatus::Dropped;
        } else {
            let from = copy.source_start().max(self.parsed_to);
            self.awaiting_source_start.push(from, index);
            self.next_due_offset = self.next_due_offset.min(from);
        }
        self.copies.push_back(TrackedCopy {
            copy,
            status,
            start_base: 0,
            end_base: 0,
        });
        if index == self.skip_candidate {
            self.refresh_next_skip_start();
        }
    }

    /// Drops the copies whose destination starts before `position`, which the parser is past.
    /// None of them is in its source still, which ends before the destination starts: one that
    /// is waiting has a source the parser never reached, as in the end of an archive.
    ///
    /// Also forgets the runs no lookup can reach anymore. A lookup by offset is at most the
    /// largest copy offset back from where the parser is, at a copy's source; a lookup by base,
    /// an end cut, is at most `margin + 1` bases before it, as the parser is at the source's
    /// end then. So a group of runs is forgotten when both bounds are past it.
    pub fn retire_before(&mut self, position: u64) {
        while let Some(front) = self.copies.front()
            && front.copy.destination < position
        {
            debug_assert!(
                !matches!(front.status, CopyStatus::InStartMargin | CopyStatus::Open),
                "{front:?} retired before its source ends"
            );
            self.copies.pop_front();
            self.front_index += 1;
        }
        if self.skip_candidate < self.front_index {
            self.skip_candidate = self.front_index;
            self.refresh_next_skip_start();
        }
        self.base_run_history.forget_before(
            self.parsed_to.saturating_sub(self.largest_copy_offset),
            self.base_index().saturating_sub(self.margin + 1),
        );
        // The queues drop their entries of retired copies as they reach their keys; without
        // blocks to parse (a FASTQ file, say), they would pile up.
        let front_index = self.front_index;
        if self.awaiting_source_start.len() > 2 * self.copies.len() + 64 {
            self.awaiting_source_start
                .retain(|index| index >= front_index);
            self.awaiting_start_cut
                .retain(|&(_, index)| index >= front_index);
            self.awaiting_source_end
                .retain(|index| index >= front_index);
            self.update_next_due();
        }
    }

    /// Ends the file being parsed, as a source does not run on into the next file: a copy in
    /// its start margin is dropped, an open one closes at the file's end. So a source that
    /// starts past the file's last base, and runs on into the next file across an archive's
    /// header, is dropped, rather than starting in the next file. And the part of a source past
    /// the file's end is forgotten: one that continues across a header, as a duplicated member
    /// can make, is followed further only if its last matches came as a copy of their own, as
    /// when the reader took the copy before them out of lz_copyback's queue.
    ///
    /// A skip ends in its file, so one still going on, or deferred, means the input was cut
    /// short: it is dropped. The skip candidate stays: in an archive, the copies of the next
    /// members keep their stops, and a stop in the bytes between files is taken where the
    /// parser next is.
    pub fn end_file(&mut self) {
        // A last line with no line end ends with its file.
        if self.base_run_history.has_open_run() {
            self.base_run_history.close_run(self.parsed_to);
        }
        if self.active_skip.take().is_some() | self.deferred_skip.take().is_some() {
            debug_assert!(
                self.parsed_to < self.file_end_offset,
                "a skip left at the end of a whole file"
            );
            self.refresh_next_skip_start();
        }
        let end_of_file = self.base_index();
        for (_, index) in self.awaiting_source_end.drain() {
            let Some(tracked) = Self::copy_in(&mut self.copies, self.front_index, index) else {
                continue;
            };
            match tracked.status {
                CopyStatus::InStartMargin => {
                    tracked.status = CopyStatus::Dropped;
                    self.stats.dropped_too_short += 1;
                }
                CopyStatus::Open => Self::close(
                    tracked,
                    end_of_file,
                    self.margin,
                    &self.base_run_history,
                    &mut self.stats,
                ),
                _ => {}
            }
        }
        // The copies still waiting have their source past the file's bytes.
        self.awaiting_start_cut.clear();
        self.update_next_due();
    }

    /// A FASTA sequence line starts at stream offset `offset`.
    #[inline(always)]
    pub(crate) fn on_sequence_line_start(&mut self, offset: u64) {
        self.base_run_history.open_run(offset);
    }

    /// The FASTA sequence line the parser is in ends at stream offset `offset`, its line end.
    #[inline(always)]
    pub(crate) fn on_sequence_line_end(&mut self, offset: u64) {
        self.base_run_history.close_run(offset);
    }

    /// Whether the parser tells the header lines, by [`Self::on_header_line`].
    #[inline(always)]
    pub(crate) fn keeps_header_marks(&self) -> bool {
        self.keeps_header_marks
    }

    /// A FASTA header line from its `>` at stream offset `start` to its line end at `end`.
    pub(crate) fn on_header_line(&mut self, start: u64, end: u64) {
        debug_assert!(self.keeps_header_marks);
        self.base_run_history.add_header_mark(start, end);
    }

    /// A FASTA comment line starts at stream offset `offset`.
    pub(crate) fn on_comment_line(&mut self, offset: u64) {
        self.last_comment_base = Some(self.base_run_history.base_index_at(offset));
    }

    /// Where the parser stops next, to ask [`Self::decide_skip_at`]: `u64::MAX` for nowhere.
    #[inline(always)]
    pub(crate) fn next_skip_start(&self) -> u64 {
        self.next_skip_start
    }

    /// Derives `next_skip_start`, its one writer: nowhere for a tracker that offers no skip;
    /// else the start of a pending deferred skip, which comes before the next copy's
    /// destination; else the destination of the skip candidate, which moves to its start cut
    /// as it opens, never earlier, so that a stop there never misses it; else nowhere. Called
    /// wherever one of those changes.
    fn refresh_next_skip_start(&mut self) {
        self.next_skip_start = if !self.offers_skips {
            u64::MAX
        } else if let Some(deferred) = &self.deferred_skip {
            deferred.start
        } else {
            (self.copy(self.skip_candidate)).map_or(u64::MAX, |tracked| tracked.copy.destination)
        };
    }

    /// Makes the next copy the skip candidate.
    fn move_to_next_skip_candidate(&mut self) {
        self.skip_candidate += 1;
        self.refresh_next_skip_start();
    }

    /// Decides whether the parser skips from stream offset `position`, where it stopped at
    /// [`Self::next_skip_start`], or past it in bytes it never read, as a tar header between
    /// two members, after [`Self::on_parsed_up_to`] at `position`: so the candidate is final,
    /// as its source ends before its destination. `line` is where the parser is, for the piece
    /// it parses next; `chunk_end` is where the chunk ends.
    ///
    /// A closed candidate starting at `position` is skipped to its end cut if its end is in the
    /// file (rule 2 of [`TrackedCopy`]), and, in a sequence line, or at a line start but the
    /// file's first byte with a margin (rule 1), if no comment line was entered fewer than
    /// `margin` bases before. In a header running into it, its start is deferred to the
    /// source's next sequence line, shifted, if that is before its end; in a comment, it is
    /// parsed. Then, or for a candidate dropped or starting before `position`, the next copy
    /// becomes the candidate; a candidate that moved its start past `position` stays one.
    ///
    /// A skip replays the runs of its source up to its end or the chunk's, see
    /// [`BaseRunHistory::replay`]. Every [`SkipDecision::ParseOn`] makes progress, so the
    /// parser cannot stop twice at one place for one stop: the candidate moves on, a deferral
    /// is taken or refused, or the next stop is past `position`.
    pub(crate) fn decide_skip_at(
        &mut self,
        position: u64,
        line: ParserLineState,
        chunk_end: u64,
    ) -> SkipDecision {
        let (candidate_before, deferred_before) =
            (self.skip_candidate, self.deferred_skip.is_some());
        let decision = self.decide(position, line, chunk_end);
        debug_assert!(
            matches!(decision, SkipDecision::Skip(_))
                || self.skip_candidate != candidate_before
                || (deferred_before && self.deferred_skip.is_none())
                || self.next_skip_start > position,
            "a stop at {position} answered with no progress"
        );
        decision
    }

    /// The decision of [`Self::decide_skip_at`].
    fn decide(&mut self, position: u64, line: ParserLineState, chunk_end: u64) -> SkipDecision {
        if let Some(deferred) = self.deferred_skip.take() {
            self.refresh_next_skip_start();
            if deferred.start < position {
                self.stats.skips_refused_unreached += 1;
                return SkipDecision::ParseOn;
            }
            debug_assert_eq!(
                line,
                ParserLineState::AtLineStart,
                "a deferred start mid-line"
            );
            // No comment rule, as the header ended the record, and no margin is needed, as the
            // line end before is in the copy; rule 2 held at the deferral.
            return self.start_skip(
                deferred.start,
                deferred.copy_offset,
                deferred.end,
                chunk_end,
            );
        }
        let tracked = (self.copy(self.skip_candidate)).expect("a stop with no skip candidate");
        match tracked.status {
            CopyStatus::Dropped => {
                self.move_to_next_skip_candidate();
                return SkipDecision::ParseOn;
            }
            CopyStatus::Closed => {}
            _ => {
                // Its source ends before its destination starts, so it is final there; were it
                // not, it is parsed, never skipped untrimmed.
                debug_assert!(false, "{tracked:?} is not final at its destination");
                self.move_to_next_skip_candidate();
                return SkipDecision::ParseOn;
            }
        }
        let (start, offset) = (tracked.copy.destination, tracked.copy.offset as u64);
        let end = start + tracked.copy.len as u64;
        if start > position {
            // It opened past the stop at its old destination.
            self.refresh_next_skip_start();
            return SkipDecision::ParseOn;
        }
        // Before the replay, which can open the next copy.
        self.move_to_next_skip_candidate();
        if start < position {
            self.stats.skips_refused_unreached += 1;
            return SkipDecision::ParseOn;
        }
        if end > self.file_end_offset {
            self.stats.skips_refused_past_file_end += 1;
            return SkipDecision::ParseOn;
        }
        match line {
            ParserLineState::InCommentLine => {
                self.stats.skips_refused_in_comment += 1;
                SkipDecision::ParseOn
            }
            ParserLineState::InHeaderLine => {
                match self.base_run_history.next_run_start_after(start - offset) {
                    Some(run_start) if run_start + offset < end => {
                        self.deferred_skip = Some(DeferredSkip {
                            start: run_start + offset,
                            copy_offset: offset,
                            end,
                        });
                        self.stats.deferred_skip_starts += 1;
                        self.refresh_next_skip_start();
                    }
                    _ => self.stats.skips_refused_without_start += 1,
                }
                SkipDecision::ParseOn
            }
            ParserLineState::AtLineStart if Some(position) == self.file_start_offset => {
                self.stats.skips_refused_at_file_start += 1;
                SkipDecision::ParseOn
            }
            ParserLineState::AtLineStart if self.margin == 0 => {
                self.stats.skips_refused_without_margin += 1;
                SkipDecision::ParseOn
            }
            ParserLineState::AtLineStart | ParserLineState::InSequenceLine => {
                if let Some(comment) = self.last_comment_base
                    && self.base_run_history.base_index_at(position) - comment < self.margin
                {
                    self.stats.skips_refused_near_comment += 1;
                    return SkipDecision::ParseOn;
                }
                self.start_skip(start, offset, end, chunk_end)
            }
        }
    }

    /// Skips from stream offset `start` to `end`, the bytes repeating those `copy_offset`
    /// before, as far as the chunk, which ends at `chunk_end`, goes; the rest stays active.
    fn start_skip(
        &mut self,
        start: u64,
        copy_offset: u64,
        end: u64,
        chunk_end: u64,
    ) -> SkipDecision {
        let base_shift = self.base_run_history.base_index_at(start)
            - self.base_run_history.base_index_at(start - copy_offset);
        self.stats.skips += 1;
        let to = end.min(chunk_end);
        let replayed_headers = self.replay_active_skip(start, to, copy_offset, base_shift);
        if end <= chunk_end {
            return SkipDecision::Skip(SkipProgress {
                resume_offset: Some(end),
                replayed_headers,
            });
        }
        self.active_skip = Some(ActiveSkip {
            resume_offset: end,
            copy_offset,
            base_shift,
        });
        SkipDecision::Skip(SkipProgress {
            resume_offset: None,
            replayed_headers,
        })
    }

    /// Takes the skipped bytes from stream offset `from` to `to` as if parsed: replays their
    /// source's runs, then moves on the copies whose keys lie there.
    fn replay_active_skip(
        &mut self,
        from: u64,
        to: u64,
        copy_offset: u64,
        base_shift: u64,
    ) -> ReplayedHeaderMarks {
        debug_assert_eq!(from, self.parsed_to, "a skip from where the parser is not");
        let replayed_headers = (self.base_run_history).replay(from, to, copy_offset, base_shift);
        self.advance_copies(from, to);
        self.stats.skipped_bytes += to - from;
        self.stats.skipped_bases +=
            self.base_run_history.base_index_at(to) - self.base_run_history.base_index_at(from);
        #[cfg(test)]
        match self.skipped_ranges.last_mut() {
            Some(last) if last.1 == from => last.1 = to,
            _ => self.skipped_ranges.push((from, to)),
        }
        self.parsed_to = to;
        replayed_headers
    }

    /// Starts the chunk from stream offset `chunk_start` to `chunk_end`: the file's start, if
    /// it is its first chunk; and the part of the active skip in the chunk, if any, as
    /// [`Self::decide_skip_at`] does, the skip ending if its end is in the chunk or at its end.
    pub(crate) fn continue_active_skip(
        &mut self,
        chunk_start: u64,
        chunk_end: u64,
    ) -> Option<SkipProgress> {
        if self.file_start_offset.is_none() {
            self.file_start_offset = Some(chunk_start);
        }
        let skip = self.active_skip?;
        // Rule 2: a skip ends in its file, so the chunks it spans are consecutive.
        debug_assert_eq!(chunk_start, self.parsed_to, "a skip across a gap");
        let to = skip.resume_offset.min(chunk_end);
        let replayed_headers =
            self.replay_active_skip(chunk_start, to, skip.copy_offset, skip.base_shift);
        let resume_offset = (skip.resume_offset <= chunk_end).then(|| {
            self.active_skip = None;
            skip.resume_offset
        });
        Some(SkipProgress {
            resume_offset,
            replayed_headers,
        })
    }

    /// The parser has parsed every byte before stream offset `offset`, and told the tracker of
    /// every line among them: a block's end, or where it stops in one. Only when a key lies
    /// before it does it do more than a compare.
    #[inline(always)]
    pub(crate) fn on_parsed_up_to(&mut self, offset: u64) {
        debug_assert!(
            offset >= self.parsed_to,
            "parsed back from {} to {offset}",
            self.parsed_to
        );
        if self.next_due_offset < offset
            || self.next_due_start_cut < self.base_run_history.base_index_at(offset)
        {
            self.advance_copies(self.parsed_to, offset);
        }
        self.parsed_to = offset;
    }

    /// Moves on the copies whose key lies in the bytes parsed from stream offset `from` to `to`,
    /// in three steps, see the module's documentation.
    #[inline(never)]
    fn advance_copies(&mut self, from: u64, to: u64) {
        self.reach_source_starts(from, to);
        self.reach_start_cuts(from, to);
        self.reach_source_ends(to);
        self.update_next_due();
    }

    /// Fixes the start cuts of the waiting copies whose source starts in the bytes from `from`
    /// to `to`, or before them in bytes not parsed: `margin` bases from there on. Their keys
    /// come in order, and their start cuts with them, as base indices only grow along the
    /// stream.
    #[inline(always)]
    fn reach_source_starts(&mut self, from: u64, to: u64) {
        while let Some((start, index)) = self.awaiting_source_start.peek() {
            if start >= to {
                break;
            }
            self.awaiting_source_start.pop();
            let Some(tracked) = Self::copy_in(&mut self.copies, self.front_index, index) else {
                continue;
            };
            if tracked.status != CopyStatus::Waiting {
                continue;
            }
            let start_cut = self.base_run_history.base_index_at(start.max(from)) + self.margin;
            tracked.status = CopyStatus::InStartMargin;
            debug_assert!(
                (self.awaiting_start_cut.back()).is_none_or(|&(back, _)| back <= start_cut),
                "the start cuts come out of order"
            );
            self.awaiting_start_cut.push_back((start_cut, index));
            // Also now, so that a source ending before its start cut is dropped.
            (self.awaiting_source_end).push(tracked.copy.source_end() - 1, index);
        }
    }

    /// Trims the copies whose start cut is in the bytes from `from` to `to` to start there, but
    /// drops those whose source ended before it.
    #[inline(always)]
    fn reach_start_cuts(&mut self, from: u64, to: u64) {
        let end_base = self.base_run_history.base_index_at(to);
        while let Some(&(start_cut, index)) = self.awaiting_start_cut.front() {
            if start_cut >= end_base {
                break;
            }
            self.awaiting_start_cut.pop_front();
            debug_assert!(
                start_cut >= self.base_run_history.base_index_at(from),
                "a start cut was passed"
            );
            let Some(tracked) = Self::copy_in(&mut self.copies, self.front_index, index) else {
                continue;
            };
            if tracked.status != CopyStatus::InStartMargin {
                continue;
            }
            let position = self.base_run_history.offset_of_base(start_cut);
            if position >= tracked.copy.source_end() {
                tracked.status = CopyStatus::Dropped;
                self.stats.dropped_too_short += 1;
                continue;
            }
            tracked.copy = tracked.copy.with_source_start(position);
            tracked.start_base = start_cut;
            tracked.status = CopyStatus::Open;
            self.stats.opened += 1;
            // The parser stops at its new destination, not its old one.
            if index == self.skip_candidate {
                self.refresh_next_skip_start();
            }
        }
    }

    /// Closes the open copies whose source ends before `to`, and drops those that end before
    /// their start cut. A copy that grew since its key was pushed goes back with its new one.
    #[inline(always)]
    fn reach_source_ends(&mut self, to: u64) {
        while let Some((last, index)) = self.awaiting_source_end.peek() {
            if last >= to {
                break;
            }
            self.awaiting_source_end.pop();
            let Some(tracked) = Self::copy_in(&mut self.copies, self.front_index, index) else {
                continue;
            };
            let source_end = tracked.copy.source_end();
            match tracked.status {
                CopyStatus::InStartMargin | CopyStatus::Open if source_end > last + 1 => {
                    self.awaiting_source_end.push(source_end - 1, index);
                }
                CopyStatus::InStartMargin => {
                    tracked.status = CopyStatus::Dropped;
                    self.stats.dropped_too_short += 1;
                }
                CopyStatus::Open => Self::close(
                    tracked,
                    self.base_run_history.base_index_at(source_end),
                    self.margin,
                    &self.base_run_history,
                    &mut self.stats,
                ),
                _ => {}
            }
        }
    }

    /// Closes the open copy `tracked`, whose source's last base comes before base index
    /// `source_end_base`: trims it to end past the base `margin` bases before that last one, if
    /// any is left, else drops it.
    fn close(
        tracked: &mut TrackedCopy,
        source_end_base: u64,
        margin: u64,
        base_run_history: &BaseRunHistory,
        stats: &mut TrackerStats,
    ) {
        // The start cut is `margin` bases past a base index, and a base of the source.
        let end_base = source_end_base - margin;
        if end_base <= tracked.start_base {
            tracked.status = CopyStatus::Dropped;
            stats.dropped_too_short += 1;
            return;
        }
        let end_cut = base_run_history.offset_of_base(end_base - 1) + 1;
        tracked.copy.len = (end_cut - tracked.copy.source_start()) as u32;
        tracked.end_base = end_base;
        tracked.status = CopyStatus::Closed;
        stats.closed += 1;
        stats.skippable_bases += end_base - tracked.start_base;
    }

    fn update_next_due(&mut self) {
        self.next_due_offset =
            (self.awaiting_source_start.next_key()).min(self.awaiting_source_end.next_key());
        self.next_due_start_cut =
            (self.awaiting_start_cut.front()).map_or(u64::MAX, |entry| entry.0);
    }
}

#[cfg(test)]
mod tests;
