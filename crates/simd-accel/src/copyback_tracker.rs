//! The LZ copies of a FASTA stream, followed as the parser reads their sources, so that each
//! comes out trimmed to what a consumer may skip at its destination, its bases counted in the
//! coordinates of the bases read: see [`TrackedCopy`].
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
//! `margin` bases before the source's last one, found in `recent_base_blocks`, the last blocks
//! with bases. Only a block holding a key does more than count its bases and remember them. A
//! source ends with its file at the latest. Once the parser is past a copy's destination, the
//! copy is done and dropped, whatever its state.

mod copy_min_queue;

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
#[derive(Clone, Debug)]
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
}

/// The copies of one stream, see the module's documentation.
pub struct CopybackTracker {
    /// How many bases a copy loses at each end.
    margin: u64,
    /// The bases parsed in the stream so far.
    base_index: u64,
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
    /// The last blocks parsed that hold bases, oldest first: the stream offset of each, the base
    /// index at its start, and its bases. Every base an end cut can be on is in one of them, see
    /// [`Self::remember_bases`].
    recent_base_blocks: VecDeque<(u64, u64, u64)>,
    /// No block ending at or before this stream offset holds a key of `awaiting_source_start`
    /// or `awaiting_source_end`.
    next_due_offset: u64,
    /// The key at the front of `awaiting_start_cut`, `u64::MAX` if none.
    next_due_start_cut: u64,
    /// Where the blocks parsed so far end.
    parsed_to: u64,
    stats: TrackerStats,
}

/// The bits of a block below bit `bit`.
#[inline(always)]
fn bits_below(bit: u64) -> u64 {
    if bit >= 64 { u64::MAX } else { (1 << bit) - 1 }
}

/// The position of the set bit of `mask` that has `n` set bits below it, `n` below
/// `mask.count_ones()`. A loop: the tracker's steps are compiled without the target features of
/// the parser's kernels, so with no BMI2.
#[inline(always)]
fn nth_set_bit(mut mask: u64, n: u64) -> u64 {
    for _ in 0..n {
        mask &= mask - 1;
    }
    mask.trailing_zeros() as u64
}

impl Default for CopybackTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CopybackTracker {
    /// A tracker with no margin, for a stream that starts at offset 0.
    pub fn new() -> Self {
        Self {
            margin: 0,
            base_index: 0,
            copies: VecDeque::new(),
            front_index: 0,
            awaiting_source_start: CopyMinQueue::new(),
            awaiting_start_cut: VecDeque::new(),
            awaiting_source_end: CopyMinQueue::new(),
            recent_base_blocks: VecDeque::new(),
            next_due_offset: u64::MAX,
            next_due_start_cut: u64::MAX,
            parsed_to: 0,
            stats: TrackerStats::default(),
        }
    }

    /// Forgets everything but the stats, for a new stream, whose offsets and base indices start
    /// at 0, its copies losing `margin` bases at each end.
    pub fn start_stream(&mut self, margin: u32) {
        self.margin = margin as u64;
        self.base_index = 0;
        self.copies.clear();
        self.front_index = 0;
        self.awaiting_source_start.clear();
        self.awaiting_start_cut.clear();
        self.awaiting_source_end.clear();
        self.recent_base_blocks.clear();
        self.next_due_offset = u64::MAX;
        self.next_due_start_cut = u64::MAX;
        self.parsed_to = 0;
    }

    pub fn stats(&self) -> TrackerStats {
        self.stats
    }

    /// The bases parsed in the stream so far.
    pub fn base_index(&self) -> u64 {
        self.base_index
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
    }

    /// Drops the copies whose destination starts before `position`, which the parser is past.
    /// None of them is in its source still, which ends before the destination starts: one that
    /// is waiting has a source the parser never reached, as in the end of an archive.
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
    pub fn end_file(&mut self) {
        let end_of_file = self.base_index;
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
                    &self.recent_base_blocks,
                    &mut self.stats,
                ),
                _ => {}
            }
        }
        // The copies still waiting have their source past the file's bytes.
        self.awaiting_start_cut.clear();
        self.update_next_due();
    }

    /// Takes the parsed block of `len` bytes at stream offset `offset`, whose bytes in FASTA
    /// sequence lines, its bases, are the bits of `bases`. A block that holds no key only counts
    /// its bases and remembers them.
    #[inline(always)]
    pub(crate) fn on_block_parsed(&mut self, offset: u64, len: usize, bases: u64) {
        let end = offset + len as u64;
        let count = bases.count_ones() as u64;
        self.remember_bases(offset, bases);
        if self.next_due_offset < end || self.next_due_start_cut < self.base_index + count {
            self.advance_copies(offset, end, bases);
        }
        self.base_index += count;
        self.parsed_to = end;
    }

    /// Adds the block at stream offset `offset`, of bases `bases`, to `recent_base_blocks` if it
    /// has any, and forgets the oldest blocks no end cut can be in. An end cut is asked for once
    /// the parser reaches its source's end, in this block or later, so `margin + 1` bases or
    /// fewer before this block's start: a block whose bases are all before that, as the next
    /// block's start says, is not needed anymore. So at most `margin + 2` blocks are kept,
    /// whatever lies between their bases.
    #[inline(always)]
    fn remember_bases(&mut self, offset: u64, bases: u64) {
        if bases != 0 {
            self.recent_base_blocks
                .push_back((offset, self.base_index, bases));
        }
        let oldest_needed = self.base_index.saturating_sub(self.margin + 1);
        while self.recent_base_blocks.len() >= 2 && self.recent_base_blocks[1].1 <= oldest_needed {
            self.recent_base_blocks.pop_front();
        }
    }

    /// The stream offset of the base of base index `base`, in `recent_base_blocks`.
    fn offset_of_base(recent_base_blocks: &VecDeque<(u64, u64, u64)>, base: u64) -> u64 {
        let &(offset, start, bases) = (recent_base_blocks.iter().rev())
            .find(|&&(_, start, _)| start <= base)
            .expect("an end cut is in the last blocks with bases");
        debug_assert!(base - start < bases.count_ones() as u64);
        offset + nth_set_bit(bases, base - start)
    }

    /// Moves on the copies whose key the block of bases `bases` from stream offset `offset` to
    /// `end` holds, in three steps, see the module's documentation.
    #[inline(never)]
    fn advance_copies(&mut self, offset: u64, end: u64, bases: u64) {
        self.reach_source_starts(offset, end, bases);
        self.reach_start_cuts(offset, end, bases);
        self.reach_source_ends(offset, end, bases);
        self.update_next_due();
    }

    /// The base index of the byte at stream offset `position`, in or around the block of bases
    /// `bases` at `offset`: how many bases the stream has before it.
    #[inline(always)]
    fn base_index_at(base_index: u64, offset: u64, bases: u64, position: u64) -> u64 {
        base_index + (bases & bits_below(position.saturating_sub(offset))).count_ones() as u64
    }

    /// Fixes the start cuts of the waiting copies whose source starts in the block, or before
    /// it in bytes not parsed: `margin` bases from there on. Their keys come in order, and
    /// their start cuts with them, as base indices only grow along the stream.
    #[inline(always)]
    fn reach_source_starts(&mut self, offset: u64, end: u64, bases: u64) {
        while let Some((start, index)) = self.awaiting_source_start.peek() {
            if start >= end {
                break;
            }
            self.awaiting_source_start.pop();
            let Some(tracked) = Self::copy_in(&mut self.copies, self.front_index, index) else {
                continue;
            };
            if tracked.status != CopyStatus::Waiting {
                continue;
            }
            let start_cut = Self::base_index_at(self.base_index, offset, bases, start.max(offset))
                + self.margin;
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

    /// Trims the copies whose start cut is in the block to start there, but drops those whose
    /// source ended before it.
    #[inline(always)]
    fn reach_start_cuts(&mut self, offset: u64, _end: u64, bases: u64) {
        let end_base = self.base_index + bases.count_ones() as u64;
        while let Some(&(start_cut, index)) = self.awaiting_start_cut.front() {
            if start_cut >= end_base {
                break;
            }
            self.awaiting_start_cut.pop_front();
            debug_assert!(start_cut >= self.base_index, "a start cut was passed");
            let Some(tracked) = Self::copy_in(&mut self.copies, self.front_index, index) else {
                continue;
            };
            if tracked.status != CopyStatus::InStartMargin {
                continue;
            }
            let position = offset + nth_set_bit(bases, start_cut - self.base_index);
            if position >= tracked.copy.source_end() {
                tracked.status = CopyStatus::Dropped;
                self.stats.dropped_too_short += 1;
                continue;
            }
            tracked.copy = tracked.copy.with_source_start(position);
            tracked.start_base = start_cut;
            tracked.status = CopyStatus::Open;
            self.stats.opened += 1;
        }
    }

    /// Closes the open copies whose source ends in the block, and drops those that end before
    /// their start cut. A copy that grew since its key was pushed goes back with its new one.
    #[inline(always)]
    fn reach_source_ends(&mut self, offset: u64, end: u64, bases: u64) {
        while let Some((last, index)) = self.awaiting_source_end.peek() {
            if last >= end {
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
                    Self::base_index_at(self.base_index, offset, bases, source_end),
                    self.margin,
                    &self.recent_base_blocks,
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
        recent_base_blocks: &VecDeque<(u64, u64, u64)>,
        stats: &mut TrackerStats,
    ) {
        // The start cut is `margin` bases past a base index, and a base of the source.
        let end_base = source_end_base - margin;
        if end_base <= tracked.start_base {
            tracked.status = CopyStatus::Dropped;
            stats.dropped_too_short += 1;
            return;
        }
        let end_cut = Self::offset_of_base(recent_base_blocks, end_base - 1) + 1;
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
