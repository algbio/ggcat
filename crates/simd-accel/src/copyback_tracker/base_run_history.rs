//! Where the bases of the last LZ window of a stream are, for [`super::CopybackTracker`]: the
//! bases of each FASTA sequence line make a run, from the line's first byte to its line end,
//! and runs of equal length at equal spacing are kept as one group. So a record on one line
//! costs a group, and a wrapped record about two, one for its full lines and one for its last.
//!
//! The parser tells the history where each sequence line starts and ends, nothing per block:
//! the run of the line it is in, the open run, is the last group, with no length until its
//! line end, and every lookup in it is linear from its start. What lies between runs holds no
//! base (line ends, headers, comments, the bytes between the files of a stream), so base
//! indices go on from one group to the next.
//!
//! The runs of a skipped range, which no parser reads, are those of its source shifted by the
//! copy's offset: [`BaseRunHistory::replay`] writes them, as a parse would have, so that a copy
//! of a copy finds them too. With header marks, the parser also tells the history where each
//! header line is, and a replay shifts those too, so that the parser can count the records it
//! skips and name the sequence it resumes in.

use std::collections::VecDeque;

/// `run_count` runs of `bases_per_run` bases, `run_spacing` bytes apart: run `j` holds the
/// bases at `[first_run_offset + j · run_spacing, + bases_per_run)`, followed by its line end
/// or its file's end, and its first base has base index `first_run_base + j · bases_per_run`.
/// `run_spacing` is 0 while `run_count` is 1. The open run is a group of one whose
/// `bases_per_run` means nothing: its bases run from `first_run_offset` to the parse front.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct RunGroup {
    first_run_offset: u64,
    first_run_base: u64,
    bases_per_run: u64,
    run_spacing: u64,
    run_count: u64,
}

impl RunGroup {
    /// The base index past the last base of the group, closed.
    fn end_base(&self) -> u64 {
        self.first_run_base + self.run_count * self.bases_per_run
    }

    /// Where the last run of the group starts.
    fn last_run_offset(&self) -> u64 {
        self.first_run_offset + (self.run_count - 1) * self.run_spacing
    }

    /// The run of the closed group that holds stream offset `offset`, or the last one before
    /// it, `offset` not before the group.
    fn run_at(&self, offset: u64) -> u64 {
        if self.run_count == 1 {
            0
        } else {
            ((offset - self.first_run_offset) / self.run_spacing).min(self.run_count - 1)
        }
    }
}

/// A piece of the source of a replay, see [`BaseRunHistory::replay`]: with `run_count` 1, a
/// run or a part of one, of `len` bases from stream offset `offset`, whose first base has base
/// index `base`, `ends_open` when its line end lies at or past the end of the source, or when
/// it is in the open run; with `run_count` 2 or more, whole runs of `len` bases, `run_spacing`
/// bytes apart, every line end in the source.
#[derive(Copy, Clone, Debug)]
struct ReplayPiece {
    offset: u64,
    base: u64,
    len: u64,
    run_count: u64,
    run_spacing: u64,
    ends_open: bool,
}

/// The header lines a replay passed, for the parser: how many, and the last one.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReplayedHeaderMarks {
    pub(crate) count: u64,
    pub(crate) last: Option<HeaderMark>,
}

/// A header line: where its `>` is, and its line end.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeaderMark {
    pub(crate) start_offset: u64,
    pub(crate) end_offset: u64,
}

/// The runs of bases of a stream, from the oldest a copy can still need, see the module's
/// documentation.
pub(crate) struct BaseRunHistory {
    /// In stream order, not overlapping, each one's `first_run_base` the end base of the one
    /// before it.
    groups: VecDeque<RunGroup>,
    /// Whether the last group is the open run.
    last_is_open: bool,
    /// The header lines of the window, in stream order, with header marks only.
    header_marks: VecDeque<HeaderMark>,
    /// The pieces of the source of a replay, kept so that a replay allocates nothing once they
    /// have grown.
    pieces: Vec<ReplayPiece>,
    /// The groups [`Self::forget_before`] dropped, so that the tests can see it happen.
    #[cfg(test)]
    forgotten: u64,
}

impl BaseRunHistory {
    pub(super) fn new() -> Self {
        Self {
            groups: VecDeque::new(),
            last_is_open: false,
            header_marks: VecDeque::new(),
            pieces: Vec::new(),
            #[cfg(test)]
            forgotten: 0,
        }
    }

    /// Forgets every run, for a new stream.
    pub(super) fn clear(&mut self) {
        self.groups.clear();
        self.header_marks.clear();
        self.last_is_open = false;
    }

    /// Whether the parser is in a sequence line, whose run is open.
    pub(super) fn has_open_run(&self) -> bool {
        self.last_is_open
    }

    fn is_open(&self, index: usize) -> bool {
        self.last_is_open && index + 1 == self.groups.len()
    }

    /// A sequence line starts at stream offset `offset`: its run is the open run.
    #[inline(always)]
    pub(super) fn open_run(&mut self, offset: u64) {
        debug_assert!(!self.last_is_open, "a run opened in an open run");
        // Empty only before the stream's first run, as the last group is never forgotten.
        let base = self.groups.back().map_or(0, RunGroup::end_base);
        self.groups.push_back(RunGroup {
            first_run_offset: offset,
            first_run_base: base,
            bases_per_run: 0,
            run_spacing: 0,
            run_count: 1,
        });
        self.last_is_open = true;
    }

    /// The open run ends at stream offset `end`, its line end or its file's end. It is closed
    /// in place, so that the history never empties, even when forgetting has left it alone,
    /// then merged into the group before it if it continues that; but an empty line leaves
    /// nothing, and nothing before it can have been forgotten, as it opens and closes in one
    /// block.
    #[inline(always)]
    pub(super) fn close_run(&mut self, end: u64) {
        debug_assert!(self.last_is_open, "no open run to close");
        self.last_is_open = false;
        let run = self.groups.back_mut().unwrap();
        if end == run.first_run_offset {
            self.groups.pop_back();
            return;
        }
        run.bases_per_run = end - run.first_run_offset;
        self.merge_last_run();
    }

    /// Merges the last group, one closed run, into the group before it, if it has that group's
    /// length and spacing: the first run merged into a group of one sets the spacing.
    #[inline(always)]
    fn merge_last_run(&mut self) {
        let len = self.groups.len();
        if len < 2 {
            return;
        }
        let (previous, run) = (&self.groups[len - 2], &self.groups[len - 1]);
        let spacing = run.first_run_offset - previous.last_run_offset();
        if previous.bases_per_run == run.bases_per_run
            && (previous.run_count == 1 || previous.run_spacing == spacing)
        {
            self.groups.pop_back();
            let previous = &mut self.groups[len - 2];
            previous.run_spacing = spacing;
            previous.run_count += 1;
        }
    }

    /// Appends the closed group `group`, the whole runs of a replay: merged into the last
    /// group only whole, when that makes one group of one run length and one spacing.
    fn push_group(&mut self, group: RunGroup) {
        debug_assert!(!self.last_is_open, "a group pushed after an open run");
        debug_assert_eq!(
            group.first_run_base,
            self.groups.back().map_or(0, RunGroup::end_base),
            "a group pushed with a gap in the base indices"
        );
        if group.run_count == 1 {
            self.groups.push_back(group);
            return self.merge_last_run();
        }
        if let Some(last) = self.groups.back_mut()
            && last.bases_per_run == group.bases_per_run
            && group.first_run_offset - last.last_run_offset() == group.run_spacing
            && (last.run_count == 1 || last.run_spacing == group.run_spacing)
        {
            last.run_spacing = group.run_spacing;
            last.run_count += group.run_count;
        } else {
            self.groups.push_back(group);
        }
    }

    /// A header line from its `>` at stream offset `start` to its line end at `end`.
    pub(super) fn add_header_mark(&mut self, start: u64, end: u64) {
        self.header_marks.push_back(HeaderMark {
            start_offset: start,
            end_offset: end,
        });
    }

    /// The index of the last group starting at or before stream offset `offset`, if any:
    /// nearly every lookup is near the parse front, in one of the last two groups.
    #[inline(always)]
    fn find(&self, offset: u64) -> Option<usize> {
        let len = self.groups.len();
        for index in (len.saturating_sub(2)..len).rev() {
            if self.groups[index].first_run_offset <= offset {
                return Some(index);
            }
        }
        (self.groups)
            .partition_point(|group| group.first_run_offset <= offset)
            .checked_sub(1)
    }

    /// As [`Self::find`], by base index.
    #[inline(always)]
    fn find_base(&self, base: u64) -> Option<usize> {
        let len = self.groups.len();
        for index in (len.saturating_sub(2)..len).rev() {
            if self.groups[index].first_run_base <= base {
                return Some(index);
            }
        }
        (self.groups)
            .partition_point(|group| group.first_run_base <= base)
            .checked_sub(1)
    }

    /// The base index of stream offset `offset`: how many bases the stream has before it. For
    /// an offset in the open run, the parser must be past it.
    #[inline(always)]
    pub(super) fn base_index_at(&self, offset: u64) -> u64 {
        let Some(index) = self.find(offset) else {
            // Before every group kept: only while nothing was forgotten.
            debug_assert!(
                (self.groups.front()).is_none_or(|group| group.first_run_base == 0),
                "a lookup at {offset}, before the groups kept"
            );
            return 0;
        };
        let group = &self.groups[index];
        if self.is_open(index) {
            return group.first_run_base + (offset - group.first_run_offset);
        }
        // Past its last run, with no division, as a block ending between lines is.
        if offset >= group.last_run_offset() + group.bases_per_run {
            return group.end_base();
        }
        let run = group.run_at(offset);
        let in_run = offset - group.first_run_offset - run * group.run_spacing;
        group.first_run_base + run * group.bases_per_run + in_run.min(group.bases_per_run)
    }

    /// The stream offset of the base of base index `base`, which the window holds.
    pub(super) fn offset_of_base(&self, base: u64) -> u64 {
        let index = (self.find_base(base)).expect("a base of the window");
        let group = &self.groups[index];
        let in_group = base - group.first_run_base;
        if self.is_open(index) {
            return group.first_run_offset + in_group;
        }
        debug_assert!(
            base < group.end_base(),
            "base {base} is past the bases seen"
        );
        group.first_run_offset
            + (in_group / group.bases_per_run) * group.run_spacing
            + in_group % group.bases_per_run
    }

    /// The start of the first run after the one holding stream offset `offset`, a base of a
    /// closed run: the next sequence line, past any header, comment or empty line between.
    pub(super) fn next_run_start_after(&self, offset: u64) -> Option<u64> {
        let index = self.find(offset).expect("a base of the window");
        debug_assert!(!self.is_open(index), "a run after the open one");
        let group = &self.groups[index];
        let run = group.run_at(offset);
        if run + 1 < group.run_count {
            Some(group.first_run_offset + (run + 1) * group.run_spacing)
        } else {
            self.groups.get(index + 1).map(|next| next.first_run_offset)
        }
    }

    /// Writes the runs of the range `[from, to)` that the parser skips, a copy of the bytes
    /// `copy_offset` before it: those of its source, shifted by `copy_offset` bytes and
    /// `base_shift` bases, as a parse of the range would have written them. The source is
    /// before the range, as a copy's ends before its destination starts, and the history holds
    /// it, the open run included: a source in the line the parser is in, as a repeat inside one
    /// line, appends nothing, as the open run already answers for the range.
    ///
    /// The pieces of the source are all collected before any is appended, as an append can
    /// close the open run, which a later piece can be read from. The first piece goes on with
    /// the open run whenever there is one at `from`, whatever the source holds: a skip starts
    /// in a sequence line, or at a line start where the source has one too. The last is left
    /// open when its line end is at or past the range's end; when it is right there, the next
    /// range starts on it, a byte with no base, and that replay closes the run first.
    ///
    /// The header lines whose line end is in the range are shifted too, and returned with the
    /// last one with a byte in the range or its line end there: a header whose line end is in a
    /// later range comes again then, counted once.
    pub(super) fn replay(
        &mut self,
        from: u64,
        to: u64,
        copy_offset: u64,
        base_shift: u64,
    ) -> ReplayedHeaderMarks {
        debug_assert!(from < to && copy_offset > 0);
        let (source_from, source_to) = (from - copy_offset, to - copy_offset);
        let mut pieces = std::mem::take(&mut self.pieces);
        pieces.clear();
        self.collect_pieces(source_from, source_to, &mut pieces);
        if self.last_is_open
            && pieces
                .first()
                .is_none_or(|piece| piece.offset > source_from)
        {
            self.close_run(from);
        }
        for (index, piece) in pieces.iter().enumerate() {
            let (offset, base) = (piece.offset + copy_offset, piece.base + base_shift);
            let continues = index == 0 && self.last_is_open;
            debug_assert!(!continues || base == self.base_index_at(offset));
            if piece.run_count >= 2 {
                let mut group = RunGroup {
                    first_run_offset: offset,
                    first_run_base: base,
                    bases_per_run: piece.len,
                    run_spacing: piece.run_spacing,
                    run_count: piece.run_count,
                };
                if continues {
                    // The line goes on into the first of the runs, where the source starts one,
                    // as only a skip with no margin can.
                    self.close_run(offset + piece.len);
                    group.first_run_offset += group.run_spacing;
                    group.first_run_base += group.bases_per_run;
                    group.run_count -= 1;
                    if group.run_count == 1 {
                        group.run_spacing = 0;
                    }
                }
                self.push_group(group);
                continue;
            }
            if !continues {
                self.open_run(offset);
                debug_assert_eq!(base, self.groups.back().unwrap().first_run_base);
            }
            if !piece.ends_open {
                self.close_run(offset + piece.len);
            }
        }
        self.pieces = pieces;
        self.replay_header_marks(source_from, source_to, copy_offset)
    }

    /// Shifts by `copy_offset` the header lines whose line end lies in `[source_from,
    /// source_to)`, and returns how many, and the last header line with a byte or its line end
    /// in the range, shifted.
    fn replay_header_marks(
        &mut self,
        source_from: u64,
        source_to: u64,
        copy_offset: u64,
    ) -> ReplayedHeaderMarks {
        // Without header marks, the parser tells no header line, and none is kept.
        if self.header_marks.is_empty() {
            return ReplayedHeaderMarks::default();
        }
        let mut replayed = ReplayedHeaderMarks::default();
        let first = (self.header_marks).partition_point(|mark| mark.end_offset < source_from);
        let shift = |mark: HeaderMark| HeaderMark {
            start_offset: mark.start_offset + copy_offset,
            end_offset: mark.end_offset + copy_offset,
        };
        let mut index = first;
        let mut ended = 0;
        while let Some(&mark) = self.header_marks.get(index)
            && mark.start_offset < source_to
        {
            if mark.end_offset < source_to {
                ended += 1;
            }
            replayed.last = Some(shift(mark));
            index += 1;
        }
        // The marks ending in the range, pushed after the reads: those of the range are all past
        // the source.
        for index in first..first + ended {
            let mark = shift(self.header_marks[index]);
            self.header_marks.push_back(mark);
        }
        replayed.count = ended as u64;
        replayed
    }

    /// The pieces of the runs in `[source_from, source_to)`, see [`ReplayPiece`]: in each
    /// group, the run holding `source_from` and the one whose line end is at or past
    /// `source_to` are pieces of their own, cut to the range, and the whole runs between are
    /// one piece.
    fn collect_pieces(&self, source_from: u64, source_to: u64, pieces: &mut Vec<ReplayPiece>) {
        let first = self.find(source_from).unwrap_or(0);
        for index in first..self.groups.len() {
            let group = self.groups[index];
            if group.first_run_offset >= source_to {
                break;
            }
            if self.is_open(index) {
                let start = group.first_run_offset.max(source_from);
                pieces.push(ReplayPiece {
                    offset: start,
                    base: group.first_run_base + (start - group.first_run_offset),
                    len: source_to - start,
                    run_count: 1,
                    run_spacing: 0,
                    ends_open: true,
                });
                break;
            }
            let run_start = |run: u64| group.first_run_offset + run * group.run_spacing;
            let run_end = |run: u64| run_start(run) + group.bases_per_run;
            let single = |run: u64| {
                let start = run_start(run).max(source_from);
                ReplayPiece {
                    offset: start,
                    base: group.first_run_base
                        + run * group.bases_per_run
                        + (start - run_start(run)),
                    len: run_end(run).min(source_to) - start,
                    run_count: 1,
                    run_spacing: 0,
                    ends_open: run_end(run) >= source_to,
                }
            };
            // The first run whose line end is past `source_from`, the last that starts before
            // `source_to`.
            let mut first_run = match source_from.checked_sub(group.first_run_offset) {
                Some(_) => group.run_at(source_from),
                None => 0,
            };
            if run_end(first_run) <= source_from {
                first_run += 1;
            }
            let last_run = group.run_at(source_to - 1);
            if first_run > last_run {
                continue;
            }
            let mut next_run = first_run;
            if run_start(first_run) < source_from || run_end(first_run) >= source_to {
                pieces.push(single(first_run));
                next_run += 1;
            }
            if next_run > last_run {
                continue;
            }
            let last_open = run_end(last_run) >= source_to;
            let whole_end = if last_open { last_run } else { last_run + 1 };
            if whole_end > next_run {
                let run_count = whole_end - next_run;
                pieces.push(ReplayPiece {
                    offset: run_start(next_run),
                    base: group.first_run_base + next_run * group.bases_per_run,
                    len: group.bases_per_run,
                    run_count,
                    run_spacing: if run_count >= 2 { group.run_spacing } else { 0 },
                    ends_open: false,
                });
            }
            if last_open {
                pieces.push(single(last_run));
            }
        }
    }

    /// Forgets the oldest groups no lookup can reach anymore: a group goes when the group after
    /// it starts at or before stream offset `offset_bound`, and its bases are all before base
    /// index `base_bound`. Testing where the next group starts, not where this one ends, keeps
    /// a lookup in the gap after a group valid. The last group is never forgotten. A header
    /// line goes when it ends before `offset_bound`.
    pub(super) fn forget_before(&mut self, offset_bound: u64, base_bound: u64) {
        while self.groups.len() >= 2
            && self.groups[1].first_run_offset <= offset_bound
            && self.groups[0].end_base() <= base_bound
        {
            self.groups.pop_front();
            #[cfg(test)]
            {
                self.forgotten += 1;
            }
        }
        while (self.header_marks.front()).is_some_and(|mark| mark.end_offset < offset_bound) {
            self.header_marks.pop_front();
        }
    }

    /// Each group's first offset and end base, `None` for the open run, oldest first.
    #[cfg(test)]
    pub(super) fn groups(&self) -> Vec<(u64, Option<u64>)> {
        (self.groups.iter().enumerate())
            .map(|(index, group)| {
                let end = (!self.is_open(index)).then(|| group.end_base());
                (group.first_run_offset, end)
            })
            .collect()
    }

    /// How many groups [`Self::forget_before`] dropped.
    #[cfg(test)]
    pub(super) fn forgotten(&self) -> u64 {
        self.forgotten
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copyback_tracker::tests::{
        Byte, classify, fasta, line_ending, plant, wrapped_records,
    };

    /// The lines of a FASTA file, cut as `classify` cuts them: each one's start, its end (its
    /// line end, or the file's end), and whether it is a sequence line, empty ones included.
    fn lines(data: &[u8]) -> Vec<(u64, u64, bool)> {
        let mut lines = Vec::new();
        let mut start = 0;
        while start < data.len() {
            let end = (data[start..].iter())
                .position(|&byte| matches!(byte, b'\r' | b'\n'))
                .map_or(data.len(), |at| start + at);
            let sequence = !matches!(data[start], b'>' | b';');
            lines.push((start as u64, end as u64, sequence));
            start = match data[end..] {
                [b'\r', b'\n', ..] => end + 2,
                _ => end + 1,
            };
        }
        lines
    }

    /// Records of `count` bases at most, wrapped at `width` bases a line (0 for one line), with
    /// one line ending throughout: some longer than many lines, some of none.
    fn wrapped(seed: u64, records: usize, width: u64, ending: &[u8]) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let mut data = Vec::new();
        for record in 0..records {
            data.extend_from_slice(format!(">record_{record}").as_bytes());
            data.extend_from_slice(ending);
            let len = match next(6) {
                0 => 0,
                1 => 20_000 + next(5_000),
                _ => next(3_000),
            };
            let line = if width == 0 { len.max(1) } else { width };
            let bases: Vec<u8> = (0..len).map(|_| b"ACGTN"[next(5) as usize]).collect();
            for line in bases.chunks(line as usize) {
                data.extend_from_slice(line);
                data.extend_from_slice(ending);
            }
        }
        data
    }

    /// The classifier's counts: the bases before each byte of `bytes` and past the last, and
    /// where each base is.
    fn counts(bytes: &[Byte]) -> (Vec<u64>, Vec<u64>) {
        let mut before = Vec::with_capacity(bytes.len() + 1);
        let mut positions = Vec::new();
        for (offset, &byte) in bytes.iter().enumerate() {
            before.push(positions.len() as u64);
            if byte == Byte::Base {
                positions.push(offset as u64);
            }
        }
        before.push(positions.len() as u64);
        (before, positions)
    }

    /// Builds the history of `data`, at stream offset 0, as the parser feeds it, and checks every
    /// lookup against the classifier's count: in each run while it is open, then everywhere.
    fn check_lookups(data: &[u8], what: &str) -> BaseRunHistory {
        let (before, positions) = counts(&classify(data));
        let mut history = BaseRunHistory::new();
        for (start, end, sequence) in lines(data) {
            if !sequence {
                continue;
            }
            history.open_run(start);
            for offset in start..=end {
                let at = format!("{what}: offset {offset} in the open run");
                assert_eq!(
                    history.base_index_at(offset),
                    before[offset as usize],
                    "{at}"
                );
            }
            for base in before[start as usize]..before[end as usize] {
                assert_eq!(
                    history.offset_of_base(base),
                    positions[base as usize],
                    "{what}: base {base} in the open run"
                );
            }
            history.close_run(end);
        }
        for (offset, &expected) in before.iter().enumerate() {
            let at = format!("{what}: offset {offset}");
            assert_eq!(history.base_index_at(offset as u64), expected, "{at}");
        }
        for (base, &expected) in positions.iter().enumerate() {
            let at = format!("{what}: base {base}");
            assert_eq!(history.offset_of_base(base as u64), expected, "{at}");
        }
        history
    }

    /// Every offset and every base is converted as the classifier counts: on random FASTA,
    /// with lines of all lengths, every line end, empty lines, headers and comments, and on
    /// wrapped records of every width, whose full lines make one group.
    #[test]
    fn lookups_match_a_per_byte_classifier() {
        for seed in 0..30 {
            check_lookups(&fasta(seed, 60_000), &format!("fasta, seed {seed}"));
            let width = [0, 1, 2, 60, 61, 80, 199, 200][seed as usize % 8];
            let ending = line_ending(seed % 2, 0);
            let records = 30;
            let data = wrapped(seed, records, width, ending);
            let what = format!("wrapped at {width}, seed {seed}");
            let history = check_lookups(&data, &what);
            // A group for the full lines of a record, and one for its last line.
            assert!(
                history.groups().len() <= 2 * records,
                "{what}: {} groups",
                history.groups().len()
            );
        }
        // A lone `\r` and `\n\r`, two line ends, among them.
        for seed in 0..10 {
            let data = wrapped(seed, 20, 70, line_ending(2, seed));
            check_lookups(&data, &format!("wrapped at 70, ending {seed}"));
        }
    }

    /// The history of `data`, at stream offset 0, as the parser leaves it at `to`: the run of
    /// the line it is in left open, its line end not parsed yet.
    fn history_up_to(data: &[u8], to: u64) -> BaseRunHistory {
        let mut history = BaseRunHistory::new();
        for (start, end, sequence) in lines(data) {
            if start >= to {
                break;
            }
            if sequence {
                history.open_run(start);
                if end < to {
                    history.close_run(end);
                }
            }
        }
        history
    }

    /// A range replayed from its source, in one piece or two as a chunk's end can cut it,
    /// answers every lookup, in it and in its source, as the parsed range does: wherever the
    /// skip contract holds at its start, as the byte before it is a base at both ends.
    #[test]
    fn a_replayed_range_answers_as_the_parsed_one() {
        let mut state = 0x1234_5678_9abc_def1u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound.max(1)
        };
        let (mut replayed, mut cut_on_line_ends, mut whole_runs) = (0, 0, 0);
        for seed in 0..24 {
            let data = match seed % 3 {
                0 => fasta(seed, 40_000),
                1 => wrapped_records(seed, 40_000, 60),
                _ => wrapped_records(seed, 40_000, 1 + seed as usize),
            };
            let (stream, copies) = plant(seed, data, 400, u64::MAX);
            let data = stream.files.into_iter().next().unwrap().1;
            let bytes = classify(&data);
            let (before, positions) = counts(&bytes);
            for copy in &copies {
                let (destination, offset) = (copy.destination, copy.offset as u64);
                let end = destination + copy.len as u64;
                let starts: Vec<u64> = (destination.max(offset + 1)..end)
                    .filter(|&from| {
                        bytes[from as usize - 1] == Byte::Base
                            && bytes[(from - 1 - offset) as usize] == Byte::Base
                    })
                    .collect();
                if starts.is_empty() {
                    continue;
                }
                let from = starts[next(starts.len() as u64) as usize];
                let to = from + 1 + next(end - from);
                // Cut at a random byte, or on a line end of the source right after a base.
                let line_end = (from - offset + 1..to - offset).find(|&at| {
                    bytes[at as usize] == Byte::LineEnd && bytes[at as usize - 1] == Byte::Base
                });
                let cut = match next(3) {
                    0 => None,
                    1 => line_end.map(|at| at + offset),
                    _ => (to - from >= 2).then(|| from + 1 + next(to - from - 1)),
                };
                cut_on_line_ends += (cut.is_some() && cut == line_end.map(|at| at + offset)) as u64;
                let mut history = history_up_to(&data, from);
                let shift = history.base_index_at(from) - history.base_index_at(from - offset);
                let mut piece_start = from;
                for piece_end in cut.into_iter().chain([to]) {
                    history.replay(piece_start, piece_end, offset, shift);
                    whole_runs += (history.pieces.iter())
                        .filter(|piece| piece.run_count >= 2)
                        .count() as u64;
                    piece_start = piece_end;
                }
                replayed += 1;
                let at = format!("seed {seed}, {copy:?}, [{from}, {to}) cut at {cut:?}");
                for position in from - offset..=to {
                    assert_eq!(
                        history.base_index_at(position),
                        before[position as usize],
                        "{at}: offset {position}"
                    );
                }
                for base in before[(from - offset) as usize]..before[to as usize] {
                    assert_eq!(
                        history.offset_of_base(base),
                        positions[base as usize],
                        "{at}: base {base}"
                    );
                }
            }
        }
        assert!(
            replayed > 2000 && cut_on_line_ends > 100 && whole_runs > 500,
            "{replayed} replayed, {cut_on_line_ends} cut on a line end, {whole_runs} whole runs"
        );
    }
}
