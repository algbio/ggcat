//! The minimizers of a whole buffer of sequences at once. The SIMD minimizer queue runs on the
//! buffer's bases split into lanes, the gaps between its sequences included, and each run of
//! windows it finds, a super-k-mer to be, is trimmed to the sequences it has windows of, and to
//! the lane: the windows that cross from a sequence to the next, or lie in the gaps, are hashed
//! for nothing and always dropped. Lanes overlap by `k - 1` bases, so that each k-mer is whole in
//! one lane only: a sequence that goes on from a lane to the next is two parts, overlapping by
//! `k - 1` bases, as a sequence too long for a buffer is two sequences. Each part has the runs a
//! queue on it alone finds.

use hashes::simd::minqueue::BatchMinQueueLanes;
use hashes::simd::nthash::CanonicalNtHashLanes;
use hashes::simd::{BaseLanes, HashKernel, PackedLanes, SIMD_LANES};
use io::simd::{SequenceBuffer, SequenceDescription};

/// A run of windows that share a minimizer, of the part of a sequence one lane holds.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SequenceRun {
    /// The lane that found it.
    pub lane: usize,
    /// The sequence, by its index in the buffer's descriptions.
    pub sequence: usize,
    /// The part: `bases` bases of the sequence from its base `offset`. The whole sequence, but
    /// where it goes on from a lane to the next.
    pub offset: usize,
    pub bases: usize,
    /// Its windows, counted from the part's first: `start..end`, of the part's `windows`.
    pub start: usize,
    pub end: usize,
    pub windows: usize,
    /// The minimizer's canonical hash, its unique flag in bit 0.
    pub value: u32,
    /// The minimizer's m-mer, by its index in the part. For a duplicated minimizer, its unique
    /// flag cleared by duplicate checking, whose run does not follow its position: that of the
    /// run's last window, moved into the part if outside it.
    pub position: usize,
    /// Whether the minimizer's forward strand is the canonical one.
    pub is_forward: bool,
}

/// The part of a sequence a lane holds, a k-mer at least, with its windows in the buffer: those
/// from `start`, its first base, to `start + windows`.
#[derive(Copy, Clone)]
struct LanePart {
    /// The sequence, by its index in the buffer's descriptions.
    sequence: usize,
    start: usize,
    /// Its first base in the sequence.
    offset: usize,
    bases: usize,
    windows: usize,
}

impl LanePart {
    /// Past the lane's last part: no window of the buffer is in it.
    const NONE: Self = Self {
        sequence: usize::MAX,
        start: usize::MAX,
        offset: 0,
        bases: 0,
        windows: 0,
    };

    /// The first part a k-mer long at least, in the lane of the bases from `lane_start` to
    /// `lane_end`, of the sequences of `descriptions` from `index` on: those of windows of
    /// `window_bases` bases.
    fn first_from(
        descriptions: &[SequenceDescription],
        mut index: usize,
        (lane_start, lane_end): (usize, usize),
        k: usize,
        window_bases: usize,
    ) -> Self {
        while let Some(sequence) = descriptions.get(index) {
            if sequence.bases_start >= lane_end {
                break;
            }
            let start = sequence.bases_start.max(lane_start);
            let end = (sequence.bases_start + sequence.bases_len).min(lane_end);
            if end >= start + k {
                return Self {
                    sequence: index,
                    start,
                    offset: start - sequence.bases_start,
                    bases: end - start,
                    windows: end - start + 1 - window_bases,
                };
            }
            index += 1;
        }
        Self::NONE
    }

    /// Where its windows end in the buffer.
    fn windows_end(&self) -> usize {
        self.start + self.windows
    }
}

/// Where a lane is in the buffer's sequences.
#[derive(Copy, Clone)]
struct LaneCursor {
    /// The lane's first part whose windows do not all come before its next run: the parts'
    /// windows come one after the other, in the order of the sequences, as the runs do.
    part: LanePart,
    /// The window that starts the lane's next run, counted from the lane's first.
    run_start: usize,
}

/// The SIMD minimizer queue on whole buffers, for super-k-mers of `k`-mers in windows of
/// `window` m-mers of `m` bases.
pub struct BufferMinimizers {
    queue: BatchMinQueueLanes,
    kernel: HashKernel,
    k: usize,
    m: usize,
    window: usize,
}

impl BufferMinimizers {
    /// For `k`-mers, and windows of `window` m-mers, two at least, of `m` bases, a k-mer's or
    /// less: on the vectors of `kernel`, which this CPU has to support.
    pub fn new(k: usize, m: usize, window: usize, kernel: HashKernel) -> Self {
        assert!(
            window + m - 1 <= k,
            "windows of {window} {m}-mers longer than a {k}-mer"
        );
        Self {
            queue: BatchMinQueueLanes::new(window),
            kernel,
            k,
            m,
            window,
        }
    }

    /// Calls `on_run` with every run of windows that share a minimizer of every part of every
    /// sequence of `buffer`, with duplicate checking or not: in order within each lane, the
    /// lanes' runs interleaved. A part's runs are those a queue on it alone finds, as
    /// [`BatchMinQueue`]'s `get_minimizer_splits` splits it with its m-mers' index and strand as
    /// their `X`. A part shorter than a k-mer has none.
    ///
    /// [`BatchMinQueue`]: hashes::rolling::batch_minqueue::BatchMinQueue
    pub fn runs<const DUPLICATES: bool>(
        &mut self,
        buffer: &SequenceBuffer,
        mut on_run: impl FnMut(&SequenceRun),
    ) {
        let descriptions = buffer.descriptions();
        let (Some(first), Some(last)) = (descriptions.first(), descriptions.last()) else {
            return;
        };
        let (k, m, window_bases) = (self.k, self.m, self.window + self.m - 1);
        let start = first.bases_start;
        let count = last.bases_start + last.bases_len - start;
        let lanes = PackedLanes::<SIMD_LANES>::new(buffer.bases(), start, count, k - 1);
        // Lanes shorter than an m-mer have no window either.
        let Ok(hashes) = CanonicalNtHashLanes::new(lanes, m) else {
            return;
        };
        let (offsets, lane_bases) = (lanes.offsets(), lanes.lane_bases());
        let next_part = |index, offset| {
            LanePart::first_from(
                descriptions,
                index,
                (offset, offset + lane_bases),
                k,
                window_bases,
            )
        };
        let mut cursors = offsets.map(|offset| LaneCursor {
            part: next_part(
                descriptions
                    .partition_point(|sequence| sequence.bases_start + sequence.bases_len <= offset),
                offset,
            ),
            run_start: 0,
        });
        self.queue.get_minimizer_splits::<_, DUPLICATES>(
            hashes,
            self.kernel,
            0,
            0,
            |lane, end, (value, extra), is_last| {
                let cursor = &mut cursors[lane];
                // The run's windows in the buffer: the last run ends at the lane's last window,
                // the others where the next starts.
                let offset = offsets[lane];
                let run_start = offset + cursor.run_start;
                let run_end = offset + end + is_last as usize;
                cursor.run_start = end;
                // The lane's runs come in order: the parts whose windows all come before this
                // run are done with.
                while cursor.part.windows_end() <= run_start {
                    cursor.part = next_part(cursor.part.sequence + 1, offset);
                }
                // The run trimmed to each part it has windows of: nearly always all in the
                // cursor's. A unique minimizer is in every window of its run, so in one part
                // only; the run of a duplicated one does not follow its position, and can go on
                // from a sequence to the next.
                let position = offset + (extra >> 1) as usize;
                loop {
                    let part = cursor.part;
                    let start = run_start.max(part.start);
                    let end = run_end.min(part.windows_end());
                    if start >= end {
                        break;
                    }
                    on_run(&SequenceRun {
                        lane,
                        sequence: part.sequence,
                        offset: part.offset,
                        bases: part.bases,
                        start: start - part.start,
                        end: end - part.start,
                        windows: part.windows,
                        value,
                        position: position.clamp(part.start, part.start + part.bases - m)
                            - part.start,
                        is_forward: extra & 1 == 1,
                    });
                    if run_end <= part.windows_end() {
                        break;
                    }
                    // The run goes on past the part, which no later run reaches.
                    cursor.part = next_part(part.sequence + 1, offset);
                }
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashes::rolling::batch_minqueue::BatchMinQueue;
    use io::simd::SequenceParser;
    use std::collections::BTreeMap;
    use utils::dna_sequence::DnaSequencesFileType;

    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Buffers of `records` records of up to `longest` bases, from `seed`, with an N now and
    /// then; random bases, or with `low_entropy` stretches of short periods, for windows to hold
    /// the same m-mer twice. Sequences shorter than `min_seqlen` are left out.
    fn buffers(
        seed: u64,
        records: usize,
        longest: usize,
        low_entropy: bool,
        min_seqlen: usize,
    ) -> Vec<SequenceBuffer> {
        let mut state = seed | 1;
        let mut parser = SequenceParser::new(false, None, min_seqlen);
        parser.reset_for_records(DnaSequencesFileType::FASTA);
        let mut buffers = Vec::new();
        let mut keep = |buffer: SequenceBuffer| {
            buffers.push(buffer);
            SequenceBuffer::new()
        };
        for _ in 0..records {
            let length = (next(&mut state) % longest as u64) as usize + 1;
            let mut bases = Vec::with_capacity(length);
            while bases.len() < length {
                let stretch = (next(&mut state) % 60 + 1) as usize;
                let period = if low_entropy && next(&mut state) % 2 == 0 {
                    (next(&mut state) % 3 + 1) as usize
                } else {
                    stretch
                };
                let motif: Vec<u8> = (0..period)
                    .map(|_| b"ACGT"[(next(&mut state) % 4) as usize])
                    .collect();
                bases.extend((0..stretch).map(|index| motif[index % period]));
                if next(&mut state) % 50 == 0 {
                    bases.push(b'N');
                }
            }
            bases.truncate(length);
            parser.push_record(b">record", &bases, &mut keep);
        }
        parser.finish(&mut keep);
        buffers
    }

    /// A run, without the lane that found it or its part.
    type Run = (usize, usize, u32, usize, bool);

    /// `run` with the position and strand of a duplicated minimizer left out: its run does not
    /// follow them.
    fn comparable<const DUP: bool>(run: Run) -> Run {
        if DUP && run.2 & 1 == 0 {
            (run.0, run.1, run.2, 0, false)
        } else {
            run
        }
    }

    /// The parts of the sequences of `buffer`, by sequence and offset, their bases: each
    /// sequence cut in the lanes of `k`-mers the queue splits the buffer into.
    fn parts(buffer: &SequenceBuffer, k: usize) -> BTreeMap<(usize, usize), usize> {
        let descriptions = buffer.descriptions();
        let start = descriptions[0].bases_start;
        let end = descriptions
            .last()
            .map_or(0, |last| last.bases_start + last.bases_len);
        let lanes = PackedLanes::<SIMD_LANES>::new(buffer.bases(), start, end - start, k - 1);
        let mut parts = BTreeMap::new();
        for (index, sequence) in descriptions.iter().enumerate() {
            for offset in lanes.offsets() {
                let part_start = sequence.bases_start.max(offset);
                let part_end =
                    (sequence.bases_start + sequence.bases_len).min(offset + lanes.lane_bases());
                if part_end >= part_start + k {
                    parts.insert(
                        (index, part_start - sequence.bases_start),
                        part_end - part_start,
                    );
                }
            }
        }
        parts
    }

    /// The canonical hashes and extras of the m-mers of `bases` packed bases from base `first`.
    fn items(buffer: &SequenceBuffer, first: usize, bases: usize, m: usize) -> Vec<(u64, u32)> {
        let lane = PackedLanes::<1>::new(buffer.bases(), first, bases, 0);
        let mut items = Vec::new();
        CanonicalNtHashLanes::new(lane, m).unwrap().for_each(
            HashKernel::Portable,
            |position, hash| {
                let extra = (position as u32) << 1 | (hash.forward & 1);
                items.push((hash.canonical[0] as u64, extra));
            },
        );
        items
    }

    /// The runs a scalar queue finds in each part of the sequences of `buffer` alone, with the
    /// one-lane hash and each m-mer's index and strand as its `X`.
    fn scalar_runs<const DUP: bool>(
        buffer: &SequenceBuffer,
        k: usize,
        m: usize,
        window: usize,
    ) -> BTreeMap<(usize, usize), Vec<Run>> {
        let descriptions = buffer.descriptions();
        let mut runs = BTreeMap::new();
        for (&(sequence, offset), &bases) in &parts(buffer, k) {
            let items = items(
                buffer,
                descriptions[sequence].bases_start + offset,
                bases,
                m,
            );
            let (mut start, mut found) = (0, Vec::new());
            BatchMinQueue::<u32>::new(window).get_minimizer_splits::<_, DUP>(
                items.into_iter(),
                0,
                0,
                |end, (value, extra), is_last| {
                    let end = end + is_last as usize;
                    let run = (
                        start,
                        end,
                        value as u32,
                        (extra >> 1) as usize,
                        extra & 1 == 1,
                    );
                    found.push(comparable::<DUP>(run));
                    start = end;
                },
            );
            runs.insert((sequence, offset), found);
        }
        runs
    }

    /// The runs of each part of the sequences of `buffer` from the whole-buffer queue on
    /// `kernel`, in order.
    fn buffer_runs<const DUP: bool>(
        buffer: &SequenceBuffer,
        k: usize,
        m: usize,
        window: usize,
        kernel: HashKernel,
    ) -> BTreeMap<(usize, usize), Vec<Run>> {
        let parts = parts(buffer, k);
        let mut runs: BTreeMap<_, Vec<Run>> = BTreeMap::new();
        BufferMinimizers::new(k, m, window, kernel).runs::<DUP>(buffer, |run| {
            assert!(run.start < run.end && run.end <= run.windows, "{run:?}");
            assert_eq!(
                parts.get(&(run.sequence, run.offset)),
                Some(&run.bases),
                "{run:?}"
            );
            assert_eq!(run.windows, run.bases + 2 - window - m, "{run:?}");
            let part = runs.entry((run.sequence, run.offset)).or_default();
            let found = (run.start, run.end, run.value, run.position, run.is_forward);
            part.push(comparable::<DUP>(found));
        });
        runs
    }

    /// Windows of a k-mer's m-mers, as the querier's, and of a k-mer's less one, as the
    /// assembler's: `(k, m, window)`.
    const SHAPES: [(usize, usize, usize); 4] =
        [(31, 15, 17), (31, 15, 16), (17, 15, 2), (32, 11, 22)];

    #[test]
    fn each_part_has_the_runs_of_a_queue_on_it_alone() {
        // Random bases and long m-mers: windows hardly ever hold one twice, so that where the
        // batches start does not choose between equal minima.
        for (seed, records, longest) in
            [(1, 1, 30_000), (2, 60, 3000), (3, 400, 300), (4, 2000, 40)]
        {
            for (k, m, window) in SHAPES {
                for buffer in buffers(seed, records, longest, false, k) {
                    let expected = scalar_runs::<true>(&buffer, k, m, window);
                    for kernel in HashKernel::supported() {
                        assert_eq!(
                            buffer_runs::<true>(&buffer, k, m, window, kernel),
                            expected,
                            "{kernel:?}, seed {seed}, k {k}, m {m}, window {window}"
                        );
                    }
                    let expected = scalar_runs::<false>(&buffer, k, m, window);
                    let found = buffer_runs::<false>(&buffer, k, m, window, HashKernel::detect());
                    assert_eq!(found, expected, "no duplicates, seed {seed}, k {k}, m {m}");
                }
            }
        }
    }

    #[test]
    fn every_kmer_is_in_one_part_and_every_window_in_one_run_with_its_minimum() {
        for (seed, records, longest) in [(5, 80, 2000), (6, 1000, 60)] {
            for (k, m, window) in [(7, 3, 5), (8, 3, 5), (25, 7, 19), (31, 12, 20)] {
                // Sequences shorter than a k-mer too: they have no part.
                for buffer in buffers(seed, records, longest, true, m) {
                    let descriptions = buffer.descriptions();
                    let parts = parts(&buffer, k);
                    // A sequence's parts overlap by k - 1 bases: each k-mer is in one.
                    for (index, sequence) in descriptions.iter().enumerate() {
                        let mut next_kmer = 0;
                        for (_, (&(_, offset), &bases)) in
                            parts.range((index, 0)..(index + 1, 0)).enumerate()
                        {
                            assert_eq!(offset, next_kmer, "k {k}, sequence {index}: {parts:?}");
                            next_kmer = offset + bases + 1 - k;
                        }
                        assert_eq!(
                            next_kmer,
                            (sequence.bases_len + 1).saturating_sub(k),
                            "k {k}, sequence {index}"
                        );
                    }
                    for kernel in HashKernel::supported() {
                        let runs = buffer_runs::<true>(&buffer, k, m, window, kernel);
                        for (&(index, offset), &bases) in &parts {
                            let first = descriptions[index].bases_start + offset;
                            let canonical: Vec<u32> = items(&buffer, first, bases, m)
                                .iter()
                                .map(|&(value, _)| value as u32)
                                .collect();
                            let windows = bases + 2 - window - m;
                            let mut next_window = 0;
                            for &(start, end, value, position, _) in
                                runs.get(&(index, offset)).map_or(&[][..], |runs| runs)
                            {
                                assert_eq!(
                                    start, next_window,
                                    "{kernel:?}, k {k}, part {index} {offset}"
                                );
                                for first in start..end {
                                    let keys: Vec<u32> = canonical[first..first + window]
                                        .iter()
                                        .map(|value| value & !1)
                                        .collect();
                                    let minimum = *keys.iter().min().unwrap();
                                    assert_eq!(
                                        value & !1,
                                        minimum,
                                        "{kernel:?}, k {k}, window {first}"
                                    );
                                    // A unique minimizer is held once by every window of its run,
                                    // at its position; the run of a duplicated one does not split
                                    // where its position moves.
                                    if value & 1 == 1 {
                                        let copies =
                                            keys.iter().filter(|&&key| key == minimum).count();
                                        assert_eq!(copies, 1, "{kernel:?}, k {k}, window {first}");
                                        assert!((first..first + window).contains(&position));
                                    }
                                }
                                next_window = end;
                            }
                            assert_eq!(
                                next_window, windows,
                                "{kernel:?}, k {k}, part {index} {offset}"
                            );
                        }
                    }
                }
            }
        }
    }
}
