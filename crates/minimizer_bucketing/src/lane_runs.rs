//! Turning minimizer runs of a lane back into super-k-mers of a record.

use config::READ_FLAG_INCL_BEGIN;
use io::compressed_read::CompressedRead;
use simd_accel::hashing::SIMD_LANES;

use crate::simd_batch::{LaneFragment, SequencesLaneBatch};

/// Marks the minimizer windows that lie entirely inside one fragment.
///
/// A window covers `span` bases, so the windows of a fragment `[start, end)`
/// are `start ..= end - span`; every other window would straddle a sequence
/// boundary and must not produce a super-k-mer.
pub fn build_valid_lanes(output: &mut Vec<u8>, batch: &SequencesLaneBatch, span: usize) {
    debug_assert!(batch.bases_per_lane >= span);
    let windows = batch.bases_per_lane + 1 - span;
    output.clear();
    output.resize(windows, 0);
    for lane in 0..SIMD_LANES {
        let bit = 1u8 << lane;
        for pair in batch.lanes[lane].windows(2) {
            let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
            if end - start < span {
                continue;
            }
            set_lane_bit(&mut output[start..=end - span], bit);
        }
    }
}

/// Sets `bit` in every byte of `masks`, eight bytes at a time.
///
/// A fragment covers hundreds of windows, so doing this a byte at a time is
/// most of what the mask costs.
#[inline]
fn set_lane_bit(masks: &mut [u8], bit: u8) {
    let repeated = (bit as u64) * 0x0101_0101_0101_0101;
    let (head, words, tail) = unsafe { masks.align_to_mut::<u64>() };
    for mask in head {
        *mask |= bit;
    }
    for word in words {
        *word |= repeated;
    }
    for mask in tail {
        *mask |= bit;
    }
}

/// A run of windows, resolved to the piece of sequence it stands for.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ResolvedRun {
    pub record_idx: u32,
    pub fragment_start: usize,
    pub fragment_end: usize,
    /// Offset of the fragment inside its record.
    pub source_start: u64,
    /// First base of the super-k-mer inside the lane.
    pub start: usize,
    /// One past its last base.
    pub end: usize,
    /// The super-k-mer starts at the beginning of the sequence, so its first
    /// k-mer is a real k-mer of the input rather than added context.
    pub include_first: bool,
    /// Likewise for its last k-mer.
    pub include_last: bool,
}

impl ResolvedRun {
    /// Offset of the super-k-mer's first k-mer inside its record.
    #[inline(always)]
    pub fn source_index(&self, position: usize) -> usize {
        self.source_start as usize + (position - self.fragment_start)
    }
}

/// Walks the fragments of one lane alongside the runs reported for it.
pub struct LaneRunResolver<'a> {
    fragments: &'a [LaneFragment],
    cursor: usize,
}

impl<'a> LaneRunResolver<'a> {
    pub fn new(fragments: &'a [LaneFragment]) -> Self {
        Self {
            fragments,
            cursor: 0,
        }
    }

    /// Resolves one run, given as the half-open window range `[run_start, run_end)`.
    ///
    /// `front_extra` asks for the base before the run, which the assembler
    /// needs because its windows are one base shorter than a k-mer. Both ends
    /// are clamped to the fragment, so the extra bases can never reach across a
    /// sequence boundary.
    #[inline(always)]
    pub fn resolve(
        &mut self,
        run_start: usize,
        run_end: usize,
        finished: bool,
        k: usize,
        front_extra: bool,
    ) -> ResolvedRun {
        // Runs of a lane arrive in order, so the cursor only moves forward.
        while (self.fragments[self.cursor + 1].lane_start as usize) <= run_start {
            self.cursor += 1;
        }
        let fragment = self.fragments[self.cursor];
        let start = fragment.lane_start as usize;
        let end = self.fragments[self.cursor + 1].lane_start as usize;
        debug_assert!(!fragment.is_sentinel());
        debug_assert!(start <= run_start && run_start < run_end);
        ResolvedRun {
            record_idx: fragment.record_idx,
            fragment_start: start,
            fragment_end: end,
            source_start: fragment.source_start,
            start: if front_extra && run_start > start {
                run_start - 1
            } else {
                run_start
            },
            end: (run_end + k - 1).min(end),
            include_first: run_start == start,
            include_last: finished,
        }
    }
}

/// Per-thread buffers reused across batches.
#[derive(Default)]
pub struct SimdScratch {
    pub valid_lanes: Vec<u8>,
    lane_bytes: Vec<u8>,
    stride: usize,
    bases_per_lane: usize,
}

impl SimdScratch {
    /// Copies each lane out of the time-major words into its own packed run.
    ///
    /// A lane's word of sixteen bases has exactly the layout of four packed
    /// read bytes, so a super-k-mer is then a plain sub-slice.
    pub fn destride(&mut self, batch: &SequencesLaneBatch) {
        let chunks = batch.bases_per_lane.div_ceil(16);
        self.stride = chunks * 4;
        self.bases_per_lane = batch.bases_per_lane;
        if self.lane_bytes.len() != SIMD_LANES * self.stride {
            self.lane_bytes.resize(SIMD_LANES * self.stride, 0);
        }
        for chunk in 0..chunks {
            let words = &batch.words[chunk * SIMD_LANES..chunk * SIMD_LANES + SIMD_LANES];
            for (lane, word) in words.iter().enumerate() {
                let at = lane * self.stride + chunk * 4;
                self.lane_bytes[at..at + 4].copy_from_slice(&word.to_le_bytes());
            }
        }
    }

    #[inline(always)]
    pub fn lane_read(&self, lane: usize) -> CompressedRead<'_> {
        CompressedRead::new_from_compressed(
            &self.lane_bytes[lane * self.stride..(lane + 1) * self.stride],
            self.bases_per_lane,
        )
    }
}

/// The begin and end flags of a super-k-mer, swapped when it is stored
/// reverse-complemented.
#[inline(always)]
pub fn super_kmer_flags(include_first: bool, include_last: bool, reverse_complement: bool) -> u8 {
    debug_assert_eq!(READ_FLAG_INCL_BEGIN, 1);
    ((include_first as u8) << (reverse_complement as u8))
        | ((include_last as u8) << (!reverse_complement as u8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd_batch::{LaneBatch, NO_RECORD};

    /// Builds a lane from the offsets its fragments start at; fragments of a
    /// lane are always back to back, the last one ending at `fill`.
    fn batch_with(starts: &[u32], fill: u32, bases_per_lane: usize) -> LaneBatch<()> {
        let mut batch = LaneBatch::new(bases_per_lane);
        for &start in starts {
            batch.lanes[0].push(LaneFragment {
                lane_start: start,
                record_idx: 0,
                source_start: start as u64,
            });
        }
        batch.lanes[0].push(LaneFragment {
            lane_start: fill,
            record_idx: NO_RECORD,
            source_start: 0,
        });
        batch.lane_fill[0] = fill;
        for lane in 1..SIMD_LANES {
            batch.lanes[lane].push(LaneFragment {
                lane_start: 0,
                record_idx: NO_RECORD,
                source_start: 0,
            });
        }
        batch.records.push(crate::simd_batch::RecordEntry {
            header: (0, 0),
            read_index: 0,
            bases_total: 0,
            flags: 0,
            stream_index: 0,
            extra: (),
        });
        batch
    }

    fn valid_windows(batch: &LaneBatch<()>, span: usize) -> Vec<usize> {
        let mut masks = Vec::new();
        build_valid_lanes_generic(&mut masks, batch, span);
        masks
            .iter()
            .enumerate()
            .filter_map(|(index, mask)| (mask & 1 != 0).then_some(index))
            .collect()
    }

    // The production helper is typed for the pipeline's batches; the test uses
    // the same code over a batch without record metadata.
    fn build_valid_lanes_generic<X>(output: &mut Vec<u8>, batch: &LaneBatch<X>, span: usize) {
        let windows = batch.bases_per_lane + 1 - span;
        output.clear();
        output.resize(windows, 0);
        for lane in 0..SIMD_LANES {
            let bit = 1u8 << lane;
            for pair in batch.lanes[lane].windows(2) {
                let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
                if end - start < span {
                    continue;
                }
                for mask in &mut output[start..=end - span] {
                    *mask |= bit;
                }
            }
        }
    }

    #[test]
    fn windows_stay_inside_their_fragment() {
        // One fragment covering the whole lane.
        assert_eq!(
            valid_windows(&batch_with(&[0], 16, 16), 5),
            (0..=11).collect::<Vec<_>>()
        );
        // A fragment exactly one window long, with the lane left unfilled.
        assert_eq!(valid_windows(&batch_with(&[0], 5, 16), 5), vec![0]);
        // Two fragments: the windows straddling their seam are gone, and so
        // are the ones that would reach past the fill level.
        assert_eq!(
            valid_windows(&batch_with(&[0, 8], 16, 16), 5),
            vec![0, 1, 2, 3, 8, 9, 10, 11]
        );
        assert_eq!(
            valid_windows(&batch_with(&[0, 8], 14, 16), 5),
            vec![0, 1, 2, 3, 8, 9]
        );
        // An empty lane has no windows at all.
        assert!(valid_windows(&batch_with(&[], 0, 16), 5).is_empty());
    }

    #[test]
    fn runs_are_trimmed_to_their_fragment() {
        const K: usize = 5;
        let fragments = vec![
            LaneFragment {
                lane_start: 0,
                record_idx: 0,
                source_start: 7,
            },
            LaneFragment {
                lane_start: 20,
                record_idx: 1,
                source_start: 0,
            },
            LaneFragment {
                lane_start: 32,
                record_idx: NO_RECORD,
                source_start: 0,
            },
        ];
        let mut resolver = LaneRunResolver::new(&fragments);

        // First run of the first fragment: no base to take before it.
        let first = resolver.resolve(0, 3, false, K, true);
        assert_eq!((first.start, first.end), (0, 7));
        assert!(first.include_first && !first.include_last);
        assert_eq!(first.record_idx, 0);
        assert_eq!(first.source_index(first.start), 7);

        // A later run takes the base before it and the k-1 bases after it.
        let middle = resolver.resolve(3, 9, false, K, true);
        assert_eq!((middle.start, middle.end), (2, 13));
        assert!(!middle.include_first && !middle.include_last);

        // The last run of a fragment stops at the fragment's end.
        let last = resolver.resolve(9, 17, true, K, true);
        assert_eq!((last.start, last.end), (8, 20));
        assert!(last.include_last);

        // The cursor follows the runs into the next fragment.
        let next = resolver.resolve(20, 29, true, K, true);
        assert_eq!((next.start, next.end), (20, 32));
        assert_eq!(next.record_idx, 1);
        assert!(next.include_first && next.include_last);

        // Without the leading extra base, nothing is taken before the run.
        let mut resolver = LaneRunResolver::new(&fragments);
        let query = resolver.resolve(4, 8, false, K, false);
        assert_eq!((query.start, query.end), (4, 12));
    }

    #[test]
    fn flags_swap_under_reverse_complement() {
        assert_eq!(super_kmer_flags(true, false, false), 1);
        assert_eq!(super_kmer_flags(false, true, false), 2);
        assert_eq!(super_kmer_flags(true, false, true), 2);
        assert_eq!(super_kmer_flags(false, true, true), 1);
        assert_eq!(super_kmer_flags(true, true, true), 3);
    }
}
