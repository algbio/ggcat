//! The exchange format between the parsing threads and the bucketing threads.
//!
//! A batch holds [`SIMD_LANES`] equally long, 2-bit packed DNA lanes in
//! time-major order, plus the metadata needed to map any lane position back to
//! the input record it came from. Every base inside a lane fragment is valid
//! DNA: non-ACGT characters are removed while parsing, and the pieces they
//! leave behind become separate fragments.

use crate::hashing::{PackedSimdSequence, SIMD_LANES};

/// `record_idx` of the sentinel that terminates each lane's fragment list.
pub const NO_RECORD: u32 = u32::MAX;

/// The record has more bases in the following batch.
pub const RECORD_CONTINUES: u8 = 1 << 0;
/// The record already had bases in a preceding batch.
pub const RECORD_CONTINUED: u8 = 1 << 1;

/// One maximal run of valid DNA inside a lane.
///
/// Fragment `i` of a lane spans `[fragments[i].lane_start,
/// fragments[i + 1].lane_start)`; the list always ends with a sentinel whose
/// `record_idx` is [`NO_RECORD`] and whose `lane_start` is the lane fill level.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LaneFragment {
    /// First base of the fragment inside its lane.
    pub lane_start: u32,
    /// Index into [`LaneBatch::records`], or [`NO_RECORD`] for the sentinel.
    pub record_idx: u32,
    /// Offset of the first base inside the logical record, counting the
    /// non-DNA characters that were removed.
    pub source_start: u64,
}

impl LaneFragment {
    #[inline(always)]
    pub fn is_sentinel(&self) -> bool {
        self.record_idx == NO_RECORD
    }
}

/// One input record, as seen by a single batch.
///
/// A record that spans several batches has one entry per batch, all carrying
/// the same `read_index`; only the last one holds the final `bases_total`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordEntry<X> {
    /// Range inside [`LaneBatch::headers`]; empty unless headers were requested.
    pub header: (u32, u32),
    /// Sequential index of the record inside its input block.
    pub read_index: u64,
    /// Logical length of the record in bases, including removed characters.
    /// Only meaningful when [`RECORD_CONTINUES`] is not set.
    pub bases_total: u64,
    pub flags: u8,
    /// Index into the caller's per-batch stream metadata.
    pub stream_index: u16,
    pub extra: X,
}

/// A full group of [`SIMD_LANES`] packed lanes plus its record metadata.
#[derive(Clone, Debug)]
pub struct LaneBatch<X> {
    /// Time-major packed words: word `SIMD_LANES * chunk + lane` holds bases
    /// `16 * chunk .. 16 * chunk + 16` of `lane`, earliest base in the low bits.
    pub words: Vec<u32>,
    pub bases_per_lane: usize,
    /// Number of bases written in each lane.
    pub lane_fill: [u32; SIMD_LANES],
    pub lanes: [Vec<LaneFragment>; SIMD_LANES],
    pub records: Vec<RecordEntry<X>>,
    pub headers: Vec<u8>,
}

impl<X> LaneBatch<X> {
    pub fn new(bases_per_lane: usize) -> Self {
        Self {
            words: vec![0; bases_per_lane.div_ceil(16) * SIMD_LANES],
            bases_per_lane,
            lane_fill: [0; SIMD_LANES],
            lanes: std::array::from_fn(|_| Vec::with_capacity(256)),
            records: Vec::with_capacity(1024),
            headers: Vec::new(),
        }
    }

    /// Clears the batch while keeping every allocation.
    ///
    /// The words are zeroed because the packer only ever sets bits: a fragment
    /// that turns out to be too short is dropped by clearing its bits again,
    /// and everything past a lane's fill level must read as zero.
    pub fn reset(&mut self) {
        self.words.fill(0);
        self.lane_fill = [0; SIMD_LANES];
        for lane in self.lanes.iter_mut() {
            lane.clear();
        }
        self.records.clear();
        self.headers.clear();
    }

    /// Reallocates the word storage when the lane size changes.
    pub fn set_bases_per_lane(&mut self, bases_per_lane: usize) {
        if self.bases_per_lane != bases_per_lane {
            self.bases_per_lane = bases_per_lane;
            self.words = vec![0; bases_per_lane.div_ceil(16) * SIMD_LANES];
        }
    }

    #[inline(always)]
    pub fn header(&self, record: usize) -> &[u8] {
        let (start, len) = self.records[record].header;
        &self.headers[start as usize..start as usize + len as usize]
    }

    #[inline(always)]
    pub fn fragments(&self, lane: usize) -> &[LaneFragment] {
        &self.lanes[lane]
    }

    pub fn packed_sequence(&self) -> PackedSimdSequence<'_, SIMD_LANES> {
        PackedSimdSequence::new(&self.words, self.bases_per_lane)
            .expect("batch words do not match the lane size")
    }

    /// Reads one base, for tests and debugging.
    #[inline(always)]
    pub fn base(&self, lane: usize, position: usize) -> u8 {
        ((self.words[(position / 16) * SIMD_LANES + lane] >> (2 * (position % 16))) & 3) as u8
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.lane_fill.iter().all(|&fill| fill == 0)
    }

    /// Checks every invariant the bucketing side relies on.
    pub fn debug_check(&self, k: usize) {
        assert_eq!(
            self.words.len(),
            self.bases_per_lane.div_ceil(16) * SIMD_LANES
        );
        for lane in 0..SIMD_LANES {
            let fragments = &self.lanes[lane];
            assert!(!fragments.is_empty(), "lane {lane} has no sentinel");
            assert!(fragments.last().unwrap().is_sentinel());
            assert_eq!(fragments.last().unwrap().lane_start, self.lane_fill[lane]);
            for pair in fragments.windows(2) {
                assert!(!pair[0].is_sentinel());
                assert!(pair[0].lane_start < pair[1].lane_start);
                assert!(
                    (pair[1].lane_start - pair[0].lane_start) as usize >= k,
                    "lane {lane} fragment shorter than k"
                );
                assert!((pair[0].record_idx as usize) < self.records.len());
            }
            assert!(self.lane_fill[lane] as usize <= self.bases_per_lane);
        }
        for record in &self.records {
            let (start, len) = record.header;
            assert!(start as usize + len as usize <= self.headers.len());
        }
    }
}

/// Where a [`crate::packer::LanePacker`] gets its batches from and sends them to.
pub trait BatchSink<X> {
    /// The batch currently being filled, allocating one if needed.
    fn current(&mut self) -> &mut LaneBatch<X>;
    /// Hands the current batch over; the next `current` call starts a fresh one.
    fn emit(&mut self, is_last: bool);
    /// Index of the active input stream inside the current batch's metadata.
    fn stream_index(&mut self) -> u16;
}

/// Collects batches in memory, for tests and benchmarks.
pub struct VecBatchSink<X> {
    batch: LaneBatch<X>,
    bases_per_lane: usize,
    pub batches: Vec<LaneBatch<X>>,
    pub stream_index: u16,
}

impl<X: Clone> VecBatchSink<X> {
    pub fn new(bases_per_lane: usize) -> Self {
        Self {
            batch: LaneBatch::new(bases_per_lane),
            bases_per_lane,
            batches: Vec::new(),
            stream_index: 0,
        }
    }
}

impl<X: Clone> BatchSink<X> for VecBatchSink<X> {
    fn current(&mut self) -> &mut LaneBatch<X> {
        &mut self.batch
    }

    fn emit(&mut self, _is_last: bool) {
        let batch = std::mem::replace(&mut self.batch, LaneBatch::new(self.bases_per_lane));
        self.batches.push(batch);
    }

    fn stream_index(&mut self) -> u16 {
        self.stream_index
    }
}

/// Helpers shared by the packing tests.
#[cfg(test)]
pub mod testing {
    use super::*;

    const BASES: [u8; 4] = [b'A', b'C', b'T', b'G'];

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Reconstructed {
        pub read_index: u64,
        pub header: Vec<u8>,
        pub bases_total: u64,
        /// One entry per fragment: its offset inside the record and its bases.
        pub fragments: Vec<(u64, String)>,
    }

    /// Rebuilds the records a sequence of batches describes, in input order.
    pub fn reconstruct<X>(batches: &[LaneBatch<X>]) -> Vec<Reconstructed> {
        let mut output: Vec<Reconstructed> = Vec::new();
        for batch in batches {
            let mut mapping = vec![usize::MAX; batch.records.len()];
            for (index, record) in batch.records.iter().enumerate() {
                let continued = record.flags & RECORD_CONTINUED != 0;
                let previous = output
                    .iter()
                    .rposition(|existing| existing.read_index == record.read_index);
                match previous {
                    Some(position) if continued => {
                        if record.flags & RECORD_CONTINUES == 0 {
                            output[position].bases_total = record.bases_total;
                        }
                        mapping[index] = position;
                    }
                    _ => {
                        mapping[index] = output.len();
                        output.push(Reconstructed {
                            read_index: record.read_index,
                            header: batch.header(index).to_vec(),
                            bases_total: record.bases_total,
                            fragments: Vec::new(),
                        });
                    }
                }
            }
            for lane in 0..SIMD_LANES {
                for pair in batch.lanes[lane].windows(2) {
                    let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
                    let bases: String = (start..end)
                        .map(|position| BASES[batch.base(lane, position) as usize] as char)
                        .collect();
                    output[mapping[pair[0].record_idx as usize]]
                        .fragments
                        .push((pair[0].source_start, bases));
                }
            }
        }
        output
    }

    /// Every base past a lane's fill level must read as zero.
    pub fn check_padding_is_zero<X>(batch: &LaneBatch<X>) {
        for lane in 0..SIMD_LANES {
            for position in batch.lane_fill[lane] as usize..batch.bases_per_lane {
                assert_eq!(
                    batch.base(lane, position),
                    0,
                    "lane {lane} position {position} is not zero"
                );
            }
        }
    }
}
