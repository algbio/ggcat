//! The packet the parsing threads hand to the bucketing threads.

use ggcat_logging::stats::StatId;
use io::sequences_reader::DnaSequencesFileType;
use io::sequences_stream::SequenceInfo;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::PacketTrait;
use simd_accel::hashing::SIMD_LANES;
use std::mem::size_of;

pub use simd_accel::batch::{
    BatchSink, LaneBatch, LaneFragment, NO_RECORD, RECORD_CONTINUED, RECORD_CONTINUES, RecordEntry,
};

/// What every record carries besides its bases.
pub type RecordExtra = (SequenceInfo, DnaSequencesFileType);

/// Eight lanes of packed DNA plus the records they were cut from.
pub type SequencesLaneBatch = LaneBatch<RecordExtra>;

/// What a record looks like to an executor, now that the bases live in lanes.
pub struct RecordInfo<'a> {
    pub ident_data: &'a [u8],
    pub format: DnaSequencesFileType,
    /// Length of the whole record, ambiguity characters included.
    pub bases_count: usize,
}

pub struct SimdSequencesBatch<F: Clone + Sync + Send + Default + 'static> {
    pub batch: SequencesLaneBatch,
    /// Per-input-block data, indexed by [`RecordEntry::stream_index`]. A batch
    /// may be filled from several input blocks, so that a run of small files
    /// does not leave most of every batch empty.
    pub stream_infos: Vec<F>,
    pub stats_block_id: StatId,
}

impl<F: Clone + Sync + Send + Default + 'static> SimdSequencesBatch<F> {
    pub fn new(bases_per_lane: usize) -> Self {
        Self {
            batch: LaneBatch::new(bases_per_lane),
            stream_infos: Vec::with_capacity(16),
            stats_block_id: StatId::default(),
        }
    }

    #[inline(always)]
    pub fn stream_info(&self, record: &RecordEntry<RecordExtra>) -> &F {
        &self.stream_infos[record.stream_index as usize]
    }

    pub fn records_count(&self) -> usize {
        self.batch.records.len()
    }
}

impl<F: Clone + Sync + Send + Default + 'static> PoolObjectTrait for SimdSequencesBatch<F> {
    type InitData = usize;

    fn allocate_new(bases_per_lane: &Self::InitData) -> Self {
        Self::new(*bases_per_lane)
    }

    fn reset(&mut self) {
        self.batch.reset();
        self.stream_infos.clear();
    }
}

impl<F: Clone + Sync + Send + Default + 'static> PacketTrait for SimdSequencesBatch<F> {
    fn get_size(&self) -> usize {
        self.batch.words.len() * size_of::<u32>()
            + (0..SIMD_LANES)
                .map(|lane| self.batch.lanes[lane].len() * size_of::<LaneFragment>())
                .sum::<usize>()
            + self.batch.records.len() * size_of::<RecordEntry<RecordExtra>>()
            + self.batch.headers.len()
    }
}
