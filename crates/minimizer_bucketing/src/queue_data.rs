use ggcat_logging::stats::StatId;
use io::sequences_stream::SequenceInfo;
use io::simd::{SequenceBuffer, SequenceDescription};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::PacketTrait;
use std::mem::size_of;

/// A batch of sequences of one input, for the bucketing threads: a buffer as the parser handed
/// it over, with what holds for all its sequences.
///
/// The buffers go round with the packets of the pool: a reader takes a packet, puts the full
/// buffer the parser handed over in it and gives the parser back the one the packet held, which
/// a reset of the packet emptied.
pub struct MinimizerBucketingQueueData<F: Clone + Sync + Send + Default + 'static> {
    pub buffer: SequenceBuffer,
    pub sequence_info: SequenceInfo,
    pub stream_info: F,
    pub stats_block_id: StatId,
}

impl<F: Clone + Sync + Send + Default + 'static> MinimizerBucketingQueueData<F> {
    #[allow(dead_code)]
    pub fn get_total_size(&self) -> usize {
        self.buffer.bases().len()
    }
}

impl<F: Clone + Sync + Send + Default + 'static> PoolObjectTrait
    for MinimizerBucketingQueueData<F>
{
    type InitData = ();

    fn allocate_new(_: &Self::InitData) -> Self {
        Self {
            buffer: SequenceBuffer::new(),
            sequence_info: SequenceInfo { color: None },
            stream_info: F::default(),
            stats_block_id: StatId::default(),
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl<F: Clone + Sync + Send + Default + 'static> PacketTrait for MinimizerBucketingQueueData<F> {
    fn get_size(&self) -> usize {
        self.buffer.bases().len()
            + self.buffer.ident().len()
            + self.buffer.len() * size_of::<SequenceDescription>()
            + self.buffer.record_indices().len() * size_of::<u64>()
    }
}
