//! The producer side: how a bucketing thread's records reach the deduplicators.

use super::BoundedDeduplicator;
use super::formats::{DedupExtraData, InputFormat, SingleOf};
use super::output::DedupOutput;
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketData;
use io::concurrent::temp_reads::extra_data::TempBuffer;
use parallel_processor::buckets::ChunkingStatus;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;

/// One deduplicator per bucket, plus the per-thread buffers that feed them.
///
/// This is `BucketsThreadDispatcher` with a different sink: a full per-thread
/// buffer goes to its bucket's deduplicator rather than straight to the bucket
/// file, and the deduplicator writes what survives. The buffers are the same
/// size and are filled the same way, so the only change a producer sees is where
/// a flush lands.
pub struct DeduplicatingDispatcher<
    'a,
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput = O,
> {
    deduplicators: &'a [BoundedDeduplicator<MultipleData, FlagsCount, O, Bypass>],
    buffers: Vec<Vec<u8>>,
    /// One format per bucket, reset per record to match the decoder.
    formats: Vec<InputFormat<MultipleData, FlagsCount>>,
    finalized: bool,
}

impl<
    'a,
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput,
> DeduplicatingDispatcher<'a, MultipleData, FlagsCount, O, Bypass>
{
    pub fn new(
        deduplicators: &'a [BoundedDeduplicator<MultipleData, FlagsCount, O, Bypass>],
        buffer_size: usize,
        k: usize,
    ) -> Self {
        Self {
            buffers: (0..deduplicators.len())
                .map(|_| Vec::with_capacity(buffer_size))
                .collect(),
            formats: (0..deduplicators.len())
                .map(|_| InputFormat::<MultipleData, FlagsCount>::new(k))
                .collect(),
            deduplicators,
            finalized: false,
        }
    }

    pub fn add_element_extended(
        &mut self,
        bucket: u16,
        extra_data: &SingleOf<MultipleData>,
        extra_data_buffer: &TempBuffer<SingleOf<MultipleData>>,
        element: &CompressedReadsBucketData<'_>,
    ) -> ChunkingStatus {
        let index = bucket as usize;
        let buffer = &mut self.buffers[index];
        let format = &mut self.formats[index];
        let mut status = ChunkingStatus::SameChunk;
        // Every record stands on its own, so a sealed buffer can hold runs from
        // several threads back to back.
        format.reset();
        // Flush before overflowing, so a buffer never reallocates.
        if format.get_size(element, extra_data) + buffer.len() > buffer.capacity()
            && !buffer.is_empty()
        {
            status = self.deduplicators[index].add(buffer);
            buffer.clear();
        }
        format.write_to(element, buffer, extra_data, extra_data_buffer);
        status
    }

    /// Hands every partial buffer over. The deduplicators themselves are
    /// finished separately, once every thread's dispatcher has been finalized.
    pub fn finalize(mut self) {
        for (index, buffer) in self.buffers.iter_mut().enumerate() {
            if !buffer.is_empty() {
                self.deduplicators[index].add(buffer);
                buffer.clear();
            }
        }
        self.finalized = true;
    }
}

impl<
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput,
> Drop for DeduplicatingDispatcher<'_, MultipleData, FlagsCount, O, Bypass>
{
    fn drop(&mut self) {
        // Dropping with records still buffered would lose them silently, which
        // is the one failure mode that produces a plausible but wrong graph.
        // Not during an unwind, though: panicking there would abort and bury
        // whatever went wrong first.
        assert!(
            self.finalized
                || std::thread::panicking()
                || self.buffers.iter().all(|buffer| buffer.is_empty()),
            "a deduplicating dispatcher was dropped with buffered records"
        );
    }
}
