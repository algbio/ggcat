//! Writes records out in the wide form the compactor reads.
//!
//! A drain writes the whole store at once, and a record no drain could make
//! room for is written on its own. Both go through a [`Batch`], which builds the
//! bytes in a buffer that belongs to the calling thread and has flushed all of
//! them to the output by the time it ends.

use super::formats::{DedupExtraData, OutputFormat};
use super::output::DedupOutput;
use io::concurrent::temp_reads::creads_utils::{CompressedReadsBucketData, DeserializedRead};
use io::concurrent::temp_reads::extra_data::TempBuffer;
use parallel_processor::buckets::ChunkingStatus;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use std::cell::RefCell;

thread_local! {
    /// Where a batch builds its output before handing it to the bucket.
    ///
    /// It belongs to the thread rather than to the deduplicator because nothing
    /// in it outlives the batch that fills it: a batch flushes it before
    /// returning, so it never carries bytes from one call to the next, and two
    /// deduplicators on one thread can share it. That is also what makes it
    /// safe to be per thread -- bytes left behind by one thread would otherwise
    /// never be flushed, since `finalize` runs on one.
    static STAGING: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub(super) struct WideEncoder<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned> {
    /// One format for the whole lifetime, never reset: the delta state a reset
    /// would clear is zero-sized for both concrete extra data types (asserted in
    /// `BoundedDeduplicator::new_with_bypass`), and a mid-stream reset without a
    /// matching reader restart is what desynchronises a stateful one.
    format: OutputFormat<MultipleData, FlagsCount>,
    /// Target size of the thread-local staging buffer; see [`STAGING`].
    staging_capacity: usize,
    /// Raised when a write started a new chunk, so `add` can hand that back to
    /// the caller, which is what makes the compactor run.
    new_chunk: bool,
}

impl<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned>
    WideEncoder<MultipleData, FlagsCount>
{
    pub(super) fn new(k: usize, staging_capacity: usize) -> Self {
        Self {
            format: OutputFormat::<MultipleData, FlagsCount>::new(k),
            staging_capacity,
            new_chunk: false,
        }
    }

    /// Lets `fill` write records to `output` through this thread's staging
    /// buffer, and flushes whatever is left once it returns. Returns how many
    /// records were written.
    pub(super) fn write_batch(
        &mut self,
        output: &dyn DedupOutput,
        fill: impl FnOnce(&mut Batch<'_, MultipleData, FlagsCount>),
    ) -> u64 {
        STAGING.with_borrow_mut(|staging| {
            staging.clear();
            staging.reserve(self.staging_capacity);
            let mut batch = Batch {
                encoder: self,
                output,
                staging,
                written: 0,
            };
            fill(&mut batch);
            batch.close()
        })
    }

    /// Whether a write since the last call started a new chunk.
    #[inline]
    pub(super) fn take_chunking(&mut self) -> ChunkingStatus {
        if std::mem::take(&mut self.new_chunk) {
            ChunkingStatus::NewChunk
        } else {
            ChunkingStatus::SameChunk
        }
    }
}

/// A run of records being written; see [`WideEncoder::write_batch`].
pub(super) struct Batch<'a, MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned> {
    encoder: &'a mut WideEncoder<MultipleData, FlagsCount>,
    output: &'a dyn DedupOutput,
    staging: &'a mut Vec<u8>,
    written: u64,
}

impl<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned>
    Batch<'_, MultipleData, FlagsCount>
{
    /// Serializes one record into the staging buffer, flushing it first if the
    /// record would not fit. `arena` is the one the record's colours live in.
    pub(super) fn write(
        &mut self,
        record: &DeserializedRead<MultipleData>,
        arena: &mut TempBuffer<MultipleData>,
    ) {
        let mut extra = record.extra;
        // Multiplicity is raised only by `combine_entries`, so a multiplicity of
        // one means the set is still the canonical one it was copied from and
        // `prepare` would be a no-op. Same reasoning the compactor uses.
        if record.multiplicity > 1 {
            extra.prepare_for_serialization(arena);
        }

        let element = CompressedReadsBucketData::new_packed_with_multiplicity(
            record.read,
            record.flags,
            record.second_bucket,
            record.multiplicity,
            record.minimizer_pos,
        );
        let needed = self.encoder.format.get_size(&element, &extra);
        if !self.staging.is_empty() && self.staging.len() + needed > self.staging.capacity() {
            self.flush();
        }
        self.encoder
            .format
            .write_to(&element, self.staging, &extra, arena);
        self.written += 1;
    }

    fn flush(&mut self) {
        if matches!(
            self.output.write_records(self.staging),
            ChunkingStatus::NewChunk
        ) {
            self.encoder.new_chunk = true;
        }
        self.staging.clear();
    }

    /// Flushes whatever the batch left behind, so the buffer never carries
    /// bytes out of the call that wrote them, and gives back any capacity a
    /// single oversized record forced it to grow by rather than letting a
    /// thread hold that for the rest of its life.
    fn close(mut self) -> u64 {
        if !self.staging.is_empty() {
            self.flush();
        }
        if self.staging.capacity() > 2 * self.encoder.staging_capacity {
            *self.staging = Vec::with_capacity(self.encoder.staging_capacity);
        }
        self.written
    }
}
