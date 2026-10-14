//! Turns the bytes of a sealed buffer back into records, each widened to the
//! colour-set type the store holds.

use super::formats::{DedupExtraData, InputFormat, SingleOf};
use io::concurrent::temp_reads::creads_utils::DeserializedRead;
use io::concurrent::temp_reads::extra_data::{
    BoundedTempBuffer, SequenceExtraDataTempBufferManagement, TempBuffer,
};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;

pub(super) struct NarrowDecoder<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned> {
    /// Reset before every record; see [`InputFormat`].
    format: InputFormat<MultipleData, FlagsCount>,
    /// The bases of the record being decoded.
    read_buf: Vec<u8>,
    /// Whatever the incoming extra data decodes through. Empty for both
    /// concrete single-entry types, which hold their colour inline.
    single_buffer: TempBuffer<SingleOf<MultipleData>>,
    /// Colours of the record being decoded, once widened. Separate from the
    /// store's arena because `copy_extra_from` and `combine_entries` both move
    /// runs from one arena into another.
    arena: TempBuffer<MultipleData>,
    /// Bytes the arena may hold before it is cleared between two records.
    arena_cap: usize,
}

impl<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned>
    NarrowDecoder<MultipleData, FlagsCount>
{
    pub(super) fn new(k: usize, arena_cap: usize) -> Self {
        Self {
            format: InputFormat::<MultipleData, FlagsCount>::new(k),
            read_buf: Vec::new(),
            single_buffer: SingleOf::<MultipleData>::new_temp_buffer(),
            arena: TempBuffer::<MultipleData>::with_budget(arena_cap),
            arena_cap,
        }
    }

    /// Calls `each` with every record of `data`, widened, and with the arena its
    /// colours live in. `data` must hold a whole number of records.
    pub(super) fn for_each(
        &mut self,
        data: &[u8],
        mut each: impl FnMut(DeserializedRead<'_, MultipleData>, &mut TempBuffer<MultipleData>),
    ) {
        let mut cursor = data;
        loop {
            // `read_from` appends without ever truncating, so the buffer has to
            // be cleared per record or it grows by one record each time and a
            // reallocation dangles the read that was just decoded.
            self.read_buf.clear();
            if self.arena.live_bytes() > self.arena_cap {
                MultipleData::clear_temp_buffer(&mut self.arena);
            }
            SingleOf::<MultipleData>::clear_temp_buffer(&mut self.single_buffer);
            self.format.reset();
            let Some(narrow) =
                self.format
                    .read_from(&mut cursor, &mut self.read_buf, &mut self.single_buffer)
            else {
                break;
            };
            // The one place a record is widened, and only ever once.
            let extra =
                MultipleData::from_single_entry(&mut self.arena, narrow.extra, &self.single_buffer)
                    .0;
            let record = DeserializedRead {
                read: narrow.read,
                extra,
                multiplicity: narrow.multiplicity,
                minimizer_pos: narrow.minimizer_pos,
                flags: narrow.flags,
                second_bucket: narrow.second_bucket,
            };
            each(record, &mut self.arena);
        }
        debug_assert!(
            cursor.is_empty(),
            "a slice handed to add() must hold a whole number of records"
        );
    }
}
