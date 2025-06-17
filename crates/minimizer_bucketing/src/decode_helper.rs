use std::{mem::take, sync::Arc};

use io::concurrent::temp_reads::{
    creads_utils::{
        AssemblerMinimizerPosition, DeserializedRead, NoMultiplicity, NoSecondBucket,
        WithMultiplicity, helpers::helper_read_bucket,
    },
    extra_data::{
        SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
        SequenceExtraDataTempBufferManagement,
    },
};
use parallel_processor::buckets::readers::typed_binary_reader::AsyncReaderThread;

use crate::split_buckets::SplittedBucket;

pub fn decode_sequences<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    FlagsCount: typenum::Unsigned,
>(
    reader_thread: Arc<AsyncReaderThread>,
    tmp_mult_buffer: &mut <MultipleData as SequenceExtraDataTempBufferManagement>::TempBuffer,
    splitted_bucket: &mut SplittedBucket,
    k: usize,
    mut callback: impl FnMut(
        DeserializedRead<MultipleData>,
        &mut <MultipleData as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ),
) {
    let single_chunks = take(&mut splitted_bucket.single_chunks);
    let multi_chunks = take(&mut splitted_bucket.multi_chunks);

    helper_read_bucket::<
        SingleData,
        NoSecondBucket,
        NoMultiplicity,
        AssemblerMinimizerPosition,
        FlagsCount,
    >(
        single_chunks,
        Some(reader_thread.clone()),
        |item, extra_buffer| {
            let (extra, extra_buffer) =
                MultipleData::from_single_entry(tmp_mult_buffer, item.extra, extra_buffer);
            let item = DeserializedRead {
                read: item.read,
                extra,
                multiplicity: item.multiplicity,
                flags: item.flags,
                second_bucket: item.second_bucket,
                minimizer_pos: item.minimizer_pos,
                is_window_duplicate: item.is_window_duplicate,
            };
            callback(item, extra_buffer);
        },
        k,
    );

    helper_read_bucket::<
        MultipleData,
        NoSecondBucket,
        WithMultiplicity,
        AssemblerMinimizerPosition,
        FlagsCount,
    >(multi_chunks, Some(reader_thread.clone()), callback, k);
}
