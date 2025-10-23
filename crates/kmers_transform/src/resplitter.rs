use crate::processor::{KmersProcessorInitData, KmersTransformProcessor};
use crate::{KmersTransformContext, KmersTransformExecutorFactory};
use config::{
    BucketIndexType, DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE, SwapPriority, get_compression_level_info,
    get_memory_mode,
};
use ggcat_logging::generate_stat_id;
use ggcat_logging::stats::StatId;
use hashes::HashableSequence;
use instrumenter::local_setup_instrumenter;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
    NoAlignment, WithMultiplicity,
};
use io::concurrent::temp_reads::creads_utils::{DeserializedRead, NoSecondBucket};
use minimizer_bucketing::decode_helper::decode_sequences;
use minimizer_bucketing::split_buckets::SplittedBucket;
use minimizer_bucketing::{
    MinimizerBucketMode, MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory,
    PushSequenceInfo,
};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};

use parallel_processor::buckets::readers::typed_binary_reader::AsyncReaderThread;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{BucketsCount, MultiThreadBuckets};
use parallel_processor::execution_manager::thread_pool::ExecutorsHandle;
use parallel_processor::memory_fs::RemoveFileMode;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

local_setup_instrumenter!();

pub struct ResplitterInitData<F: KmersTransformExecutorFactory> {
    pub _resplit_stat_id: StatId,
    pub subsplit_buckets_count: BucketsCount,
    pub splitted_bucket: SplittedBucket,
    pub process_handle: ExecutorsHandle<KmersTransformProcessor<F>>,
}

pub struct KmersTransformResplitter<F: KmersTransformExecutorFactory>(PhantomData<F>);

impl<F: KmersTransformExecutorFactory> KmersTransformResplitter<F> {
    pub fn do_resplit(
        global_context: &KmersTransformContext<F>,
        reader_thread: Arc<AsyncReaderThread>,
        mut resplit_data: ResplitterInitData<F>,
    ) {
        let mut resplitter = F::new_resplitter(
            &global_context.global_extra_data,
            &resplit_data.subsplit_buckets_count,
        );

        static RESPLIT_INDEX: AtomicUsize = AtomicUsize::new(0);

        let buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            resplit_data.subsplit_buckets_count,
            global_context.temp_dir.join(format!(
                "resplit-{}",
                RESPLIT_INDEX.fetch_add(1, Ordering::Relaxed)
            )),
            None,
            &(
                get_memory_mode(SwapPriority::ResplitBuckets as usize),
                MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
                get_compression_level_info(),
            ),
            &MinimizerBucketMode::Compacted,
        ));

        let mut thread_local_buffers = BucketsThreadDispatcher::<
            _,
            CompressedReadsBucketDataSerializer<
                _,
                NoSecondBucket, // This is always zero but it is needed to preserve consistency
                WithMultiplicity,
                AssemblerMinimizerPosition,
                <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FlagsCount,
            >,
        >::new(
            &buckets,
            BucketsThreadBuffer::new(
                DEFAULT_PER_CPU_BUFFER_SIZE,
                &resplit_data.subsplit_buckets_count,
            ),
            global_context.k,
        );

        let mut preprocess_info = Default::default();
        let mut sequences_count = vec![0; resplit_data.subsplit_buckets_count.total_buckets_count];

        decode_sequences::<
            F::AssociatedExtraData,
            F::AssociatedExtraDataWithMultiplicity,
            F::FlagsCount,
            NoAlignment,
        >(
            reader_thread,
            &mut Default::default(),
            &mut resplit_data.splitted_bucket,
            global_context.k,
            |read, extra_buffer| {
                let DeserializedRead {
                    read,
                    extra,
                    multiplicity,
                    flags,
                    second_bucket: _,
                    minimizer_pos: _,
                } = read;

                resplitter.reprocess_sequence(flags, &extra, &extra_buffer, &mut preprocess_info);

                resplitter.process_sequence::<_, _, true>(
                    &preprocess_info,
                    read,
                    0..read.bases_count(),
                    0,
                    resplit_data.subsplit_buckets_count.normal_buckets_count_log,
                    0,
                    #[inline(always)]
                    |info| {
                        let PushSequenceInfo {
                            bucket,
                            second_bucket: _,
                            sequence,
                            extra_data,
                            temp_buffer,
                            minimizer_pos,
                            flags,
                            rc,
                        } = info;

                        sequences_count[bucket as usize] += 1;
                        thread_local_buffers.add_element_extended(
                            bucket as BucketIndexType,
                            &extra_data,
                            temp_buffer,
                            &CompressedReadsBucketData::new_packed_with_multiplicity_opt_rc(
                                sequence,
                                flags,
                                0,
                                rc,
                                multiplicity,
                                minimizer_pos,
                            ),
                        );
                    },
                );
            },
        );

        thread_local_buffers.finalize();

        let buckets = buckets.finalize();

        for (bucket, sequences_count) in buckets.into_iter().zip(sequences_count) {
            let debug_bucket_first_path = bucket.chunks[0].clone();

            resplit_data.process_handle.create_new_address(
                Arc::new(KmersProcessorInitData {
                    process_stat_id: generate_stat_id!(),
                    splitted_bucket: Mutex::new(Some(SplittedBucket::from_multi_chunks(
                        bucket.chunks.into_iter(),
                        RemoveFileMode::Remove {
                            remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                        },
                        DEFAULT_PREFETCH_AMOUNT,
                        sequences_count,
                    ))),
                    is_resplitted: true,
                    resplit_config: None,
                    debug_bucket_first_path: Some(debug_bucket_first_path),
                    extra_bucket_data: bucket.extra_bucket_data,
                    processor_handle: resplit_data.process_handle.clone(),
                }),
                true,
            );
        }
    }
}
