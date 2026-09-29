use crate::processor::{KmersProcessorInitData, KmersTransformProcessor};
use crate::{KmersTransformContext, KmersTransformExecutorFactory};
use config::{
    BucketIndexType, DEFAULT_PER_CPU_BUFFER_SIZE, KEEP_FILES, MIN_RESPLIT_PART_SIZE,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE, SwapPriority,
    get_compression_level_info, get_memory_mode,
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
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{
    BucketsCount, LockFreeBucket, MultiChunkBucket, MultiThreadBuckets,
};
use parallel_processor::execution_manager::thread_pool::ExecutorsHandle;
use parallel_processor::memory_fs::RemoveFileMode;
use parking_lot::Mutex;
use std::any::TypeId;
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

/// The sub-buckets of a resplit, with the writer chosen by the extra data type
enum ResplitBuckets {
    Compressed(Arc<MultiThreadBuckets<CompressedBinaryWriter>>),
    LockFree(Arc<MultiThreadBuckets<LockFreeBinaryWriter>>),
}

/// State shared between all the parts of a resplitted bucket, processed in parallel
pub struct ResplitShared<F: KmersTransformExecutorFactory> {
    buckets: Mutex<Option<ResplitBuckets>>,
    sequences_count: Mutex<Vec<u64>>,
    remaining_parts: AtomicUsize,
    subsplit_buckets_count: BucketsCount,
    process_handle: ExecutorsHandle<KmersTransformProcessor<F>>,
    pub(crate) total_size: u64,
}

pub struct KmersTransformResplitter<F: KmersTransformExecutorFactory>(PhantomData<F>);

impl<F: KmersTransformExecutorFactory> KmersTransformResplitter<F> {
    /// Starts the resplit of a bucket, splitting its chunks in multiple parts that are processed in parallel.
    /// Returns the shared state if the current thread processed the last part and the resplit is complete.
    pub fn do_resplit(
        global_context: &KmersTransformContext<F>,
        reader_thread: Arc<AsyncReaderThread>,
        resplit_data: ResplitterInitData<F>,
        parts_count: usize,
    ) -> Option<Arc<ResplitShared<F>>> {
        static RESPLIT_INDEX: AtomicUsize = AtomicUsize::new(0);

        let path = global_context.temp_dir.join(format!(
            "resplit-{}",
            RESPLIT_INDEX.fetch_add(1, Ordering::Relaxed)
        ));
        let file_mode = get_memory_mode(SwapPriority::ResplitBuckets as usize);
        let buckets = if TypeId::of::<F::AssociatedExtraDataWithMultiplicity>()
            == TypeId::of::<colors::parsers::separate::MinBkMultipleColors>()
        {
            ResplitBuckets::LockFree(Arc::new(MultiThreadBuckets::new(
                resplit_data.subsplit_buckets_count,
                path,
                None,
                &(file_mode, MINIMIZER_BUCKETS_CHECKPOINT_SIZE),
                &MinimizerBucketMode::Compacted,
            )))
        } else {
            ResplitBuckets::Compressed(Arc::new(MultiThreadBuckets::new(
                resplit_data.subsplit_buckets_count,
                path,
                None,
                &(
                    file_mode,
                    MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
                    get_compression_level_info(),
                ),
                &MinimizerBucketMode::Compacted,
            )))
        };

        let SplittedBucket {
            single_chunks,
            multi_chunks,
            sequences_count: _,
            total_size,
            sub_bucket,
        } = resplit_data.splitted_bucket;

        // Each part has a fixed overhead, avoid splitting small buckets
        let parts_count = parts_count
            .min((total_size / MIN_RESPLIT_PART_SIZE) as usize)
            .min(single_chunks.len() + multi_chunks.len())
            .max(1);

        // Balance the chunks between the parts, assigning the largest chunks first to the smallest part
        let mut parts: Vec<(u64, SplittedBucket)> = (0..parts_count)
            .map(|_| {
                (
                    0,
                    SplittedBucket {
                        single_chunks: vec![],
                        multi_chunks: vec![],
                        sequences_count: 0,
                        total_size: 0,
                        sub_bucket,
                    },
                )
            })
            .collect();

        let mut all_chunks: Vec<_> = single_chunks
            .into_iter()
            .map(|c| (false, c))
            .chain(multi_chunks.into_iter().map(|c| (true, c)))
            .collect();
        all_chunks.sort_by_key(|c| std::cmp::Reverse(c.1.get_length()));
        for (is_multi, chunk) in all_chunks {
            let part = parts.iter_mut().min_by_key(|p| p.0).unwrap();
            part.0 += chunk.get_length();
            part.1.total_size += chunk.get_length();
            if is_multi {
                part.1.multi_chunks.push(chunk);
            } else {
                part.1.single_chunks.push(chunk);
            }
        }

        let shared = Arc::new(ResplitShared {
            buckets: Mutex::new(Some(buckets)),
            sequences_count: Mutex::new(vec![
                0;
                resplit_data
                    .subsplit_buckets_count
                    .total_buckets_count
            ]),
            remaining_parts: AtomicUsize::new(parts_count),
            subsplit_buckets_count: resplit_data.subsplit_buckets_count,
            process_handle: resplit_data.process_handle.clone(),
            total_size,
        });

        let mut parts = parts.into_iter().map(|p| p.1);
        let first_part = parts.next().unwrap();

        // Schedule the other parts with high priority, as they are on the critical path
        for part in parts {
            resplit_data.process_handle.create_new_address(
                Arc::new(KmersProcessorInitData {
                    process_stat_id: generate_stat_id!(),
                    splitted_bucket: Mutex::new(Some(part)),
                    is_resplitted: false,
                    resplit_config: None,
                    resplit_part: Some(shared.clone()),
                    debug_bucket_first_path: None,
                    extra_bucket_data: None,
                    processor_handle: resplit_data.process_handle.clone(),
                }),
                true,
            );
        }

        Self::process_part(global_context, reader_thread, shared, first_part)
    }

    /// Processes a part of a resplitted bucket, if it is the last part to complete, finalizes the resplit.
    pub fn process_part(
        global_context: &KmersTransformContext<F>,
        reader_thread: Arc<AsyncReaderThread>,
        shared: Arc<ResplitShared<F>>,
        splitted_bucket: SplittedBucket,
    ) -> Option<Arc<ResplitShared<F>>> {
        let part_buckets = match shared.buckets.lock().as_ref().unwrap() {
            ResplitBuckets::Compressed(buckets) => ResplitBuckets::Compressed(buckets.clone()),
            ResplitBuckets::LockFree(buckets) => ResplitBuckets::LockFree(buckets.clone()),
        };
        let sequences_count = match part_buckets {
            ResplitBuckets::Compressed(buckets) => Self::process_part_with_writer(
                global_context,
                reader_thread,
                &shared,
                buckets,
                splitted_bucket,
            ),
            ResplitBuckets::LockFree(buckets) => Self::process_part_with_writer(
                global_context,
                reader_thread,
                &shared,
                buckets,
                splitted_bucket,
            ),
        };

        {
            let mut total_counts = shared.sequences_count.lock();
            for (total, count) in total_counts.iter_mut().zip(sequences_count) {
                *total += count;
            }
        }

        // All the other parts dropped their buckets reference before decrementing the counter
        if shared.remaining_parts.fetch_sub(1, Ordering::AcqRel) != 1 {
            return None;
        }

        let buckets: Vec<MultiChunkBucket> = match shared.buckets.lock().take().unwrap() {
            ResplitBuckets::Compressed(buckets) => buckets.finalize(),
            ResplitBuckets::LockFree(buckets) => buckets.finalize(),
        };
        let sequences_count = std::mem::take(&mut *shared.sequences_count.lock());

        for (bucket, sequences_count) in buckets.into_iter().zip(sequences_count) {
            let debug_bucket_first_path = bucket.chunks[0].clone();

            shared.process_handle.create_new_address(
                Arc::new(KmersProcessorInitData {
                    process_stat_id: generate_stat_id!(),
                    splitted_bucket: Mutex::new(Some(SplittedBucket::from_multi_chunks(
                        bucket.chunks.into_iter(),
                        RemoveFileMode::Remove {
                            remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                        },
                        sequences_count,
                    ))),
                    is_resplitted: true,
                    resplit_config: None,
                    resplit_part: None,
                    debug_bucket_first_path: Some(debug_bucket_first_path),
                    extra_bucket_data: bucket.extra_bucket_data,
                    processor_handle: shared.process_handle.clone(),
                }),
                true,
            );
        }

        Some(shared)
    }

    fn process_part_with_writer<Writer: LockFreeBucket>(
        global_context: &KmersTransformContext<F>,
        reader_thread: Arc<AsyncReaderThread>,
        shared: &ResplitShared<F>,
        buckets: Arc<MultiThreadBuckets<Writer>>,
        mut splitted_bucket: SplittedBucket,
    ) -> Vec<u64> {
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
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &shared.subsplit_buckets_count),
            global_context.k,
        );

        let mut resplitter = F::new_resplitter(
            &global_context.global_extra_data,
            &shared.subsplit_buckets_count,
        );

        let mut preprocess_info = Default::default();
        let mut sequences_count = vec![0; shared.subsplit_buckets_count.total_buckets_count];

        decode_sequences::<
            F::AssociatedExtraData,
            F::AssociatedExtraDataWithMultiplicity,
            F::FlagsCount,
            NoAlignment,
        >(
            Some(reader_thread),
            &mut Default::default(),
            &mut splitted_bucket,
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
                    shared.subsplit_buckets_count.normal_buckets_count_log,
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
        drop(buckets);
        sequences_count
    }
}
