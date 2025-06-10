use crate::processor::{KmersProcessorInitData, KmersTransformProcessor};
use crate::reads_buffer::{DeserializedReadIndependent, ReadsBuffer};
use crate::resplitter::{KmersTransformResplitter, ResplitterInitData};
use crate::{KmersTransformContext, KmersTransformExecutorFactory, KmersTransformGlobalExtraData};
use config::{
    DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    KMERS_TRANSFORM_READS_CHUNKS_SIZE, MAX_KMERS_TRANSFORM_READERS_PER_BUCKET,
    MAXIMUM_JIT_PROCESSED_BUCKETS, MIN_BUCKET_CHUNKS_FOR_READING_THREAD,
    PARTIAL_VECS_CHECKPOINT_SIZE, SwapPriority, USE_SECOND_BUCKET, get_compression_level_info,
    get_memory_mode,
};
use ggcat_logging::stats::StatId;
use ggcat_logging::{generate_stat_id, stats};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::creads_utils::helpers::helper_read_bucket_with_opt_multiplicity;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, BucketModeFromBoolean, BucketModeOption, CompressedReadsBucketData,
    CompressedReadsBucketDataSerializer, DeserializedRead, MultiplicityModeFromBoolean,
    MultiplicityModeOption, ReadsCheckpointData,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use minimizer_bucketing::counters_analyzer::BucketCounter;
use minimizer_bucketing::{MinimizerBucketMode, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::readers::async_binary_reader::{
    AllowedCheckpointStrategy, AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{ExtraBucketData, LockFreeBucket};
use parallel_processor::execution_manager::executor::{
    AddressProducer, AsyncExecutor, ExecutorReceiver,
};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait, PacketsPool};
use parallel_processor::execution_manager::thread_pool::ExecutorsHandle;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, SumMode};
use parallel_processor::mt_debug_counters::declare_counter_i64;
use replace_with::replace_with_or_abort;
use std::cell::Cell;
use std::cmp::{Reverse, max, min};
use std::collections::{BinaryHeap, VecDeque};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use utils::track;

local_setup_instrumenter!();

pub(crate) struct KmersTransformReader<F: KmersTransformExecutorFactory> {
    _phantom: PhantomData<F>,
}

pub struct InputBucketDesc {
    pub(crate) paths: Vec<PathBuf>,
    pub(crate) sub_bucket_counters: Vec<BucketCounter>,
    #[allow(dead_code)]
    pub(crate) compaction_delta: i64,
    pub(crate) out_data_format: MinimizerBucketMode,
    pub(crate) resplitted: bool,
    pub(crate) rewritten: bool,
    pub(crate) used_hash_bits: usize,
    pub(crate) extra_bucket_data: Option<ExtraBucketData>,
}

impl PoolObjectTrait for InputBucketDesc {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            paths: vec![],
            sub_bucket_counters: Vec::new(),
            compaction_delta: 0,
            resplitted: false,
            rewritten: false,
            used_hash_bits: 0,
            out_data_format: MinimizerBucketMode::Compacted,
            extra_bucket_data: None,
        }
    }

    fn reset(&mut self) {
        self.resplitted = false;
        self.sub_bucket_counters.clear();
        self.extra_bucket_data = None;
    }
}
impl PacketTrait for InputBucketDesc {
    fn get_size(&self) -> usize {
        1 // TODO: Maybe specify size
    }
}

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_addr_wait_reader", SumMode, false);

static PACKET_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_packet_wait_reader", SumMode, false);

static START_PACKET_ALLOC_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_packet_alloc_reader_startup", SumMode, false);

static PACKET_ALLOC_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_packet_alloc_reader", SumMode, false);

#[derive(Clone)]
struct RewriterInitData {
    pub _rewrite_stat_id: StatId,
    pub buckets_hash_bits: usize,
    pub used_hash_bits: usize,
    pub data_format: MinimizerBucketMode,
}

enum AddressModeDeclaration<F: KmersTransformExecutorFactory> {
    Resplit(ResplitterInitData<F>),
    Process(KmersProcessorInitData),
    Rewrite(CompressedBinaryWriter, AtomicU64, RewriterInitData),
}

enum AllocatedAddressMode<F: KmersTransformExecutorFactory> {
    Send(AddressProducer<ReadsBuffer<F::AssociatedExtraDataWithMultiplicity>>),
    Rewrite(CompressedBinaryWriter, AtomicU64, RewriterInitData),
}

struct BucketsInfo<F: KmersTransformExecutorFactory> {
    _stats_id: StatId,
    readers: Vec<AsyncBinaryReader>,
    concurrency: usize,
    addresses: Vec<AddressModeDeclaration<F>>,
    buckets_remapping: Arc<Vec<usize>>,
    _second_buckets_log_max: usize,
    total_file_size: usize,
    _used_hash_bits: usize,
    data_format: MinimizerBucketMode,
    extra_bucket_data: Option<ExtraBucketData>,
    allow_online_processing: bool,
}

impl<F: KmersTransformExecutorFactory> KmersTransformReader<F> {
    fn compute_buckets(
        global_context: &KmersTransformContext<F>,
        read_handle: &ExecutorsHandle<KmersTransformReader<F>>,
        file: Packet<InputBucketDesc>,
    ) -> BucketsInfo<F> {
        let second_buckets_log_max = min(
            file.sub_bucket_counters.len().ilog2() as usize,
            global_context.max_second_buckets_count_log2,
        );

        // Id used to track this bucket for stats
        let stats_id = generate_stat_id!();
        stats!(
            let mut bucket_stat = ggcat_logging::stats::KmersTransformInputBucketStats::default();
            bucket_stat.stat_id = stats_id;
            bucket_stat.paths = file.paths.clone();
            bucket_stat.sub_bucket_counters = file.sub_bucket_counters.iter().map(|b| b.count).collect();
            bucket_stat.compaction_delta = file.compaction_delta;
            bucket_stat.compacted = file.out_data_format == MinimizerBucketMode::Compacted;
            bucket_stat.resplitted = file.resplitted;
            bucket_stat.rewritten = file.rewritten;
        );

        let readers: Vec<_> = file
            .paths
            .iter()
            .map(|path| {
                AsyncBinaryReader::new(
                    path,
                    true,
                    RemoveFileMode::Remove {
                        remove_fs: file.rewritten || !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                )
            })
            .collect();

        let second_buckets_max = 1 << second_buckets_log_max;

        let mut buckets_remapping = vec![0; second_buckets_max];

        let mut queue = BinaryHeap::new();
        queue.push((Reverse(0), 0, false));

        let mut sequences_count = 0;

        let mut bucket_sizes: VecDeque<_> = (0..(1 << second_buckets_log_max))
            .map(|i| {
                sequences_count += file.sub_bucket_counters[i].count;
                (file.sub_bucket_counters[i].clone(), i)
            })
            .collect();

        let total_file_size = readers.iter().map(|r| r.get_file_size()).sum();

        bucket_sizes.make_contiguous().sort();

        let mut has_outliers = false;

        let total_sequences = global_context.total_sequences.load(Ordering::Relaxed);
        let total_kmers = global_context.total_kmers.load(Ordering::Relaxed);
        let unique_kmers = global_context.unique_kmers.load(Ordering::Relaxed);

        let unique_estimator_factor = if total_sequences > 0 {
            if F::HAS_COLORS {
                (unique_kmers as f64 / total_sequences as f64)
                    .max(total_kmers as f64 / total_sequences as f64 / 48.0)
            } else {
                unique_kmers as f64 / total_sequences as f64
            }
        } else {
            global_context.k as f64 / 2.0
        };

        while bucket_sizes.len() > 0 {
            let buckets_count = queue.len();
            let mut smallest_bucket = queue.pop().unwrap();

            let biggest_sub_bucket = bucket_sizes.pop_back().unwrap();

            // TODO: Find why there is a deadlock if the bucket is always considered an outlier
            // let is_outlier = !file.resplitted && (total_sequences > 0) && true;
            let is_outlier = false;

            //  !file.resplitted
            //     && (total_sequences > 0)
            //     && (biggest_sub_bucket.0.count as f64 * unique_estimator_factor
            //         >= (MAX_INTERMEDIATE_MAP_SIZE / F::MapProcessorType::MAP_SIZE as u64) as f64);

            // if is_outlier {
            //     ggcat_logging::info!(
            //         "Is outlier bucket with count {} and {} >= {}",
            //         biggest_sub_bucket.0.count,
            //         biggest_sub_bucket.0.count as f64 * unique_estimator_factor,
            //         MAX_INTERMEDIATE_MAP_SIZE / F::MapProcessorType::MAP_SIZE as u64
            //     );
            // }

            // Alloc a new bucket
            if (smallest_bucket.2 == is_outlier)
                && smallest_bucket.0.0 > 0
                && (biggest_sub_bucket.0.count + smallest_bucket.0.0) as f64
                    * unique_estimator_factor
                    > global_context.min_bucket_size as f64
            {
                // Restore the sub bucket
                bucket_sizes.push_back(biggest_sub_bucket);

                // Push the current bucket
                queue.push(smallest_bucket);

                // Add the new bucket
                queue.push((Reverse(0), buckets_count, false));
                continue;
            }

            // Assign the sub-bucket to the current smallest bucket
            smallest_bucket.0.0 += biggest_sub_bucket.0.count;
            smallest_bucket.2 |= is_outlier;
            has_outliers |= is_outlier;
            buckets_remapping[biggest_sub_bucket.1] = smallest_bucket.1;
            queue.push(smallest_bucket);
        }

        let mut addresses: Vec<_> = (0..queue.len()).map(|_| None).collect();
        let mut dbg_counters: Vec<_> = vec![0; queue.len()];

        let allow_online_processing = !has_outliers
            && queue.len()
                <= MAXIMUM_JIT_PROCESSED_BUCKETS.min(global_context.compute_threads_count);

        stats!(
            bucket_stat.allow_online_processing = allow_online_processing;
            bucket_stat.queue = queue.clone();
        );

        for (count, index, outlier) in queue.into_iter() {
            dbg_counters[index] = count.0;
            addresses[index] = if outlier {
                // ggcat_logging::info!("Sub-bucket {} is an outlier with size {}!", index, count.0);

                let resplit_stat_id = generate_stat_id!();

                stats!(
                    bucket_stat.resplits.push(ggcat_logging::stats::ResplitInfo {
                        resplit_id: resplit_stat_id,
                        bucket_index: index,
                    });
                );

                Some(AddressModeDeclaration::Resplit(ResplitterInitData {
                    _resplit_stat_id: resplit_stat_id,
                    bucket_size: count.0 as usize,
                    data_format: file.out_data_format,
                    read_handle: Arc::new(read_handle.clone()),
                }))
            } else {
                if allow_online_processing {
                    let process_stat_id = generate_stat_id!();

                    stats!(
                        bucket_stat.processed.push(ggcat_logging::stats::ProcessOnlineInfo {
                            process_id: process_stat_id,
                            bucket_index: index,
                        });
                    );
                    Some(AddressModeDeclaration::Process(KmersProcessorInitData {
                        process_stat_id,
                        sequences_count: count.0 as usize,
                        sub_bucket: index,
                        is_resplitted: file.resplitted,
                        debug_bucket_first_path: file.paths.first().cloned(),
                    }))
                } else {
                    static SUBSPLIT_INDEX: AtomicUsize = AtomicUsize::new(0);

                    let rewrite_stat_id = generate_stat_id!();

                    let writer = CompressedBinaryWriter::new(
                        &global_context.temp_dir.join("bucket-rewrite"),
                        &(
                            get_memory_mode(SwapPriority::ResultBuckets),
                            PARTIAL_VECS_CHECKPOINT_SIZE,
                            get_compression_level_info(),
                        ),
                        SUBSPLIT_INDEX.fetch_add(1, Ordering::Relaxed),
                        &file.out_data_format,
                    );

                    stats!(
                        bucket_stat.rewrites.push(ggcat_logging::stats::RewriteInfo {
                            rewrite_id: rewrite_stat_id,
                            bucket_index: index,
                            file_name: writer.get_path(),
                        });
                    );

                    Some(AddressModeDeclaration::Rewrite(
                        writer,
                        AtomicU64::new(0),
                        RewriterInitData {
                            _rewrite_stat_id: rewrite_stat_id,
                            buckets_hash_bits: second_buckets_max.ilog2() as usize,
                            used_hash_bits: file.used_hash_bits,
                            data_format: file.out_data_format,
                        },
                    ))
                }
            };
        }

        let addresses: Vec<_> = addresses.into_iter().map(|a| a.unwrap()).collect();

        let threads_ratio =
            global_context.compute_threads_count as f64 / global_context.read_threads_count as f64;

        let addr_concurrency = max(1, (addresses.len() as f64 / threads_ratio + 0.5) as usize);
        let chunks_concurrency = max(
            1,
            readers.iter().map(|r| r.get_chunks_count()).sum::<usize>()
                / MIN_BUCKET_CHUNKS_FOR_READING_THREAD,
        );

        let concurrency = min(
            min(
                MAX_KMERS_TRANSFORM_READERS_PER_BUCKET,
                global_context.read_threads_count,
            ),
            min(addr_concurrency, chunks_concurrency),
        );

        stats!(
            bucket_stat.threads_ratio = threads_ratio;
            bucket_stat.addr_concurrency = addr_concurrency;
            bucket_stat.chunks_concurrency = chunks_concurrency;
            bucket_stat.concurrency = concurrency;
            bucket_stat.total_file_size = total_file_size;
            bucket_stat.used_hash_bits = file.used_hash_bits;
        );

        stats!(
            stats.transform.buckets.push(bucket_stat);
        );

        //     ggcat_logging::info!(
        //     "File:{}\nChunks {} concurrency: {} REMAPPINGS: {:?} // {:?} // {:?} RATIO: {:.2} ADDR_COUNT: {}",
        //     file.path.display(),
        //     reader.get_chunks_count(),
        //     concurrency,
        //     &buckets_remapping,
        //     dbg_counters,
        //     file.sub_bucket_counters
        //         .iter()
        //         .map(|x| x.count)
        //         .collect::<Vec<_>>(),
        //     sequences_size_ratio,
        //     addresses.len()
        // );

        BucketsInfo {
            _stats_id: stats_id,
            readers,
            concurrency,
            addresses,
            buckets_remapping: Arc::new(buckets_remapping),
            _second_buckets_log_max: second_buckets_log_max,
            total_file_size,
            _used_hash_bits: file.used_hash_bits,
            data_format: file.out_data_format,
            extra_bucket_data: file.extra_bucket_data,
            allow_online_processing,
        }
    }

    fn flush_rewrite_bucket<
        BucketMode: BucketModeOption,
        MultiplicityMode: MultiplicityModeOption,
    >(
        input_buffer: &mut Packet<ReadsBuffer<F::AssociatedExtraDataWithMultiplicity>>,
        writer: &CompressedBinaryWriter,
        seq_count: &AtomicU64,
        rewrite_buffer: &mut Vec<u8>,
        k: usize,
    ) {
        assert_eq!(rewrite_buffer.len(), 0);

        let mut serializer = CompressedReadsBucketDataSerializer::<
            _,
            <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
            BucketMode,
            MultiplicityMode,
            AssemblerMinimizerPosition,
        >::new(k);

        for DeserializedReadIndependent {
            read,
            extra,
            multiplicity,
            flags,
            minimizer_pos,
            is_window_duplicate,
        } in input_buffer.reads.iter()
        {
            // sequences_count[input_packet.sub_bucket] += 1;

            let element_to_write = CompressedReadsBucketData::new_packed_with_multiplicity(
                read.as_reference(&input_buffer.reads_buffer),
                flags,
                0,
                multiplicity,
                minimizer_pos,
                is_window_duplicate,
            );

            if serializer.get_size(&element_to_write, &extra) + rewrite_buffer.len()
                > rewrite_buffer.capacity()
            {
                writer.write_data(&rewrite_buffer[..]);
                rewrite_buffer.clear();
                serializer.reset();
            }

            serializer.write_to(
                &element_to_write,
                rewrite_buffer,
                &extra,
                &input_buffer.extra_buffer,
            );
        }
        seq_count.fetch_add(input_buffer.reads.len() as u64, Ordering::Relaxed);

        if rewrite_buffer.len() > 0 {
            writer.write_data(&rewrite_buffer[..]);
            rewrite_buffer.clear();
        }

        input_buffer.reset();
    }

    #[instrumenter::track]
    fn read_bucket<const WITH_SECOND_BUCKET: bool, const WITH_MULTIPLICITY: bool>(
        global_context: &KmersTransformContext<F>,
        bucket_info: &BucketsInfo<F>,
        addresses: &[AllocatedAddressMode<F>],
        async_reader_thread: Arc<AsyncReaderThread>,
        packets_pool: Arc<PacketsPool<ReadsBuffer<F::AssociatedExtraDataWithMultiplicity>>>,
        minimizer_size: usize,
    ) {
        if bucket_info.readers.iter().all(|r| r.is_finished()) {
            return;
        }

        let mut buffers = Vec::with_capacity(addresses.len());

        let mut rewrite_buffer = Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes());

        track!(
            {
                for _ in 0..addresses.len() {
                    buffers.push(packets_pool.alloc_packet());
                }
            },
            START_PACKET_ALLOC_COUNTER
        );

        let has_single_addr = addresses.len() == 1;

        for reader in &bucket_info.readers {
            if reader.is_finished() {
                continue;
            }

            let data_format: MinimizerBucketMode = reader.get_data_format_info().unwrap();
            let checkpoint_rewrite_info = Cell::new(None);

            let address_is_rewrite: Vec<_> = addresses
                .iter()
                .map(|a| matches!(a, AllocatedAddressMode::Rewrite(_, _, _)))
                .collect();
            let buckets_remapping_passtrough_check = bucket_info.buckets_remapping.clone();

            helper_read_bucket_with_opt_multiplicity::<
                F::AssociatedExtraData,
                F::AssociatedExtraDataWithMultiplicity,
                F::FLAGS_COUNT,
                BucketModeFromBoolean<USE_SECOND_BUCKET>,
                AssemblerMinimizerPosition,
            >(
                reader,
                async_reader_thread.clone(),
                matches!(data_format, MinimizerBucketMode::Compacted),
                AllowedCheckpointStrategy::AllowPasstrough(Arc::new(move |data| {
                    if let Some(data) = data {
                        address_is_rewrite
                            [buckets_remapping_passtrough_check[data.target_subbucket as usize]]
                    } else {
                        false
                    }
                })),
                |passtrough| {
                    let checkpoint_rewrite_info: ReadsCheckpointData =
                        checkpoint_rewrite_info.get().unwrap();
                    let bucket = checkpoint_rewrite_info.target_subbucket as usize;
                    let sequences_count = checkpoint_rewrite_info.sequences_count;
                    if let AllocatedAddressMode::Rewrite(writer, out_sequences_count, _) =
                        &addresses[bucket_info.buckets_remapping[bucket]]
                    {
                        out_sequences_count.fetch_add(sequences_count as u64, Ordering::Relaxed);
                        writer.set_checkpoint_data::<()>(None, Some(passtrough));
                    } else {
                        unreachable!();
                    }
                },
                |checkpoint_data| {
                    checkpoint_rewrite_info.set(checkpoint_data);
                },
                |read_info, extra_buffer| {
                    let DeserializedRead {
                        read,
                        mut extra,
                        multiplicity,
                        flags,
                        second_bucket,
                        minimizer_pos,
                        is_window_duplicate,
                    } = read_info;

                    let bucket = if has_single_addr {
                        0
                    } else {
                        let orig_bucket = checkpoint_rewrite_info
                            .get()
                            .map(|i| i.target_subbucket)
                            .unwrap_or(if USE_SECOND_BUCKET {
                                second_bucket as u16
                            } else {
                                // F::PreprocessorType::get_rewrite_bucket(
                                //         global_extra_data.get_k(),
                                //         global_extra_data.get_m(),
                                //         &read_info,
                                //         bucket_info.used_hash_bits,
                                //         bucket_info.second_buckets_log_max,
                                // )
                                unimplemented!("Restore the above if needed!");
                            }) as usize;
                        bucket_info.buckets_remapping[orig_bucket]
                    };

                    let ind_read = CompressedReadIndipendent::from_read(
                        &read,
                        &mut buffers[bucket].reads_buffer,
                    );
                    extra = F::AssociatedExtraDataWithMultiplicity::copy_extra_from(
                        extra,
                        extra_buffer,
                        &mut buffers[bucket].extra_buffer,
                    );

                    buffers[bucket]
                        .reads
                        .push::<WITH_MULTIPLICITY>(DeserializedReadIndependent {
                            read: ind_read,
                            extra,
                            flags,
                            multiplicity,
                            minimizer_pos,
                            is_window_duplicate,
                        });

                    let packets_pool = &packets_pool;
                    if buffers[bucket].reads.len() == buffers[bucket].reads.capacity() {
                        match &addresses[bucket] {
                            AllocatedAddressMode::Send(address) => {
                                replace_with_or_abort(&mut buffers[bucket], |mut buffer| {
                                    buffer.reads.minimizer_size = minimizer_size;
                                    buffer.sub_bucket = bucket;
                                    buffer.reads.extra_bucket_data = bucket_info.extra_bucket_data;
                                    address.send_packet(buffer);
                                    track!(packets_pool.alloc_packet(), PACKET_ALLOC_COUNTER)
                                });
                            }
                            AllocatedAddressMode::Rewrite(writer, seq_count, _) => {
                                Self::flush_rewrite_bucket::<
                                    BucketModeFromBoolean<WITH_SECOND_BUCKET>,
                                    MultiplicityModeFromBoolean<WITH_MULTIPLICITY>,
                                >(
                                    &mut buffers[bucket],
                                    writer,
                                    seq_count,
                                    &mut rewrite_buffer,
                                    global_context.k,
                                );
                            }
                        }
                    }
                    F::AssociatedExtraDataWithMultiplicity::clear_temp_buffer(extra_buffer);
                },
                global_context.k,
            );
        }

        for (bucket, (mut packet, address)) in buffers.drain(..).zip(addresses.iter()).enumerate() {
            if packet.reads.len() > 0 {
                packet.reads.minimizer_size = minimizer_size;
                packet.sub_bucket = bucket;
                packet.reads.extra_bucket_data = bucket_info.extra_bucket_data;
                match address {
                    AllocatedAddressMode::Send(address) => {
                        address.send_packet(packet);
                    }
                    AllocatedAddressMode::Rewrite(writer, seq_count, _) => {
                        Self::flush_rewrite_bucket::<
                            BucketModeFromBoolean<WITH_SECOND_BUCKET>,
                            MultiplicityModeFromBoolean<WITH_MULTIPLICITY>,
                        >(
                            &mut packet,
                            writer,
                            seq_count,
                            &mut rewrite_buffer,
                            global_context.k,
                        );
                    }
                }
            }
        }
    }
}

pub(crate) struct TransformReaderContext<F: KmersTransformExecutorFactory> {
    pub resplit_handle: ExecutorsHandle<KmersTransformResplitter<F>>,
    pub process_handle: ExecutorsHandle<KmersTransformProcessor<F>>,
    pub global: Arc<KmersTransformContext<F>>,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformReader<F> {
    type InputPacket = InputBucketDesc;
    type OutputPacket = ReadsBuffer<F::AssociatedExtraDataWithMultiplicity>;
    type GlobalParams = TransformReaderContext<F>;
    type InitData = ExecutorsHandle<Self>;
    const ALLOW_PARALLEL_ADDRESS_EXECUTION: bool = false;

    fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }

    fn executor_main<'a>(
        &'a mut self,
        context: &'a TransformReaderContext<F>,
        mut receiver: ExecutorReceiver<Self>,
    ) {
        let mut async_threads = Vec::new();

        let packets_pool = Arc::new(PacketsPool::new(
            context.global.max_buckets * 2 * MAX_KMERS_TRANSFORM_READERS_PER_BUCKET,
            KMERS_TRANSFORM_READS_CHUNKS_SIZE,
        ));

        while let Ok(address) = track!(receiver.obtain_address(), ADDR_WAITING_COUNTER) {
            let file = track!(address.receive_packet().unwrap(), PACKET_WAITING_COUNTER);
            let is_main_bucket = !file.resplitted && !file.rewritten;
            let is_resplitted = file.resplitted;
            let extra_bucket_data = file.extra_bucket_data;
            let debug_paths = file.paths.clone();
            let mut buckets_info =
                Self::compute_buckets(&context.global, address.get_init_data(), file);

            let addresses: Vec<_> = {
                let mut out_addresses = vec![];
                if buckets_info.allow_online_processing {
                    context.process_handle.create_new_addresses(
                        buckets_info.addresses.iter().filter_map(|a| match a {
                            AddressModeDeclaration::Process(p) => Some(Arc::new(p.clone())),
                            AddressModeDeclaration::Rewrite(..) => None,
                            AddressModeDeclaration::Resplit(_) => unreachable!(),
                        }),
                        &mut out_addresses,
                        Some(context.global.compute_threads_count),
                        false,
                    );
                } else {
                    context.resplit_handle.create_new_addresses(
                        buckets_info.addresses.iter().filter_map(|a| match a {
                            AddressModeDeclaration::Process(_) => unreachable!(),
                            AddressModeDeclaration::Rewrite(..) => None,
                            AddressModeDeclaration::Resplit(r) => Some(Arc::new(r.clone())),
                        }),
                        &mut out_addresses,
                        Some(context.global.compute_threads_count),
                        false,
                    );
                }
                let mut out_addresses = out_addresses.into_iter();

                buckets_info
                    .addresses
                    .drain(..)
                    .map(|addr| match addr {
                        AddressModeDeclaration::Rewrite(x, y, z) => {
                            AllocatedAddressMode::Rewrite(x, y, z)
                        }
                        AddressModeDeclaration::Process(_) | AddressModeDeclaration::Resplit(_) => {
                            AllocatedAddressMode::Send(out_addresses.next().unwrap())
                        }
                    })
                    .collect()
            };

            // FIXME: Better threads management
            while async_threads.len() < buckets_info.concurrency {
                async_threads.push(AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE / 2, 4));
            }

            // ggcat_logging::info!(
            //     "Reading with concurrency: {} and max buckets: {} with addrs: {}",
            //     buckets_info.concurrency,
            //     global_context.max_buckets,
            //     buckets_info.addresses.len()
            // );

            let minimizer_size = if is_resplitted {
                context.global.global_extra_data.get_m_resplit()
            } else {
                context.global.global_extra_data.get_m()
            };

            let address = &address;
            let buckets_info = &buckets_info;

            address.spawn_executors(buckets_info.concurrency, |ex_idx| {
                let async_thread = async_threads[ex_idx].clone();

                match buckets_info.data_format {
                    MinimizerBucketMode::Single => {
                        Self::read_bucket::<USE_SECOND_BUCKET, false>(
                            &context.global,
                            buckets_info,
                            &addresses,
                            async_thread,
                            packets_pool.clone(),
                            minimizer_size,
                        );
                    }
                    MinimizerBucketMode::Compacted => {
                        Self::read_bucket::<false, true>(
                            &context.global,
                            buckets_info,
                            &addresses,
                            async_thread,
                            packets_pool.clone(),
                            minimizer_size,
                        );
                    }
                }
            });

            for addr in addresses {
                if let AllocatedAddressMode::Rewrite(writer, seq_count, init_data) = addr {
                    let new_bucket_address = address
                        .get_init_data()
                        .create_new_address(Arc::new(address.get_init_data().clone()), true);

                    context
                        .global
                        .rewritten_buckets_count
                        .fetch_add(1, Ordering::Relaxed);

                    new_bucket_address.send_packet(Packet::new_simple(InputBucketDesc {
                        paths: {
                            let path = writer.get_path();
                            writer.finalize();
                            vec![path]
                        },
                        sub_bucket_counters: vec![BucketCounter {
                            count: seq_count.into_inner(),
                        }],
                        compaction_delta: 0,
                        resplitted: false,
                        rewritten: true,
                        used_hash_bits: init_data.used_hash_bits + init_data.buckets_hash_bits,
                        out_data_format: init_data.data_format,
                        extra_bucket_data,
                    }));
                }
            }

            if is_main_bucket {
                context
                    .global
                    .processed_buckets_count
                    .fetch_add(1, Ordering::Relaxed);
                context
                    .global
                    .processed_buckets_size
                    .fetch_add(buckets_info.total_file_size, Ordering::Relaxed);
            } else if is_resplitted {
                context
                    .global
                    .processed_extra_buckets_count
                    .fetch_add(1, Ordering::Relaxed);
                context
                    .global
                    .processed_extra_buckets_size
                    .fetch_add(buckets_info.total_file_size, Ordering::Relaxed);
            }

            assert!(track!(
                address
                    .receive_packet()
                    .map(|r| { println!("R: {:?} vs first: {:?}", r.paths, debug_paths) })
                    .is_none(),
                PACKET_WAITING_COUNTER
            ));
        }
    }
}
