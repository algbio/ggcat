use crate::processor::{KmersProcessorInitData, KmersTransformProcessor};
use crate::reads_buffer::ReadsBuffer;
use crate::resplitter::{KmersTransformResplitter, ResplitterInitData};
use crate::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformGlobalExtraData,
    KmersTransformMapProcessor,
};
use config::{
    get_compression_level_info, get_memory_mode, SwapPriority, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MAXIMUM_JIT_PROCESSED_BUCKETS, MAX_INTERMEDIATE_MAP_SIZE, MIN_BUCKET_CHUNKS_FOR_READING_THREAD,
    PACKETS_PRIORITY_DEFAULT, PACKETS_PRIORITY_REWRITTEN, PARTIAL_VECS_CHECKPOINT_SIZE,
    PRIORITY_SCHEDULING_BASE, PRIORITY_SCHEDULING_LOW, USE_SECOND_BUCKET, WORKERS_PRIORITY_BASE,
};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::creads_utils::{
    BucketModeFromBoolean, CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
    MultiplicityModeFromBoolean, MultiplicityModeOption, NoSecondBucket,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::creads_helper;
use minimizer_bucketing::counters_analyzer::BucketCounter;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;
use minimizer_bucketing::{MinimizerBucketMode, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::readers::async_binary_reader::{
    AllowedCheckpointStrategy, AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::{PoolObject, PoolObjectTrait};
use parallel_processor::execution_manager::packet::{Packet, PacketTrait, PacketsPool};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, SumMode};
use parallel_processor::mt_debug_counters::declare_counter_i64;
use parallel_processor::scheduler::{PriorityScheduler, ThreadPriorityHandle};
use parallel_processor::utils::replace_with_async::replace_with_async;
use std::cmp::{max, min, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
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
        }
    }

    fn reset(&mut self) {
        self.resplitted = false;
        self.sub_bucket_counters.clear();
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
    pub buckets_hash_bits: usize,
    pub used_hash_bits: usize,
    pub data_format: MinimizerBucketMode,
}

enum AddressMode {
    Send(ExecutorAddress),
    Rewrite(CompressedBinaryWriter, AtomicU64, RewriterInitData),
}

struct BucketsInfo {
    readers: Vec<AsyncBinaryReader>,
    concurrency: usize,
    addresses: Arc<Vec<AddressMode>>,
    register_addresses: Vec<ExecutorAddress>,
    buckets_remapping: Arc<Vec<usize>>,
    second_buckets_log_max: usize,
    total_file_size: usize,
    used_hash_bits: usize,
    data_format: MinimizerBucketMode,
}

impl<F: KmersTransformExecutorFactory> KmersTransformReader<F> {
    fn compute_buckets(
        global_context: &KmersTransformContext<F>,
        file: Packet<InputBucketDesc>,
    ) -> BucketsInfo {
        let second_buckets_log_max = min(
            file.sub_bucket_counters.len().ilog2() as usize,
            global_context.max_second_buckets_count_log2,
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

            let is_outlier = !file.resplitted
                && (total_sequences > 0)
                && (biggest_sub_bucket.0.count as f64 * unique_estimator_factor
                    >= (MAX_INTERMEDIATE_MAP_SIZE / F::MapProcessorType::MAP_SIZE as u64) as f64);

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
                && smallest_bucket.0 .0 > 0
                && (biggest_sub_bucket.0.count + smallest_bucket.0 .0) as f64
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
            smallest_bucket.0 .0 += biggest_sub_bucket.0.count;
            smallest_bucket.2 |= is_outlier;
            has_outliers |= is_outlier;
            buckets_remapping[biggest_sub_bucket.1] = smallest_bucket.1;
            queue.push(smallest_bucket);
        }

        let mut addresses: Vec<_> = (0..queue.len()).map(|_| None).collect();
        let mut register_addresses = Vec::new();
        let mut dbg_counters: Vec<_> = vec![0; queue.len()];

        let allow_online_processing = !has_outliers && queue.len() <= MAXIMUM_JIT_PROCESSED_BUCKETS;

        for (count, index, outlier) in queue.into_iter() {
            dbg_counters[index] = count.0;
            addresses[index] = if outlier {
                // ggcat_logging::info!("Sub-bucket {} is an outlier with size {}!", index, count.0);
                let new_address =
                    KmersTransformResplitter::<F>::generate_new_address(ResplitterInitData {
                        bucket_size: count.0 as usize,
                        data_format: file.out_data_format,
                    });
                register_addresses.push(new_address.clone());
                Some(AddressMode::Send(new_address))
            } else {
                if allow_online_processing {
                    let new_address = KmersTransformProcessor::<F>::generate_new_address(
                        KmersProcessorInitData {
                            sequences_count: count.0 as usize,
                            sub_bucket: index,
                            is_resplitted: file.resplitted,
                            bucket_paths: file.paths.clone(),
                        },
                    );
                    register_addresses.push(new_address.clone());
                    Some(AddressMode::Send(new_address))
                } else {
                    static SUBSPLIT_INDEX: AtomicUsize = AtomicUsize::new(0);

                    let writer = CompressedBinaryWriter::new(
                        &global_context.temp_dir.join(&format!("bucket-rewrite-",)),
                        &(
                            get_memory_mode(SwapPriority::ResultBuckets),
                            PARTIAL_VECS_CHECKPOINT_SIZE,
                            get_compression_level_info(),
                        ),
                        SUBSPLIT_INDEX.fetch_add(1, Ordering::Relaxed),
                        &file.out_data_format,
                    );

                    Some(AddressMode::Rewrite(
                        writer,
                        AtomicU64::new(0),
                        RewriterInitData {
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
            min(4, global_context.read_threads_count),
            min(addr_concurrency, chunks_concurrency),
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
            readers,
            concurrency,
            addresses: Arc::new(addresses),
            register_addresses,
            buckets_remapping: Arc::new(buckets_remapping),
            second_buckets_log_max,
            total_file_size,
            used_hash_bits: file.used_hash_bits,
            data_format: file.out_data_format,
        }
    }

    fn flush_rewrite_bucket<MultiplicityMode: MultiplicityModeOption>(
        input_buffer: &mut Packet<ReadsBuffer<F::AssociatedExtraData>>,
        writer: &CompressedBinaryWriter,
        seq_count: &AtomicU64,
        rewrite_buffer: &mut Vec<u8>,
    ) {
        assert_eq!(rewrite_buffer.len(), 0);

        let mut serializer = CompressedReadsBucketDataSerializer::<
            _,
            <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
            NoSecondBucket,
            MultiplicityMode,
        >::new();

        for (flags, extra, bases, multiplicity) in input_buffer.reads.iter() {
            // sequences_count[input_packet.sub_bucket] += 1;

            let element_to_write = CompressedReadsBucketData::new_packed_with_multiplicity(
                bases.as_reference(&input_buffer.reads_buffer),
                flags,
                0,
                multiplicity,
            );

            if serializer.get_size(&element_to_write, extra) + rewrite_buffer.len()
                > rewrite_buffer.capacity()
            {
                writer.write_data(&rewrite_buffer[..]);
                rewrite_buffer.clear();
                serializer.reset();
            }

            serializer.write_to(
                &element_to_write,
                rewrite_buffer,
                extra,
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
    async fn read_bucket<const WITH_MULTIPLICITY: bool>(
        global_context: &KmersTransformContext<F>,
        ops: &ExecutorAddressOperations<'_, Self>,
        bucket_info: &BucketsInfo,
        async_reader_thread: Arc<AsyncReaderThread>,
        packets_pool: Arc<PoolObject<PacketsPool<ReadsBuffer<F::AssociatedExtraData>>>>,
        thread_handle: &ThreadPriorityHandle,
    ) {
        if bucket_info.readers.iter().all(|r| r.is_finished()) {
            return;
        }

        let mut buffers = Vec::with_capacity(bucket_info.addresses.len());

        let mut rewrite_buffer = Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes());

        track!(
            {
                for _ in 0..bucket_info.addresses.len() {
                    buffers.push(packets_pool.alloc_packet().await);
                }
            },
            START_PACKET_ALLOC_COUNTER
        );

        let global_extra_data = &global_context.global_extra_data;

        let has_single_addr = bucket_info.addresses.len() == 1;

        for reader in &bucket_info.readers {
            if reader.is_finished() {
                continue;
            }

            let data_format: MinimizerBucketMode = reader.get_data_format_info().unwrap();
            let mut checkpoint_rewrite_info;

            let addresses_passtrough_check = bucket_info.addresses.clone();
            let buckets_remapping_passtrough_check = bucket_info.buckets_remapping.clone();

            creads_helper! {
                helper_read_bucket_with_opt_multiplicity::<
                    F::AssociatedExtraData,
                    F::FLAGS_COUNT,
                    BucketModeFromBoolean<USE_SECOND_BUCKET>
                >(
                    reader,
                    async_reader_thread.clone(),
                    matches!(data_format, MinimizerBucketMode::Compacted),
                    AllowedCheckpointStrategy::AllowPasstrough(Arc::new(move |data| {
                        if let Some(data) = data {
                            matches!(&addresses_passtrough_check[buckets_remapping_passtrough_check[data.target_subbucket as usize]], AddressMode::Rewrite(_, _, _))
                        } else {
                            false
                        }
                    })),
                    |passtrough| {
                        let bucket = checkpoint_rewrite_info.unwrap().target_subbucket as usize;
                        let sequences_count = checkpoint_rewrite_info.unwrap().sequences_count;
                        if let AddressMode::Rewrite(writer, out_sequences_count, _) = &bucket_info.addresses[bucket_info.buckets_remapping[bucket]] {
                            out_sequences_count.fetch_add(sequences_count as u64, Ordering::Relaxed);
                            writer.set_checkpoint_data::<()>(None, Some(passtrough));
                        } else {
                            unreachable!();
                        }
                    },
                    |checkpoint_data| { checkpoint_rewrite_info = checkpoint_data; } ,
                    |read_info, extra_buffer| {
                        let bucket = if has_single_addr {
                            0
                        } else {
                            let orig_bucket = checkpoint_rewrite_info.map(|i| i.target_subbucket)
                            .unwrap_or_else(|| F::PreprocessorType::get_rewrite_bucket(
                                global_extra_data.get_k(),
                                global_extra_data.get_m(),
                                &read_info,
                                bucket_info.used_hash_bits,
                                bucket_info.second_buckets_log_max,
                            )) as usize;

                            bucket_info.buckets_remapping[orig_bucket]
                        };

                        let (flags, _second_bucket, mut extra_data, read, multiplicity) = read_info;

                        let ind_read = CompressedReadIndipendent::from_read(
                            &read,
                            &mut buffers[bucket].reads_buffer,
                        );
                        extra_data = F::AssociatedExtraData::copy_extra_from(
                            extra_data,
                            extra_buffer,
                            &mut buffers[bucket].extra_buffer,
                        );

                        buffers[bucket]
                            .reads
                            .push::<WITH_MULTIPLICITY>(ind_read, extra_data, flags, multiplicity);

                        let packets_pool = &packets_pool;
                        if buffers[bucket].reads.len() == buffers[bucket].reads.capacity() {
                            match &bucket_info.addresses[bucket] {
                                AddressMode::Send(address) => {
                                    replace_with_async(&mut buffers[bucket], |mut buffer| async {
                                        buffer.sub_bucket = bucket;
                                        ops.packet_send(address.clone(), buffer, thread_handle);
                                        track!(packets_pool.alloc_packet().await, PACKET_ALLOC_COUNTER)
                                    })
                                    .await;
                                }
                                AddressMode::Rewrite(writer, seq_count, _) => {
                                    Self::flush_rewrite_bucket::<MultiplicityModeFromBoolean<WITH_MULTIPLICITY>>(
                                        &mut buffers[bucket],
                                        writer,
                                        seq_count,
                                        &mut rewrite_buffer,
                                    );
                                }
                            }
                        }
                        F::AssociatedExtraData::clear_temp_buffer(extra_buffer);
                    },
                    thread_handle
                );
            }
        }

        for (bucket, (mut packet, address)) in buffers
            .drain(..)
            .zip(bucket_info.addresses.iter())
            .enumerate()
        {
            if packet.reads.len() > 0 {
                packet.sub_bucket = bucket;
                match address {
                    AddressMode::Send(address) => {
                        ops.packet_send(address.clone(), packet, &thread_handle);
                    }
                    AddressMode::Rewrite(writer, seq_count, _) => {
                        Self::flush_rewrite_bucket::<MultiplicityModeFromBoolean<WITH_MULTIPLICITY>>(
                            &mut packet,
                            writer,
                            seq_count,
                            &mut rewrite_buffer,
                        );
                    }
                }
            }
        }
    }
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformReader<F> {
    type InputPacket = InputBucketDesc;
    type OutputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type GlobalParams = KmersTransformContext<F>;
    type InitData = ();

    fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_context: &'a KmersTransformContext<F>,
        mut receiver: ExecutorReceiver<Self>,
        _memory_tracker: MemoryTracker<Self>,
    ) -> impl Future<Output = ()> + 'a {
        async move {
            let mut async_threads = Vec::new();

            let thread_handle = PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_BASE);

            while let Ok((address, _)) = track!(
                receiver
                    .obtain_address_with_priority(WORKERS_PRIORITY_BASE, &thread_handle)
                    .await,
                ADDR_WAITING_COUNTER
            ) {
                let file = track!(
                    address.receive_packet(&thread_handle).await.unwrap(),
                    PACKET_WAITING_COUNTER
                );
                let is_main_bucket = !file.resplitted && !file.rewritten;
                let is_resplitted = file.resplitted;
                let buckets_info = Self::compute_buckets(global_context, file);

                let reader_lock = global_context.reader_init_lock.lock().await;

                address.declare_addresses(
                    buckets_info.register_addresses.clone(),
                    PACKETS_PRIORITY_DEFAULT,
                );

                // FIXME: Better threads management
                while async_threads.len() < buckets_info.concurrency {
                    async_threads.push(AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE / 2, 4));
                }

                let mut spawner = address.make_spawner();

                // ggcat_logging::info!(
                //     "Reading with concurrency: {} and max buckets: {} with addrs: {}",
                //     buckets_info.concurrency,
                //     global_context.max_buckets,
                //     buckets_info.addresses.len()
                // );

                for ex_idx in 0..buckets_info.concurrency {
                    let async_thread = async_threads[ex_idx].clone();

                    let address = &address;
                    let buckets_info = &buckets_info;
                    let packets_pool = address
                        .pool_alloc_await(
                            max(
                                global_context.max_buckets / 2,
                                2 * buckets_info.addresses.len(),
                            ),
                            &thread_handle,
                        )
                        .await;

                    spawner.spawn_executor(async move {
                        let thread_handle =
                            PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_LOW);
                        match buckets_info.data_format {
                            MinimizerBucketMode::Single => {
                                Self::read_bucket::<false>(
                                    global_context,
                                    address,
                                    buckets_info,
                                    async_thread,
                                    packets_pool,
                                    &thread_handle,
                                )
                                .await;
                            }
                            MinimizerBucketMode::Compacted => {
                                Self::read_bucket::<true>(
                                    global_context,
                                    address,
                                    buckets_info,
                                    async_thread,
                                    packets_pool,
                                    &thread_handle,
                                )
                                .await;
                            }
                        }
                    });
                }

                drop(reader_lock);
                spawner.executors_await().await;
                drop(spawner);

                for addr in Arc::try_unwrap(buckets_info.addresses)
                    .map_err(|_| ())
                    .unwrap()
                {
                    if let AddressMode::Rewrite(writer, seq_count, init_data) = addr {
                        let new_bucket_address =
                            KmersTransformReader::<F>::generate_new_address(());

                        global_context
                            .rewritten_buckets_count
                            .fetch_add(1, Ordering::Relaxed);

                        address.declare_addresses(
                            vec![new_bucket_address.clone()],
                            PACKETS_PRIORITY_REWRITTEN,
                        );
                        address.get_context().send_packet(
                            new_bucket_address,
                            Packet::new_simple(InputBucketDesc {
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
                                used_hash_bits: init_data.used_hash_bits
                                    + init_data.buckets_hash_bits,
                                out_data_format: init_data.data_format,
                            }),
                        );
                    }
                }

                if is_main_bucket {
                    global_context
                        .processed_buckets_count
                        .fetch_add(1, Ordering::Relaxed);
                    global_context
                        .processed_buckets_size
                        .fetch_add(buckets_info.total_file_size, Ordering::Relaxed);
                } else if is_resplitted {
                    global_context
                        .processed_extra_buckets_count
                        .fetch_add(1, Ordering::Relaxed);
                    global_context
                        .processed_extra_buckets_size
                        .fetch_add(buckets_info.total_file_size, Ordering::Relaxed);
                }

                assert!(track!(
                    address.receive_packet(&thread_handle).await.is_none(),
                    PACKET_WAITING_COUNTER
                ));
            }
        }
    }
}

//
//     const MEMORY_FIELDS_COUNT: usize = 0;
//     const MEMORY_FIELDS: &'static [&'static str] = &[];
//
//     type BuildParams = (AsyncBinaryReader, usize, Vec<usize>, Vec<ExecutorAddress>);

//     fn allocate_new_group<E: ExecutorOperations<Self>>(
//         global_params: Arc<KmersTransformContext<F>>,
//         _memory_params: Option<Self::MemoryParams>,
//         packet: Option<Packet<InputBucketDesc>>,
//         mut ops: E,
//     ) -> (Self::BuildParams, usize) {
//     }
//
//     fn required_pool_items(&self) -> u64 {
//         1
//     }
//
//     fn pre_execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         (reader, second_buckets_log_max, remappings, addresses): Self::BuildParams,
//         mut ops: E,
//     ) {
//     }
//
//     fn finalize<E: ExecutorOperations<Self>>(&mut self, _ops: E) {
//         assert_eq!(buffers.len(), 0);
//     }
