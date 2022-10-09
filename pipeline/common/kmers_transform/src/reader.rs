use crate::processor::{KmersProcessorInitData, KmersTransformProcessor};
use crate::reads_buffer::ReadsBuffer;
use crate::resplitter::{KmersTransformResplitter, ResplitterInitData};
use crate::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformMapProcessor,
    KmersTransformPreprocessor,
};
use config::{
    get_memory_mode, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MAXIMUM_JIT_PROCESSED_BUCKETS, MAX_INTERMEDIATE_MAP_SIZE, MIN_BUCKET_CHUNKS_FOR_READING_THREAD,
    PACKETS_PRIORITY_DEFAULT, PACKETS_PRIORITY_REWRITTEN, PARTIAL_VECS_CHECKPOINT_SIZE,
    USE_SECOND_BUCKET,
};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use minimizer_bucketing::counters_analyzer::BucketCounter;
use minimizer_bucketing::MinimizerBucketingExecutorFactory;
use parallel_processor::buckets::bucket_writer::BucketItem;
use parallel_processor::buckets::readers::async_binary_reader::{
    AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::counter_stats::counter::{AtomicCounter, SumMode};
use parallel_processor::counter_stats::declare_counter_i64;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::{PoolObject, PoolObjectTrait};
use parallel_processor::execution_manager::packet::{Packet, PacketTrait, PacketsPool};
use parallel_processor::memory_fs::RemoveFileMode;
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

pub struct KmersTransformReader<F: KmersTransformExecutorFactory> {
    _phantom: PhantomData<F>,
}

pub struct InputBucketDesc {
    pub(crate) path: PathBuf,
    pub(crate) sub_bucket_counters: Vec<BucketCounter>,
    pub(crate) resplitted: bool,
    pub(crate) rewritten: bool,
    pub(crate) used_hash_bits: usize,
}

impl PoolObjectTrait for InputBucketDesc {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            path: PathBuf::new(),
            sub_bucket_counters: Vec::new(),
            resplitted: false,
            rewritten: false,
            used_hash_bits: 0,
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
}

enum AddressMode {
    Send(ExecutorAddress),
    Rewrite(CompressedBinaryWriter, AtomicU64, RewriterInitData),
}

struct BucketsInfo {
    reader: AsyncBinaryReader,
    concurrency: usize,
    addresses: Vec<AddressMode>,
    register_addresses: Vec<ExecutorAddress>,
    buckets_remapping: Vec<usize>,
    second_buckets_log_max: usize,
    file_size: usize,
    used_hash_bits: usize,
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

        let reader = AsyncBinaryReader::new(
            &file.path,
            true,
            RemoveFileMode::Remove {
                remove_fs: file.rewritten || !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        );

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

        let file_size = reader.get_file_size();

        bucket_sizes.make_contiguous().sort();

        let mut has_outliers = false;

        let total_sequences = global_context.total_sequences.load(Ordering::Relaxed);
        let unique_kmers = global_context.unique_kmers.load(Ordering::Relaxed);

        let unique_estimator_factor = if total_sequences > 0 {
            unique_kmers as f64 / total_sequences as f64
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
            //     println!(
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
                // println!("Sub-bucket {} is an outlier with size {}!", index, count.0);
                let new_address =
                    KmersTransformResplitter::<F>::generate_new_address(ResplitterInitData {
                        bucket_size: count.0 as usize,
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
                            bucket_path: file.path.clone(),
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
                            DEFAULT_LZ4_COMPRESSION_LEVEL,
                        ),
                        SUBSPLIT_INDEX.fetch_add(1, Ordering::Relaxed),
                    );

                    Some(AddressMode::Rewrite(
                        writer,
                        AtomicU64::new(0),
                        RewriterInitData {
                            buckets_hash_bits: second_buckets_max.ilog2() as usize,
                            used_hash_bits: file.used_hash_bits,
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
            reader.get_chunks_count() / MIN_BUCKET_CHUNKS_FOR_READING_THREAD,
        );

        let concurrency = min(
            min(4, global_context.read_threads_count),
            min(addr_concurrency, chunks_concurrency),
        );

        //     println!(
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
            reader,
            concurrency,
            addresses,
            register_addresses,
            buckets_remapping,
            second_buckets_log_max,
            file_size,
            used_hash_bits: file.used_hash_bits,
        }
    }

    fn flush_rewrite_bucket(
        input_buffer: &mut Packet<ReadsBuffer<F::AssociatedExtraData>>,
        writer: &CompressedBinaryWriter,
        seq_count: &AtomicU64,
        rewrite_buffer: &mut Vec<u8>,
    ) {
        assert_eq!(rewrite_buffer.len(), 0);
        for (flags, extra, bases) in &input_buffer.reads {
            // sequences_count[input_packet.sub_bucket] += 1;

            let element_to_write = CompressedReadsBucketHelper::<
                _,
                <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
                false,
            >::new_packed(
                bases.as_reference(&input_buffer.reads_buffer), *flags, 0
            );

            if element_to_write.get_size(extra) + rewrite_buffer.len() > rewrite_buffer.capacity() {
                writer.write_data(&rewrite_buffer[..]);
                rewrite_buffer.clear();
            }

            element_to_write.write_to(rewrite_buffer, extra, &input_buffer.extra_buffer);
        }
        seq_count.fetch_add(input_buffer.reads.len() as u64, Ordering::Relaxed);

        if rewrite_buffer.len() > 0 {
            writer.write_data(&rewrite_buffer[..]);
            rewrite_buffer.clear();
        }

        input_buffer.reset();
    }

    #[instrumenter::track]
    async fn read_bucket(
        global_context: &KmersTransformContext<F>,
        ops: &ExecutorAddressOperations<'_, Self>,
        bucket_info: &BucketsInfo,
        async_reader_thread: Arc<AsyncReaderThread>,
        packets_pool: Arc<PoolObject<PacketsPool<ReadsBuffer<F::AssociatedExtraData>>>>,
    ) {
        if bucket_info.reader.is_finished() {
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

        let preprocessor = F::new_preprocessor(&global_context.global_extra_data);

        let global_extra_data = &global_context.global_extra_data;

        let has_single_addr = bucket_info.addresses.len() == 1;

        let mut items_iterator = bucket_info
            .reader
            .get_items_stream::<CompressedReadsBucketHelper<
                F::AssociatedExtraData,
                F::FLAGS_COUNT,
                { USE_SECOND_BUCKET },
            >>(
                async_reader_thread.clone(),
                Vec::new(),
                F::AssociatedExtraData::new_temp_buffer(),
            );

        while let Some((read_info, extra_buffer)) = items_iterator.next() {
            let bucket = if has_single_addr {
                0
            } else {
                let orig_bucket = preprocessor.get_sequence_bucket(
                    global_extra_data,
                    &read_info,
                    bucket_info.used_hash_bits,
                    bucket_info.second_buckets_log_max,
                ) as usize;

                bucket_info.buckets_remapping[orig_bucket]
            };

            let (flags, _second_bucket, mut extra_data, read) = read_info;

            let ind_read =
                CompressedReadIndipendent::from_read(&read, &mut buffers[bucket].reads_buffer);
            extra_data = F::AssociatedExtraData::copy_extra_from(
                extra_data,
                extra_buffer,
                &mut buffers[bucket].extra_buffer,
            );

            buffers[bucket].reads.push((flags, extra_data, ind_read));

            let packets_pool = &packets_pool;
            if buffers[bucket].reads.len() == buffers[bucket].reads.capacity() {
                match &bucket_info.addresses[bucket] {
                    AddressMode::Send(address) => {
                        replace_with_async(&mut buffers[bucket], |mut buffer| async move {
                            buffer.sub_bucket = bucket;
                            ops.packet_send(address.clone(), buffer);
                            track!(packets_pool.alloc_packet().await, PACKET_ALLOC_COUNTER)
                        })
                        .await;
                    }
                    AddressMode::Rewrite(writer, seq_count, _) => {
                        Self::flush_rewrite_bucket(
                            &mut buffers[bucket],
                            writer,
                            seq_count,
                            &mut rewrite_buffer,
                        );
                    }
                }
            }
            F::AssociatedExtraData::clear_temp_buffer(extra_buffer);
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
                        ops.packet_send(address.clone(), packet);
                    }
                    AddressMode::Rewrite(writer, seq_count, _) => {
                        Self::flush_rewrite_bucket(
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

    type AsyncExecutorFuture<'a> = impl Future<Output = ()> + 'a;

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
    ) -> Self::AsyncExecutorFuture<'a> {
        async move {
            let mut async_threads = Vec::new();

            while let Ok((address, _)) =
                track!(receiver.obtain_address().await, ADDR_WAITING_COUNTER)
            {
                let file = track!(
                    address.receive_packet().await.unwrap(),
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

                // println!(
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
                        .pool_alloc_await(max(
                            global_context.max_buckets / 2,
                            2 * buckets_info.addresses.len(),
                        ))
                        .await;

                    spawner.spawn_executor(async move {
                        Self::read_bucket(
                            global_context,
                            address,
                            buckets_info,
                            async_thread,
                            packets_pool,
                        )
                        .await;
                    });
                }

                drop(reader_lock);
                spawner.executors_await().await;
                drop(spawner);

                for addr in buckets_info.addresses {
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
                                path: {
                                    let path = writer.get_path();
                                    writer.finalize();
                                    path
                                },
                                sub_bucket_counters: vec![BucketCounter {
                                    count: seq_count.into_inner(),
                                }],
                                resplitted: false,
                                rewritten: true,
                                used_hash_bits: init_data.used_hash_bits
                                    + init_data.buckets_hash_bits,
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
                        .fetch_add(buckets_info.file_size, Ordering::Relaxed);
                } else if is_resplitted {
                    global_context
                        .processed_extra_buckets_count
                        .fetch_add(1, Ordering::Relaxed);
                    global_context
                        .processed_extra_buckets_size
                        .fetch_add(buckets_info.file_size, Ordering::Relaxed);
                }

                assert!(track!(
                    address.receive_packet().await.is_none(),
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
