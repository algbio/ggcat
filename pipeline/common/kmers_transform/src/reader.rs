use crate::processor::KmersTransformProcessor;
use crate::reads_buffer::ReadsBuffer;
use crate::resplitter::KmersTransformResplitter;
use crate::{KmersTransformContext, KmersTransformExecutorFactory, KmersTransformPreprocessor};
use config::{
    DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MIN_BUCKET_CHUNKS_FOR_READING_THREAD, USE_SECOND_BUCKET,
};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use minimizer_bucketing::counters_analyzer::BucketCounter;
use parallel_processor::buckets::readers::async_binary_reader::{
    AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::utils::replace_with_async::replace_with_async;
use replace_with::replace_with_or_abort;
use std::cmp::{max, min, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct KmersTransformReader<F: KmersTransformExecutorFactory> {
    _phantom: PhantomData<F>,
}

pub struct InputBucketDesc {
    pub(crate) path: PathBuf,
    pub(crate) sub_bucket_counters: Vec<BucketCounter>,
    pub(crate) resplitted: bool,
}

impl PoolObjectTrait for InputBucketDesc {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            path: PathBuf::new(),
            sub_bucket_counters: Vec::new(),
            resplitted: false,
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

// impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformReader<F> {
//     type InitData = (Arc<KmersTransformContext<F>>, MemoryTracker<Self>);
//
//     fn allocate_new((context, memory_tracker): &Self::InitData) -> Self {
//         Self {
//             context: context.clone(),
//             _mem_tracker: memory_tracker.clone(),
//             buffers: vec![],
//             addresses: vec![],
//             preprocessor: F::new_preprocessor(&context.global_extra_data),
//             async_reader: None,
//             _phantom: Default::default(),
//         }
//     }
//
//     fn reset(&mut self) {
//         buffers.clear();
//         bucket_info.addresses.clear();
//         self.async_reader.take();
//     }
// }
struct BucketsInfo {
    reader: AsyncBinaryReader,
    concurrency: usize,
    addresses: Vec<ExecutorAddress>,
    buckets_remapping: Vec<usize>,
    second_buckets_log_max: usize,
}

impl<F: KmersTransformExecutorFactory> KmersTransformReader<F> {
    fn compute_buckets(
        global_context: &KmersTransformContext<F>,
        file: Packet<InputBucketDesc>,
    ) -> BucketsInfo {
        let second_buckets_log_max = global_context.max_second_buckets_count_log2;

        let reader = AsyncBinaryReader::new(
            &file.path,
            true,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        );

        let second_buckets_max = 1 << second_buckets_log_max;

        let mut buckets_remapping = vec![0; second_buckets_max];

        let mut queue = BinaryHeap::new();
        queue.push((Reverse(0), 0, false));

        let mut sequences_count = 0;

        let mut bucket_sizes: VecDeque<_> = (0..min(
            file.sub_bucket_counters.len(),
            (1 << second_buckets_log_max),
        ))
            .map(|i| {
                sequences_count += file.sub_bucket_counters[i].count;
                (file.sub_bucket_counters[i].clone(), i)
            })
            .collect();

        let file_size = reader.get_file_size();

        let sequences_size_ratio =
            file_size as f64 / (sequences_count * global_context.k as u64) as f64 * 2.67;

        bucket_sizes.make_contiguous().sort();

        let unique_estimator_factor = (sequences_size_ratio * sequences_size_ratio).min(1.0);

        while bucket_sizes.len() > 0 {
            let buckets_count = queue.len();
            let mut smallest_bucket = queue.pop().unwrap();

            let biggest_sub_bucket = bucket_sizes.pop_back().unwrap();

            let uniq_estimate_count =
                (unique_estimator_factor * (biggest_sub_bucket.0.count as f64)) as u64;

            // Alloc a new bucket
            if (smallest_bucket.2 == biggest_sub_bucket.0.is_outlier)
                && smallest_bucket.0 .0 > 0
                && smallest_bucket.0 .0 + uniq_estimate_count > global_context.min_bucket_size
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
            smallest_bucket.0 .0 += uniq_estimate_count;
            smallest_bucket.2 |= biggest_sub_bucket.0.is_outlier;
            buckets_remapping[biggest_sub_bucket.1] = smallest_bucket.1;
            queue.push(smallest_bucket);
        }

        let mut addresses: Vec<_> = vec![None; queue.len()];
        let mut dbg_counters: Vec<_> = vec![0; queue.len()];

        for (count, index, outlier) in queue.into_iter() {
            dbg_counters[index] = count.0;
            addresses[index] = Some(if outlier {
                println!("Sub-bucket {} is an outlier with size {}!", index, count.0);
                KmersTransformResplitter::<F>::generate_new_address()
            } else {
                KmersTransformProcessor::<F>::generate_new_address()
            });
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

        println!(
            "File:{}\nChunks {} concurrency: {} REMAPPINGS: {:?} // {:?} // {:?} RATIO: {:.2} ADDR_COUNT: {}",
            file.path.display(),
            reader.get_chunks_count(),
            concurrency,
            &buckets_remapping,
            dbg_counters,
            file.sub_bucket_counters
                .iter()
                .map(|x| x.count)
                .collect::<Vec<_>>(),
            sequences_size_ratio,
            addresses.len()
        );

        BucketsInfo {
            reader,
            concurrency,
            addresses,
            buckets_remapping,
            second_buckets_log_max,
        }
    }

    async fn read_bucket(
        global_context: &KmersTransformContext<F>,
        ops: &ExecutorAddressOperations<'_, Self>,
        bucket_info: &BucketsInfo,
        buffers: &mut Vec<Packet<ReadsBuffer<F::AssociatedExtraData>>>,
        async_reader_thread: Arc<AsyncReaderThread>,
    ) {
        if bucket_info.reader.is_finished() {
            return;
        }

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
                bucket_info.buckets_remapping[preprocessor
                    .get_sequence_bucket(global_extra_data, &read_info)
                    as usize
                    % (1 << bucket_info.second_buckets_log_max)]
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

            if buffers[bucket].reads.len() == buffers[bucket].reads.capacity() {
                replace_with_async(&mut buffers[bucket], |buffer| async move {
                    ops.packet_send(bucket_info.addresses[bucket].clone(), buffer);
                    ops.packet_alloc().await
                })
                .await;
            }
            F::AssociatedExtraData::clear_temp_buffer(extra_buffer);
        }

        for (packet, address) in buffers.drain(..).zip(bucket_info.addresses.iter()) {
            if packet.reads.len() > 0 {
                ops.packet_send(address.clone(), packet);
            }
        }
    }
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformReader<F> {
    type InputPacket = InputBucketDesc;
    type OutputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type GlobalParams = KmersTransformContext<F>;
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
        memory_tracker: MemoryTracker<Self>,
    ) -> Self::AsyncExecutorFuture<'a> {
        async move {
            let mut async_threads = Vec::new();

            while let Ok(address) = receiver.obtain_address().await {
                let file = address.receive_packet().await.unwrap();
                let buckets_info = Self::compute_buckets(global_context, file);

                address.declare_addresses(buckets_info.addresses.clone());

                // FIXME: Better threads management
                while async_threads.len() < buckets_info.concurrency {
                    async_threads.push(AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE / 2, 4));
                }

                let mut spawner = address.make_spawner();

                for ex_idx in 0..buckets_info.concurrency {
                    let mut buffers = Vec::with_capacity(global_context.buckets_count);
                    for i in 0..global_context.buckets_count {
                        buffers.push(address.packet_alloc().await);
                    }

                    let async_thread = async_threads[ex_idx].clone();

                    let address = &address;
                    let buckets_info = &buckets_info;

                    spawner.spawn_executor(async move {
                        Self::read_bucket(
                            global_context,
                            address,
                            buckets_info,
                            &mut buffers,
                            async_thread,
                        )
                        .await;
                    });
                }

                spawner.executors_await().await;
                assert!(address.receive_packet().await.is_none());
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
