use crate::config::{
    DEFAULT_PREFETCH_AMOUNT, MIN_BUCKET_CHUNKS_FOR_READING_THREAD, USE_SECOND_BUCKET,
};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use crate::pipeline_common::kmers_transform::processor::KmersTransformProcessor;
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::resplitter::KmersTransformResplitter;
use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformPreprocessor,
};
use crate::pipeline_common::minimizer_bucketing::counters_analyzer::BucketCounter;
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::KEEP_FILES;
use parallel_processor::buckets::readers::async_binary_reader::AsyncBinaryReader;
use parallel_processor::execution_manager::executor::{Executor, ExecutorOperations, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::memory_fs::RemoveFileMode;
use replace_with::replace_with_or_abort;
use std::cmp::{max, min, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct KmersTransformReader<F: KmersTransformExecutorFactory> {
    context: Arc<KmersTransformContext<F>>,
    mem_tracker: MemoryTracker<Self>,
    buffers: Vec<Packet<ReadsBuffer<F::AssociatedExtraData>>>,
    addresses: Vec<ExecutorAddress>,
    preprocessor: F::PreprocessorType,
    async_reader: Option<AsyncBinaryReader>,
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

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformReader<F> {
    type InitData = (Arc<KmersTransformContext<F>>, MemoryTracker<Self>);

    fn allocate_new((context, memory_tracker): &Self::InitData) -> Self {
        Self {
            context: context.clone(),
            mem_tracker: memory_tracker.clone(),
            buffers: vec![],
            addresses: vec![],
            preprocessor: F::new_preprocessor(&context.global_extra_data),
            async_reader: None,
            _phantom: Default::default(),
        }
    }

    fn reset(&mut self) {
        self.buffers.clear();
        self.addresses.clear();
        self.async_reader.take();
    }
}

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformReader<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::NeedsInitPacket;

    const MEMORY_FIELDS_COUNT: usize = 0;
    const MEMORY_FIELDS: &'static [&'static str] = &[];

    const BASE_PRIORITY: u64 = 0;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 0;
    const STRICT_POOL_ALLOC: bool = true;

    type InputPacket = InputBucketDesc;
    type OutputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = (AsyncBinaryReader, usize, Vec<usize>, Vec<ExecutorAddress>);

    fn allocate_new_group<E: ExecutorOperations<Self>>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        common_packet: Option<Packet<Self::InputPacket>>,
        mut ops: E,
    ) -> (Self::BuildParams, usize) {
        let file = common_packet.unwrap();

        let second_buckets_log_max = global_params.max_second_buckets_count_log2;

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

        let mut bucket_sizes: VecDeque<_> = (0..(1 << second_buckets_log_max))
            .map(|i| {
                if !file.resplitted && i < file.sub_bucket_counters.len() {
                    sequences_count += file.sub_bucket_counters[i].count;
                    (file.sub_bucket_counters[i].clone(), i)
                } else {
                    (
                        BucketCounter {
                            count: 1,
                            is_outlier: false,
                        },
                        i,
                    )
                }
            })
            .collect();

        let file_size = reader.get_file_size();

        let sequences_size_ratio =
            file_size as f64 / (sequences_count * global_params.k as u64) as f64 * 2.67;

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
                && smallest_bucket.0 .0 + uniq_estimate_count > global_params.min_bucket_size
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
            global_params.compute_threads_count as f64 / global_params.read_threads_count as f64;

        let addr_concurrency = max(1, (addresses.len() as f64 / threads_ratio + 0.5) as usize);
        let chunks_concurrency = max(
            1,
            reader.get_chunks_count() / MIN_BUCKET_CHUNKS_FOR_READING_THREAD,
        );

        let max_concurrency = min(
            min(4, global_params.read_threads_count),
            min(addr_concurrency, chunks_concurrency),
        );

        println!(
            "Chunks {} concurrency: {} REMAPPINGS: {:?} // {:?} // {:?} RATIO: {:.2}",
            reader.get_chunks_count(),
            max_concurrency,
            &buckets_remapping,
            dbg_counters,
            file.sub_bucket_counters
                .iter()
                .map(|x| x.count)
                .collect::<Vec<_>>(),
            sequences_size_ratio
        );

        ops.declare_addresses(addresses.clone());

        (
            (reader, second_buckets_log_max, buckets_remapping, addresses),
            max_concurrency,
        )
    }

    fn required_pool_items(&self) -> u64 {
        1
    }

    fn pre_execute<E: ExecutorOperations<Self>>(
        &mut self,
        (async_binary_reader, second_buckets_log_max, remappings, addresses): Self::BuildParams,
        mut ops: E,
    ) {
        if async_binary_reader.is_finished() {
            return;
        }

        self.buffers
            .extend((0..addresses.len()).map(|_| ops.packet_alloc_force()));

        self.addresses = addresses;
        self.async_reader = Some(async_binary_reader);

        let async_reader_thread = self.context.async_readers.get();
        let preprocessor = &mut self.preprocessor;
        let global_extra_data = &self.context.global_extra_data;

        let has_single_addr = self.addresses.len() == 1;

        self.async_reader
            .as_ref()
            .unwrap()
            .decode_all_bucket_items::<CompressedReadsBucketHelper<
                F::AssociatedExtraData,
                F::FLAGS_COUNT,
                { USE_SECOND_BUCKET },
            >, _>(
                async_reader_thread.clone(),
                Vec::new(),
                F::AssociatedExtraData::new_temp_buffer(),
                |read_info, extra_buffer| {
                    let bucket = if has_single_addr {
                        0
                    } else {
                        remappings[preprocessor.get_sequence_bucket(global_extra_data, &read_info)
                            as usize
                            % (1 << second_buckets_log_max)]
                    };

                    let (flags, _second_bucket, mut extra_data, read) = read_info;

                    let ind_read = CompressedReadIndipendent::from_read(
                        &read,
                        &mut self.buffers[bucket].reads_buffer,
                    );
                    self.buffers[bucket]
                        .reads
                        .push((flags, extra_data, ind_read));
                    extra_data = F::AssociatedExtraData::copy_extra_from(
                        extra_data,
                        extra_buffer,
                        &mut self.buffers[bucket].extra_buffer,
                    );

                    if self.buffers[bucket].reads.len() == self.buffers[bucket].reads.capacity() {
                        replace_with_or_abort(&mut self.buffers[bucket], |buffer| {
                            ops.packet_send(self.addresses[bucket].clone(), buffer);
                            ops.packet_alloc()
                        });
                    }
                    F::AssociatedExtraData::clear_temp_buffer(extra_buffer);
                },
            );
        for (packet, address) in self.buffers.drain(..).zip(self.addresses.drain(..)) {
            if packet.reads.len() > 0 {
                ops.packet_send(address, packet);
            }
        }
    }

    fn execute<E: ExecutorOperations<Self>>(
        &mut self,
        _input_packet: Packet<Self::InputPacket>,
        _ops: E,
    ) {
        panic!("Multiple packet processing not supported!");
    }

    fn finalize<E: ExecutorOperations<Self>>(&mut self, _ops: E) {
        assert_eq!(self.buffers.len(), 0);
    }

    fn is_finished(&self) -> bool {
        self.async_reader.is_some()
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
