use crate::reader::{InputBucketDesc, KmersTransformReader};
use crate::reads_buffer::ReadsBuffer;
use crate::{KmersTransformContext, KmersTransformExecutorFactory};
use config::{
    get_memory_mode, BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL,
    DEFAULT_PER_CPU_BUFFER_SIZE, MAXIMUM_JIT_PROCESSED_BUCKETS, MAX_RESPLIT_BUCKETS_COUNT_LOG,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, PACKETS_PRIORITY_DONE_RESPLIT,
};
use hashes::HashableSequence;
use instrumenter::local_setup_instrumenter;
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use minimizer_bucketing::counters_analyzer::BucketCounter;
use minimizer_bucketing::{MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::counter_stats::counter::{AtomicCounter, SumMode};
use parallel_processor::counter_stats::declare_counter_i64;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::packet::Packet;
use std::cmp::{max, min};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use utils::track;

local_setup_instrumenter!();

pub struct KmersTransformResplitter<F: KmersTransformExecutorFactory>(PhantomData<F>);

static BUCKET_RESPLIT_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct BucketsResplitInfo {
    buckets: Arc<MultiThreadBuckets<CompressedBinaryWriter>>,
    subsplit_buckets_count_log: usize,
    output_addresses: Vec<ExecutorAddress>,
    executors_count: usize,
    global_counters: Vec<AtomicU64>,
}

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_addr_wait_resplitter", SumMode, false);

static PACKET_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_packet_wait_resplitter", SumMode, false);

impl<F: KmersTransformExecutorFactory> KmersTransformResplitter<F> {
    fn init_processing(
        global_context: &KmersTransformContext<F>,
        init_data: &ResplitterInitData,
    ) -> BucketsResplitInfo {
        let subsplit_buckets_count_log = min(
            MAX_RESPLIT_BUCKETS_COUNT_LOG,
            max(
                MAXIMUM_JIT_PROCESSED_BUCKETS,
                init_data.bucket_size / (global_context.min_bucket_size as usize),
            )
            .next_power_of_two()
            .ilog2() as usize,
        );

        let buckets = Arc::new(MultiThreadBuckets::new(
            1 << subsplit_buckets_count_log,
            global_context.temp_dir.join(format!(
                "resplit-bucket{}",
                BUCKET_RESPLIT_COUNTER.fetch_add(1, Ordering::Relaxed)
            )),
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        ));

        let output_addresses: Vec<_> = (0..(1 << subsplit_buckets_count_log))
            .map(|_| KmersTransformReader::<F>::generate_new_address(()))
            .collect();

        // TODO: Find best count of writing threads
        let executors_count = 4; //global_context.read_threads_count;
        BucketsResplitInfo {
            // (
            //     (
            buckets,
            subsplit_buckets_count_log,
            output_addresses,
            global_counters: (0..(1 << subsplit_buckets_count_log))
                .map(|_| AtomicU64::new(0))
                .collect(),
            executors_count,
            // )
        }
    }

    #[instrumenter::track]
    async fn do_resplit(
        global_context: &KmersTransformContext<F>,
        resplit_info: &BucketsResplitInfo,
        ops: &ExecutorAddressOperations<'_, Self>,
    ) {
        let mut resplitter = F::new_resplitter(&global_context.global_extra_data);
        let mut thread_local_buffers = BucketsThreadDispatcher::new(
            &resplit_info.buckets,
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, resplit_info.buckets.count()),
        );

        let mut local_counters = vec![0u8; resplit_info.global_counters.len()];

        // mem_tracker.update_memory_usage(&[
        //     DEFAULT_PER_CPU_BUFFER_SIZE.octets as usize * mt_buckets.count()
        // ]);

        while let Some(input_packet) = track!(ops.receive_packet().await, PACKET_WAITING_COUNTER) {
            let input_packet = input_packet.deref();

            let mut preprocess_info = Default::default();

            for (flags, extra, bases) in &input_packet.reads {
                let sequence = bases.as_reference(&input_packet.reads_buffer);
                resplitter.reprocess_sequence(
                    *flags,
                    extra,
                    &input_packet.extra_buffer,
                    &mut preprocess_info,
                );

                resplitter.process_sequence::<_, _>(
                            &preprocess_info,
                            sequence,
                            0..sequence.bases_count(),
                            0, resplit_info.subsplit_buckets_count_log, 0,
                            |bucket, _next_bucket, seq, flags, extra, extra_buffer| {

                                let bucket = bucket as usize;

                                let counter = &mut local_counters[bucket];

                                *counter = counter.wrapping_add(1);
                                if *counter == 0 {
                                    resplit_info.global_counters[bucket as usize]
                                        .fetch_add(256, Ordering::Relaxed);
                                }

                                thread_local_buffers.add_element_extended(
                                    bucket as BucketIndexType,
                                    &extra,
                                    extra_buffer,
                                    &CompressedReadsBucketHelper::<
                                        _,
                                        <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
                                        false,
                                    >::new_packed(seq, flags, 0),
                                );
                            },
                        );
            }
        }

        for (bucket, counter) in local_counters.into_iter().enumerate() {
            resplit_info.global_counters[bucket].fetch_add(counter as u64, Ordering::Relaxed);
        }

        thread_local_buffers.finalize();
    }
}

#[derive(Clone)]
pub struct ResplitterInitData {
    pub bucket_size: usize,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformResplitter<F> {
    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = InputBucketDesc;
    type GlobalParams = KmersTransformContext<F>;
    type AsyncExecutorFuture<'a> = impl Future<Output = ()> + 'a;
    type InitData = ResplitterInitData;

    fn new() -> Self {
        Self(PhantomData)
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_context: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        _memory_tracker: MemoryTracker<Self>,
    ) -> Self::AsyncExecutorFuture<'a> {
        async move {
            while let Ok((address, init_data)) =
                track!(receiver.obtain_address().await, ADDR_WAITING_COUNTER)
            {
                let resplit_info = Self::init_processing(global_context, &init_data);

                let mut spawner = address.make_spawner();

                for _ in 0..resplit_info.executors_count {
                    spawner.spawn_executor(async {
                        Self::do_resplit(global_context, &resplit_info, &address).await
                    });
                }
                spawner.executors_await().await;
                drop(spawner);

                global_context.extra_buckets_count.fetch_add(
                    1 << resplit_info.subsplit_buckets_count_log,
                    Ordering::Relaxed,
                );
                address.declare_addresses(
                    resplit_info.output_addresses.clone(),
                    PACKETS_PRIORITY_DONE_RESPLIT,
                );

                for ((i, bucket), sub_bucket_count) in
                    resplit_info.buckets.finalize().into_iter().enumerate().zip(
                        resplit_info
                            .global_counters
                            .into_iter()
                            .map(|x| BucketCounter {
                                count: x.into_inner(),
                                is_outlier: false,
                            }),
                    )
                {
                    address.packet_send(
                        resplit_info.output_addresses[i].clone(),
                        Packet::new_simple(InputBucketDesc {
                            path: bucket,
                            sub_bucket_counters: vec![sub_bucket_count],
                            resplitted: true,
                            rewritten: false,
                            used_hash_bits: 0,
                        }),
                    );
                }
            }
        }
    }
}

//
//     const MEMORY_FIELDS_COUNT: usize = 1;
//     const MEMORY_FIELDS: &'static [&'static str] = &["TEMP_BUFFER"];
//
//     const BASE_PRIORITY: u64 = 1;
//     const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
//     const STRICT_POOL_ALLOC: bool = false;
//
//     fn pre_execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         (mt_buckets, buckets_count_log, out_addresses, counters): Self::BuildParams,
//         _ops: E,
//     ) {
//     }
//
//     fn execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         input_packet: Packet<Self::InputPacket>,
//         _ops: E,
//     ) {
//     }
//
//     fn finalize<E: ExecutorOperations<Self>>(&mut self, mut ops: E) {
//     }
//
//     fn is_finished(&self) -> bool {
//         false
//     }
//
//     fn get_current_memory_params(&self) -> Self::MemoryParams {
//         ()
//     }
// }
