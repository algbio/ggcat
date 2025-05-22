use crate::reader::{InputBucketDesc, KmersTransformReader};
use crate::reads_buffer::ReadsBuffer;
use crate::{KmersTransformContext, KmersTransformExecutorFactory};
use config::{
    get_compression_level_info, get_memory_mode, BucketIndexType, SwapPriority,
    DEFAULT_PER_CPU_BUFFER_SIZE, MAXIMUM_JIT_PROCESSED_BUCKETS, MAX_RESPLIT_BUCKETS_COUNT_LOG,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, PACKETS_PRIORITY_DONE_RESPLIT, PRIORITY_SCHEDULING_HIGH,
    USE_SECOND_BUCKET, WORKERS_PRIORITY_HIGH,
};
use ggcat_logging::stats;
use ggcat_logging::stats::StatId;
use hashes::HashableSequence;
use instrumenter::local_setup_instrumenter;
use io::concurrent::temp_reads::creads_utils::{BucketModeFromBoolean, BucketModeOption};
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer, NoMultiplicity,
    WithMultiplicity,
};
use io::concurrent::temp_reads::creads_utils::{MultiplicityModeOption, NoSecondBucket};
use minimizer_bucketing::counters_analyzer::BucketCounter;
use minimizer_bucketing::{
    MinimizerBucketMode, MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory,
    PushSequenceInfo,
};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, SumMode};
use parallel_processor::mt_debug_counters::declare_counter_i64;
use parallel_processor::scheduler::{PriorityScheduler, ThreadPriorityHandle};
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
    data_format: MinimizerBucketMode,
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
        let total_sequences = global_context.total_sequences.load(Ordering::Relaxed);
        let unique_kmers = global_context.unique_kmers.load(Ordering::Relaxed);

        let unique_estimator_factor = if total_sequences > 0 {
            unique_kmers as f64 / total_sequences as f64
        } else {
            global_context.k as f64 / 2.0
        };

        let subsplit_buckets_count_log = min(
            MAX_RESPLIT_BUCKETS_COUNT_LOG,
            max(
                MAXIMUM_JIT_PROCESSED_BUCKETS,
                init_data.bucket_size / (global_context.min_bucket_size as usize)
                    * unique_estimator_factor as usize,
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
            None,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                get_compression_level_info(),
            ),
            &init_data.data_format,
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
            data_format: init_data.data_format,
            // )
        }
    }

    #[instrumenter::track]
    async fn do_resplit(
        global_context: &KmersTransformContext<F>,
        resplit_info: &BucketsResplitInfo,
        ops: &ExecutorAddressOperations<'_, Self>,
        thread_handle: &ThreadPriorityHandle,
    ) {
        match resplit_info.data_format {
            MinimizerBucketMode::Single => Self::do_resplit_internal::<
                BucketModeFromBoolean<USE_SECOND_BUCKET>,
                NoMultiplicity,
            >(
                global_context, resplit_info, ops, thread_handle
            )
            .await,
            MinimizerBucketMode::Compacted => {
                Self::do_resplit_internal::<NoSecondBucket, WithMultiplicity>(
                    global_context,
                    resplit_info,
                    ops,
                    thread_handle,
                )
                .await
            }
        }
    }

    async fn do_resplit_internal<
        BucketMode: BucketModeOption,
        MultiplicityMode: MultiplicityModeOption,
    >(
        global_context: &KmersTransformContext<F>,
        resplit_info: &BucketsResplitInfo,
        ops: &ExecutorAddressOperations<'_, Self>,
        thread_handle: &ThreadPriorityHandle,
    ) {
        let buckets_count = 1 << resplit_info.subsplit_buckets_count_log;
        let mut resplitter = F::new_resplitter(&global_context.global_extra_data);
        let mut thread_local_buffers = BucketsThreadDispatcher::<
            _,
            CompressedReadsBucketDataSerializer<
                _,
                <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
                BucketMode, // This is always zero but it is needed to preserve consistency
                MultiplicityMode,
            >,
        >::new(
            &resplit_info.buckets,
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, resplit_info.buckets.count()),
            global_context.k,
        );

        let mut local_counters = vec![0u8; buckets_count];

        // mem_tracker.update_memory_usage(&[
        //     DEFAULT_PER_CPU_BUFFER_SIZE.octets as usize * mt_buckets.count()
        // ]);

        while let Some(input_packet) = track!(
            ops.receive_packet(thread_handle).await,
            PACKET_WAITING_COUNTER
        ) {
            let input_packet = input_packet.deref();

            let mut preprocess_info = Default::default();

            for (flags, extra, bases, multiplicity) in input_packet.reads.iter() {
                let sequence = bases.as_reference(&input_packet.reads_buffer);
                resplitter.reprocess_sequence(
                    flags,
                    extra,
                    &input_packet.extra_buffer,
                    &mut preprocess_info,
                );

                resplitter.process_sequence::<_, _, false>(
                    &preprocess_info,
                    sequence,
                    0..sequence.bases_count(),
                    0,
                    resplit_info.subsplit_buckets_count_log,
                    0,
                    #[inline(always)]
                    |info| {
                        let PushSequenceInfo {
                            bucket,
                            second_bucket,
                            sequence,
                            extra_data,
                            temp_buffer,
                            flags,
                            rc,
                        } = info;

                        // rc should always be false for resplitting, as all the k-mers already have a fixed orientation
                        debug_assert!(!rc);
                        let bucket = bucket as usize;

                        let counter = &mut local_counters[bucket];

                        *counter = counter.wrapping_add(1);
                        if *counter == 0 {
                            resplit_info.global_counters[bucket as usize]
                                .fetch_add(256, Ordering::Relaxed);
                        }

                        thread_local_buffers.add_element_extended(
                            bucket as BucketIndexType,
                            &extra_data,
                            temp_buffer,
                            &CompressedReadsBucketData::new_packed_with_multiplicity(
                                sequence,
                                flags,
                                0,
                                multiplicity,
                            ),
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
    pub _resplit_stat_id: StatId,
    pub bucket_size: usize,
    pub data_format: MinimizerBucketMode,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformResplitter<F> {
    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = InputBucketDesc;
    type GlobalParams = KmersTransformContext<F>;
    type InitData = ResplitterInitData;

    fn new() -> Self {
        Self(PhantomData)
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_context: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        _memory_tracker: MemoryTracker<Self>,
    ) -> impl Future<Output = ()> + 'a {
        async move {
            let thread_handle = PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_HIGH);

            while let Ok((address, init_data)) = track!(
                receiver
                    .obtain_address_with_priority(WORKERS_PRIORITY_HIGH, &thread_handle)
                    .await,
                ADDR_WAITING_COUNTER
            ) {
                stats!(
                    let start_resplit_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                );

                let resplit_info = Self::init_processing(global_context, &init_data);

                let mut spawner = address.make_spawner();

                for _ in 0..resplit_info.executors_count {
                    spawner.spawn_executor(async {
                        let thread_handle =
                            PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_HIGH);

                        Self::do_resplit(global_context, &resplit_info, &address, &thread_handle)
                            .await
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

                let buckets = resplit_info.buckets.finalize_single();

                stats!(
                    let end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                );

                stats!(
                    stats.transform.resplits.push(ggcat_logging::stats::ResplitFinalInfo {
                        resplit_id: init_data._resplit_stat_id,
                        start_time: start_resplit_time.into(),
                        end_time: end_time.into(),
                        out_files: buckets
                            .iter()
                            .map(|addr| addr.path.clone())
                            .collect(),
                    });
                );

                for ((i, bucket), sub_bucket_count) in buckets.into_iter().enumerate().zip(
                    resplit_info
                        .global_counters
                        .into_iter()
                        .map(|x| BucketCounter {
                            count: x.into_inner(),
                        }),
                ) {
                    address.packet_send(
                        resplit_info.output_addresses[i].clone(),
                        Packet::new_simple(InputBucketDesc {
                            paths: vec![bucket.path],
                            sub_bucket_counters: vec![sub_bucket_count],
                            compaction_delta: 0,
                            resplitted: true,
                            rewritten: false,
                            used_hash_bits: 0,
                            out_data_format: resplit_info.data_format,
                        }),
                        &thread_handle,
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
