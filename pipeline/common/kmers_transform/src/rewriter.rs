use crate::reader::{InputBucketDesc, KmersTransformReader};
use crate::reads_buffer::ReadsBuffer;
use crate::{KmersTransformContext, KmersTransformExecutorFactory};
use config::{
    get_memory_mode, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    PACKETS_PRIORITY_REWRITTEN, PARTIAL_VECS_CHECKPOINT_SIZE,
};
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use minimizer_bucketing::counters_analyzer::BucketCounter;
use minimizer_bucketing::MinimizerBucketingExecutorFactory;
use parallel_processor::buckets::single::SingleBucketThreadDispatcher;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::counter_stats::counter::{AtomicCounter, SumMode};
use parallel_processor::counter_stats::declare_counter_i64;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::packet::Packet;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use utils::track;

pub struct KmersTransformRewriter<F: KmersTransformExecutorFactory>(PhantomData<F>);

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_addr_wait_rewriter", SumMode, false);

static PACKET_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_packet_wait_rewriter", SumMode, false);

static SUBSPLIT_INDEX: AtomicU64 = AtomicU64::new(0);

impl<F: KmersTransformExecutorFactory> KmersTransformRewriter<F> {
    #[instrumenter::track]
    async fn do_rewrite(
        global_context: &KmersTransformContext<F>,
        ops: &ExecutorAddressOperations<'_, Self>,
        buckets: &mut Vec<Option<Arc<MultiThreadBuckets<CompressedBinaryWriter>>>>,
    ) -> Vec<u64> {
        let mut single_dispatchers: Vec<_> = (0..buckets.len()).map(|_| None).collect();
        let mut sequences_count = vec![0; buckets.len()];

        while let Some(input_packet) = track!(ops.receive_packet().await, PACKET_WAITING_COUNTER) {
            let input_packet = input_packet.deref();

            if buckets[input_packet.sub_bucket].is_none() {
                buckets[input_packet.sub_bucket] = Some(Self::alloc_bucket(&global_context));

                let bucket = buckets[input_packet.sub_bucket].as_ref().unwrap().deref();
                // Workaround for array lifetime complain
                let bucket =
                    unsafe { &*(bucket as *const MultiThreadBuckets<CompressedBinaryWriter>) };

                single_dispatchers[input_packet.sub_bucket] = Some(
                    SingleBucketThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, 0, bucket),
                );
            }

            let target_bucket = single_dispatchers[input_packet.sub_bucket]
                .as_mut()
                .unwrap();

            for (flags, extra, bases) in &input_packet.reads {
                sequences_count[input_packet.sub_bucket] += 1;
                target_bucket.add_element_extended(
                    extra,
                    &input_packet.extra_buffer,
                    &CompressedReadsBucketHelper::<
                        _,
                        <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
                        false,
                    >::new_packed(bases.as_reference(&input_packet.reads_buffer), *flags, 0),
                );
            }
        }

        single_dispatchers.into_iter().for_each(|tls| {
            if let Some(tls) = tls {
                tls.finalize()
            }
        });
        sequences_count
    }

    fn alloc_bucket(
        global_context: &KmersTransformContext<F>,
    ) -> Arc<MultiThreadBuckets<CompressedBinaryWriter>> {
        Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            1,
            global_context.temp_dir.join(&format!(
                "bucket-rewrite{}-",
                SUBSPLIT_INDEX.fetch_add(1, Ordering::Relaxed)
            )),
            &(
                get_memory_mode(SwapPriority::ResultBuckets),
                PARTIAL_VECS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        ))
    }
}

#[derive(Clone)]
pub struct RewriterInitData {
    pub buckets_count: usize,
    pub buckets_hash_bits: usize,
    pub used_hash_bits: usize,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformRewriter<F> {
    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = InputBucketDesc;
    type GlobalParams = KmersTransformContext<F>;
    type InitData = RewriterInitData;

    type AsyncExecutorFuture<'a> = impl Future<Output = ()> + 'a;

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
                let mut buckets = (0..init_data.buckets_count).map(|_| None).collect();

                let sequences_count =
                    Self::do_rewrite(global_context, &address, &mut buckets).await;

                for (bucket, seq_count) in buckets
                    .into_iter()
                    .zip(sequences_count)
                    .filter(|(b, _)| b.is_some())
                    .map(|(b, c)| (b.unwrap(), c))
                {
                    let new_bucket_address = KmersTransformReader::<F>::generate_new_address(());

                    global_context
                        .rewritten_buckets_count
                        .fetch_add(1, Ordering::Relaxed);

                    address.declare_addresses(
                        vec![new_bucket_address.clone()],
                        PACKETS_PRIORITY_REWRITTEN,
                    );
                    address.packet_send(
                        new_bucket_address,
                        Packet::new_simple(InputBucketDesc {
                            path: bucket.finalize().into_iter().next().unwrap(),
                            sub_bucket_counters: vec![BucketCounter {
                                count: seq_count,
                                is_outlier: false,
                            }],
                            resplitted: false,
                            rewritten: true,
                            used_hash_bits: init_data.used_hash_bits + init_data.buckets_hash_bits,
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
