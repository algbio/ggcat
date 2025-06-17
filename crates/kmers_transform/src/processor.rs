use crate::{
    GroupProcessStats, KmersTransformContext, KmersTransformExecutorFactory,
    KmersTransformFinalExecutor, KmersTransformMapProcessor,
};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use ggcat_logging::stats::StatId;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use minimizer_bucketing::decode_helper::decode_sequences;
use minimizer_bucketing::split_buckets::SplittedBucket;
use parallel_processor::buckets::readers::typed_binary_reader::AsyncReaderThread;
use parallel_processor::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, SumMode};
use parallel_processor::mt_debug_counters::declare_counter_i64;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use utils::track;

pub struct KmersTransformProcessor<F: KmersTransformExecutorFactory>(PhantomData<F>);

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_addr_wait_processor", SumMode, false);

pub struct KmersProcessorInitData {
    pub process_stat_id: StatId,
    pub splitted_bucket: Mutex<Option<SplittedBucket>>,
    pub is_resplitted: bool,
    pub debug_bucket_first_path: Option<PathBuf>,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformProcessor<F> {
    type InputPacket = ();
    type OutputPacket = ();
    type GlobalParams = KmersTransformContext<F>;
    type InitData = KmersProcessorInitData;
    const ALLOW_PARALLEL_ADDRESS_EXECUTION: bool = false;

    fn new() -> Self {
        Self(PhantomData)
    }

    fn executor_main<'a>(
        &'a mut self,
        global_context: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
    ) {
        let mut map_processor = F::new_map_processor(&global_context.global_extra_data);
        let mut final_executor = F::new_final_executor(&global_context.global_extra_data);

        let reader_thread = AsyncReaderThread::new(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes(), 8);

        let mut packet = Packet::new_simple(<F::MapProcessorType as KmersTransformMapProcessor<
            F,
        >>::MapStruct::allocate_new(
            &F::get_packets_init_data(&global_context.global_extra_data),
        ));

        let mut tmp_mult_buffer = <F::AssociatedExtraDataWithMultiplicity>::new_temp_buffer();

        while let Ok(address) = track!(receiver.obtain_address(), ADDR_WAITING_COUNTER) {
            let proc_info = address.get_init_data();
            map_processor.process_group_start(packet, &global_context.global_extra_data);

            let mut real_size = 0;

            let mut splitted_bucket = proc_info.splitted_bucket.lock().take().unwrap();

            decode_sequences::<
                F::AssociatedExtraData,
                F::AssociatedExtraDataWithMultiplicity,
                F::FlagsCount,
            >(
                reader_thread.clone(),
                &mut tmp_mult_buffer,
                &mut splitted_bucket,
                global_context.k,
                |read, extra_buffer| {
                    real_size += 1;
                    map_processor.process_group_add_sequence(&read, extra_buffer);
                    F::AssociatedExtraDataWithMultiplicity::clear_temp_buffer(extra_buffer);
                },
            );

            let GroupProcessStats {
                total_kmers,
                unique_kmers,
                ..
            } = map_processor.get_stats();

            packet = map_processor.process_group_finalize(&global_context.global_extra_data);

            if real_size != splitted_bucket.sequences_count {
                ggcat_logging::info!(
                    "Found bucket ==> {:?} // EXPECTED_SIZE: {} REAL_SIZE: {} SUB: {}",
                    proc_info
                        .debug_bucket_first_path
                        .as_ref()
                        .map(|p| p.display()),
                    splitted_bucket.sequences_count,
                    real_size,
                    splitted_bucket.sub_bucket
                );
            }

            packet = final_executor.process_map(&global_context.global_extra_data, packet);

            if !proc_info.is_resplitted {
                global_context
                    .total_sequences
                    .fetch_add(real_size as u64, Ordering::Relaxed);
                global_context
                    .total_kmers
                    .fetch_add(total_kmers, Ordering::Relaxed);
                global_context
                    .unique_kmers
                    .fetch_add(unique_kmers, Ordering::Relaxed);

                global_context
                    .processed_subbuckets_count
                    .fetch_add(1, Ordering::Relaxed);

                global_context
                    .processed_buckets_size
                    .fetch_add(splitted_bucket.total_size as usize, Ordering::Relaxed);
            }

            packet.reset();
        }
        final_executor.finalize(&global_context.global_extra_data);
    }
}
