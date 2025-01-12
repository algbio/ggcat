use crate::reads_buffer::ReadsBuffer;
use crate::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
    KmersTransformMapProcessor,
};
use config::{PRIORITY_SCHEDULING_HIGH, WORKERS_PRIORITY_BASE};
use parallel_processor::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, SumMode};
use parallel_processor::mt_debug_counters::declare_counter_i64;
use parallel_processor::scheduler::PriorityScheduler;
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use utils::track;

pub struct KmersTransformProcessor<F: KmersTransformExecutorFactory>(PhantomData<F>);

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_addr_wait_processor", SumMode, false);

static PACKET_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("kt_packet_wait_processor", SumMode, false);

#[derive(Clone)]
pub struct KmersProcessorInitData {
    pub sequences_count: usize,
    pub sub_bucket: usize,
    pub is_resplitted: bool,
    pub bucket_paths: Vec<PathBuf>,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformProcessor<F> {
    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = ();
    type GlobalParams = KmersTransformContext<F>;
    type InitData = KmersProcessorInitData;

    fn new() -> Self {
        Self(PhantomData)
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_context: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        memory_tracker: MemoryTracker<Self>,
    ) -> impl Future<Output = ()> + 'a {
        async move {
            let mut map_processor =
                F::new_map_processor(&global_context.global_extra_data, memory_tracker.clone());
            let mut final_executor = F::new_final_executor(&global_context.global_extra_data);

            let mut packet = Packet::new_simple(
                <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct::allocate_new(&()),
            );

            let thread_handle = PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_HIGH);

            while let Ok((address, proc_info)) = track!(
                receiver
                    .obtain_address_with_priority(WORKERS_PRIORITY_BASE, &thread_handle)
                    .await,
                ADDR_WAITING_COUNTER
            ) {
                map_processor.process_group_start(packet, &global_context.global_extra_data);

                let mut real_size = 0;
                let mut total_kmers = 0;
                let mut unique_kmers = 0;

                while let Some(input_packet) = track!(
                    address.receive_packet(&thread_handle).await,
                    PACKET_WAITING_COUNTER
                ) {
                    real_size += input_packet.reads.len() as usize;
                    let stats = map_processor.process_group_batch_sequences(
                        &global_context.global_extra_data,
                        &input_packet.reads,
                        &input_packet.extra_buffer,
                        &input_packet.reads_buffer,
                    );
                    total_kmers += stats.total_kmers;
                    unique_kmers += stats.unique_kmers;
                }

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
                }

                packet = map_processor.process_group_finalize(&global_context.global_extra_data);

                // static MAX_PACKET_SIZE: AtomicUsize = AtomicUsize::new(0);
                let current_size = packet.get_size();

                if real_size != proc_info.sequences_count {
                    //MAX_PACKET_SIZE.fetch_max(current_size, Ordering::Relaxed) < current_size {
                    ggcat_logging::info!(
                        "Found bucket with max size {} ==> {} // EXPECTED_SIZE: {} REAL_SIZE: {} SUB: {}",
                        current_size,
                        proc_info.bucket_paths[0].display(),
                        proc_info.sequences_count,
                        real_size,
                        proc_info.sub_bucket
                    );
                }

                packet = final_executor.process_map(&global_context.global_extra_data, packet);
                packet.reset();
                // address.packet_send(
                //     global_context
                //         .finalizer_address
                //         .read()
                //         .as_ref()
                //         .unwrap()
                //         .clone(),
                //     packet,
                // );
            }
            final_executor.finalize(&global_context.global_extra_data);
        }
    }
}
//     const MEMORY_FIELDS_COUNT: usize = 2;
//     const MEMORY_FIELDS: &'static [&'static str] = &["MAP_SIZE", "CORRECT_READS"];
