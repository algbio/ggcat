use crate::reads_buffer::ReadsBuffer;
use crate::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
    KmersTransformMapProcessor,
};
use parallel_processor::counter_stats::counter::{AtomicCounter, SumMode};
use parallel_processor::counter_stats::declare_counter_i64;
use parallel_processor::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
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
    pub bucket_path: PathBuf,
}

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformProcessor<F> {
    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = ();
    type GlobalParams = KmersTransformContext<F>;
    type InitData = KmersProcessorInitData;

    type AsyncExecutorFuture<'a> = impl Future<Output = ()> + 'a;

    fn new() -> Self {
        Self(PhantomData)
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_context: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        memory_tracker: MemoryTracker<Self>,
    ) -> Self::AsyncExecutorFuture<'a> {
        async move {
            let mut map_processor =
                F::new_map_processor(&global_context.global_extra_data, memory_tracker.clone());
            let mut final_executor = F::new_final_executor(&global_context.global_extra_data);

            let mut packet = Packet::new_simple(
                <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct::allocate_new(&()),
            );

            while let Ok((address, proc_info)) =
                track!(receiver.obtain_address().await, ADDR_WAITING_COUNTER)
            {
                map_processor.process_group_start(packet, &global_context.global_extra_data);

                let mut real_size = 0;

                while let Some(input_packet) =
                    track!(address.receive_packet().await, PACKET_WAITING_COUNTER)
                {
                    real_size += input_packet.reads.len();
                    map_processor.process_group_batch_sequences(
                        &global_context.global_extra_data,
                        &input_packet.reads,
                        &input_packet.extra_buffer,
                        &input_packet.reads_buffer,
                    );
                }

                packet = map_processor.process_group_finalize(&global_context.global_extra_data);

                static MAX_PACKET_SIZE: AtomicUsize = AtomicUsize::new(0);
                let current_size = packet.get_size();

                if real_size != proc_info.sequences_count {
                    //MAX_PACKET_SIZE.fetch_max(current_size, Ordering::Relaxed) < current_size {
                    println!(
                        "Found bucket with max size {} ==> {} // EXPECTED_SIZE: {} REAL_SIZE: {} SUB: {}",
                        current_size,
                        proc_info.bucket_path.display(),
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
