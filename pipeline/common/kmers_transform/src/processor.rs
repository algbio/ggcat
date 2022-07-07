use crate::reads_buffer::ReadsBuffer;
use crate::{KmersTransformContext, KmersTransformExecutorFactory, KmersTransformMapProcessor};
use parallel_processor::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct KmersTransformProcessor<F: KmersTransformExecutorFactory>(PhantomData<F>);

// impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformProcessor<F> {
//     type InitData = (Arc<KmersTransformContext<F>>, MemoryTracker<Self>);
//
//     fn allocate_new((context, memory_tracker): &Self::InitData) -> Self {
//         Self {
//             context: context.clone(),
//             map_processor: ,
//             initialized: false,
//         }
//     }
//
//     fn reset(&mut self) {
//         self.initialized = false;
//     }
// }

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformProcessor<F> {
    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
    type GlobalParams = KmersTransformContext<F>;
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

            while let Ok(address) = receiver.obtain_address().await {
                map_processor.process_group_start(
                    address.packet_alloc().await,
                    &global_context.global_extra_data,
                );

                while let Some(input_packet) = address.receive_packet().await {
                    map_processor.process_group_batch_sequences(
                        &global_context.global_extra_data,
                        &input_packet.reads,
                        &input_packet.extra_buffer,
                        &input_packet.reads_buffer,
                    );
                }

                let packet =
                    map_processor.process_group_finalize(&global_context.global_extra_data);
                address.packet_send(
                    global_context
                        .finalizer_address
                        .read()
                        .as_ref()
                        .unwrap()
                        .clone(),
                    packet,
                );
            }
        }
    }
}
//     const MEMORY_FIELDS_COUNT: usize = 2;
//     const MEMORY_FIELDS: &'static [&'static str] = &["MAP_SIZE", "CORRECT_READS"];
//
//     fn pre_execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         _reinit_params: Self::BuildParams,
//         mut ops: E,
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
