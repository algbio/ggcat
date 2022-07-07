use crate::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
    KmersTransformMapProcessor,
};
use parallel_processor::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct KmersTransformWriter<F: KmersTransformExecutorFactory>(PhantomData<F>);

impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformWriter<F> {
    type InputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
    type OutputPacket = ();
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
            // Only one address
            let address = receiver.obtain_address().await.unwrap();

            let mut spawner = address.make_spawner();

            for _ in 0..global_context.compute_threads_count {
                spawner.spawn_executor(async {
                    let mut final_executor =
                        F::new_final_executor(&global_context.global_extra_data);
                    while let Some(packet) = address.receive_packet().await {
                        final_executor.process_map(&global_context.global_extra_data, packet);
                    }
                    final_executor.finalize(&global_context.global_extra_data);
                });
            }
            spawner.executors_await().await;
        }
    }
}
//
//     const MEMORY_FIELDS_COUNT: usize = 1;
//     const MEMORY_FIELDS: &'static [&'static str] = &["BUFFER_SIZES"];
//     fn allocate_new_group<E: ExecutorOperations<Self>>(
//         global_params: Arc<Self::GlobalParams>,
//         _memory_params: Option<Self::MemoryParams>,
//         _common_packet: Option<Packet<Self::InputPacket>>,
//         _ops: E,
//     ) -> (Self::BuildParams, usize) {
//         let threads_count = global_params.compute_threads_count;
//         ((), threads_count)
//     }
//
//     fn required_pool_items(&self) -> u64 {
//         0
//     }
//
//     fn pre_execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         _reinit_params: Self::BuildParams,
//         _ops: E,
//     ) {
//         self.final_executor = Some(F::new_final_executor(&self.context.global_extra_data));
//     }
//
//     fn execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         input_packet: Packet<Self::InputPacket>,
//         _ops: E,
//     ) {
//         self.final_executor
//             .as_mut()
//             .unwrap()
//             .process_map(&self.context.global_extra_data, input_packet);
//     }
//
//     fn finalize<E: ExecutorOperations<Self>>(&mut self, _ops: E) {
//         self.final_executor
//             .take()
//             .unwrap()
//             .finalize(&self.context.global_extra_data);
//     }
//
//     fn is_finished(&self) -> bool {
//         false
//     }
//     fn get_current_memory_params(&self) -> Self::MemoryParams {
//         ()
//     }
// }
