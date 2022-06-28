use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
    KmersTransformMapProcessor,
};
use parallel_processor::execution_manager::executor::{Executor, ExecutorOperations, ExecutorType};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use std::sync::Arc;

pub struct KmersTransformWriter<F: KmersTransformExecutorFactory> {
    context: Arc<KmersTransformContext<F>>,
    mem_tracker: MemoryTracker<Self>,
    final_executor: Option<F::FinalExecutorType>,
}

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformWriter<F> {
    type InitData = (Arc<KmersTransformContext<F>>, MemoryTracker<Self>);

    fn allocate_new((context, mem_tracker): &Self::InitData) -> Self {
        Self {
            context: context.clone(),
            mem_tracker: mem_tracker.clone(),
            final_executor: None,
        }
    }

    fn reset(&mut self) {}
}

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformWriter<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const MEMORY_FIELDS_COUNT: usize = 1;
    const MEMORY_FIELDS: &'static [&'static str] = &["BUFFER_SIZES"];

    const BASE_PRIORITY: u64 = 0;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 100;
    const STRICT_POOL_ALLOC: bool = false;

    type InputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
    type OutputPacket = ();
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = ();

    fn allocate_new_group<E: ExecutorOperations<Self>>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _ops: E,
    ) -> (Self::BuildParams, usize) {
        let threads_count = global_params.compute_threads_count;
        ((), threads_count)
    }

    fn required_pool_items(&self) -> u64 {
        0
    }

    fn pre_execute<E: ExecutorOperations<Self>>(
        &mut self,
        _reinit_params: Self::BuildParams,
        _ops: E,
    ) {
        self.final_executor = Some(F::new_final_executor(&self.context.global_extra_data));
    }

    fn execute<E: ExecutorOperations<Self>>(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        _ops: E,
    ) {
        self.final_executor
            .as_mut()
            .unwrap()
            .process_map(&self.context.global_extra_data, input_packet);
    }

    fn finalize<E: ExecutorOperations<Self>>(&mut self, _ops: E) {
        self.final_executor
            .take()
            .unwrap()
            .finalize(&self.context.global_extra_data);
    }

    fn is_finished(&self) -> bool {
        false
    }
    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
