use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformMapProcessor,
};
use parallel_processor::execution_manager::executor::{Executor, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use std::sync::Arc;

pub struct KmersTransformProcessor<F: KmersTransformExecutorFactory> {
    context: Arc<KmersTransformContext<F>>,
    map_processor: F::MapProcessorType,
    initialized: bool,
}

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformProcessor<F> {
    type InitData = (Arc<KmersTransformContext<F>>, MemoryTracker<Self>);

    fn allocate_new((context, memory_tracker): &Self::InitData) -> Self {
        Self {
            context: context.clone(),
            map_processor: F::new_map_processor(&context.global_extra_data, memory_tracker.clone()),
            initialized: false,
        }
    }

    fn reset(&mut self) {
        self.initialized = false;
    }
}

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformProcessor<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const MEMORY_FIELDS_COUNT: usize = 2;
    const MEMORY_FIELDS: &'static [&'static str] = &["MAP_SIZE", "CORRECT_READS"];

    const BASE_PRIORITY: u64 = 2;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
    const STRICT_POOL_ALLOC: bool = false;

    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = ();

    fn allocate_new_group<D: FnOnce(Vec<ExecutorAddress>)>(
        _global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _executors_initializer: D,
    ) -> (Self::BuildParams, usize) {
        ((), 1)
    }

    fn required_pool_items(&self) -> u64 {
        0
    }

    fn pre_execute<
        PF: FnMut() -> Packet<Self::OutputPacket>,
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        _reinit_params: Self::BuildParams,
        mut packet_alloc_force: PF,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        self.map_processor
            .process_group_start(packet_alloc_force(), &self.context.global_extra_data);
        self.initialized = true;
    }

    fn execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        self.map_processor.process_group_batch_sequences(
            &self.context.global_extra_data,
            &input_packet.reads,
            &input_packet.reads_buffer,
        );
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(
        &mut self,
        mut packet_send: S,
    ) {
        let packet = self
            .map_processor
            .process_group_finalize(&self.context.global_extra_data);
        packet_send(
            self.context
                .finalizer_address
                .read()
                .as_ref()
                .unwrap()
                .clone(),
            packet,
        );
    }

    fn is_finished(&self) -> bool {
        false
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
