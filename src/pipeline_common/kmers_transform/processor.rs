use crate::config::{DEFAULT_PREFETCH_AMOUNT, USE_SECOND_BUCKET};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformMapProcessor,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::KEEP_FILES;
use hashbrown::HashMap;
use parallel_processor::buckets::readers::async_binary_reader::AsyncBinaryReader;
use parallel_processor::execution_manager::executor::{Executor, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::memory_fs::RemoveFileMode;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct KmersTransformProcessor<F: KmersTransformExecutorFactory> {
    context: Option<Arc<KmersTransformContext<F>>>,
    map_processor: Option<F::MapProcessorType>,
}

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformProcessor<F> {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            context: None,
            map_processor: None,
        }
    }

    fn reset(&mut self) {
        self.context.take();
        self.map_processor.take();
    }
}

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformProcessor<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const BASE_PRIORITY: u64 = 2;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
    const STRICT_POOL_ALLOC: bool = false;

    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = Arc<KmersTransformContext<F>>;

    fn allocate_new_group<D: FnOnce(Vec<ExecutorAddress>)>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _executors_initializer: D,
    ) -> (Self::BuildParams, usize) {
        (global_params, 1)
    }

    fn required_pool_items(&self) -> u64 {
        if self.context.is_some() {
            1
        } else {
            0
        }
    }

    fn pre_execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        reinit_params: Self::BuildParams,
        mut packet_alloc: P,
        _packet_send: S,
    ) {
        self.map_processor = Some(F::new_map_processor(&reinit_params.global_extra_data));

        self.map_processor
            .as_mut()
            .unwrap()
            .process_group_start(packet_alloc(), &reinit_params.global_extra_data);
        self.context = Some(reinit_params);
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
        self.map_processor
            .as_mut()
            .unwrap()
            .process_group_batch_sequences(
                &self.context.as_ref().unwrap().global_extra_data,
                &input_packet.reads,
                &input_packet.reads_buffer,
            );
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(
        &mut self,
        mut packet_send: S,
    ) {
        let context = self.context.as_ref().unwrap();

        let packet = self
            .map_processor
            .as_mut()
            .unwrap()
            .process_group_finalize(&context.global_extra_data);
        packet_send(
            context.finalizer_address.read().as_ref().unwrap().clone(),
            packet,
        );
        // self.context.take();
    }

    fn is_finished(&self) -> bool {
        false
    }

    fn get_total_memory(&self) -> u64 {
        0
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
