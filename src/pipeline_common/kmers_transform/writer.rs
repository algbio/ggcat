use crate::config::{DEFAULT_PREFETCH_AMOUNT, USE_SECOND_BUCKET};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
    KmersTransformMapProcessor,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::KEEP_FILES;
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

pub struct KmersTransformWriter<F: KmersTransformExecutorFactory> {
    context: Option<Arc<KmersTransformContext<F>>>,
    final_executor: Option<F::FinalExecutorType>,
}

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformWriter<F> {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            context: None,
            final_executor: None,
        }
    }

    fn reset(&mut self) {
        self.final_executor.take();
        self.context.take();
    }
}

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformWriter<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    type InputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
    type OutputPacket = ();
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = Arc<KmersTransformContext<F>>;

    fn allocate_new_group<D: FnOnce(Vec<ExecutorAddress>)>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _executors_initializer: D,
    ) -> (Self::BuildParams, usize) {
        let threads_count = global_params.compute_threads_count;
        (global_params, threads_count)
    }

    fn required_pool_items(&self) -> u64 {
        0
    }

    fn pre_execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        reinit_params: Self::BuildParams,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        if self.final_executor.is_none() {
            self.final_executor = Some(F::new_final_executor(&reinit_params.global_extra_data));
        }
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
        let context = self.context.as_ref().unwrap();
        self.final_executor
            .as_mut()
            .unwrap()
            .process_map(&context.global_extra_data, input_packet);
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, _packet_send: S) {
        let context = self.context.as_ref().unwrap();
        self.final_executor
            .take()
            .unwrap()
            .finalize(&context.global_extra_data);
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
