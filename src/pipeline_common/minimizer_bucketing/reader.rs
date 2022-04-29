use crate::io::sequences_reader::SequencesReader;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutionContext;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{Executor, ExecutorType, Packet};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::threadpools_chain::ObjectsPoolManager;
use std::marker::PhantomData;
use std::mem::swap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct MinimizerBucketingFilesReader<
    GlobalData: 'static,
    FileInfo: Clone + Sync + Send + Default + 'static,
> {
    context: Option<Arc<MinimizerBucketingExecutionContext<GlobalData>>>,
    _phantom: PhantomData<FileInfo>,
}

impl<GlobalData: 'static, FileInfo: Clone + Sync + Send + Default + 'static> PoolObjectTrait
    for MinimizerBucketingFilesReader<GlobalData, FileInfo>
{
    type InitData = ();

    fn allocate_new(_: &Self::InitData) -> Self {
        Self {
            context: None,
            _phantom: PhantomData,
        }
    }

    fn reset(&mut self) {}
}

impl<GlobalData: Sync + Send + 'static, FileInfo: Clone + Sync + Send + Default + 'static> Executor
    for MinimizerBucketingFilesReader<GlobalData, FileInfo>
{
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SingleUnit;
    const EXECUTOR_TYPE_INDEX: u64 = 0;
    type InputPacket = (PathBuf, FileInfo);
    type OutputPacket = MinimizerBucketingQueueData<FileInfo>;
    type GlobalParams = MinimizerBucketingExecutionContext<GlobalData>;
    type MemoryParams = ();

    type BuildParams = Arc<MinimizerBucketingExecutionContext<GlobalData>>;

    fn allocate_new_group(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
    ) -> Self::BuildParams {
        global_params
    }

    fn get_maximum_concurrency(&self) -> usize {
        self.context.as_ref().unwrap().read_threads_count
    }

    fn reinitialize<P: FnMut() -> Packet<Self::OutputPacket>>(
        &mut self,
        context: &Self::BuildParams,
        _packet_alloc: P,
    ) {
        self.context = Some(context.clone());
    }

    fn execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        mut packet_alloc: P,
        mut packet_send: S,
    ) {
        let mut data_packet = packet_alloc();
        let file_info = input_packet.get_value().1.clone();

        let context = self.context.as_mut().unwrap();

        let data = data_packet.get_value_mut();
        data.file_info = file_info.clone();
        data.start_read_index = 0;

        let mut read_index = 0;

        context.current_file.fetch_add(1, Ordering::Relaxed);

        SequencesReader::process_file_extended(
            &input_packet.get_value().0,
            |x| {
                let mut data = data_packet.get_value_mut();

                if x.seq.len() < context.common.k {
                    return;
                }

                if unlikely(!data.push_sequences(x)) {
                    assert!(
                        data.start_read_index as usize + data.sequences.len()
                            <= read_index as usize
                    );

                    let mut tmp_data = packet_alloc();
                    swap(&mut data_packet, &mut tmp_data);
                    data = data_packet.get_value_mut();
                    data.file_info = file_info.clone();
                    data.start_read_index = read_index;

                    packet_send(context.executor_group_address.clone(), tmp_data);

                    if !data.push_sequences(x) {
                        panic!("Out of memory!");
                    }
                }
                read_index += 1;
            },
            false,
        );

        if data_packet.get_value().sequences.len() > 0 {
            packet_send(context.executor_group_address.clone(), data_packet);
        }

        context.processed_files.fetch_add(1, Ordering::Relaxed);
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, _packet_send: S) {
    }

    fn get_total_memory(&self) -> u64 {
        0
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
