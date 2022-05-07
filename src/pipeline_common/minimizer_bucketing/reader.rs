use crate::io::sequences_reader::SequencesReader;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutionContext;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{Executor, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use replace_with::replace_with_or_abort;
use std::marker::PhantomData;
use std::ops::DerefMut;
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
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const BASE_PRIORITY: u64 = 0;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
    const STRICT_POOL_ALLOC: bool = true;

    type InputPacket = (PathBuf, FileInfo);
    type OutputPacket = MinimizerBucketingQueueData<FileInfo>;
    type GlobalParams = MinimizerBucketingExecutionContext<GlobalData>;
    type MemoryParams = ();

    type BuildParams = Arc<MinimizerBucketingExecutionContext<GlobalData>>;

    fn allocate_new_group<D: FnOnce(Vec<ExecutorAddress>)>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _executors_initializer: D,
    ) -> (Self::BuildParams, usize) {
        let read_threads_count = global_params.read_threads_count;
        (global_params, read_threads_count)
    }

    fn required_pool_items(&self) -> u64 {
        1
    }

    fn pre_execute<
        PF: FnMut() -> Packet<Self::OutputPacket>,
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        context: Self::BuildParams,
        _packet_alloc_force: PF,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        self.context = Some(context);
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
        // println!("Executing thing {}!", input_packet.0.display());
        let mut data_packet = packet_alloc();
        let file_info = input_packet.1.clone();

        let context = self.context.as_mut().unwrap();

        let data = data_packet.deref_mut();
        data.file_info = file_info.clone();
        data.start_read_index = 0;

        let mut read_index = 0;

        context.current_file.fetch_add(1, Ordering::Relaxed);

        SequencesReader::process_file_extended(
            &input_packet.0,
            |x| {
                let mut data = data_packet.deref_mut();

                if x.seq.len() < context.common.k {
                    return;
                }

                if unlikely(!data.push_sequences(x)) {
                    assert!(
                        data.start_read_index as usize + data.sequences.len()
                            <= read_index as usize
                    );

                    replace_with_or_abort(&mut data_packet, |packet| {
                        packet_send(
                            context
                                .executor_group_address
                                .read()
                                .as_ref()
                                .unwrap()
                                .clone(),
                            packet,
                        );
                        packet_alloc()
                    });

                    data = data_packet.deref_mut();
                    data.file_info = file_info.clone();
                    data.start_read_index = read_index;

                    if !data.push_sequences(x) {
                        panic!("Out of memory!");
                    }
                }
                read_index += 1;
            },
            false,
        );

        if data_packet.sequences.len() > 0 {
            packet_send(
                context
                    .executor_group_address
                    .read()
                    .as_ref()
                    .unwrap()
                    .clone(),
                data_packet,
            );
        }

        context.processed_files.fetch_add(1, Ordering::Relaxed);
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, _packet_send: S) {
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
