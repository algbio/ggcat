use crate::queue_data::MinimizerBucketingQueueData;
use crate::MinimizerBucketingExecutionContext;
use io::sequences_reader::SequencesReader;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{Executor, ExecutorOperations, ExecutorType};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use replace_with::replace_with_or_abort;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct MinimizerBucketingFilesReader<
    GlobalData: Sync + Send + 'static,
    FileInfo: Clone + Sync + Send + Default + 'static,
> {
    context: Arc<MinimizerBucketingExecutionContext<GlobalData>>,
    mem_tracker: MemoryTracker<Self>,
    _phantom: PhantomData<FileInfo>,
}

impl<GlobalData: Sync + Send + 'static, FileInfo: Clone + Sync + Send + Default + 'static>
    PoolObjectTrait for MinimizerBucketingFilesReader<GlobalData, FileInfo>
{
    type InitData = (
        Arc<MinimizerBucketingExecutionContext<GlobalData>>,
        MemoryTracker<Self>,
    );

    fn allocate_new((context, mem_tracker): &Self::InitData) -> Self {
        Self {
            context: context.clone(),
            mem_tracker: mem_tracker.clone(),
            _phantom: PhantomData,
        }
    }

    fn reset(&mut self) {}
}

impl<GlobalData: Sync + Send + 'static, FileInfo: Clone + Sync + Send + Default + 'static> Executor
    for MinimizerBucketingFilesReader<GlobalData, FileInfo>
{
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const MEMORY_FIELDS_COUNT: usize = 1;
    const MEMORY_FIELDS: &'static [&'static str] = &["SEQ_BUFFER"];

    const BASE_PRIORITY: u64 = 0;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
    const STRICT_POOL_ALLOC: bool = true;

    type InputPacket = (PathBuf, FileInfo);
    type OutputPacket = MinimizerBucketingQueueData<FileInfo>;
    type GlobalParams = MinimizerBucketingExecutionContext<GlobalData>;
    type MemoryParams = ();

    type BuildParams = ();

    fn allocate_new_group<E: ExecutorOperations<Self>>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _ops: E,
    ) -> (Self::BuildParams, usize) {
        let read_threads_count = global_params.read_threads_count;
        ((), read_threads_count)
    }

    fn required_pool_items(&self) -> u64 {
        1
    }

    fn pre_execute<E: ExecutorOperations<Self>>(
        &mut self,
        _reinit_params: Self::BuildParams,
        _ops: E,
    ) {
    }

    fn execute<E: ExecutorOperations<Self>>(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        mut ops: E,
    ) {
        // println!("Executing thing {}!", input_packet.0.display());
        let mut data_packet = ops.packet_alloc();
        let file_info = input_packet.1.clone();

        let data = data_packet.deref_mut();
        data.file_info = file_info.clone();
        data.start_read_index = 0;

        let mut read_index = 0;

        self.context.current_file.fetch_add(1, Ordering::Relaxed);

        let mut max_len = 0;

        SequencesReader::process_file_extended(
            &input_packet.0,
            |x| {
                let mut data = data_packet.deref_mut();

                if x.seq.len() < self.context.common.k {
                    return;
                }

                max_len = max(
                    max_len,
                    x.ident.len() + x.seq.len() + x.qual.map(|q| q.len()).unwrap_or(0),
                );

                if unlikely(!data.push_sequences(x)) {
                    assert!(
                        data.start_read_index as usize + data.sequences.len()
                            <= read_index as usize
                    );

                    replace_with_or_abort(&mut data_packet, |packet| {
                        ops.packet_send(
                            self.context
                                .executor_group_address
                                .read()
                                .as_ref()
                                .unwrap()
                                .clone(),
                            packet,
                        );
                        ops.packet_alloc()
                    });

                    self.mem_tracker.update_memory_usage(&[max_len]);

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
            ops.packet_send(
                self.context
                    .executor_group_address
                    .read()
                    .as_ref()
                    .unwrap()
                    .clone(),
                data_packet,
            );
        }

        self.context.processed_files.fetch_add(1, Ordering::Relaxed);
    }

    fn finalize<E: ExecutorOperations<Self>>(&mut self, _ops: E) {}

    fn is_finished(&self) -> bool {
        false
    }
    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
