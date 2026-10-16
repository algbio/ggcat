use crate::MinimzerBucketingFilesReaderInputPacket;
use crate::queue_data::MinimizerBucketingQueueData;
use crate::{MinimizerBucketingExecutionContext, MinimizerBucketingExecutorFactory};
use ggcat_logging::stats;
use io::sequences_stream::GenericSequencesStream;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::atomic::Ordering;

pub struct MinimizerBucketingFilesReader<
    Factory: MinimizerBucketingExecutorFactory,
    SequencesStream: GenericSequencesStream,
> {
    // mem_tracker: MemoryTracker<Self>,
    _phantom: PhantomData<(Factory::GlobalData, Factory::StreamInfo, SequencesStream)>,
}
unsafe impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    Sync for MinimizerBucketingFilesReader<Factory, SequencesStream>
{
}
unsafe impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    Send for MinimizerBucketingFilesReader<Factory, SequencesStream>
{
}

impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    MinimizerBucketingFilesReader<Factory, SequencesStream>
{
    fn execute(
        &self,
        context: &MinimizerBucketingExecutionContext<Factory>,
        ops: &ExecutorAddressOperations<Self>,
    ) {
        let packets_pool = &context.packets_pool;

        let mut sequences_stream = SequencesStream::new();

        while let Some(input_packet) = ops.receive_packet() {
            let stream_info = input_packet.stream_info.clone();

            context.current_file.fetch_add(1, Ordering::Relaxed);

            stats!(
                let mut stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                let mut last_stats_id = None;
            );

            let records = sequences_stream.read_block(
                &input_packet.sequences,
                &context.parser_settings,
                |buffer, sequence_info| {
                    let mut packet = packets_pool.alloc_packet();
                    let data = packet.deref_mut();
                    // The parser goes on in the buffer the packet held, emptied by its reset.
                    let empty = std::mem::replace(&mut data.buffer, buffer);
                    data.sequence_info = sequence_info;
                    data.stream_info = stream_info.clone();

                    stats!(
                        let stat_end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                        let chunk_index = { let counter = &mut ggcat_logging::get_stat!(stats.input_counter); *counter += 1; *counter };
                        let stat_seq_count = data.buffer.len();
                        let stat_seq_size = data.get_total_size();
                        let stats_id = ggcat_logging::generate_stat_id!();
                        data.stats_block_id = stats_id;
                        last_stats_id = Some(stats_id);
                    );

                    context
                        .executor_group_address
                        .read()
                        .as_ref()
                        .unwrap()
                        .send_packet(packet);

                    stats!(let finished_send_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed(););
                    stats!(
                        stats.assembler.input_chunks.push(ggcat_logging::stats::InputChunkStats {
                            id: stats_id,
                            index: chunk_index,
                            sequences_count: stat_seq_count,
                            sequences_size: stat_seq_size,
                            start_time: stat_start_time.into(),
                            end_time: stat_end_time.into(),
                            finished_send_time: finished_send_time.into(),
                            is_last: false,
                        });
                    );
                    stats!(stat_start_time = finished_send_time);

                    empty
                },
            );

            // Which packet was the input's last is only known once it is read.
            stats!(if let Some(id) = last_stats_id {
                let mut stats = ggcat_logging::stats::STATS.lock();
                if let Some(chunk) = stats
                    .assembler
                    .input_chunks
                    .iter_mut()
                    .rev()
                    .find(|chunk| chunk.id == id)
                {
                    chunk.is_last = true;
                }
            });

            Factory::input_records(&context.common.global_data, &stream_info, records);
            context.processed_files.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    PoolObjectTrait for MinimzerBucketingFilesReaderInputPacket<Factory, SequencesStream>
{
    type InitData = ();

    fn allocate_new(_: &Self::InitData) -> Self {
        unreachable!()
    }

    fn reset(&mut self) {}
}

impl<
    Factory: MinimizerBucketingExecutorFactory,
    SequencesStream: GenericSequencesStream + Sync + Send + 'static,
> AsyncExecutor for MinimizerBucketingFilesReader<Factory, SequencesStream>
{
    type InputPacket = MinimzerBucketingFilesReaderInputPacket<Factory, SequencesStream>;
    type OutputPacket = MinimizerBucketingQueueData<Factory::StreamInfo>;
    type GlobalParams = MinimizerBucketingExecutionContext<Factory>;
    type InitData = ();
    const ALLOW_PARALLEL_ADDRESS_EXECUTION: bool = true;

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn executor_main<'a>(
        &'a mut self,
        global_params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
    ) {
        while let Ok(address) = receiver.obtain_address() {
            self.execute(global_params, &address);
        }
    }
}

//
// impl X {
//     const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;
//
//     const MEMORY_FIELDS_COUNT: usize = 1;
//     const MEMORY_FIELDS: &'static [&'static str] = &["SEQ_BUFFER"];
// }
