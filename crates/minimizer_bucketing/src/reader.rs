use crate::MinimzerBucketingFilesReaderInputPacket;
use crate::queue_data::MinimizerBucketingQueueData;
use crate::{MinimizerBucketingExecutionContext, MinimizerBucketingExecutorFactory};
use ggcat_logging::stats;
use io::sequences_stream::GenericSequencesStream;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use replace_with::replace_with_or_abort;
use std::cmp::max;
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

        while let Some(mut input_packet) = ops.receive_packet() {
            let mut data_packet = packets_pool.alloc_packet();
            let stream_info = input_packet.stream_info.clone();

            let data = data_packet.deref_mut();
            data.stream_info = stream_info.clone();
            data.start_read_index = 0;

            let mut read_index = 0;

            context.current_file.fetch_add(1, Ordering::Relaxed);

            let mut max_len = 0;

            stats!(
                let mut stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            );

            sequences_stream.read_block(
                &mut input_packet.sequences,
                context.copy_ident,
                context.partial_read_copyback,
                |x, seq_info| {
                    let mut data = data_packet.deref_mut();

                    if x.seq.len() < context.common.ignored_length {
                        return;
                    }

                    max_len = max(max_len, x.ident_data.len() + x.seq.len());

                    if unlikely(!data.push_sequences(x, seq_info)) {
                        assert!(
                            data.start_read_index as usize + data.sequences.len()
                                <= read_index as usize
                        );

                        replace_with_or_abort(&mut data_packet, |packet| {
                            stats!(
                                let stat_end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                                let chunk_index = { let counter = &mut ggcat_logging::get_stat!(stats.input_counter); *counter += 1; *counter };
                                let stat_seq_count = packet.sequences.len();
                                let stat_seq_size = packet.get_total_size();
                                let stats_id = ggcat_logging::generate_stat_id!();
                                let mut packet = packet;
                                packet.stats_block_id = stats_id;
                            );

                            context.executor_group_address
                                .read()
                                .as_ref()
                                .unwrap()
                                .send_packet(packet);

                            let new_packet = packets_pool.alloc_packet();

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

                            new_packet
                        });

                        // mem_tracker.update_memory_usage(&[max_len]);

                        data = data_packet.deref_mut();
                        data.stream_info = stream_info.clone();
                        data.start_read_index = read_index;

                        if !data.push_sequences(x, seq_info) {
                            panic!("BUG: Out of memory!");
                        }
                    }
                    read_index += 1;
                },
            );

            if data_packet.sequences.len() > 0 {
                stats!(
                    let stat_end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                    let chunk_index = { let counter = &mut ggcat_logging::get_stat!(stats.input_counter); *counter += 1; *counter };
                    let stat_seq_count = data_packet.sequences.len();
                    let stat_seq_size = data_packet.get_total_size();
                    let stats_id = ggcat_logging::generate_stat_id!();
                    data_packet.stats_block_id = stats_id;
                );

                context
                    .executor_group_address
                    .read()
                    .as_ref()
                    .unwrap()
                    .send_packet(data_packet);

                stats!(
                    let finished_send_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                );

                stats!(
                    stats.assembler.input_chunks.push(ggcat_logging::stats::InputChunkStats {
                        id: stats_id,
                        index: chunk_index,
                        sequences_count: stat_seq_count,
                        sequences_size: stat_seq_size,
                        start_time: stat_start_time.into(),
                        end_time: stat_end_time.into(),
                        finished_send_time: finished_send_time.into(),
                        is_last: true,
                    });
                );
            }

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
