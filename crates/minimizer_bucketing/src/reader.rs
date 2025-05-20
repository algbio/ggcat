use crate::queue_data::MinimizerBucketingQueueData;
use crate::MinimizerBucketingExecutionContext;
use config::{PRIORITY_SCHEDULING_LOW, WORKERS_PRIORITY_BASE};
use ggcat_logging::stats;
use io::sequences_stream::GenericSequencesStream;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::scheduler::PriorityScheduler;
use replace_with::replace_with_or_abort;
use std::cmp::max;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::atomic::Ordering;

pub struct MinimizerBucketingFilesReader<
    GlobalData: Sync + Send + 'static,
    StreamInfo: Sync + Send + Clone + Default + 'static,
    SequencesStream: GenericSequencesStream,
> {
    // mem_tracker: MemoryTracker<Self>,
    _phantom: PhantomData<(GlobalData, StreamInfo, SequencesStream)>,
}
unsafe impl<
        GlobalData: Sync + Send + 'static,
        StreamInfo: Sync + Send + Clone + Default + 'static,
        SequencesStream: GenericSequencesStream,
    > Sync for MinimizerBucketingFilesReader<GlobalData, StreamInfo, SequencesStream>
{
}
unsafe impl<
        GlobalData: Sync + Send + 'static,
        StreamInfo: Sync + Send + Clone + Default + 'static,
        SequencesStream: GenericSequencesStream,
    > Send for MinimizerBucketingFilesReader<GlobalData, StreamInfo, SequencesStream>
{
}

impl<
        GlobalData: Sync + Send + 'static,
        StreamInfo: Sync + Send + Clone + Default + 'static,
        SequencesStream: GenericSequencesStream,
    > MinimizerBucketingFilesReader<GlobalData, StreamInfo, SequencesStream>
{
    async fn execute(
        &self,
        context: &MinimizerBucketingExecutionContext<GlobalData>,
        ops: &ExecutorAddressOperations<'_, Self>,
    ) {
        let thread_handle = PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_LOW);

        let packets_pool = ops.pool_alloc_await(0, &thread_handle, true).await;

        let mut sequences_stream = SequencesStream::new();

        while let Some(mut input_packet) = ops.receive_packet(&thread_handle).await {
            let mut data_packet = packets_pool.alloc_packet().await;
            let stream_info = input_packet.1.clone();

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
                &mut input_packet.0,
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

                            ops.packet_send(
                                context
                                    .executor_group_address
                                    .read()
                                    .as_ref()
                                    .unwrap()
                                    .clone(),
                                packet,
                                &thread_handle,
                            );
                            let new_packet = packets_pool.alloc_packet_blocking();

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

                ops.packet_send(
                    context
                        .executor_group_address
                        .read()
                        .as_ref()
                        .unwrap()
                        .clone(),
                    data_packet,
                    &thread_handle,
                );

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

impl<
        GlobalData: Sync + Send + 'static,
        StreamInfo: Sync + Send + Clone + Default + 'static,
        SequencesStream: GenericSequencesStream,
    > AsyncExecutor for MinimizerBucketingFilesReader<GlobalData, StreamInfo, SequencesStream>
{
    type InputPacket = (SequencesStream::SequenceBlockData, StreamInfo);
    type OutputPacket = MinimizerBucketingQueueData<StreamInfo>;
    type GlobalParams = MinimizerBucketingExecutionContext<GlobalData>;
    type InitData = ();

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        _memory_tracker: MemoryTracker<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let thread_handle = PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_LOW);

            while let Ok((address, _)) = receiver
                .obtain_address_with_priority(WORKERS_PRIORITY_BASE, &thread_handle)
                .await
            {
                let read_threads_count = global_params.read_threads_count;

                let mut spawner = address.make_spawner();
                for _ in 0..read_threads_count {
                    spawner.spawn_executor(async {
                        self.execute(global_params, &address).await;
                    });
                }
                spawner.executors_await().await;
            }
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
