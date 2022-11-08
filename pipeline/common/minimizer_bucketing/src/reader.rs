use crate::queue_data::MinimizerBucketingQueueData;
use crate::MinimizerBucketingExecutionContext;
use io::sequences_stream::GenericSequencesStream;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
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
        let packets_pool = ops.pool_alloc_await(0).await;

        let mut sequences_stream = SequencesStream::new();

        while let Some(mut input_packet) = ops.receive_packet().await {
            let mut data_packet = packets_pool.alloc_packet().await;
            let stream_info = input_packet.1.clone();

            let data = data_packet.deref_mut();
            data.stream_info = stream_info.clone();
            data.start_read_index = 0;

            let mut read_index = 0;

            context.current_file.fetch_add(1, Ordering::Relaxed);

            let mut max_len = 0;

            sequences_stream.read_block(
                &mut input_packet.0,
                context.copy_ident,
                context.partial_read_copyback,
                |x, _seq_data| {
                    let mut data = data_packet.deref_mut();

                    if x.seq.len() < context.common.k {
                        return;
                    }

                    max_len = max(max_len, x.ident_data.len() + x.seq.len());

                    if unlikely(!data.push_sequences(x)) {
                        assert!(
                            data.start_read_index as usize + data.sequences.len()
                                <= read_index as usize
                        );

                        replace_with_or_abort(&mut data_packet, |packet| {
                            ops.packet_send(
                                context
                                    .executor_group_address
                                    .read()
                                    .as_ref()
                                    .unwrap()
                                    .clone(),
                                packet,
                            );
                            packets_pool.alloc_packet_blocking()
                        });

                        // mem_tracker.update_memory_usage(&[max_len]);

                        data = data_packet.deref_mut();
                        data.stream_info = stream_info.clone();
                        data.start_read_index = read_index;

                        if !data.push_sequences(x) {
                            panic!("BUG: Out of memory!");
                        }
                    }
                    read_index += 1;
                },
            );

            if data_packet.sequences.len() > 0 {
                ops.packet_send(
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

    type AsyncExecutorFuture<'a> = impl Future<Output = ()> + Sync + Send + 'a;

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
    ) -> Self::AsyncExecutorFuture<'a> {
        async move {
            while let Ok((address, _)) = receiver.obtain_address().await {
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
