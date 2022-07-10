use crate::queue_data::MinimizerBucketingQueueData;
use crate::MinimizerBucketingExecutionContext;
use io::sequences_reader::SequencesReader;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::utils::replace_with_async::replace_with_async;
use replace_with::replace_with_or_abort;
use std::cmp::max;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct MinimizerBucketingFilesReader<
    GlobalData: Sync + Send + 'static,
    FileInfo: Clone + Sync + Send + Default + 'static,
> {
    // mem_tracker: MemoryTracker<Self>,
    _phantom: PhantomData<(FileInfo, GlobalData)>,
}

impl<GlobalData: Sync + Send + 'static, FileInfo: Clone + Sync + Send + Default + 'static>
    MinimizerBucketingFilesReader<GlobalData, FileInfo>
{
    async fn execute(
        &self,
        context: &MinimizerBucketingExecutionContext<GlobalData>,
        ops: &ExecutorAddressOperations<'_, Self>,
    ) {
        let packets_pool = ops.pool_alloc_await().await;

        while let Some(input_packet) = ops.receive_packet().await {
            let mut data_packet = packets_pool.alloc_packet().await;
            let file_info = input_packet.1.clone();

            let data = data_packet.deref_mut();
            data.file_info = file_info.clone();
            data.start_read_index = 0;

            let mut read_index = 0;

            context.current_file.fetch_add(1, Ordering::Relaxed);

            let mut max_len = 0;

            SequencesReader::process_file_extended(
                &input_packet.0,
                |x| {
                    let mut data = data_packet.deref_mut();

                    if x.seq.len() < context.common.k {
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

impl<GlobalData: Sync + Send + 'static, FileInfo: Clone + Sync + Send + Default + 'static>
    AsyncExecutor for MinimizerBucketingFilesReader<GlobalData, FileInfo>
{
    type InputPacket = (PathBuf, FileInfo);
    type OutputPacket = MinimizerBucketingQueueData<FileInfo>;
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
        memory_tracker: MemoryTracker<Self>,
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
