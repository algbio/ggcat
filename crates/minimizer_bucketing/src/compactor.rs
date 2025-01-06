use std::{future::Future, marker::PhantomData, path::PathBuf};

use crate::{
    queue_data::MinimizerBucketingQueueData, MinimizerBucketingExecutionContext,
    MinimizerBucketingExecutorFactory,
};
use parallel_processor::{
    buckets::ChunkingStatus,
    execution_manager::{
        executor::{AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver},
        memory_tracker::MemoryTracker,
    },
    memory_fs::MemoryFs,
    mt_debug_counters::{
        counter::{AtomicCounter, SumMode},
        declare_counter_i64,
    },
};
use utils::track;

pub struct MinimizerBucketingCompactor<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static>
{
    _phantom: PhantomData<E>, // mem_tracker: MemoryTracker<Self>,
}

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("mb_addr_wait_compactor", SumMode, false);

#[derive(Clone, Debug)]
pub struct CompactorInitData {
    pub bucket_index: u16,
}

impl<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static> AsyncExecutor
    for MinimizerBucketingCompactor<E>
{
    type InputPacket = MinimizerBucketingQueueData<E::StreamInfo>;
    type OutputPacket = ();
    type GlobalParams = MinimizerBucketingExecutionContext<E::GlobalData>;
    type InitData = CompactorInitData;

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
            //     while let Ok((_, init_data)) =
            //         track!(receiver.obtain_address().await, ADDR_WAITING_COUNTER)
            //     {
            //         let bucket_index = init_data.bucket_index as usize;
            //         let mut buckets = global_params.buckets.get_stored_buckets().lock();

            //         // TODO: Choose buckets
            //         let bucket_path = buckets[bucket_index].remove(0);

            //         drop(buckets);

            //         // TODO: Process buckets
            //         MemoryFs::ensure_flushed(&bucket_path);

            //         let new_path = append_ext("test", bucket_path.clone());

            //         pub fn append_ext(ext: impl AsRef<std::ffi::OsStr>, path: PathBuf) -> PathBuf {
            //             let mut os_string: std::ffi::OsString = path.into();
            //             os_string.push(".");
            //             os_string.push(ext.as_ref());
            //             os_string.into()
            //         }

            //         std::fs::rename(&bucket_path, &new_path).unwrap();

            //         // Update the final buckets with new info
            //         let mut buckets = global_params.buckets.get_stored_buckets().lock();
            //         buckets[bucket_index].push(new_path);
            //     }
        }
    }
}

// impl<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static> MinimizerBucketingCompactor<E> {
//     async fn execute(
//         &self,
//         context: &MinimizerBucketingExecutionContext<E::GlobalData>,
//         ops: &ExecutorAddressOperations<'_, Self>,
//     ) {
//         for address in ops.get_address()

//     }
// }
