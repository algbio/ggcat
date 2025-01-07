pub mod extra_data;

use std::{
    future::Future,
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    queue_data::MinimizerBucketingQueueData, MinimizerBucketingExecutionContext,
    MinimizerBucketingExecutorFactory,
};
use config::{
    get_compression_level_info, get_memory_mode, SwapPriority, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
};
use io::concurrent::temp_reads::{
    creads_utils::CompressedReadsBucketDataSerializer,
    extra_data::SequenceExtraDataTempBufferManagement,
};
use parallel_processor::{
    buckets::{
        readers::async_binary_reader::{AsyncBinaryReader, AsyncReaderThread},
        writers::compressed_binary_writer::CompressedBinaryWriter,
        LockFreeBucket,
    },
    execution_manager::{
        executor::{AsyncExecutor, ExecutorReceiver},
        memory_tracker::MemoryTracker,
    },
    memory_fs::RemoveFileMode,
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
            let read_thread = AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE, 4);

            static COMPACTED_INDEX: AtomicUsize = AtomicUsize::new(0);

            while let Ok((_, init_data)) =
                track!(receiver.obtain_address().await, ADDR_WAITING_COUNTER)
            {
                let bucket_index = init_data.bucket_index as usize;
                let mut buckets = global_params.buckets.get_stored_buckets().lock();

                // TODO: Choose buckets
                let bucket_path = buckets[bucket_index].remove(0);

                drop(buckets);

                let new_path = bucket_path.parent().unwrap().join(format!(
                    "compacted-{}.dat",
                    COMPACTED_INDEX.fetch_add(1, Ordering::Relaxed)
                ));

                // TODO: Process buckets

                let reader = AsyncBinaryReader::new(
                    &bucket_path,
                    true,
                    RemoveFileMode::Remove {
                        remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                );

                let mut items = reader.get_items_stream::<CompressedReadsBucketDataSerializer<
                    E::ExtraData,
                    E::FLAGS_COUNT,
                    false,
                >>(
                    read_thread.clone(),
                    Vec::new(),
                    E::ExtraData::new_temp_buffer(),
                );

                // let mut serializer = CompressedReadsBucketDataSerializer::<
                //     ExtraCompactedData<E::ColorsManager>,
                //     E::FLAGS_COUNT,
                //     false,
                // >::new();

                let new_bucket = CompressedBinaryWriter::new(
                    &new_path,
                    &(
                        get_memory_mode(SwapPriority::MinimizerBuckets),
                        MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                        get_compression_level_info(),
                    ),
                    0,
                );

                let new_path = new_bucket.get_path();

                let mut buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

                while let Some(((flags, _, extra, read), extra_buffer)) = items.next() {
                    // serializer.write_to(
                    //     &CompressedReadsBucketData::new_packed(read, flags, 0),
                    //     &mut buffer,
                    //     &extra,
                    //     extra_buffer,
                    // );

                    if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                        new_bucket.write_data(&buffer);
                        buffer.clear();
                    }
                }

                if buffer.len() > 0 {
                    new_bucket.write_data(&buffer);
                }

                new_bucket.finalize();

                // Update the final buckets with new info
                let mut buckets = global_params.buckets.get_stored_buckets().lock();
                buckets[bucket_index].push(new_path);
            }
        }
    }
}
