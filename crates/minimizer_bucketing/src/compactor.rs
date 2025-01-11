pub mod extra_data;

use std::{
    borrow::Borrow,
    cmp::Reverse,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    queue_data::MinimizerBucketingQueueData, MinimizerBucketMode,
    MinimizerBucketingExecutionContext, MinimizerBucketingExecutorFactory,
};
use colors::non_colored::NonColoredManager;
use config::{
    get_compression_level_info, get_memory_mode, MultiplicityCounterType, SwapPriority,
    DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, WORKERS_PRIORITY_HIGH,
};
use io::{
    compressed_read::CompressedReadIndipendent,
    concurrent::temp_reads::{
        creads_utils::{
            CompressedReadsBucketData, CompressedReadsBucketDataSerializer, NoSecondBucket,
            WithMultiplicity,
        },
        extra_data::SequenceExtraDataTempBufferManagement,
    },
};
use io::{
    compressed_read::{BorrowableCompressedRead, CompressedRead},
    creads_helper,
};
use parallel_processor::{buckets::bucket_writer::BucketItemSerializer, memory_fs::MemoryFs};
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
use rustc_hash::FxHashMap;
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

struct SuperKmerEntry(*const Vec<u8>, CompressedReadIndipendent);

impl SuperKmerEntry {
    fn get_read(&self) -> CompressedRead {
        let storage = unsafe { &*self.0 };
        self.1.as_reference(storage)
    }
}

impl<'a> Borrow<BorrowableCompressedRead> for SuperKmerEntry {
    fn borrow(&self) -> &BorrowableCompressedRead {
        let storage = unsafe { &*self.0 };
        self.1.as_reference(storage).get_borrowable()
    }
}

impl Hash for SuperKmerEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.get_read().get_borrowable().hash(state);
    }
}

impl PartialEq for SuperKmerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.get_read().get_borrowable() == other.get_read().get_borrowable()
    }
}

impl Eq for SuperKmerEntry {}

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

            while let Ok((_, init_data)) = track!(
                receiver
                    .obtain_address_with_priority(WORKERS_PRIORITY_HIGH)
                    .await,
                ADDR_WAITING_COUNTER
            ) {
                let mut chosen_buckets = vec![];

                let bucket_index = init_data.bucket_index as usize;
                let mut buckets = global_params.buckets.get_stored_buckets().lock();

                let mut total_size = 0;

                // Avoid crashing in case there are no chunks and this is called
                if buckets[bucket_index].chunks.is_empty() {
                    continue;
                }

                buckets[bucket_index].chunks.sort_by_cached_key(|c| {
                    let file_size = MemoryFs::get_file_size(c).unwrap();
                    total_size += file_size;
                    Reverse(file_size)
                });

                let mut chosen_size = 0;

                // Choose the buckets to compact, taking all the buckets that strictly do not exceed half of the total buckets size.
                // this allows to keep a linear time complexity

                let mut last = buckets[bucket_index].chunks.pop().unwrap();
                let mut last_size = MemoryFs::get_file_size(&last).unwrap();

                while chosen_size + last_size < total_size / 2 {
                    chosen_size += last_size;
                    chosen_buckets.push(last);

                    last = buckets[bucket_index].chunks.pop().unwrap();
                    last_size = MemoryFs::get_file_size(&last).unwrap();
                }

                // Add back the last unused chunk
                buckets[bucket_index].chunks.push(last);

                // Do not compact if we have only one bucket
                if chosen_buckets.len() == 1 {
                    buckets[bucket_index]
                        .chunks
                        .extend(chosen_buckets.drain(..));
                    continue;
                }

                drop(buckets);

                // println!("Compacting bucket {} with total total size {} => chosen {} chunks with total size {}", bucket_index, total_size, chosen_buckets.len(), chosen_size);

                let new_path = global_params.output_path.join(format!(
                    "compacted-{}.dat",
                    COMPACTED_INDEX.fetch_add(1, Ordering::Relaxed)
                ));

                let mut super_kmers_hashmap: FxHashMap<
                    SuperKmerEntry,
                    (u8, MultiplicityCounterType),
                > = FxHashMap::default();
                let mut kmers_storage = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

                let mut sequences_delta = 0;

                for bucket_path in chosen_buckets {
                    // Reading the buckets
                    let reader = AsyncBinaryReader::new(
                        &bucket_path,
                        true,
                        RemoveFileMode::Remove {
                            remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                        },
                        DEFAULT_PREFETCH_AMOUNT,
                    );

                    let format_data: MinimizerBucketMode = reader.get_data_format_info().unwrap();
                    creads_helper! {
                        helper_read_bucket_with_opt_multiplicity::<
                            E::ExtraData,
                            E::FLAGS_COUNT,
                            NoSecondBucket
                        >(
                            &reader,
                            read_thread.clone(),
                            matches!(format_data, MinimizerBucketMode::Compacted),
                            |data, _extra_buffer| {
                                let (flags, _, _extra, read, multiplicity) = data;

                                sequences_delta -= 1;

                                if let Some(entry) = super_kmers_hashmap.get_mut(
                                    read.get_borrowable(),
                                ) {
                                    // Combine the flags from the two super-kmers
                                    entry.0 |= flags;
                                    entry.1 += multiplicity;
                                } else {
                                    let new_read = CompressedReadIndipendent::from_read(&read, &mut kmers_storage);
                                    assert!(!super_kmers_hashmap.contains_key(read.get_borrowable()));
                                    assert!(!super_kmers_hashmap.contains_key(&SuperKmerEntry(&kmers_storage as *const _, new_read)));
                                    super_kmers_hashmap.insert(
                                        SuperKmerEntry(&kmers_storage as *const _, new_read),
                                        (flags, multiplicity),
                                    );
                                }
                            }
                        );
                    }
                }

                let new_bucket = CompressedBinaryWriter::new(
                    &new_path,
                    &(
                        get_memory_mode(SwapPriority::MinimizerBuckets),
                        MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                        get_compression_level_info(),
                    ),
                    0,
                    &MinimizerBucketMode::Compacted,
                );

                let mut serializer = CompressedReadsBucketDataSerializer::<
                    NonColoredManager,
                    E::FLAGS_COUNT,
                    NoSecondBucket,
                    WithMultiplicity,
                >::new();

                let mut buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

                // TODO: SUPPORT COLORS?
                let out_extra_buffer =
                    <NonColoredManager as SequenceExtraDataTempBufferManagement>::new_temp_buffer();
                let empty_extra = NonColoredManager::default();

                for (read, (flags, multiplicity)) in super_kmers_hashmap {
                    let read = read.get_read();
                    sequences_delta += 1;

                    serializer.write_to(
                        &CompressedReadsBucketData::new_packed_with_multiplicity(
                            read,
                            flags,
                            0,
                            multiplicity,
                        ),
                        &mut buffer,
                        &empty_extra,
                        &out_extra_buffer,
                    );
                    if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                        new_bucket.write_data(&buffer);
                        buffer.clear();
                    }
                }

                if buffer.len() > 0 {
                    new_bucket.write_data(&buffer);
                }

                let new_path = new_bucket.get_path();
                new_bucket.finalize();

                // Update the final buckets with new info
                let mut buckets = global_params.buckets.get_stored_buckets().lock();
                buckets[bucket_index].was_compacted = true;
                buckets[bucket_index].chunks.push(new_path);
                global_params.common.compaction_offsets[bucket_index]
                    .fetch_add(sequences_delta, Ordering::Relaxed);
            }
        }
    }
}
