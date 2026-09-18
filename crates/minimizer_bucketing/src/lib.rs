pub mod compactor;
pub mod decode_helper;
pub mod deduplicator;
pub mod lane_runs;
mod reader;
pub mod resplit_bucket;
pub mod simd_batch;
mod sink;
pub mod split_buckets;

use crate::compactor::BucketsCompactor;
use crate::deduplicator::{
    BoundedDeduplicator, BucketOutput, BypassPolicy, DedupExtraData, DeduplicatingDispatcher,
    LazyBucketOutput, LazyBuckets, deduplicator_memory,
};
use crate::lane_runs::SimdScratch;
use crate::reader::MinimizerBucketingFilesReader;
use crate::simd_batch::{
    RECORD_CONTINUED, RECORD_CONTINUES, RecordInfo, SequencesLaneBatch, SimdSequencesBatch,
};
use bincode::{Decode, Encode};
use config::{
    BucketIndexType, DEFAULT_PER_CPU_BUFFER_SIZE, MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
    MINIMIZER_DEDUPLICATION_MEMORY, SIMD_LANE_BASES, SwapPriority,
};
use ggcat_logging::stats;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketData;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression, TempBuffer,
};
use io::sequences_stream::{GenericSequencesStream, SequenceInfo};
use parallel_processor::buckets::writers::compressed_binary_writer::{
    CompressedBinaryWriter, CompressionLevelInfo,
};
use parallel_processor::buckets::{
    BucketsCount, ChunkingStatus, LockFreeBucket, MultiChunkBucket, MultiThreadBuckets,
};
use parallel_processor::execution_manager::executor::{
    AddressProducer, AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::packet::PacketsPool;
use parallel_processor::execution_manager::scheduler::Scheduler;
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Mutex, RwLock};
use resplit_bucket::RewriteBucketCompute;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// The record encoding of a temporary bucket chunk, stamped into every chunk
/// file's header and read back to pick the decoder.
#[derive(Encode, Decode, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MinimizerBucketMode {
    /// Uncompacted, wide: the combined extra data and a multiplicity, as the
    /// deduplicator drains it. Read only by the compactor, so it never reaches
    /// [`crate::split_buckets`].
    Single,
    /// Uncompacted, narrow: one extra data entry and no multiplicity, exactly
    /// as the bucketing threads encoded it. Written only when a bucket bypasses
    /// its deduplicator, and likewise read only by the compactor.
    UncompactedNarrow,
    /// Compacted, narrow: the multiplicity-one half of a compaction.
    SingleGrouped,
    /// Compacted, wide: the multiplicity-above-one half of a compaction.
    Compacted,
}

pub struct MinimzerBucketingFilesReaderInputPacket<
    Factory: MinimizerBucketingExecutorFactory,
    SequencesStream: GenericSequencesStream,
> {
    pub sequences: SequencesStream::SequenceBlockData,
    pub stream_info: Factory::StreamInfo,
}

pub trait MinimizerInputSequence: hashes::HashableSequence + Copy {
    fn get_subslice(&self, range: Range<usize>) -> Self;
    fn seq_len(&self) -> usize;
    fn debug_to_string(&self) -> String;
}

impl<'a> MinimizerInputSequence for CompressedRead<'a> {
    fn get_subslice(&self, range: Range<usize>) -> Self {
        self.sub_slice(range.into())
    }

    fn seq_len(&self) -> usize {
        hashes::HashableSequence::bases_count(self)
    }

    fn debug_to_string(&self) -> String {
        self.to_string()
    }
}

impl MinimizerInputSequence for &[u8] {
    #[inline(always)]
    fn get_subslice(&self, range: Range<usize>) -> Self {
        &self[range]
    }

    fn seq_len(&self) -> usize {
        self.len()
    }

    fn debug_to_string(&self) -> String {
        std::str::from_utf8(self).unwrap().to_string()
    }
}

pub struct PushSequenceInfo<'a, S, F: MinimizerBucketingExecutorFactory> {
    pub bucket: BucketIndexType,
    pub second_bucket: BucketIndexType,
    pub sequence: S,
    pub extra_data: F::ReadExtraData,
    pub temp_buffer: &'a TempBuffer<F::ReadExtraData>,
    pub minimizer_pos: u16,
    pub flags: u8,
    pub rc: bool,
}

pub trait MinimizerBucketingExecutorFactory: Sync + Send + Sized + 'static {
    type GlobalData: Sync + Send + 'static;
    type ReadExtraData: SequenceExtraDataConsecutiveCompression;
    type PreprocessInfo: Default;
    type StreamInfo: Clone + Sync + Send + Default + 'static;

    type RewriteBucketCompute: RewriteBucketCompute;

    type FlagsCount: typenum::Unsigned + Sync + Send;

    type ExecutorType: MinimizerBucketingExecutor<Self>;

    fn new(global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>)
    -> Self::ExecutorType;
}

pub trait MinimizerBucketingExecutor<Factory: MinimizerBucketingExecutorFactory>:
    'static + Sync + Send
{
    fn preprocess_dna_sequence(
        &mut self,
        stream_info: &Factory::StreamInfo,
        sequence_info: SequenceInfo,
        read_index: u64,
        record: &RecordInfo<'_>,
        preprocess_info: &mut Factory::PreprocessInfo,
    );

    fn reprocess_sequence(
        &mut self,
        flags: u8,
        intermediate_data: &Factory::ReadExtraData,
        intermediate_data_buffer: &TempBuffer<Factory::ReadExtraData>,
        preprocess_info: &mut Factory::PreprocessInfo,
    );

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(PushSequenceInfo<S, Factory>),
        const SEPARATE_DUPLICATES: bool,
    >(
        &mut self,
        preprocess_info: &Factory::PreprocessInfo,
        sequence: S,
        range: Range<usize>,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        push_sequence: F,
    );

    /// Cuts every lane of a parsed batch into super-k-mers.
    ///
    /// `preprocess` is indexed by the record index a lane fragment carries.
    fn process_simd_batch<F, const SEPARATE_DUPLICATES: bool>(
        &mut self,
        batch: &SequencesLaneBatch,
        preprocess: &[Factory::PreprocessInfo],
        scratch: &mut SimdScratch,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        push_sequence: F,
    ) where
        F: for<'a, 'b> FnMut(PushSequenceInfo<'a, CompressedRead<'b>, Factory>);
}

pub struct MinimizerBucketingCommonData<GlobalData> {
    pub k: usize,
    pub m: usize,
    pub ignored_length: usize,
    pub canonical: bool,
    pub buckets_count: BucketsCount,
    pub second_buckets_count: BucketsCount,
    pub compaction_offsets: Vec<AtomicI64>,
    pub global_data: GlobalData,
}

impl<GlobalData> MinimizerBucketingCommonData<GlobalData> {
    pub fn new(
        k: usize,
        m: usize,
        buckets_count: BucketsCount,
        ignored_length: usize,
        second_buckets_count: BucketsCount,
        global_data: GlobalData,
        canonical: bool,
    ) -> Self {
        Self {
            k,
            m,
            ignored_length,
            buckets_count,
            second_buckets_count,
            compaction_offsets: (0..buckets_count.total_buckets_count)
                .map(|_| AtomicI64::new(0))
                .collect(),
            canonical,
            global_data,
        }
    }
}

pub struct MinimizerBucketingExecutionContext<
    E: MinimizerBucketingExecutorFactory + Sync + Send + 'static,
> {
    pub uncompacted_buckets: Mutex<Option<Arc<MultiThreadBuckets<UncompactedWriter>>>>,
    pub uncompacted_buckets_finalized: Mutex<Vec<Mutex<MultiChunkBucket>>>,
    /// The narrow buckets bypassed records go to, built only if some bucket
    /// ever stands its deduplicator down.
    pub narrow_buckets: Arc<LazyBuckets<UncompactedWriter>>,
    /// Written once by the leader, then only read. This must not be a lock:
    /// the read happens around a whole compaction, and holding one there
    /// serializes every thread that compacts a bucket at the end of the phase.
    pub narrow_buckets_finalized: OnceLock<Vec<Mutex<MultiChunkBucket>>>,
    pub compacted_buckets: Option<Vec<Mutex<MultiChunkBucket>>>,
    pub common: Arc<MinimizerBucketingCommonData<E::GlobalData>>,
    pub current_file: AtomicUsize,
    pub executor_group_address: RwLock<Option<AddressProducer<SimdSequencesBatch<E::StreamInfo>>>>,
    pub processed_files: AtomicUsize,
    pub total_files: usize,
    pub read_threads_count: usize,
    pub threads_count: usize,
    pub output_path: PathBuf,

    pub seq_count: AtomicU64,
    pub last_total_count: AtomicU64,
    pub tot_bases_count: AtomicU64,

    pub target_chunk_size: u64,

    pub packets_pool: PacketsPool<SimdSequencesBatch<E::StreamInfo>>,
    pub bases_per_lane: usize,

    pub copy_ident: bool,

    pub forward_only: bool,
}

pub struct GenericMinimizerBucketing;

struct MinimizerBucketingExecWriter<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + Copy + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + DedupExtraData,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> {
    _phantom: PhantomData<(SingleData, MultipleData, Executor)>, // mem_tracker: MemoryTracker<Self>,
}

/// What the uncompacted buckets are written through.
/// Still compressed to exploit similarities between superkmers and colorsets,
/// even if in the new implementation the deduplication already partially happened
type UncompactedWriter = CompressedBinaryWriter;

/// The init data `UncompactedWriter` takes.
fn uncompacted_bucket_init() -> <UncompactedWriter as LockFreeBucket>::InitData {
    (
        // Always prefer memory for the uncompacted temp files
        MemoryFileMode::PreferMemory {
            swap_priority: SwapPriority::MinimizerUncompressedTempBuckets,
        },
        MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
        // Avoid compressing too much the temporary reads
        CompressionLevelInfo {
            fast_disk: 0,
            slow_disk: 0,
        },
    )
}

/// One deduplicator per uncompacted bucket, shared by every bucketing thread.
type Deduplicators<MultipleData, Executor> = Vec<
    BoundedDeduplicator<
        MultipleData,
        <Executor as MinimizerBucketingExecutorFactory>::FlagsCount,
        BucketOutput<UncompactedWriter>,
        LazyBucketOutput<UncompactedWriter>,
    >,
>;

struct WriterContext<
    MultipleData: DedupExtraData,
    Executor: MinimizerBucketingExecutorFactory + Sync + Send + 'static,
> {
    global: Arc<MinimizerBucketingExecutionContext<Executor>>,
    /// Sits between the per-thread buffers and the bucket files: a flushed
    /// buffer goes here, and only what survives deduplication is written.
    ///
    /// Taken out and dropped once they are drained, because each one holds an
    /// `Arc` of the buckets it writes to and `MultiThreadBuckets::finalize`
    /// requires being the sole owner to take ownership of the inner data.
    deduplicators: Mutex<Option<Arc<Deduplicators<MultipleData, Executor>>>>,
}

impl<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + Copy + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + DedupExtraData,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> MinimizerBucketingExecWriter<SingleData, MultipleData, Executor>
{
    fn execute(
        &self,
        context: &WriterContext<MultipleData, Executor>,
        ops: &ExecutorAddressOperations<Self>,
        compactor: &mut Option<BucketsCompactor<SingleData, MultipleData, Executor::FlagsCount>>,
    ) {
        let deduplicators_held = context
            .deduplicators
            .lock()
            .clone()
            .expect("the deduplicators were finalized while a writer was still running");
        let deduplicators = deduplicators_held.as_slice();
        let context = context.global.deref();

        let uncompacted_buckets_lock = context.uncompacted_buckets.lock();
        let uncompacted_buckets = uncompacted_buckets_lock.as_ref().unwrap().clone();

        // Uncompacted buckets carry the combinable extra data and a
        // multiplicity, the same shape the compacted ones do, so that a reader
        // sees one format either side of a compaction -- and so that the
        // deduplicator below can fold repeats together on the way through.
        let mut tmp_reads_buffer = DeduplicatingDispatcher::<
            MultipleData,
            Executor::FlagsCount,
            BucketOutput<UncompactedWriter>,
            LazyBucketOutput<UncompactedWriter>,
        >::new(
            deduplicators,
            DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes() as usize,
            context.common.k,
        );

        drop(uncompacted_buckets_lock);

        // self.mem_tracker.update_memory_usage(&[
        //     DEFAULT_PER_CPU_BUFFER_SIZE.octets as usize * context.buckets.count()
        // ]);

        stats!(
            let thread_id = ggcat_logging::generate_stat_id!();
        );

        // The executor keeps no state between batches, so one is enough.
        let mut buckets_processor = Executor::new(&context.common);
        let mut preprocess_infos: Vec<Executor::PreprocessInfo> = Vec::new();
        let mut scratch = SimdScratch::default();
        let first_bits = context.common.buckets_count.normal_buckets_count_log;
        let second_bits = context.common.second_buckets_count.normal_buckets_count_log;

        while let Some(input_packet) = ops.receive_packet() {
            let input_packet = input_packet.deref();
            let batch = &input_packet.batch;
            let records_count = batch.records.len();

            stats!(
                let stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            );

            // Grown but never shrunk, so the colour buffers stay allocated.
            if preprocess_infos.len() < records_count {
                preprocess_infos.resize_with(records_count, Default::default);
            }

            let mut total_bases = 0u64;
            let mut sequences_count = 0u64;
            for (index, record) in batch.records.iter().enumerate() {
                buckets_processor.preprocess_dna_sequence(
                    input_packet.stream_info(record),
                    record.extra.0,
                    record.read_index,
                    &RecordInfo {
                        ident_data: batch.header(index),
                        format: record.extra.1,
                        bases_count: record.bases_total as usize,
                    },
                    &mut preprocess_infos[index],
                );
                // A record split over several batches is counted once, by the
                // batch that holds its end.
                if record.flags & RECORD_CONTINUED == 0 {
                    sequences_count += 1;
                }
                if record.flags & RECORD_CONTINUES == 0 {
                    total_bases += record.bases_total;
                }
            }
            buckets_processor.process_simd_batch::<_, true>(
                batch,
                &preprocess_infos[..records_count],
                &mut scratch,
                0,
                first_bits,
                second_bits,
                |info| {
                    let PushSequenceInfo {
                        bucket,
                        second_bucket,
                        sequence,
                        minimizer_pos,
                        flags,
                        extra_data,
                        temp_buffer,
                        rc,
                    } = info;

                    let chunking_status = tmp_reads_buffer.add_element_extended(
                        bucket,
                        &extra_data,
                        temp_buffer,
                        &CompressedReadsBucketData::new_packed_opt_rc(
                            sequence,
                            flags,
                            second_bucket as u8,
                            rc,
                            minimizer_pos,
                        ),
                    );

                    // A new chunk was produced, compact it
                    if let ChunkingStatus::NewChunk = chunking_status {
                        if compactor.is_none() {
                            *compactor = Some(BucketsCompactor::new(
                                context.common.k,
                                &context.common.second_buckets_count,
                                context.target_chunk_size,
                            ));
                        }
                        let compactor = unsafe { compactor.as_mut().unwrap_unchecked() };

                        // Only some of the buckets may have stood down, and the
                        // set does not exist at all until one does.
                        let narrow = context.narrow_buckets.peek();
                        compactor.compact_buckets(
                            &uncompacted_buckets.get_stored_buckets()[bucket as usize],
                            narrow
                                .as_ref()
                                .map(|n| &n.get_stored_buckets()[bucket as usize]),
                            &context.compacted_buckets.as_ref().unwrap()[bucket as usize],
                            bucket as usize,
                            &context.output_path,
                        );
                    }
                },
            );
            context
                .seq_count
                .fetch_add(sequences_count, Ordering::Relaxed);
            let total_bases_count = context
                .tot_bases_count
                .fetch_add(total_bases, Ordering::Relaxed)
                + total_bases;

            stats!(
                let end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            );

            stats!(stats.assembler.input_process_stats.push(
                ggcat_logging::stats::InputChunkProcessStats {
                    id: input_packet.stats_block_id,
                    start_time: stat_start_time.into(),
                    end_time: end_time.into(),
                    thread_id,
                }
            ));

            const TOTAL_BASES_DIFF_LOG: u64 = 10000000000;

            let do_print_log = context
                .last_total_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                    if total_bases_count > x + TOTAL_BASES_DIFF_LOG {
                        Some(total_bases_count)
                    } else {
                        None
                    }
                })
                .is_ok();

            if do_print_log {
                let current_file = context.current_file.load(Ordering::Relaxed);
                let processed_files = context.processed_files.load(Ordering::Relaxed);

                ggcat_logging::info!(
                    "Elaborated {} sequences! [{}bp] ({}[{}]/{} => {:.2}%) {}",
                    context.seq_count.load(Ordering::Relaxed),
                    context.tot_bases_count.load(Ordering::Relaxed),
                    processed_files,
                    current_file,
                    context.total_files,
                    processed_files as f64 / max(1, context.total_files) as f64 * 100.0,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
            }
        }

        tmp_reads_buffer.finalize();
        drop(uncompacted_buckets);
    }

    /// The rendezvous that finalizes the buckets, kept out of [`Self::execute`]
    /// because the barrier counts every thread of the pool. A thread that never
    /// obtained the shared address still has to arrive here, or the threads
    /// that did would wait for it forever.
    fn finalize_buckets(
        &self,
        context: &WriterContext<MultipleData, Executor>,
        receiver: &ExecutorReceiver<Self>,
        compactor: &mut Option<BucketsCompactor<SingleData, MultipleData, Executor::FlagsCount>>,
    ) {
        let deduplicators = &context.deduplicators;
        let context = context.global.deref();
        {
            let status = receiver.wait_for_executors();
            if status.is_leader() {
                // Every thread has left `execute` by now, so every dispatcher
                // has handed its partial buffers over and the deduplicators
                // hold everything that has not reached a bucket yet. They have
                // to be drained before the buckets they write into are
                // finalized.
                // let mut stats = DedupStats::default();
                if let Some(deduplicators) = deduplicators.lock().take() {
                    for deduplicator in deduplicators.iter() {
                        // stats.accumulate(
                        deduplicator.finish();
                        // );
                    }
                    // ggcat_logging::info!("Super-kmer deduplication: {}", stats.report());
                    // Releases the last reference each one holds to the buckets.
                    drop(deduplicators);
                }

                let uncompacted = context.uncompacted_buckets.lock().take().unwrap();
                *context.uncompacted_buckets_finalized.lock() = uncompacted
                    .finalize()
                    .into_iter()
                    .map(|b| Mutex::new(b))
                    .collect();

                // Only present if some bucket stood its deduplicator down.
                if let Some(narrow) = context.narrow_buckets.take() {
                    let _ = context
                        .narrow_buckets_finalized
                        .set(narrow.finalize().into_iter().map(Mutex::new).collect());
                }
            }

            receiver.wait_for_executors();

            if let Some(ref compacted_buckets) = context.compacted_buckets {
                let mut uncompacted_lock = context.uncompacted_buckets_finalized.lock();
                while let Some(bucket) = uncompacted_lock.pop() {
                    let bucket_index = uncompacted_lock.len();
                    drop(uncompacted_lock);

                    if compactor.is_none() {
                        *compactor = Some(BucketsCompactor::new(
                            context.common.k,
                            &context.common.second_buckets_count,
                            context.target_chunk_size,
                        ));
                    }
                    let compactor = compactor.as_mut().unwrap();

                    // Indexed rather than popped: `finalize` returns buckets in
                    // index order, so position is the bucket index in both, and
                    // the uncompacted list is the one driving the loop.
                    let narrow = context
                        .narrow_buckets_finalized
                        .get()
                        .and_then(|narrow| narrow.get(bucket_index));

                    compactor.compact_buckets(
                        &bucket,
                        narrow,
                        &compacted_buckets[bucket_index],
                        bucket_index,
                        &context.output_path,
                    );

                    // if bucket.

                    uncompacted_lock = context.uncompacted_buckets_finalized.lock();
                }
            }
        }
    }
}

impl<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + Copy + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + DedupExtraData,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> AsyncExecutor for MinimizerBucketingExecWriter<SingleData, MultipleData, Executor>
{
    type InputPacket = SimdSequencesBatch<Executor::StreamInfo>;
    type OutputPacket = ();
    type GlobalParams = WriterContext<MultipleData, Executor>;
    type InitData = ();
    const ALLOW_PARALLEL_ADDRESS_EXECUTION: bool = true;

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn executor_main<'a>(
        &'a mut self,
        params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
    ) {
        let mut compactor = None;

        // Held across the finalization below, as it was when that code lived
        // inside `execute`: releasing the address deactivates the packet queue,
        // which is what stops other threads from picking the work up.
        let address = receiver.obtain_address().ok();
        if let Some(address) = &address {
            self.execute(params, address, &mut compactor);
        }

        // Unconditional: the finalization barrier counts the whole pool, and
        // the shared address runs out as soon as the readers are done, so some
        // threads routinely get here without having obtained one.
        self.finalize_buckets(params, &receiver, &mut compactor);
        drop(address);

        // No more packets should arrive
        while let Ok(address) = receiver.obtain_address() {
            assert!(address.receive_packet().is_none());
        }
    }
}

// const STRICT_POOL_ALLOC: bool = false;
//
// const MEMORY_FIELDS_COUNT: usize = 1;
// const MEMORY_FIELDS: &'static [&'static str] = &["TMP_READS_BUFFER"];
//     fn pre_execute<EX: ExecutorOperations<Self>>(
//         &mut self,
//         _reinit_params: Self::BuildParams,
//         _ops: EX,
//     ) {
//     }
//
//     fn execute<EX: ExecutorOperations<Self>>(
//         &mut self,
//         input_packet: Packet<Self::InputPacket>,
//         _ops: EX,
//     ) {
//     }
//
//     fn finalize<EX: ExecutorOperations<Self>>(&mut self, _ops: EX) {
//         self.tmp_reads_buffer.take().unwrap().finalize();
//     }
// }

impl GenericMinimizerBucketing {
    pub fn do_bucketing<
        SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + Copy + 'static,
        MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + DedupExtraData,
        Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
        SequenceType: GenericSequencesStream,
    >(
        input_blocks: impl ExactSizeIterator<
            Item = MinimzerBucketingFilesReaderInputPacket<Executor, SequenceType>,
        >,
        output_path: &Path,
        buckets_count: BucketsCount,
        second_buckets_count: BucketsCount,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: Executor::GlobalData,
        copy_ident: bool,
        ignored_length: usize,
        chunking_size_threshold: Option<u64>,
        target_chunk_size: u64,
        forward_only: bool,
    ) -> Vec<MultiChunkBucket> {
        let read_threads_count = max(1, threads_count / 2);
        let compute_threads_count = max(1, threads_count.saturating_sub(read_threads_count / 4));

        let uncompacted_buckets = Arc::new(MultiThreadBuckets::<UncompactedWriter>::new(
            buckets_count,
            output_path.join("bucket"),
            chunking_size_threshold,
            &uncompacted_bucket_init(),
            &MinimizerBucketMode::Single,
        ));

        // A bucket that stands its deduplicator down writes the bucketing
        // threads' own records, which the compactor reads alongside the wide
        // ones. An extra data type that cannot be combined stands down from the
        // first record, so it needs these to exist -- and they are only ever
        // compacted, never returned, so compaction has to be enabled.
        assert!(
            chunking_size_threshold.is_some() || MultipleData::ALLOW_COMBINE,
            "an extra data type that cannot be combined requires compaction"
        );
        let narrow_init = uncompacted_bucket_init();
        let narrow_path = output_path.join("narrow");
        let narrow_count = buckets_count.clone();
        let narrow_buckets = Arc::new(LazyBuckets::new(move || {
            Arc::new(MultiThreadBuckets::<UncompactedWriter>::new(
                narrow_count.clone(),
                narrow_path.clone(),
                chunking_size_threshold,
                &narrow_init,
                &MinimizerBucketMode::UncompactedNarrow,
            ))
        }));

        // One deduplicator per bucket, sized so the whole array stays inside a
        // fixed budget however many buckets there are.
        let dedup_memory = deduplicator_memory(
            uncompacted_buckets.get_buckets_count().total_buckets_count,
            MINIMIZER_DEDUPLICATION_MEMORY,
        );
        let deduplicators: Arc<Deduplicators<MultipleData, Executor>> = Arc::new(
            (0..uncompacted_buckets.get_buckets_count().total_buckets_count)
                .map(|index| {
                    BoundedDeduplicator::new_with_bypass(
                        dedup_memory,
                        k,
                        BucketOutput::new(uncompacted_buckets.clone(), index as u16),
                        Some(LazyBucketOutput::new(narrow_buckets.clone(), index as u16)),
                        BypassPolicy::from_config(),
                    )
                })
                .collect(),
        );
        ggcat_logging::info!(
            "Deduplicating {} uncompacted buckets with {} each ({:.2} total)",
            uncompacted_buckets.get_buckets_count().total_buckets_count,
            MemoryDataSize::from_bytes(dedup_memory),
            MemoryDataSize::from_bytes(
                dedup_memory * 13 / 4 * uncompacted_buckets.get_buckets_count().total_buckets_count
            ),
        );

        let compacted_buckets = if chunking_size_threshold.is_some() {
            Some(uncompacted_buckets.create_matching_multichunks())
        } else {
            None
        };

        let global_context = Arc::new(MinimizerBucketingExecutionContext::<Executor> {
            uncompacted_buckets: Mutex::new(Some(uncompacted_buckets)),
            uncompacted_buckets_finalized: Mutex::new(vec![]),
            narrow_buckets: narrow_buckets.clone(),
            narrow_buckets_finalized: OnceLock::new(),
            compacted_buckets: compacted_buckets,
            current_file: AtomicUsize::new(0),
            executor_group_address: RwLock::new(None),
            processed_files: AtomicUsize::new(0),
            total_files: input_blocks.len(),
            common: Arc::new(MinimizerBucketingCommonData::new(
                k,
                m,
                buckets_count,
                ignored_length,
                second_buckets_count,
                global_data,
                !forward_only,
            )),
            threads_count: compute_threads_count,
            output_path: output_path.to_path_buf(),

            seq_count: AtomicU64::new(0),
            last_total_count: AtomicU64::new(0),
            tot_bases_count: AtomicU64::new(0),

            target_chunk_size,

            packets_pool: PacketsPool::new(compute_threads_count * 4, SIMD_LANE_BASES),
            bases_per_lane: SIMD_LANE_BASES,
            read_threads_count,
            copy_ident,
            forward_only,
        });

        {
            let scheduler = Scheduler::new(threads_count);

            let mut disk_thread_pool = ExecThreadPool::<
                MinimizerBucketingFilesReader<Executor, SequenceType>,
            >::new(
                global_context.read_threads_count, "mm_disk", false
            );
            let mut compute_thread_pool = ExecThreadPool::<
                MinimizerBucketingExecWriter<SingleData, MultipleData, Executor>,
            >::new(
                compute_threads_count, "mm_compute", false
            );

            let compute_thread_pool_handle = compute_thread_pool.start(
                scheduler.clone(),
                &Arc::new(WriterContext {
                    global: global_context.clone(),
                    deduplicators: Mutex::new(Some(deduplicators)),
                }),
            );

            let compute_address =
                compute_thread_pool_handle.create_new_address(Arc::new(()), false);
            *global_context.executor_group_address.write() = Some(compute_address);

            let disk_thread_pool_handle =
                disk_thread_pool.start(scheduler.clone(), &global_context);
            disk_thread_pool_handle.add_input_data((), input_blocks.into_iter());

            drop(disk_thread_pool_handle);
            drop(compute_thread_pool_handle);

            disk_thread_pool.join();

            Option::take(&mut global_context.executor_group_address.write());
            compute_thread_pool.join();
        }

        let global_context = Arc::try_unwrap(global_context)
            .unwrap_or_else(|_| panic!("Cannot get execution context!"));

        if let Some(compacted_buckets) = global_context.compacted_buckets {
            compacted_buckets
                .into_iter()
                .map(|b| b.into_inner())
                .collect()
        } else {
            global_context
                .uncompacted_buckets_finalized
                .into_inner()
                .into_iter()
                .map(|b| b.into_inner())
                .collect()
        }
    }
}
