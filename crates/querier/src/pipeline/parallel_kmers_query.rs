use crate::pipeline::counters_sorting::CounterEntry;
use crate::pipeline::querier_minimizer_bucketing::{
    QuerierMinimizerBucketingExecutorFactory, QuerierMinimizerBucketingGlobalData,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use colors::colors_manager::color_types::{
    MinimizerBucketingSeqColorDataType, SingleKmerColorDataType,
};
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use config::{
    get_memory_mode, BucketIndexType, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE,
    MINIMUM_SUBBUCKET_KMERS_COUNT, RESPLITTING_MAX_K_M_DIFFERENCE,
};
use hashbrown::HashMap;
use hashes::HashFunction;
use hashes::HashFunctionFactory;
use hashes::{ExtendableHashTraitType, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedRead;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};
use io::varint::{decode_varint, encode_varint};
use kmers_transform::processor::KmersTransformProcessor;
use kmers_transform::{
    GroupProcessStats, KmersTransform, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
    KmersTransformMapProcessor, KmersTransformPreprocessor,
};
use minimizer_bucketing::{MinimizerBucketingCommonData, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::min;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::counters_sorting::CounterEntrySerializer;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum QueryKmersReferenceData<CX: MinimizerBucketingSeqColorData> {
    Graph(CX),
    Query(NonZeroU64),
}

impl<CX: MinimizerBucketingSeqColorData> SequenceExtraDataTempBufferManagement
    for QueryKmersReferenceData<CX>
{
    type TempBuffer = (CX::TempBuffer,);

    #[inline(always)]
    fn new_temp_buffer() -> (CX::TempBuffer,) {
        (CX::new_temp_buffer(),)
    }

    #[inline(always)]
    fn clear_temp_buffer(buffer: &mut (CX::TempBuffer,)) {
        CX::clear_temp_buffer(&mut buffer.0);
    }

    fn copy_temp_buffer(dest: &mut (CX::TempBuffer,), src: &(CX::TempBuffer,)) {
        CX::copy_temp_buffer(&mut dest.0, &src.0);
    }

    #[inline(always)]
    fn copy_extra_from(extra: Self, src: &(CX::TempBuffer,), dst: &mut (CX::TempBuffer,)) -> Self {
        match extra {
            QueryKmersReferenceData::Graph(color) => {
                QueryKmersReferenceData::Graph(CX::copy_extra_from(color, &src.0, &mut dst.0))
            }
            QueryKmersReferenceData::Query(index) => QueryKmersReferenceData::Query(index),
        }
    }
}

impl<CX: MinimizerBucketingSeqColorData> SequenceExtraDataConsecutiveCompression
    for QueryKmersReferenceData<CX>
{
    type LastData = CX::LastData;

    #[inline(always)]
    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        last_data: Self::LastData,
    ) -> Option<Self> {
        match reader.read_u8().ok()? {
            0 => Some(Self::Graph(CX::decode_extended(
                &mut buffer.0,
                reader,
                last_data,
            )?)),
            _ => Some(Self::Query(
                NonZeroU64::new(decode_varint(|| reader.read_u8().ok())? + 1).unwrap(),
            )),
        }
    }

    #[inline(always)]
    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: Self::LastData,
    ) {
        match self {
            Self::Graph(cx) => {
                writer.write_u8(0).unwrap();
                CX::encode_extended(cx, &buffer.0, writer, last_data);
            }
            Self::Query(val) => {
                writer.write_u8(1).unwrap();
                encode_varint(|bytes| writer.write_all(bytes), val.get() - 1).unwrap();
            }
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        match self {
            Self::Graph(cx) => cx.max_size() + 1,
            Self::Query(_) => 10 + 1,
        }
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        match self {
            Self::Graph(cx) => cx.obtain_last_data(last_data),
            Self::Query(_) => Self::LastData::default(),
        }
    }
}

struct GlobalQueryMergeData {
    k: usize,
    m: usize,
    counters_buckets: Arc<MultiThreadBuckets<LockFreeBinaryWriter>>,
    global_resplit_data: Arc<MinimizerBucketingCommonData<QuerierMinimizerBucketingGlobalData>>,
}

struct ParallelKmersQueryFactory<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
>(PhantomData<(H, MH, CX)>);

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersQueryFactory<H, MH, CX>
{
    type SequencesResplitterFactory = QuerierMinimizerBucketingExecutorFactory<H, CX>;
    type GlobalExtraData = GlobalQueryMergeData;
    type AssociatedExtraData = QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>;

    type PreprocessorType = ParallelKmersQueryPreprocessor<H, MH, CX>;
    type MapProcessorType = ParallelKmersQueryMapProcessor<H, MH, CX>;
    type FinalExecutorType = ParallelKmersQueryFinalExecutor<H, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;
    const HAS_COLORS: bool = CX::COLORS_ENABLED;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType {
        QuerierMinimizerBucketingExecutorFactory::new(&global_data.global_resplit_data)
    }

    fn new_preprocessor(_global_data: &Arc<Self::GlobalExtraData>) -> Self::PreprocessorType {
        Self::PreprocessorType {
            _phantom: PhantomData,
        }
    }

    fn new_map_processor(
        _global_data: &Arc<Self::GlobalExtraData>,
        _mem_tracker: MemoryTracker<KmersTransformProcessor<Self>>,
    ) -> Self::MapProcessorType {
        Self::MapProcessorType {
            map_packet: None,
            _phantom: PhantomData,
        }
    }

    fn new_final_executor(global_data: &Arc<Self::GlobalExtraData>) -> Self::FinalExecutorType {
        let counters_buffers = BucketsThreadBuffer::new(
            DEFAULT_PER_CPU_BUFFER_SIZE,
            global_data.counters_buckets.count(),
        );

        Self::FinalExecutorType {
            counters_tmp: BucketsThreadDispatcher::new(
                &global_data.counters_buckets,
                counters_buffers,
            ),
            query_map: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersQueryPreprocessor<
    H: HashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    _phantom: PhantomData<(H, MH, CX)>,
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformPreprocessor<ParallelKmersQueryFactory<H, MH, CX>>
    for ParallelKmersQueryPreprocessor<H, MH, CX>
{
    fn get_sequence_bucket<C>(
        &self,
        global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
        seq_data: &(u8, u8, C, CompressedRead),
        used_hash_bits: usize,
        bucket_bits_count: usize,
    ) -> BucketIndexType {
        let read = &seq_data.3;

        let hashes = H::new(read.sub_slice(0..global_data.k), global_data.m);

        let minimizer = hashes
            .iter()
            .min_by_key(|k| H::get_full_minimizer(k.to_unextendable()))
            .unwrap();

        H::get_bucket(
            used_hash_bits,
            bucket_bits_count,
            minimizer.to_unextendable(),
        )
    }
}

struct ParallelKmersQueryMapPacket<MH: HashFunctionFactory, CX: Sync + Send + 'static> {
    phmap: HashMap<MH::HashTypeUnextendable, CX>,
    query_reads: Vec<(u64, MH::HashTypeUnextendable)>,
}

impl<MH: HashFunctionFactory, CX: Sync + Send + 'static> PoolObjectTrait
    for ParallelKmersQueryMapPacket<MH, CX>
{
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            phmap: HashMap::new(),
            query_reads: Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.phmap = HashMap::with_capacity(32768);
        self.query_reads.clear();
        self.query_reads.shrink_to(32768);
    }
}
impl<MH: HashFunctionFactory, CX: Sync + Send + 'static> PacketTrait
    for ParallelKmersQueryMapPacket<MH, CX>
{
    fn get_size(&self) -> usize {
        (self.phmap.len() + self.query_reads.len()) * 16 // TODO: Compute correct values
    }
}

struct ParallelKmersQueryMapProcessor<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    map_packet: Option<Packet<ParallelKmersQueryMapPacket<MH, SingleKmerColorDataType<CX>>>>,
    _phantom: PhantomData<(H, CX)>,
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformMapProcessor<ParallelKmersQueryFactory<H, MH, CX>>
    for ParallelKmersQueryMapProcessor<H, MH, CX>
{
    type MapStruct = ParallelKmersQueryMapPacket<MH, SingleKmerColorDataType<CX>>;
    const MAP_SIZE: usize = size_of::<MH::HashTypeUnextendable>() + 8;

    fn process_group_start(
        &mut self,
        map_struct: Packet<Self::MapStruct>,
        _global_data: &GlobalQueryMergeData,
    ) {
        self.map_packet = Some(map_struct);
    }

    fn process_group_batch_sequences(
        &mut self,
        global_data: &GlobalQueryMergeData,
        batch: &Vec<(
            u8,
            QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>,
            CompressedReadIndipendent,
        )>,
        extra_data_buffer: &<QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ref_sequences: &Vec<u8>,
    ) -> GroupProcessStats {
        let k = global_data.k;
        let map_packet = self.map_packet.as_mut().unwrap();

        let mut kmers_count = 0;

        for (_, sequence_type, read) in batch.iter() {
            let hashes = MH::new(read.as_reference(ref_sequences), k);

            kmers_count += (read.bases_count() - k + 1) as u64;

            match sequence_type {
                QueryKmersReferenceData::Graph(col_info) => {
                    for (hash, color) in hashes
                        .iter()
                        .zip(col_info.get_iterator(&extra_data_buffer.0))
                    {
                        map_packet.phmap.insert(hash.to_unextendable(), color);
                    }
                }
                QueryKmersReferenceData::Query(index) => {
                    for hash in hashes.iter() {
                        map_packet
                            .query_reads
                            .push((index.get(), hash.to_unextendable()));
                    }
                }
            }
        }

        GroupProcessStats {
            total_kmers: kmers_count,
            unique_kmers: kmers_count,
        }
    }

    fn process_group_finalize(
        &mut self,
        _global_data: &GlobalQueryMergeData,
    ) -> Packet<Self::MapStruct> {
        self.map_packet.take().unwrap()
    }
}

struct ParallelKmersQueryFinalExecutor<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    counters_tmp: BucketsThreadDispatcher<
        LockFreeBinaryWriter,
        CounterEntrySerializer<SingleKmerColorDataType<CX>>,
    >,
    query_map: HashMap<(u64, SingleKmerColorDataType<CX>), u64>,
    _phantom: PhantomData<(H, MH, CX)>,
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformFinalExecutor<ParallelKmersQueryFactory<H, MH, CX>>
    for ParallelKmersQueryFinalExecutor<H, MH, CX>
{
    type MapStruct = ParallelKmersQueryMapPacket<MH, SingleKmerColorDataType<CX>>;

    fn process_map(
        &mut self,
        _global_data: &GlobalQueryMergeData,
        map_struct: Packet<Self::MapStruct>,
    ) -> Packet<ParallelKmersQueryMapPacket<MH, SingleKmerColorDataType<CX>>> {
        let map_struct_ref = map_struct.deref();

        for (query_index, kmer_hash) in &map_struct_ref.query_reads {
            if let Some(entry_color) = map_struct_ref.phmap.get(kmer_hash) {
                *self
                    .query_map
                    .entry((*query_index, entry_color.clone()))
                    .or_insert(0) += 1;
            }
        }

        for ((query_index, color_index), counter) in self.query_map.drain() {
            self.counters_tmp.add_element(
                (query_index % 0xFF) as BucketIndexType,
                &color_index,
                &CounterEntry {
                    query_index,
                    counter,
                    _phantom: PhantomData,
                },
            )
        }

        map_struct
    }

    fn finalize(self, _global_data: &GlobalQueryMergeData) {
        self.counters_tmp.finalize();
    }
}

pub fn parallel_kmers_counting<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
    P: AsRef<Path> + Sync,
>(
    file_inputs: Vec<PathBuf>,
    buckets_counters_path: PathBuf,
    buckets_count: usize,
    out_directory: P,
    k: usize,
    m: usize,
    threads_count: usize,
) -> Vec<PathBuf> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: kmers counting".to_string());

    let counters_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
        buckets_count,
        out_directory.as_ref().join("counters"),
        None,
        &(
            get_memory_mode(SwapPriority::QueryCounters),
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
    ));

    let global_data = Arc::new(GlobalQueryMergeData {
        k,
        m,
        counters_buckets,
        global_resplit_data: Arc::new(MinimizerBucketingCommonData::new(
            k,
            if k > RESPLITTING_MAX_K_M_DIFFERENCE + 1 {
                k - RESPLITTING_MAX_K_M_DIFFERENCE
            } else {
                min(m, 2)
            }, // m
            buckets_count,
            0,
            1,
            QuerierMinimizerBucketingGlobalData {
                queries_count: Default::default(),
            },
        )),
    });

    KmersTransform::<ParallelKmersQueryFactory<H, MH, CX>>::new(
        file_inputs.into_iter().map(|x| vec![x]).collect(),
        out_directory.as_ref(),
        buckets_counters_path,
        buckets_count,
        global_data.clone(),
        threads_count,
        k,
        MINIMUM_SUBBUCKET_KMERS_COUNT as u64,
    )
    .parallel_kmers_transform();

    let global_data =
        Arc::try_unwrap(global_data).unwrap_or_else(|_| panic!("Cannot unwrap global data!"));
    global_data.counters_buckets.finalize_single()
}
