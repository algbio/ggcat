use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_MINIMIZER_MASK, DEFAULT_PER_CPU_BUFFER_SIZE,
    EXTRA_BUFFERS_COUNT, RESPLITTING_MAX_K_M_DIFFERENCE,
};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint, encode_varint};
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory, ReadDispatchInfo,
};
use crate::pipeline_common::minimizer_bucketing::{
    MinimizerBucketingCommonData, MinimizerBucketingExecutorFactory,
};
use crate::query_pipeline::counters_sorting::CounterEntry;
use crate::query_pipeline::querier_minimizer_bucketing::QuerierMinimizerBucketingExecutorFactory;
use crate::query_pipeline::QueryPipeline;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::get_memory_mode;
use byteorder::{ReadBytesExt, WriteBytesExt};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::generic_binary_reader::{
    ChunkDecoder, GenericChunkedBinaryReader,
};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::min;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum QueryKmersReferenceData<CX: MinimizerBucketingSeqColorData> {
    Graph(CX),
    Query(NonZeroU64),
}

impl<CX: MinimizerBucketingSeqColorData> SequenceExtraData for QueryKmersReferenceData<CX> {
    #[inline(always)]
    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self> {
        match reader.read_u8().ok()? {
            0 => Some(Self::Graph(CX::decode(reader)?)),
            _ => Some(Self::Query(
                NonZeroU64::new(decode_varint(|| reader.read_u8().ok())? + 1).unwrap(),
            )),
        }
    }

    #[inline(always)]
    fn encode<'a>(&self, writer: &'a mut impl Write) {
        match self {
            Self::Graph(cx) => {
                writer.write_u8(0).unwrap();
                CX::encode(cx, writer);
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
}

struct GlobalQueryMergeData<'a> {
    k: usize,
    m: usize,
    counters_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
    global_resplit_data: MinimizerBucketingCommonData<()>,
}

struct ParallelKmersQueryFactory<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, MH, CX)>,
);

impl<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersQueryFactory<H, MH, CX>
{
    type SequencesResplitterFactory = QuerierMinimizerBucketingExecutorFactory<H, CX>;
    type GlobalExtraData<'a> = GlobalQueryMergeData<'a>;
    type AssociatedExtraData = QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>;
    type ExecutorType<'a> = ParallelKmersQuery<'a, H, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;

    fn new_resplitter<'a, 'b: 'a>(
        global_data: &'a Self::GlobalExtraData<'b>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType<'a>
    {
        QuerierMinimizerBucketingExecutorFactory::new(&global_data.global_resplit_data)
    }

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
        let mut counters_buffers = Box::new(BucketsThreadBuffer::new(
            DEFAULT_PER_CPU_BUFFER_SIZE,
            global_data.counters_buckets.count(),
        ));

        let buffers = unsafe { &mut *(counters_buffers.deref_mut() as *mut BucketsThreadBuffer) };

        Self::ExecutorType::<'a> {
            counters_tmp: BucketsThreadDispatcher::new(&global_data.counters_buckets, buffers),
            counters_buffers,
            phset: hashbrown::HashSet::new(),
            query_map: hashbrown::HashMap::new(),
            query_reads: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersQuery<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> {
    counters_tmp: BucketsThreadDispatcher<'x, LockFreeBinaryWriter>,
    // This field has to appear after hashes_tmp so that it's dropped only when not used anymore
    counters_buffers: Box<BucketsThreadBuffer>,
    phset: hashbrown::HashSet<MH::HashTypeUnextendable>,
    query_map: hashbrown::HashMap<u64, u64>,
    query_reads: Vec<(u64, MH::HashTypeUnextendable)>,
    _phantom: PhantomData<(H, CX)>,
}

impl<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutor<'x, ParallelKmersQueryFactory<H, MH, CX>>
    for ParallelKmersQuery<'x, H, MH, CX>
{
    fn preprocess_bucket<'y: 'x>(
        &mut self,
        global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
        _flags: u8,
        input_extra_data: <ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::AssociatedExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<
        <ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::AssociatedExtraData,
    >{
        let hashes = H::new(read.sub_slice(0..global_data.k), global_data.m);

        let minimizer = hashes
            .iter()
            .min_by_key(|k| {
                H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(k.to_unextendable())
            })
            .unwrap();

        let bucket = H::get_second_bucket(minimizer.to_unextendable());

        ReadDispatchInfo {
            bucket,
            hash: H::get_sorting_hash(minimizer.to_unextendable()),
            flags: 0,
            extra_data: input_extra_data,
        }
    }

    fn maybe_swap_bucket<'y: 'x>(
        &mut self,
        _global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
    }

    fn process_group<'y: 'x, D: ChunkDecoder>(
        &mut self,
        global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
        mut reader: GenericChunkedBinaryReader<D>,
    ) {
        let k = global_data.k;

        self.phset.clear();
        self.query_reads.clear();
        self.query_map.clear();

        reader.decode_all_bucket_items::<CompressedReadsBucketHelper<
            QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>,
            <ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT,
        >, _>(Vec::new(), |(_flags, sequence_type, read)| {
            let hashes = MH::new(read, k);

            match sequence_type {
                QueryKmersReferenceData::Graph(_col_info) => {
                    for hash in hashes.iter() {
                        self.phset.insert(hash.to_unextendable());
                    }
                }
                QueryKmersReferenceData::Query(index) => {
                    for hash in hashes.iter() {
                        self.query_reads.push((index.get(), hash.to_unextendable()));
                    }
                }
            }
        });

        for (query_index, kmer_hash) in self.query_reads.drain(..) {
            if self.phset.contains(&kmer_hash) {
                *self.query_map.entry(query_index).or_insert(0) += 1;
            }
        }

        for (query_index, counter) in self.query_map.drain() {
            self.counters_tmp.add_element(
                (query_index % 0xFF) as BucketIndexType,
                &(),
                &CounterEntry {
                    query_index,
                    counter,
                },
            )
        }
    }

    fn finalize<'y: 'x>(
        self,
        _global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
        self.counters_tmp.finalize();
    }
}

impl QueryPipeline {
    pub fn parallel_kmers_counting<
        H: HashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
        P: AsRef<Path> + std::marker::Sync,
    >(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        out_directory: P,
        k: usize,
        m: usize,
        threads_count: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: kmers counting".to_string());

        let mut counters_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            out_directory.as_ref().join("counters"),
            &(
                get_memory_mode(SwapPriority::QueryCounters),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        );

        let global_data = GlobalQueryMergeData {
            k,
            m,
            counters_buckets: &counters_buckets,
            global_resplit_data: MinimizerBucketingCommonData {
                k,
                m: if k > RESPLITTING_MAX_K_M_DIFFERENCE + 1 {
                    k - RESPLITTING_MAX_K_M_DIFFERENCE
                } else {
                    min(m, 2)
                },
                buckets_count,
                global_data: (),
            },
        };

        KmersTransform::<ParallelKmersQueryFactory<H, MH, CX>>::new(
            file_inputs,
            buckets_count,
            EXTRA_BUFFERS_COUNT,
            global_data,
        )
        .parallel_kmers_transform(threads_count);

        counters_buckets.finalize()
    }
}
