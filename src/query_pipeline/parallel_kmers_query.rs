use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::{BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE};
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::hashes::{ExtendableHashTraitType, HashableSequence};
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::io::varint::{decode_varint, decode_varint_flags, encode_varint};
use crate::io::DataReader;
use crate::pipeline_common::kmers_transform::structs::ReadRef;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory, ReadDispatchInfo,
    MERGE_BUCKETS_COUNT,
};
use crate::query_pipeline::counters_sorting::CounterEntry;
use crate::query_pipeline::QueryPipeline;
use crate::utils::compressed_read::CompressedRead;
use byteorder::{ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use parallel_processor::binary_writer::{BinaryWriter, StorageMode};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum QueryKmersReferenceData<CX: MinimizerBucketingSeqColorData> {
    Graph(CX),
    Query(NonZeroU64),
}

impl<CX: MinimizerBucketingSeqColorData> SequenceExtraData for QueryKmersReferenceData<CX> {
    #[inline(always)]
    fn decode(mut reader: impl Read) -> Option<Self> {
        match reader.read_u8().ok()? {
            0 => Some(Self::Graph(CX::decode(reader)?)),
            _ => Some(Self::Query(
                NonZeroU64::new(decode_varint(|| reader.read_u8().ok())? + 1).unwrap(),
            )),
        }
    }

    #[inline(always)]
    fn encode(&self, mut writer: impl Write) {
        match self {
            Self::Graph(cx) => {
                writer.write_u8(0);
                CX::encode(cx, writer);
            }
            Self::Query(val) => {
                writer.write_u8(1);
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
    buckets_count: usize,
    counters_buckets: &'a MultiThreadBuckets<BinaryWriter>,
}

struct ParallelKmersQueryFactory<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, MH, CX)>,
);

impl<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersQueryFactory<H, MH, CX>
{
    type GlobalExtraData<'a> = GlobalQueryMergeData<'a>;
    type InputBucketExtraData = QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>;
    type IntermediateExtraData = QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>;
    type ExecutorType<'a> = ParallelKmersQuery<'a, H, MH, CX>;

    const FLAGS_COUNT: usize = 0;

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
        Self::ExecutorType::<'a> {
            counters_tmp: BucketsThreadDispatcher::new(
                MAX_COUNTERS_FOR_FLUSH,
                &global_data.counters_buckets,
            ),
            phset: hashbrown::HashSet::new(),
            query_map: hashbrown::HashMap::new(),
            query_reads: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersQuery<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> {
    counters_tmp: BucketsThreadDispatcher<'x, BinaryWriter, CounterEntry>,
    phset: hashbrown::HashSet<MH::HashTypeUnextendable>,
    query_map: hashbrown::HashMap<u64, u64>,
    query_reads: Vec<(u64, MH::HashTypeUnextendable)>,
    _phantom: PhantomData<(H, CX)>,
}

const MAX_COUNTERS_FOR_FLUSH: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);

impl<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutor<'x, ParallelKmersQueryFactory<H, MH, CX>>
    for ParallelKmersQuery<'x, H, MH, CX>
{
    fn preprocess_bucket(
        &mut self,
        global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
        input_extra_data: <ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::InputBucketExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<
        <ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::IntermediateExtraData,
    >{
        let hashes = H::new(read.sub_slice(0..global_data.k), global_data.m);

        let minimizer = hashes
            .iter()
            .min_by_key(|k| H::get_full_minimizer(k.to_unextendable()))
            .unwrap();

        let bucket = H::get_second_bucket(minimizer.to_unextendable())
            % (MERGE_BUCKETS_COUNT as BucketIndexType);

        ReadDispatchInfo {
            bucket,
            hash: H::get_sorting_hash(minimizer.to_unextendable()),
            flags: 0,
            extra_data: input_extra_data,
        }
    }

    fn maybe_swap_bucket(
        &mut self,
        _global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
    ) {
    }

    fn process_group(
        &mut self,
        global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
        reads: &[ReadRef],
        memory: &[u8],
    ) {
        let k = global_data.k;

        self.phset.clear();
        self.query_reads.clear();
        self.query_map.clear();

        for packed_read in reads {
            let (_, read, sequence_type) = packed_read
                .unpack::<QueryKmersReferenceData::<CX::MinimizerBucketingSeqColorDataType>>
                (memory, <ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT);

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
        }

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

    fn finalize(
        self,
        _global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
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
        R: DataReader,
    >(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        out_directory: P,
        k: usize,
        m: usize,
        threads_count: usize,
        save_memory: bool,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: kmers counting".to_string());

        let mut counters_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                out_directory.as_ref().join("counters"),
                StorageMode::Plain {
                    buffer_size: DEFAULT_OUTPUT_BUFFER_SIZE,
                },
            ),
            None,
        );

        let global_data = GlobalQueryMergeData {
            k,
            m,
            buckets_count,
            counters_buckets: &counters_buckets,
        };

        KmersTransform::parallel_kmers_transform::<ParallelKmersQueryFactory<H, MH, CX>>(
            file_inputs,
            buckets_count,
            threads_count,
            global_data,
            save_memory,
        );

        counters_buckets.finalize()
    }
}
