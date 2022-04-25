use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::{BucketIndexType, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint, encode_varint};
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory,
    KmersTransformPreprocessor,
};
use crate::query_pipeline::counters_sorting::CounterEntry;
use crate::query_pipeline::QueryPipeline;
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::get_memory_mode;
use byteorder::{ReadBytesExt, WriteBytesExt};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
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
}

struct ParallelKmersQueryFactory<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, MH, CX)>,
);

impl<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersQueryFactory<H, MH, CX>
{
    type GlobalExtraData<'a> = GlobalQueryMergeData<'a>;
    type AssociatedExtraData = QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>;
    type ExecutorType<'a> = ParallelKmersQuery<'a, H, MH, CX>;
    type PreprocessorType<'a> = ParallelKmersQueryPreprocessor<'a, H, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;

    fn new_preprocessor<'a>(
        _global_data: &Self::GlobalExtraData<'a>,
        _buckets_count_log: usize,
    ) -> Self::PreprocessorType<'a> {
        todo!()
    }

    fn new_executor<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
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

struct ParallelKmersQueryPreprocessor<
    'x,
    H: HashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    _phantom: PhantomData<&'x (H, MH, CX)>,
}

impl<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformPreprocessor<'x, ParallelKmersQueryFactory<H, MH, CX>>
    for ParallelKmersQueryPreprocessor<'x, H, MH, CX>
{
    fn get_sequence_bucket<'y: 'x, 'a, C>(
        &self,
        _seq_data: &(
            u8,
            u8,
            C,
            &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
        ),
    ) -> BucketIndexType {
        todo!()
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
    fn process_group_start<'y: 'x>(
        &mut self,
        _global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
        self.phset.clear();
        self.query_reads.clear();
        self.query_map.clear();
    }

    fn process_group_batch_sequences<'y: 'x>(
        &mut self,
        global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
        batch: &Vec<(
            u8,
            QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>,
            CompressedReadIndipendent,
        )>,
        ref_sequences: &Vec<u8>,
    ) {
        let k = global_data.k;

        for (_, sequence_type, read) in batch.iter() {
            let hashes = MH::new(read.as_reference(ref_sequences), k);

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
    }

    fn process_group_finalize<'y: 'x>(
        &mut self,
        _global_data: &<ParallelKmersQueryFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
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
        };

        KmersTransform::<ParallelKmersQueryFactory<H, MH, CX>>::new(
            file_inputs,
            buckets_count,
            global_data,
        )
        .parallel_kmers_transform(threads_count);

        counters_buckets.finalize()
    }
}
