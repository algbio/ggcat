use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::{BucketIndexType, MinimizerType, SortingHashType, DEFAULT_MINIMIZER_MASK};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsWriter, IntermediateSequencesStorage, SequenceExtraData,
};
use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use crate::io::varint::{decode_varint, encode_varint};
use crate::pipeline_common::minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutionContext,
    MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory, MinimizerInputSequence,
};
use crate::query_pipeline::parallel_kmers_query::QueryKmersReferenceData;
use crate::query_pipeline::QueryPipeline;
use crate::rolling::kseq_iterator::{RollingKseqImpl, RollingKseqIterator};
use crate::rolling::minqueue::RollingMinQueue;
use crate::rolling::quality_check::{RollingQualityCheck, LOGPROB_MULTIPLIER, SCORES_INDEX};
use crate::KEEP_FILES;
use bstr::ByteSlice;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use hashbrown::HashMap;
use itertools::Itertools;
use nix::sys::ptrace::cont;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadChainObject, ThreadPoolDefinition, ThreadPoolsChain,
};
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::cmp::{max, min};
use std::hash::Hasher;
use std::intrinsics::unlikely;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::mem::swap;
use std::num::NonZeroU64;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, Thread};
use std::time::{Duration, Instant};

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

struct ContextExtraData {}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct KmersQueryData(pub u64);

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum FileType {
    Graph,
    Query,
}

impl Default for FileType {
    fn default() -> Self {
        Self::Graph
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum ReadType {
    Graph,
    Query(NonZeroU64),
}

impl Default for ReadType {
    fn default() -> Self {
        Self::Graph
    }
}

impl SequenceExtraData for KmersQueryData {
    #[inline(always)]
    fn decode<'a>(mut reader: &'a mut impl Read) -> Option<Self> {
        Some(Self(decode_varint(|| reader.read_u8().ok())?))
    }

    #[inline(always)]
    fn encode<'a>(&self, mut writer: &'a mut impl Write) {
        encode_varint(|b| writer.write_all(b), self.0);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        10
    }
}

pub struct QuerierMinimizerBucketingExecutor<'a, H: HashFunctionFactory, CX: ColorsManager> {
    minimizer_queue: RollingMinQueue<H>,
    global_data: &'a MinimizerBucketingCommonData<()>,
    _phantom: PhantomData<CX>,
}

pub struct QuerierMinimizerBucketingExecutorFactory<H: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, CX)>,
);

impl<H: HashFunctionFactory, CX: ColorsManager> MinimizerBucketingExecutorFactory
    for QuerierMinimizerBucketingExecutorFactory<H, CX>
{
    type GlobalData = ();
    type ExtraData = QueryKmersReferenceData<CX::MinimizerBucketingSeqColorDataType>;
    type PreprocessInfo = ReadType;
    type FileInfo = FileType;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;

    type ExecutorType<'a> = QuerierMinimizerBucketingExecutor<'a, H, CX>;

    fn new<'a>(
        global_data: &'a MinimizerBucketingCommonData<Self::GlobalData>,
    ) -> Self::ExecutorType<'a> {
        Self::ExecutorType::<'a> {
            minimizer_queue: RollingMinQueue::new(global_data.k - global_data.m),
            global_data,
            _phantom: PhantomData,
        }
    }
}

impl<'a, H: HashFunctionFactory, CX: ColorsManager>
    MinimizerBucketingExecutor<'a, QuerierMinimizerBucketingExecutorFactory<H, CX>>
    for QuerierMinimizerBucketingExecutor<'a, H, CX>
{
    fn preprocess_fasta(
        &mut self,
        file_info: &<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::FileInfo,
        read_index: u64,
        preprocess_info: &mut <QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        _sequence: &FastaSequence,
    ) {
        *preprocess_info = match file_info {
            FileType::Graph => ReadType::Graph,
            FileType::Query => ReadType::Query(NonZeroU64::new(read_index + 1).unwrap()),
        }
    }

    fn reprocess_sequence(
        &mut self,
        flags: u8,
        extra_data: &<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData,
        preprocess_info: &mut <QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        todo!()
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(BucketIndexType, S, u8, <QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData, SortingHashType),
        const MINIMIZER_MASK: MinimizerType
    >(
        &mut self,
        preprocess_info: &<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        mut push_sequence: F,
    ){
        let hashes = H::new(sequence, self.global_data.m);

        let mut rolling_iter = self
            .minimizer_queue
            .make_iter::<_, { DEFAULT_MINIMIZER_MASK }>(hashes.iter().map(|x| x.to_unextendable()));

        let mut last_index = 0;
        let mut last_hash = rolling_iter.next().unwrap();

        for (index, min_hash) in rolling_iter.enumerate() {
            if H::get_full_minimizer::<MINIMIZER_MASK>(min_hash)
                != H::get_full_minimizer::<MINIMIZER_MASK>(last_hash)
            {
                let bucket = H::get_first_bucket(last_hash);
                let sorting_hash = H::get_sorting_hash(last_hash);

                push_sequence(
                    bucket,
                    sequence.get_subslice(last_index..(index + self.global_data.k)),
                    0,
                    match preprocess_info {
                        ReadType::Graph => QueryKmersReferenceData::Graph(
                            CX::MinimizerBucketingSeqColorDataType::create(
                                0, // FIXME! build the correct colors!
                            ),
                        ),

                        ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
                    },
                    sorting_hash,
                );

                last_index = index + 1;
                last_hash = min_hash;
            }
        }

        push_sequence(
            H::get_first_bucket(last_hash),
            sequence.get_subslice(last_index..sequence.seq_len()),
            0,
            match preprocess_info {
                ReadType::Graph => {
                    QueryKmersReferenceData::Graph(CX::MinimizerBucketingSeqColorDataType::create(
                        0, // FIXME! build the correct colors!
                    ))
                }

                ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
            },
            H::get_sorting_hash(last_hash),
        );
    }
}

impl QueryPipeline {
    pub fn minimizer_bucketing<H: HashFunctionFactory, CX: ColorsManager>(
        graph_file: PathBuf,
        query_file: PathBuf,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: graph + query bucketing".to_string());

        let mut input_files = vec![(graph_file, FileType::Graph), (query_file, FileType::Query)];

        GenericMinimizerBucketing::do_bucketing::<QuerierMinimizerBucketingExecutorFactory<H, CX>>(
            input_files,
            output_path,
            buckets_count,
            threads_count,
            k,
            m,
            (),
        )
    }
}
