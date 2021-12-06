use crate::assemble_pipeline::current_kmers_merge::KmersFlags;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsWriter, IntermediateSequencesStorage, SequenceExtraData,
};
use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use crate::io::varint::{decode_varint, encode_varint};
use crate::pipeline_common::minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingExecutionContext, MinimizerBucketingExecutor,
};
use crate::query_pipeline::QueryPipeline;
use crate::rolling::kseq_iterator::{RollingKseqImpl, RollingKseqIterator};
use crate::rolling::minqueue::RollingMinQueue;
use crate::rolling::quality_check::{RollingQualityCheck, LOGPROB_MULTIPLIER, SCORES_INDEX};
use crate::types::BucketIndexType;
use crate::KEEP_FILES;
use bstr::ByteSlice;
use byteorder::ReadBytesExt;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use hashbrown::HashMap;
use itertools::Itertools;
use nix::sys::ptrace::cont;
use object_pool::Pool;
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

const CHUNKS_SIZE: usize = 1024 * 1024 * 16;
const MAX_READING_THREADS: usize = 2;
const WATERMARK_HIGH: usize = 64;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct KmersQueryData(pub u64);

impl SequenceExtraData for KmersQueryData {
    #[inline(always)]
    fn decode(mut reader: impl Read) -> Option<Self> {
        Some(Self(decode_varint(|| reader.read_u8().ok())?))
    }

    #[inline(always)]
    fn encode(&self, mut writer: impl Write) {
        encode_varint(|b| writer.write_all(b), self.0);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        10
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct KmersReferenceData<CX: MinimizerBucketingSeqColorData>(pub CX);

impl<CX: MinimizerBucketingSeqColorData> SequenceExtraData for KmersReferenceData<CX> {
    #[inline(always)]
    fn decode(mut reader: impl Read) -> Option<Self> {
        Some(Self(CX::decode(reader)?))
    }

    #[inline(always)]
    fn encode(&self, mut writer: impl Write) {
        self.0.encode(writer);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.0.max_size()
    }
}

pub struct QuerierMinimizerBucketingExecutor<H: HashFunctionFactory, CX: ColorsManager> {
    minimizer_queue: RollingMinQueue<H>,
    _phantom: PhantomData<CX>,
}

impl<H: HashFunctionFactory, CX: ColorsManager> MinimizerBucketingExecutor
    for QuerierMinimizerBucketingExecutor<H, CX>
{
    type GlobalData = ();
    type ExtraData = KmersReferenceData<CX::MinimizerBucketingSeqColorDataType>;
    type PreprocessInfo = u64;
    type FileInfo = u64;

    fn new<C>(
        global_data: &MinimizerBucketingExecutionContext<Self::ExtraData, C, Self::GlobalData>,
    ) -> Self {
        Self {
            minimizer_queue: RollingMinQueue::new(global_data.k - global_data.m),
            _phantom: PhantomData,
        }
    }

    fn preprocess_fasta<C>(
        &mut self,
        _global_data: &MinimizerBucketingExecutionContext<Self::ExtraData, C, Self::GlobalData>,
        file_info: &Self::FileInfo,
        preprocess_info: &mut Self::PreprocessInfo,
        _sequence: &FastaSequence,
    ) {
        *preprocess_info = *file_info;
    }

    fn process_sequence<C, F: FnMut(BucketIndexType, &[u8], Self::ExtraData)>(
        &mut self,
        global_data: &MinimizerBucketingExecutionContext<Self::ExtraData, C, Self::GlobalData>,
        preprocess_info: &Self::PreprocessInfo,
        sequence: &[u8],
        _range: Range<usize>,
        mut push_sequence: F,
    ) {
        let hashes = H::new(&sequence[..], global_data.m);

        let mut rolling_iter = self
            .minimizer_queue
            .make_iter(hashes.iter().map(|x| x.to_unextendable()));

        let mut last_index = 0;
        let mut last_hash = rolling_iter.next().unwrap();

        for (index, min_hash) in rolling_iter.enumerate() {
            if min_hash != last_hash {
                let bucket =
                    H::get_bucket(last_hash) % (global_data.buckets_count as BucketIndexType);

                push_sequence(
                    bucket,
                    &sequence[last_index..(index + global_data.k - 1)],
                    KmersReferenceData(CX::MinimizerBucketingSeqColorDataType::create(
                        *preprocess_info,
                    )),
                );

                last_index = index + 1;
                last_hash = min_hash;
            }
        }

        push_sequence(
            H::get_bucket(last_hash) % (global_data.buckets_count as BucketIndexType),
            &sequence[last_index..sequence.len()],
            KmersReferenceData(CX::MinimizerBucketingSeqColorDataType::create(
                *preprocess_info,
            )),
        );
    }
}

impl QueryPipeline {
    pub fn minimizer_bucketing<H: HashFunctionFactory, CX: ColorsManager>(
        input_files: Vec<PathBuf>,
        output_path: &Path,
        output_suffix: &str,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: bucketing/".to_string() + output_suffix);

        let mut input_files: Vec<_> = input_files
            .into_iter()
            .enumerate()
            .map(|(i, f)| (f, i as u64))
            .collect();

        GenericMinimizerBucketing::do_bucketing::<QuerierMinimizerBucketingExecutor<H, CX>>(
            input_files,
            output_path,
            buckets_count,
            threads_count,
            k,
            m,
            None,
            (),
        )
    }
}
