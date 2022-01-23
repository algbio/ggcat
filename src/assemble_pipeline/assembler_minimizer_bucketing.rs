use crate::assemble_pipeline::parallel_kmers_merge::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::{BucketIndexType, MinimizerType};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::sequences_reader::FastaSequence;
use crate::pipeline_common::minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
};
use crate::rolling::minqueue::RollingMinQueue;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};

pub struct AssemblerMinimizerBucketingExecutor<'a, H: HashFunctionFactory, CX: ColorsManager> {
    minimizer_queue: RollingMinQueue<H>,
    global_data: &'a MinimizerBucketingCommonData<()>,
    _phantom: PhantomData<CX>,
}

pub struct AssemblerPreprocessInfo<CX: ColorsManager> {
    color_info: CX::MinimizerBucketingSeqColorDataType,
    include_first: bool,
    include_last: bool,
}

impl<CX: ColorsManager> Default for AssemblerPreprocessInfo<CX> {
    fn default() -> Self {
        Self {
            color_info: CX::MinimizerBucketingSeqColorDataType::default(),
            include_first: false,
            include_last: false,
        }
    }
}

pub struct AssemblerMinimizerBucketingExecutorFactory<H: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, CX)>,
);

impl<H: HashFunctionFactory, CX: ColorsManager> MinimizerBucketingExecutorFactory
    for AssemblerMinimizerBucketingExecutorFactory<H, CX>
{
    type GlobalData = ();
    type ExtraData = CX::MinimizerBucketingSeqColorDataType;
    type PreprocessInfo = AssemblerPreprocessInfo<CX>;
    type FileInfo = u64;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;

    type ExecutorType<'a> = AssemblerMinimizerBucketingExecutor<'a, H, CX>;

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
    MinimizerBucketingExecutor<'a, AssemblerMinimizerBucketingExecutorFactory<H, CX>>
    for AssemblerMinimizerBucketingExecutor<'a, H, CX>
{
    fn preprocess_fasta(
        &mut self,
        file_info: &<AssemblerMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::FileInfo,
        _read_index: u64,
        preprocess_info: &mut <AssemblerMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        _sequence: &FastaSequence,
    ) {
        *preprocess_info = AssemblerPreprocessInfo {
            color_info: CX::MinimizerBucketingSeqColorDataType::create(*file_info),
            include_first: true,
            include_last: true,
        };
    }

    #[inline(always)]
    fn reprocess_sequence(
        &mut self,
        flags: u8,
        extra_data: &<AssemblerMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData,
        preprocess_info: &mut <AssemblerMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        preprocess_info.include_first = (flags & READ_FLAG_INCL_BEGIN) != 0;
        preprocess_info.include_last = (flags & READ_FLAG_INCL_END) != 0;
        preprocess_info.color_info = *extra_data;
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(BucketIndexType, S, u8, <AssemblerMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData),
        const MINIMIZER_MASK: MinimizerType
    >(
        &mut self,
        preprocess_info: &<AssemblerMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        mut push_sequence: F,
    ){
        let hashes = H::new(sequence, self.global_data.m);

        let mut rolling_iter = self
            .minimizer_queue
            .make_iter::<_, MINIMIZER_MASK>(hashes.iter().map(|x| x.to_unextendable()));

        let mut last_index = 0;
        let mut last_hash = rolling_iter.next().unwrap();
        let mut include_first = preprocess_info.include_first;

        // If we do not include the first base (so the minimizer value is different), it should not be further split
        let additional_offset = if !include_first {
            last_hash = rolling_iter.next().unwrap();
            1
        } else {
            0
        };

        let end_index = sequence.seq_len() - self.global_data.k;

        for (index, min_hash) in rolling_iter.enumerate() {
            let index = index + additional_offset;

            if (H::get_full_minimizer::<MINIMIZER_MASK>(min_hash)
                != H::get_full_minimizer::<MINIMIZER_MASK>(last_hash))
                && (preprocess_info.include_last || end_index != index)
            {
                let bucket = H::get_first_bucket(last_hash);

                push_sequence(
                    bucket,
                    sequence.get_subslice((max(1, last_index) - 1)..(index + self.global_data.k)),
                    include_first as u8,
                    preprocess_info.color_info,
                );
                last_index = index + 1;
                last_hash = min_hash;
                include_first = false;
            }
        }

        let start_index = max(1, last_index) - 1;
        let include_last = preprocess_info.include_last; // Always include the last element of the sequence in the last entry
        push_sequence(
            H::get_first_bucket(last_hash),
            sequence.get_subslice(start_index..sequence.seq_len()),
            include_first as u8 | ((include_last as u8) << 1),
            preprocess_info.color_info,
        );
    }
}

impl AssemblePipeline {
    pub fn minimizer_bucketing<H: HashFunctionFactory, CX: ColorsManager>(
        input_files: Vec<PathBuf>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: reads bucketing".to_string());

        let input_files: Vec<_> = input_files
            .into_iter()
            .enumerate()
            .map(|(i, f)| (f, i as u64))
            .collect();

        GenericMinimizerBucketing::do_bucketing::<AssemblerMinimizerBucketingExecutorFactory<H, CX>>(
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
