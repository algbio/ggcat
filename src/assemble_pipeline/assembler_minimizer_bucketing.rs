use crate::assemble_pipeline::parallel_kmers_merge::KmersFlags;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::BucketIndexType;
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::sequences_reader::FastaSequence;
use crate::io::DataWriter;
use crate::pipeline_common::minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingExecutionContext, MinimizerBucketingExecutor,
};
use crate::rolling::minqueue::RollingMinQueue;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::max;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};

pub struct AssemblerMinimizerBucketingExecutor<H: HashFunctionFactory, CX: ColorsManager> {
    minimizer_queue: RollingMinQueue<H>,
    _phantom: PhantomData<CX>,
}

impl<H: HashFunctionFactory, CX: ColorsManager> MinimizerBucketingExecutor
    for AssemblerMinimizerBucketingExecutor<H, CX>
{
    type GlobalData = ();
    type ExtraData = KmersFlags<CX::MinimizerBucketingSeqColorDataType>;
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
        _read_index: u64,
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
        let mut include_first = true;

        for (index, min_hash) in rolling_iter.enumerate() {
            if min_hash != last_hash {
                let bucket =
                    H::get_first_bucket(last_hash) % (global_data.buckets_count as BucketIndexType);

                push_sequence(
                    bucket,
                    &sequence[(max(1, last_index) - 1)..(index + global_data.k)],
                    KmersFlags(
                        include_first as u8,
                        CX::MinimizerBucketingSeqColorDataType::create(*preprocess_info),
                    ),
                );
                last_index = index + 1;
                last_hash = min_hash;
                include_first = false;
            }
        }

        let start_index = max(1, last_index) - 1;
        let include_last = true; // Always include the last element of the sequence in the last entry
        push_sequence(
            H::get_first_bucket(last_hash) % (global_data.buckets_count as BucketIndexType),
            &sequence[start_index..sequence.len()],
            KmersFlags(
                include_first as u8 | ((include_last as u8) << 1),
                CX::MinimizerBucketingSeqColorDataType::create(*preprocess_info),
            ),
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
        quality_threshold: Option<f64>,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: reads bucketing".to_string());

        let mut input_files: Vec<_> = input_files
            .into_iter()
            .enumerate()
            .map(|(i, f)| (f, i as u64))
            .collect();

        GenericMinimizerBucketing::do_bucketing::<AssemblerMinimizerBucketingExecutor<H, CX>>(
            input_files,
            output_path,
            buckets_count,
            threads_count,
            k,
            m,
            quality_threshold,
            (),
        )
    }
}
