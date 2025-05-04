pub mod rewrite_bucket;

use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use config::{BucketIndexType, ColorIndexType};
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use hashes::default::MNHFactory;
use hashes::rolling::minqueue::RollingMinQueue;
use hashes::HashFunction;
use hashes::MinimizerHashFunctionFactory;
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_stream::general::{GeneralSequenceBlockData, GeneralSequencesStream};
use io::sequences_stream::SequenceInfo;
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
};
use parallel_processor::buckets::MultiChunkBucket;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct AssemblerMinimizerBucketingExecutor<CX: ColorsManager> {
    minimizer_queue: RollingMinQueue,
    global_data: Arc<MinimizerBucketingCommonData<()>>,
    _phantom: PhantomData<CX>,
}

pub struct AssemblerPreprocessInfo<CX: ColorsManager> {
    color_info: MinimizerBucketingSeqColorDataType<CX>,
    color_info_buffer: <MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    include_first: bool,
    include_last: bool,
}

impl<CX: ColorsManager> Default for AssemblerPreprocessInfo<CX> {
    fn default() -> Self {
        Self {
            color_info: MinimizerBucketingSeqColorDataType::<CX>::default(),
            color_info_buffer:
                    <MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::new_temp_buffer(),
            include_first: false,
            include_last: false,
        }
    }
}

#[derive(Clone, Default)]
pub struct InputFileInfo {
    file_color: ColorIndexType,
}

pub struct AssemblerMinimizerBucketingExecutorFactory<CX: ColorsManager>(PhantomData<CX>);

impl<CX: ColorsManager> MinimizerBucketingExecutorFactory
    for AssemblerMinimizerBucketingExecutorFactory<CX>
{
    type GlobalData = ();
    type ExtraData = MinimizerBucketingSeqColorDataType<CX>;
    type PreprocessInfo = AssemblerPreprocessInfo<CX>;
    type StreamInfo = InputFileInfo;

    type ColorsManager = CX;
    type RewriteBucketCompute = rewrite_bucket::RewriteBucketComputeAssembler;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;

    type ExecutorType = AssemblerMinimizerBucketingExecutor<CX>;

    fn new(
        global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>,
    ) -> Self::ExecutorType {
        Self::ExecutorType {
            minimizer_queue: RollingMinQueue::new(global_data.k - global_data.m),
            global_data: global_data.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<CX: ColorsManager> MinimizerBucketingExecutor<AssemblerMinimizerBucketingExecutorFactory<CX>>
    for AssemblerMinimizerBucketingExecutor<CX>
{
    fn preprocess_dna_sequence(
        &mut self,
        stream_info: &<AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::StreamInfo,
        sequence_info: SequenceInfo,
        _read_index: u64,
        sequence: &DnaSequence,
        preprocess_info: &mut <AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        MinimizerBucketingSeqColorDataType::<CX>::clear_temp_buffer(
            &mut preprocess_info.color_info_buffer,
        );

        preprocess_info.color_info = MinimizerBucketingSeqColorDataType::<CX>::create(
            SingleSequenceInfo {
                static_color: sequence_info.color.unwrap_or(stream_info.file_color),
                sequence_ident: match sequence.format {
                    DnaSequencesFileType::FASTA | DnaSequencesFileType::FASTQ => {
                        SequenceIdent::FASTA(sequence.ident_data)
                    }
                    DnaSequencesFileType::GFA => SequenceIdent::GFA {
                        colors: sequence.ident_data,
                    },
                    DnaSequencesFileType::BINARY => {
                        todo!()
                    }
                },
            },
            &mut preprocess_info.color_info_buffer,
        );
        preprocess_info.include_first = true;
        preprocess_info.include_last = true;
    }

    #[inline(always)]
    fn reprocess_sequence(
        &mut self,
        flags: u8,
        extra_data: &<AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData,
        extra_data_buffer: &<MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        preprocess_info: &mut <AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        MinimizerBucketingSeqColorDataType::<CX>::clear_temp_buffer(
            &mut preprocess_info.color_info_buffer,
        );
        preprocess_info.color_info = MinimizerBucketingSeqColorDataType::<CX>::copy_extra_from(
            extra_data.clone(),
            extra_data_buffer,
            &mut preprocess_info.color_info_buffer,
        );
        preprocess_info.include_first = (flags & READ_FLAG_INCL_BEGIN) != 0;
        preprocess_info.include_last = (flags & READ_FLAG_INCL_END) != 0;
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(BucketIndexType, BucketIndexType, S, u8, <AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData, &<<AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer),
    >(
        &mut self,
        preprocess_info: &<AssemblerMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        mut push_sequence: F,
    ){
        let hashes = MNHFactory::new(sequence, self.global_data.m);

        let mut rolling_iter = self
            .minimizer_queue
            .make_iter(hashes.iter().map(|x| x.to_unextendable()));

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

            if (MNHFactory::get_full_minimizer(min_hash)
                != MNHFactory::get_full_minimizer(last_hash))
                && (preprocess_info.include_last || end_index != index)
            {
                push_sequence(
                    MNHFactory::get_bucket(used_bits, first_bits, last_hash),
                    MNHFactory::get_bucket(used_bits + first_bits, second_bits, last_hash),
                    sequence.get_subslice((max(1, last_index) - 1)..(index + self.global_data.k)),
                    include_first as u8,
                    preprocess_info
                        .color_info
                        .get_subslice((max(1, last_index) - 1)..(index + 1)), // FIXME: Check if the subslice is correct
                    &preprocess_info.color_info_buffer,
                );
                last_index = index + 1;
                last_hash = min_hash;
                include_first = false;
            }
        }

        let start_index = max(1, last_index) - 1;
        let include_last = preprocess_info.include_last; // Always include the last element of the sequence in the last entry
        push_sequence(
            MNHFactory::get_bucket(used_bits, first_bits, last_hash),
            MNHFactory::get_bucket(used_bits + first_bits, second_bits, last_hash),
            sequence.get_subslice(start_index..sequence.seq_len()),
            include_first as u8 | ((include_last as u8) << 1),
            preprocess_info
                .color_info
                .get_subslice(start_index..(sequence.seq_len() + 1 - self.global_data.k)), // FIXME: Check if the subslice is correct,
            &preprocess_info.color_info_buffer,
        );
    }
}

#[dynamic_dispatch(H = [
    hashes::cn_nthash::CanonicalNtHashIteratorFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_nthash::ForwardNtHashIteratorFactory
], CX = [
    #[cfg(not(feature = "devel-build"))] colors::bundles::multifile_building::ColorBundleMultifileBuilding,
    colors::non_colored::NonColoredManager,
])]
pub fn minimizer_bucketing<CX: ColorsManager>(
    input_blocks: Vec<GeneralSequenceBlockData>,
    output_path: &Path,
    buckets_count: usize,
    threads_count: usize,
    k: usize,
    m: usize,
    minimizer_bucketing_compaction_threshold: Option<u64>,
) -> (Vec<MultiChunkBucket>, PathBuf) {
    MNHFactory::initialize(k);

    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: reads bucketing".to_string());

    let mut input_files: Vec<_> = input_blocks
        .into_iter()
        .enumerate()
        .map(|(i, f)| {
            (
                f,
                InputFileInfo {
                    file_color: i as ColorIndexType,
                },
            )
        })
        .collect();

    input_files.sort_by_cached_key(|(file, _)| {
        let bases_count = file.estimated_bases_count().unwrap();
        bases_count
    });
    input_files.reverse();

    GenericMinimizerBucketing::do_bucketing::<
        AssemblerMinimizerBucketingExecutorFactory<CX>,
        GeneralSequencesStream,
    >(
        input_files.into_iter(),
        output_path,
        buckets_count,
        threads_count,
        k,
        m,
        (),
        Some(k - 1),
        false,
        k,
        minimizer_bucketing_compaction_threshold,
    )
}
