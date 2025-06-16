pub mod rewrite_bucket;

use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::color_types::{
    MinimizerBucketingMultipleSeqColorDataType, MinimizerBucketingSeqColorDataType,
};
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use config::{BucketIndexType, ColorIndexType};
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use hashes::HashFunction;
use hashes::default::MNHFactory;
use hashes::rolling::batch_minqueue::BatchMinQueue;
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_stream::SequenceInfo;
use io::sequences_stream::general::{GeneralSequenceBlockData, GeneralSequencesStream};
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
};
use minimizer_bucketing::{MinimzerBucketingFilesReaderInputPacket, PushSequenceInfo};
use parallel_processor::buckets::{BucketsCount, MultiChunkBucket};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default)]
struct MinimizerExtraData {
    index: u32,
    is_forward: bool,
}

pub struct AssemblerMinimizerBucketingExecutor<CD: MinimizerBucketingSeqColorData> {
    minimizer_queue: BatchMinQueue<MinimizerExtraData>,
    global_data: Arc<MinimizerBucketingCommonData<()>>,
    pub duplicates_bucket: BucketIndexType,
    _phantom: PhantomData<CD>,
}

pub struct AssemblerPreprocessInfo<CD: MinimizerBucketingSeqColorData> {
    color_info: CD,
    color_info_buffer: <CD as SequenceExtraDataTempBufferManagement>::TempBuffer,
    include_first: bool,
    include_last: bool,
}

impl<CD: MinimizerBucketingSeqColorData> Default for AssemblerPreprocessInfo<CD> {
    fn default() -> Self {
        Self {
            color_info: CD::default(),
            color_info_buffer: <CD as SequenceExtraDataTempBufferManagement>::new_temp_buffer(),
            include_first: false,
            include_last: false,
        }
    }
}

#[derive(Clone, Default)]
pub struct InputFileInfo {
    file_color: ColorIndexType,
}

pub struct AssemblerMinimizerBucketingExecutorFactory<CD: MinimizerBucketingSeqColorData>(
    PhantomData<CD>,
);

impl<CD: MinimizerBucketingSeqColorData> MinimizerBucketingExecutorFactory
    for AssemblerMinimizerBucketingExecutorFactory<CD>
{
    type GlobalData = ();
    type ReadExtraData = CD;
    type PreprocessInfo = AssemblerPreprocessInfo<CD>;
    type StreamInfo = InputFileInfo;

    type RewriteBucketCompute = rewrite_bucket::RewriteBucketComputeAssembler;

    type FlagsCount = typenum::U2;

    type ExecutorType = AssemblerMinimizerBucketingExecutor<CD>;

    fn new(
        global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>,
    ) -> Self::ExecutorType {
        Self::ExecutorType {
            minimizer_queue: BatchMinQueue::new(global_data.k - global_data.m),
            global_data: global_data.clone(),
            duplicates_bucket: global_data.buckets_count.normal_buckets_count as u16,
            _phantom: PhantomData,
        }
    }
}

impl<CD: MinimizerBucketingSeqColorData> AssemblerMinimizerBucketingExecutorFactory<CD> {
    pub fn new_with_duplicates(
        global_data: &Arc<MinimizerBucketingCommonData<()>>,
        duplicates_bucket: u16,
    ) -> AssemblerMinimizerBucketingExecutor<CD> {
        AssemblerMinimizerBucketingExecutor {
            minimizer_queue: BatchMinQueue::new(global_data.k - global_data.m),
            global_data: global_data.clone(),
            duplicates_bucket,
            _phantom: PhantomData,
        }
    }
}

impl<CD: MinimizerBucketingSeqColorData>
    MinimizerBucketingExecutor<AssemblerMinimizerBucketingExecutorFactory<CD>>
    for AssemblerMinimizerBucketingExecutor<CD>
{
    fn preprocess_dna_sequence(
        &mut self,
        stream_info: &<AssemblerMinimizerBucketingExecutorFactory<CD> as MinimizerBucketingExecutorFactory>::StreamInfo,
        sequence_info: SequenceInfo,
        _read_index: u64,
        sequence: &DnaSequence,
        preprocess_info: &mut <AssemblerMinimizerBucketingExecutorFactory<CD> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        CD::clear_temp_buffer(&mut preprocess_info.color_info_buffer);

        preprocess_info.color_info = CD::create(
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
        extra_data: &CD,
        extra_data_buffer: &<CD as SequenceExtraDataTempBufferManagement>::TempBuffer,
        preprocess_info: &mut <AssemblerMinimizerBucketingExecutorFactory<CD> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        CD::clear_temp_buffer(&mut preprocess_info.color_info_buffer);
        preprocess_info.color_info = CD::copy_extra_from(
            extra_data.clone(),
            extra_data_buffer,
            &mut preprocess_info.color_info_buffer,
        );
        preprocess_info.include_first = (flags & READ_FLAG_INCL_BEGIN) != 0;
        preprocess_info.include_last = (flags & READ_FLAG_INCL_END) != 0;
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(PushSequenceInfo<S, AssemblerMinimizerBucketingExecutorFactory<CD>>),
        const SEPARATE_DUPLICATES: bool,
    >(
        &mut self,
        preprocess_info: &<AssemblerMinimizerBucketingExecutorFactory<CD> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        mut push_sequence: F,
    ) {
        let hashes = MNHFactory::new(sequence, self.global_data.m);

        let mut last_index = 1;
        let mut include_first = preprocess_info.include_first;

        // If we do not include the first base (so the minimizer value is different), it should not be further split
        let skip_before = if !include_first { 1 } else { 0 };
        let skip_after = if !preprocess_info.include_last { 1 } else { 0 };

        #[inline]
        #[cold]
        fn cold() {}

        self.minimizer_queue
            .get_minimizer_splits::<_, SEPARATE_DUPLICATES>(
                hashes.iter_enumerate().map(|(index, x)| {
                    (
                        x.to_unextendable()
            // Set the unique flag if the minimizer is not rc_symmetric
            | (SEPARATE_DUPLICATES || !x.is_rc_symmetric()) as u64,
                        MinimizerExtraData {
                            index: index as u32,
                            is_forward: x.is_forward(),
                        },
                    )
                }),
                skip_before,
                skip_after,
                #[inline(always)]
                |index, min_hash, is_last, is_dupl| {
                    let include_last = preprocess_info.include_last && is_last;

                    // HS2
                    let (bucket, rc, minimizer_pos, is_window_duplicate) = if SEPARATE_DUPLICATES
                        && (min_hash.0
                            & BatchMinQueue::<MinimizerExtraData>::unique_flag::<
                                SEPARATE_DUPLICATES,
                            >()
                            == 0)
                    {
                        cold();
                        (self.duplicates_bucket, false, 1, false)
                    } else {
                        let rc = SEPARATE_DUPLICATES && !min_hash.1.is_forward;
                        (
                            MNHFactory::get_bucket(used_bits, first_bits, min_hash.0),
                            rc,
                            if is_dupl {
                                cold();
                                0
                            } else {
                                if rc {
                                    (index + self.global_data.k - 1 - (min_hash.1.index as usize) - self.global_data.m) as u16
                                } else {
                                    (min_hash.1.index - (last_index as u32 - 1)) as u16
                                }
                            },
                            is_dupl
                        )
                    };

                    push_sequence(
                        PushSequenceInfo {
                            bucket,
                            second_bucket: MNHFactory::get_bucket(used_bits + first_bits, second_bits, min_hash.0),
                            sequence: sequence.get_subslice((last_index - 1)..(index + self.global_data.k - 1)),
                            extra_data: preprocess_info
                                .color_info
                                .get_subslice((last_index - 1)..index, rc), // FIXME: Check if the subslice is correct
                            temp_buffer: &preprocess_info.color_info_buffer,
                            minimizer_pos,
                            flags: ((include_first as u8) << (rc as u8)) | ((include_last as u8) << (!rc as u8)),
                            rc,
                            is_window_duplicate,
                        },
                    );
                    last_index = index;
                    include_first = false;
                },
            );
    }
}

#[dynamic_dispatch(H = [
    hashes::cn_nthash::CanonicalNtHashIteratorFactory,
], CX = [
    #[cfg(not(feature = "devel-build"))] colors::bundles::multifile_building::ColorBundleMultifileBuilding,
    colors::non_colored::NonColoredManager,
])]
pub fn minimizer_bucketing<CX: ColorsManager>(
    input_blocks: Vec<GeneralSequenceBlockData>,
    output_path: &Path,
    buckets_count: BucketsCount,
    second_buckets_count: BucketsCount,
    threads_count: usize,
    k: usize,
    m: usize,
    chunking_size_threshold: Option<u64>,
    target_chunk_size: u64,
) -> Vec<MultiChunkBucket> {
    MNHFactory::initialize(k);

    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: reads bucketing".to_string());

    let mut input_files: Vec<_> = input_blocks
        .into_iter()
        .enumerate()
        .map(|(i, f)| MinimzerBucketingFilesReaderInputPacket {
            sequences: f,
            stream_info: InputFileInfo {
                file_color: i as ColorIndexType,
            },
        })
        .collect();

    input_files.sort_by_cached_key(
        |f: &MinimzerBucketingFilesReaderInputPacket<
            AssemblerMinimizerBucketingExecutorFactory<MinimizerBucketingSeqColorDataType<CX>>,
            GeneralSequencesStream,
        >| {
            let bases_count = f.sequences.estimated_bases_count().unwrap();
            bases_count
        },
    );
    input_files.reverse();

    GenericMinimizerBucketing::do_bucketing::<
        MinimizerBucketingSeqColorDataType<CX>,
        MinimizerBucketingMultipleSeqColorDataType<CX>,
        AssemblerMinimizerBucketingExecutorFactory<MinimizerBucketingSeqColorDataType<CX>>,
        GeneralSequencesStream,
    >(
        input_files.into_iter(),
        output_path,
        buckets_count,
        second_buckets_count,
        threads_count,
        k,
        m,
        (),
        Some(k - 1),
        false,
        k,
        chunking_size_threshold,
        target_chunk_size,
    )
}
