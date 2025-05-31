use colors::colors_manager::color_types::{
    MinimizerBucketingSeqColorDataType, SingleKmerColorDataType,
};
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use config::BucketIndexType;
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_stream::SequenceInfo;
use io::sequences_stream::fasta::FastaFileSequencesStream;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence, PushSequenceInfo,
};
use parallel_processor::buckets::{DuplicatesBuckets, SingleBucket};
use parallel_processor::fast_smart_bucket_sort::FastSortable;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct DumperKmersReferenceData<
    CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()> + Clone + FastSortable,
> {
    pub(crate) color: CX,
}

impl<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()> + Clone + FastSortable>
    HasEmptyExtraBuffer for DumperKmersReferenceData<CX>
{
}

impl<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()> + Clone + FastSortable>
    SequenceExtraDataConsecutiveCompression for DumperKmersReferenceData<CX>
{
    type LastData = CX::LastData;

    #[inline(always)]
    fn decode_extended(
        _buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        last_data: CX::LastData,
    ) -> Option<Self> {
        Some(Self {
            color: CX::decode_extended(&mut (), reader, last_data)?,
        })
    }

    #[inline(always)]
    fn encode_extended(
        &self,
        _buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: CX::LastData,
    ) {
        CX::encode_extended(&self.color, &(), writer, last_data);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.color.max_size()
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        self.color.obtain_last_data(last_data)
    }
}

pub struct ReadTypeBuffered<CX: ColorsManager> {
    colors_buffer: (<MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,),
    read_data: Option<ReadData<CX>>,
}

#[derive(Clone)]
pub struct ReadData<CX: ColorsManager> {
    colors: MinimizerBucketingSeqColorDataType<CX>,
}

impl<CX: ColorsManager> Default for ReadTypeBuffered<CX> {
    fn default() -> Self {
        Self {
            colors_buffer: (MinimizerBucketingSeqColorDataType::<CX>::new_temp_buffer(),),
            read_data: None,
        }
    }
}

pub struct DumperMinimizerBucketingGlobalData {
    colors_count: u64,
    buckets_count_log: u32,
}

pub struct DumperMinimizerBucketingExecutor<CX: ColorsManager> {
    global_data: Arc<MinimizerBucketingCommonData<DumperMinimizerBucketingGlobalData>>,
    _phantom: PhantomData<CX>,
}

pub struct RewriteBucketComputeDumper;

impl RewriteBucketCompute for RewriteBucketComputeDumper {
    fn get_rewrite_bucket<C>(
        _k: usize,
        _m: usize,
        _seq_data: &(
            u8,
            u8,
            C,
            io::compressed_read::CompressedRead,
            config::MultiplicityCounterType,
        ),
        _used_hash_bits: usize,
        _bucket_bits_count: usize,
    ) -> BucketIndexType {
        unimplemented!()
    }
}

pub struct DumperMinimizerBucketingExecutorFactory<CX: ColorsManager>(PhantomData<CX>);

impl<CX: ColorsManager> MinimizerBucketingExecutorFactory
    for DumperMinimizerBucketingExecutorFactory<CX>
{
    type GlobalData = DumperMinimizerBucketingGlobalData;
    type ExtraData = DumperKmersReferenceData<SingleKmerColorDataType<CX>>;
    type PreprocessInfo = ReadTypeBuffered<CX>;
    type StreamInfo = ();

    type ColorsManager = CX;
    type RewriteBucketCompute = RewriteBucketComputeDumper;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;

    type ExecutorType = DumperMinimizerBucketingExecutor<CX>;

    fn new(
        global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>,
    ) -> Self::ExecutorType {
        Self::ExecutorType {
            global_data: global_data.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<CX: ColorsManager> MinimizerBucketingExecutor<DumperMinimizerBucketingExecutorFactory<CX>>
    for DumperMinimizerBucketingExecutor<CX>
{
    fn preprocess_dna_sequence(
        &mut self,
        _stream_info: &<DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::StreamInfo,
        sequence_info: SequenceInfo,
        _read_index: u64,
        sequence: &DnaSequence,
        preprocess_info: &mut <DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        MinimizerBucketingSeqColorDataType::<CX>::clear_temp_buffer(
            &mut preprocess_info.colors_buffer.0,
        );

        preprocess_info.read_data = {
            {
                let color = MinimizerBucketingSeqColorDataType::<CX>::create(
                    SingleSequenceInfo {
                        static_color: sequence_info.color.unwrap_or(0),
                        sequence_ident: match sequence.format {
                            DnaSequencesFileType::FASTA => {
                                SequenceIdent::FASTA(sequence.ident_data)
                            }
                            DnaSequencesFileType::GFA => SequenceIdent::GFA {
                                colors: sequence.ident_data,
                            },
                            DnaSequencesFileType::FASTQ => {
                                todo!()
                            }
                            DnaSequencesFileType::BINARY => {
                                todo!()
                            }
                        },
                    },
                    &mut preprocess_info.colors_buffer.0,
                );

                if CX::COLORS_ENABLED
                    && (color.debug_count() != sequence.seq.len() - self.global_data.k + 1)
                {
                    ggcat_logging::error!(
                        "WARN: Sequence does not have enough colors, please check matching k size:\n{}\n{}",
                        std::str::from_utf8(sequence.ident_data).unwrap(),
                        std::str::from_utf8(sequence.seq).unwrap()
                    );
                }

                Some(ReadData { colors: color })
            }
        }
    }

    fn reprocess_sequence(
        &mut self,
        _flags: u8,
        _extra_data: &<DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData,
        _extra_data_buffer: &<<DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
        _preprocess_info: &mut <DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        unimplemented!()
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(PushSequenceInfo<S, DumperMinimizerBucketingExecutorFactory<CX>>),
        const SEPARATE_DUPLICATES: bool,
    >(
        &mut self,
        preprocess_info: &<DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        _used_bits: usize,
        _first_bits: usize,
        _second_bits: usize,
        mut push_sequence: F,
    ) {
        let mut rolling_iter = preprocess_info
            .read_data
            .as_ref()
            .unwrap()
            .colors
            .get_iterator(&preprocess_info.colors_buffer.0);

        let mut last_index = 0;
        let mut last_color = rolling_iter.next().unwrap();

        for (index, kmer_color) in rolling_iter.enumerate() {
            if kmer_color != last_color {
                push_sequence(PushSequenceInfo {
                    bucket: CX::get_bucket_from_color(
                        &last_color,
                        self.global_data.global_data.colors_count,
                        self.global_data.global_data.buckets_count_log,
                    ),
                    second_bucket: 0,
                    sequence: sequence.get_subslice(last_index..(index + self.global_data.k)),
                    extra_data: DumperKmersReferenceData { color: last_color },
                    temp_buffer: &(),
                    minimizer_pos: 0,
                    flags: 0,
                    rc: false,
                    is_window_duplicate: false,
                });
                last_index = index + 1;
                last_color = kmer_color;
            }
        }

        push_sequence(PushSequenceInfo {
            bucket: CX::get_bucket_from_color(
                &last_color,
                self.global_data.global_data.colors_count,
                self.global_data.global_data.buckets_count_log,
            ),
            second_bucket: 0,
            sequence: sequence.get_subslice(last_index..sequence.seq_len()),
            extra_data: DumperKmersReferenceData { color: last_color },
            temp_buffer: &(),
            minimizer_pos: 0,
            flags: 0,
            rc: false,
            is_window_duplicate: false,
        });
    }
}

pub fn minimizer_bucketing<CX: ColorsManager>(
    graph_file: PathBuf,
    buckets_count: usize,
    threads_count: usize,
    temp_dir: &Path,
    k: usize,
    m: usize,
    colors_count: u64,
) -> (Vec<SingleBucket>, PathBuf) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: unitigs reorganization".to_string());

    let input_files = vec![((graph_file, None), ())];

    GenericMinimizerBucketing::do_bucketing_no_max_usage::<
        DumperMinimizerBucketingExecutorFactory<CX>,
        FastaFileSequencesStream,
    >(
        input_files.into_iter(),
        temp_dir,
        buckets_count,
        threads_count,
        k,
        m,
        DumperMinimizerBucketingGlobalData {
            colors_count,
            buckets_count_log: buckets_count.ilog2(),
        },
        None,
        CX::COLORS_ENABLED,
        k,
        DuplicatesBuckets::None,
    )
}
