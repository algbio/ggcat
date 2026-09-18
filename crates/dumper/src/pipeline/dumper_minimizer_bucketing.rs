use colors::colors_manager::color_types::{
    MinimizerBucketingSeqColorDataType, SingleKmerColorDataType,
};
use colors::colors_manager::{
    ColorsManager, MinimizerBucketingSeqColorData, MinimizerBucketingSeqColorDataIterable,
};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use config::BucketIndexType;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement, TempBuffer,
};
use io::sequences_reader::DnaSequencesFileType;
use io::sequences_stream::SequenceInfo;
use io::sequences_stream::fasta::FastaFileSequencesStream;
use minimizer_bucketing::lane_runs::SimdScratch;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;
use minimizer_bucketing::simd_batch::{RecordInfo, SequencesLaneBatch};
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
    MinimzerBucketingFilesReaderInputPacket, PushSequenceInfo,
};
use parallel_processor::buckets::{BucketsCount, MultiChunkBucket};
use parallel_processor::fast_smart_bucket_sort::FastSortable;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::io::{BufRead, Write};
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
        reader: &mut impl BufRead,
        last_data: CX::LastData,
        read_flags: u8,
    ) -> Option<Self> {
        Some(Self {
            color: CX::decode_extended(&mut (), reader, last_data, read_flags)?,
        })
    }

    #[inline(always)]
    fn encode_extended(
        &self,
        _buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: CX::LastData,
        sequence_length: usize,
        reverse_complement: bool,
        read_flags: u8,
    ) {
        CX::encode_extended(
            &self.color,
            &(),
            writer,
            last_data,
            sequence_length,
            reverse_complement,
            read_flags,
        );
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.color.max_size()
    }

    fn obtain_last_data(
        &self,
        last_data: Self::LastData,
        reverse_complement: bool,
    ) -> Self::LastData {
        self.color.obtain_last_data(last_data, reverse_complement)
    }
}

impl<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()> + Copy + FastSortable>
    SequenceExtraDataCombiner for DumperKmersReferenceData<CX>
{
    type SingleDataType = Self;
    const ALLOW_COMBINE: bool = false;

    fn combine_entries(
        &mut self,
        _out_buffer: &mut Self::TempBuffer,
        _color: Self,
        _in_buffer: &Self::TempBuffer,
    ) {
        unimplemented!()
    }

    fn to_single(
        &self,
        in_buffer: &Self::TempBuffer,
        out_buffer: &mut TempBuffer<Self::SingleDataType>,
    ) -> Self::SingleDataType {
        Self::copy_extra_from(*self, in_buffer, out_buffer)
    }

    fn prepare_for_serialization(&mut self, _buffer: &mut Self::TempBuffer) {}

    fn from_single_entry<'a>(
        out_buffer: &'a mut Self::TempBuffer,
        single: Self::SingleDataType,
        _in_buffer: &'a TempBuffer<Self::SingleDataType>,
    ) -> (Self, &'a mut Self::TempBuffer) {
        // Both buffers are `()` here, so handing back the destination is the
        // same thing as handing back the source.
        (single, out_buffer)
    }
}

pub struct ReadTypeBuffered<CX: ColorsManager> {
    colors_buffer: (TempBuffer<MinimizerBucketingSeqColorDataType<CX>>,),
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
    buckets_count_log: usize,
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
    type ReadExtraData = DumperKmersReferenceData<SingleKmerColorDataType<CX>>;
    type PreprocessInfo = ReadTypeBuffered<CX>;
    type StreamInfo = ();

    type RewriteBucketCompute = RewriteBucketComputeDumper;

    type FlagsCount = typenum::U0;

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
        record: &RecordInfo<'_>,
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
                        sequence_ident: match record.format {
                            DnaSequencesFileType::FASTA | DnaSequencesFileType::FASTQ => {
                                SequenceIdent::FASTA(record.ident_data)
                            }
                            DnaSequencesFileType::GFA => SequenceIdent::GFA {
                                colors: record.ident_data,
                            },
                            DnaSequencesFileType::BINARY => {
                                todo!()
                            }
                        },
                    },
                    &mut preprocess_info.colors_buffer.0,
                );

                if CX::COLORS_ENABLED
                    && (color.debug_count() != record.bases_count - self.global_data.k + 1)
                {
                    ggcat_logging::error!(
                        "WARN: Sequence does not have enough colors, please check matching k size:\n{}",
                        String::from_utf8_lossy(record.ident_data),
                    );
                }

                Some(ReadData { colors: color })
            }
        }
    }

    fn reprocess_sequence(
        &mut self,
        _flags: u8,
        _extra_data: &<DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ReadExtraData,
        _extra_data_buffer: &TempBuffer<<DumperMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ReadExtraData>,
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
        push_sequence: F,
    ) {
        let colors = preprocess_info
            .read_data
            .as_ref()
            .unwrap()
            .colors
            .get_iterator(&preprocess_info.colors_buffer.0);
        self.split_by_colors(sequence, colors, push_sequence);
    }

    fn process_simd_batch<F, const SEPARATE_DUPLICATES: bool>(
        &mut self,
        batch: &SequencesLaneBatch,
        preprocess: &[ReadTypeBuffered<CX>],
        scratch: &mut SimdScratch,
        _used_bits: usize,
        _first_bits: usize,
        _second_bits: usize,
        mut push_sequence: F,
    ) where
        F: for<'a, 'b> FnMut(
            PushSequenceInfo<'a, CompressedRead<'b>, DumperMinimizerBucketingExecutorFactory<CX>>,
        ),
    {
        let k = self.global_data.k;
        scratch.destride(batch);
        let scratch = &*scratch;
        for lane in 0..simd_accel::hashing::SIMD_LANES {
            let read = scratch.lane_read(lane);
            for pair in batch.fragments(lane).windows(2) {
                let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
                let preprocess_info = &preprocess[pair[0].record_idx as usize];
                let data = preprocess_info.read_data.as_ref().unwrap();
                // A unitig longer than a lane is split, so its colours have to
                // be taken from where the fragment starts inside the record.
                let first = pair[0].source_start as usize;
                let fragment_colors = data
                    .colors
                    .get_subslice(first..first + (end - start) - k + 1, false);
                let colors = fragment_colors.get_iterator(&preprocess_info.colors_buffer.0);
                self.split_by_colors(
                    read.sub_slice((start..end).into()),
                    colors,
                    &mut push_sequence,
                );
            }
        }
    }
}

impl<CX: ColorsManager> DumperMinimizerBucketingExecutor<CX> {
    /// Cuts a sequence wherever the colour of its k-mers changes.
    fn split_by_colors<
        S: MinimizerInputSequence,
        F: FnMut(PushSequenceInfo<S, DumperMinimizerBucketingExecutorFactory<CX>>),
    >(
        &self,
        sequence: S,
        mut colors: impl Iterator<Item = SingleKmerColorDataType<CX>>,
        mut push_sequence: F,
    ) {
        let mut last_index = 0;
        let mut last_color = colors.next().unwrap();

        for (index, kmer_color) in colors.enumerate() {
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
        });
    }
}

pub fn minimizer_bucketing<CX: ColorsManager>(
    graph_file: PathBuf,
    buckets_count: BucketsCount,
    second_buckets_count: BucketsCount,
    threads_count: usize,
    temp_dir: &Path,
    k: usize,
    m: usize,
    colors_count: u64,
    chunking_size_threshold: Option<u64>,
    target_chunk_size: u64,
) -> Vec<MultiChunkBucket> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: unitigs reorganization".to_string());

    let input_files = vec![MinimzerBucketingFilesReaderInputPacket {
        sequences: (graph_file, None),
        stream_info: (),
    }];

    GenericMinimizerBucketing::do_bucketing::<
        DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
        DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
        DumperMinimizerBucketingExecutorFactory<CX>,
        FastaFileSequencesStream,
    >(
        input_files.into_iter(),
        temp_dir,
        buckets_count,
        second_buckets_count,
        threads_count,
        k,
        m,
        DumperMinimizerBucketingGlobalData {
            colors_count,
            buckets_count_log: buckets_count.normal_buckets_count_log,
        },
        CX::COLORS_ENABLED,
        k,
        chunking_size_threshold,
        target_chunk_size,
        false,
    )
}
