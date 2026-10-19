pub mod rewrite_bucket;

use ::dynamic_dispatch::dynamic_dispatch;
use colors::color_assigner::ColorAssigner;
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
use hashes::simd::{HashKernel, SIMD_LANES};
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::TempBuffer;
use io::sequences_stream::SequenceInfo;
use io::sequences_stream::general::{GeneralSequenceBlockData, GeneralSequencesStream};
use io::simd::SequenceBuffer;
use minimizer_bucketing::buffer_minimizers::BufferMinimizers;
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence, process_buffer_by_sequences,
};
use minimizer_bucketing::{MinimzerBucketingFilesReaderInputPacket, PushSequenceInfo};
use parallel_processor::buckets::{BucketsCount, MultiChunkBucket};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use utils::dna_sequence::{DnaSequence, DnaSequencesFileType};

/// A minimizer's m-mer: its index in the sequence, shifted left by one, with bit 0 set if its
/// forward strand is the canonical one, as the SIMD minimizer queue's extras hold it
/// (`hashes::simd::minqueue::MinimizerLanes::extra`). The index is kept modulo 2^31.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct MinimizerExtraData(u32);

impl MinimizerExtraData {
    #[inline(always)]
    fn new(index: usize, is_forward: bool) -> Self {
        Self((index as u32) << 1 | is_forward as u32)
    }

    #[inline(always)]
    fn index(self) -> u32 {
        self.0 >> 1
    }

    #[inline(always)]
    fn is_forward(self) -> bool {
        self.0 & 1 != 0
    }
}

pub struct AssemblerMinimizerBucketingExecutor<CD: MinimizerBucketingSeqColorData> {
    minimizer_queue: BatchMinQueue<MinimizerExtraData>,
    /// For whole buffers of input sequences; none for windows of a single m-mer.
    buffer_minimizers: Option<BufferMinimizers>,
    global_data: Arc<MinimizerBucketingCommonData<()>>,
    pub duplicates_bucket: BucketIndexType,
    _phantom: PhantomData<CD>,
    canonical: bool,
}

pub struct AssemblerPreprocessInfo<CD: MinimizerBucketingSeqColorData> {
    color_info: CD,
    color_info_buffer: TempBuffer<CD>,
    include_first: bool,
    include_last: bool,
}

impl<CD: MinimizerBucketingSeqColorData> Default for AssemblerPreprocessInfo<CD> {
    fn default() -> Self {
        Self {
            color_info: CD::default(),
            color_info_buffer: CD::new_temp_buffer(),
            include_first: false,
            include_last: false,
        }
    }
}

/// The whole-buffer SIMD queue for super-k-mers of `k`-mers with minimizers of `m` bases: their
/// windows of `k - m` m-mers, a k-mer less a base, two at least.
fn buffer_minimizers(k: usize, m: usize) -> Option<BufferMinimizers> {
    (k - m >= 2).then(|| BufferMinimizers::new(k, m, k - m, HashKernel::detect()))
}

#[inline]
#[cold]
fn cold() {}

/// What cutting a super-k-mer out of a sequence takes from the executor.
#[derive(Copy, Clone)]
struct SuperkmerFormat {
    k: usize,
    m: usize,
    canonical: bool,
    duplicates_bucket: BucketIndexType,
}

impl SuperkmerFormat {
    /// Hands `push_sequence` the super-k-mer of the part of `sequence` from its base `offset`
    /// whose windows, of k - 1 bases, run from `last_index - 1` to `index`, as the scalar queue's
    /// split callback numbers them on that part, with its minimizer `min_hash`, its m-mer counted
    /// from the part's start, and whether it includes the first and the last k-mer it holds.
    #[inline(always)]
    fn push<
        S: MinimizerInputSequence,
        CD: MinimizerBucketingSeqColorData,
        F: FnMut(PushSequenceInfo<S, AssemblerMinimizerBucketingExecutorFactory<CD>>),
        const SEPARATE_DUPLICATES: bool,
    >(
        self,
        preprocess_info: &AssemblerPreprocessInfo<CD>,
        sequence: S,
        offset: usize,
        (last_index, index): (usize, usize),
        min_hash: (u64, MinimizerExtraData),
        (include_first, include_last): (bool, bool),
        (used_bits, first_bits, second_bits): (usize, usize, usize),
        push_sequence: &mut F,
    ) {
        // HS2
        let (bucket, rc, minimizer_pos) = if SEPARATE_DUPLICATES
            && (min_hash.0
                & BatchMinQueue::<MinimizerExtraData>::unique_flag::<SEPARATE_DUPLICATES>()
                == 0)
        {
            cold();
            (self.duplicates_bucket, false, 0)
        } else {
            let rc = self.canonical && SEPARATE_DUPLICATES && !min_hash.1.is_forward();
            (
                MNHFactory::get_bucket(used_bits, first_bits, min_hash.0),
                rc,
                if rc {
                    (index + self.k - 1 - (min_hash.1.index() as usize) - self.m) as u16
                } else {
                    (min_hash.1.index() - (last_index as u32 - 1)) as u16
                },
            )
        };

        push_sequence(PushSequenceInfo {
            bucket,
            second_bucket: MNHFactory::get_bucket(used_bits + first_bits, second_bits, min_hash.0),
            sequence: sequence
                .get_subslice((offset + last_index - 1)..(offset + index + self.k - 1)),
            extra_data: preprocess_info
                .color_info
                .get_subslice((offset + last_index - 1)..(offset + index), rc), // FIXME: Check if the subslice is correct
            temp_buffer: &preprocess_info.color_info_buffer,
            minimizer_pos,
            flags: ((include_first as u8) << (rc as u8)) | ((include_last as u8) << (!rc as u8)),
            rc,
        });
    }
}

impl<CD: MinimizerBucketingSeqColorData> AssemblerMinimizerBucketingExecutor<CD> {
    fn superkmer_format(&self) -> SuperkmerFormat {
        SuperkmerFormat {
            k: self.global_data.k,
            m: self.global_data.m,
            canonical: self.canonical,
            duplicates_bucket: self.duplicates_bucket,
        }
    }
}

#[derive(Clone, Default)]
pub struct InputFileInfo {
    /// The colour of the input's sequences that come with none.
    file_color: ColorIndexType,
    /// The input's index in the inputs of `colors`, which gives the colours of its files as
    /// they start, when building a coloured graph.
    input: usize,
    colors: Option<Arc<ColorAssigner>>,
}

pub trait CanonicalMode {
    const CANONICAL: bool;
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
            buffer_minimizers: buffer_minimizers(global_data.k, global_data.m),
            global_data: global_data.clone(),
            duplicates_bucket: global_data.buckets_count.normal_buckets_count as u16,
            canonical: global_data.canonical,
            _phantom: PhantomData,
        }
    }

    fn file_info(
        _global_data: &Self::GlobalData,
        stream_info: &Self::StreamInfo,
        name: &str,
        info: SequenceInfo,
    ) -> SequenceInfo {
        match &stream_info.colors {
            Some(colors) => SequenceInfo {
                color: Some(colors.file_color(stream_info.input, name)),
            },
            None => info,
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
            buffer_minimizers: buffer_minimizers(global_data.k, global_data.m),
            global_data: global_data.clone(),
            duplicates_bucket,
            canonical: global_data.canonical,
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
        sequence: &DnaSequence<CompressedRead>,
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
        extra_data_buffer: &TempBuffer<CD>,
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

        let format = self.superkmer_format();
        self.minimizer_queue
            .get_minimizer_splits::<_, SEPARATE_DUPLICATES>(
                hashes.iter_enumerate().map(|(index, x)| {
                    (
                        x.to_unextendable()
            // Set the unique flag if the minimizer is not rc_symmetric
            | (SEPARATE_DUPLICATES && !x.is_rc_symmetric()) as u64,
                        MinimizerExtraData::new(index, x.is_forward()),
                    )
                }),
                skip_before,
                skip_after,
                #[inline(always)]
                |index, min_hash, is_last| {
                    let include_last = preprocess_info.include_last && is_last;
                    format.push::<_, _, _, SEPARATE_DUPLICATES>(
                        preprocess_info,
                        sequence,
                        0,
                        (last_index, index),
                        min_hash,
                        (include_first, include_last),
                        (used_bits, first_bits, second_bits),
                        &mut push_sequence,
                    );
                    last_index = index;
                    include_first = false;
                },
            );
    }

    /// The input sequences of `buffer` split all at once, by the SIMD minimizer queue on the
    /// whole buffer: each run of windows it finds in a part of a sequence is the super-k-mer the
    /// scalar queue's split would give on that part, as the same callback cuts it out. The parts
    /// of a sequence in two lanes overlap by k - 1 bases, as the parts of one too long for a
    /// buffer do: each k-mer is in one. Each lane keeps the preprocessing of the sequence it is
    /// in.
    fn process_buffer<
        F: FnMut(PushSequenceInfo<CompressedRead, AssemblerMinimizerBucketingExecutorFactory<CD>>),
    >(
        &mut self,
        stream_info: &InputFileInfo,
        sequence_info: SequenceInfo,
        buffer: &SequenceBuffer,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        mut push_sequence: F,
    ) {
        let Some(mut minimizers) = self.buffer_minimizers.take() else {
            process_buffer_by_sequences(
                self,
                stream_info,
                sequence_info,
                buffer,
                used_bits,
                first_bits,
                second_bits,
                push_sequence,
            );
            return;
        };
        let format = self.superkmer_format();
        let (descriptions, record_indices) = (buffer.descriptions(), buffer.record_indices());
        let mut lane_infos: [AssemblerPreprocessInfo<CD>; SIMD_LANES] = Default::default();
        let mut lane_sequences = [usize::MAX; SIMD_LANES];
        minimizers.runs::<true>(buffer, |run| {
            let simd_sequence = buffer.sequence(&descriptions[run.sequence]);
            let sequence = DnaSequence {
                ident_data: simd_sequence.ident_data,
                seq: CompressedRead::from(simd_sequence.seq),
                format: simd_sequence.format,
            };
            let info = &mut lane_infos[run.lane];
            if lane_sequences[run.lane] != run.sequence {
                self.preprocess_dna_sequence(
                    stream_info,
                    sequence_info,
                    record_indices.get(run.sequence).copied().unwrap_or(0),
                    &sequence,
                    info,
                );
                lane_sequences[run.lane] = run.sequence;
            }
            // Input sequences hold all their k-mers: no m-mer to skip at either end.
            debug_assert!(info.include_first && info.include_last);
            // As the scalar queue's split callback numbers them: the windows from the one before
            // the run, but at the part's start, to the one after, but at its end.
            let is_last = run.end == run.windows;
            let (last_index, index) = (run.start.max(1), run.end - is_last as usize);
            format.push::<_, _, _, true>(
                info,
                sequence.seq,
                run.offset,
                (last_index, index),
                (
                    run.value as u64,
                    MinimizerExtraData::new(run.position, run.is_forward),
                ),
                (run.start == 0, is_last),
                (used_bits, first_bits, second_bits),
                &mut push_sequence,
            );
        });
        self.buffer_minimizers = Some(minimizers);
    }
}

/// Splits the sequences of `input_blocks`, of the estimated `input_sizes`, into the buckets
/// of their minimizers. For a coloured graph, `colors` gives the colours of the inputs' files,
/// archives' members each as it starts: the inputs are those it gives, in its order.
#[dynamic_dispatch(H = [
    hashes::cn_nthash::CanonicalNtHashIteratorFactory,
], CX = [
    #[cfg(feature = "enable-colors")] colors::bundles::multifile_building::ColorBundleMultifileBuilding,
    colors::non_colored::NonColoredManager,
])]
pub fn minimizer_bucketing<CX: ColorsManager>(
    input_blocks: Vec<GeneralSequenceBlockData>,
    input_sizes: Vec<u64>,
    colors: Option<Arc<ColorAssigner>>,
    output_path: &Path,
    buckets_count: BucketsCount,
    second_buckets_count: BucketsCount,
    threads_count: usize,
    k: usize,
    m: usize,
    chunking_size_threshold: Option<u64>,
    target_chunk_size: u64,
    forward_only: bool,
) -> Vec<MultiChunkBucket> {
    MNHFactory::initialize(k);

    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: reads bucketing".to_string());

    assert_eq!(input_blocks.len(), input_sizes.len());
    // Largest inputs first, by the sizes the assembler already estimated.
    let mut input_files: Vec<_> = input_blocks
        .into_iter()
        .zip(input_sizes)
        .enumerate()
        .map(|(i, (sequences, size))| {
            (
                size,
                MinimzerBucketingFilesReaderInputPacket {
                    sequences,
                    stream_info: InputFileInfo {
                        file_color: colors
                            .as_ref()
                            .map_or(i as ColorIndexType, |colors| colors.input_color(i)),
                        input: i,
                        colors: colors.clone(),
                    },
                },
            )
        })
        .collect();
    input_files.sort_by_key(|(size, _)| *size);
    input_files.reverse();

    GenericMinimizerBucketing::do_bucketing::<
        MinimizerBucketingSeqColorDataType<CX>,
        MinimizerBucketingMultipleSeqColorDataType<CX>,
        AssemblerMinimizerBucketingExecutorFactory<MinimizerBucketingSeqColorDataType<CX>>,
        GeneralSequencesStream,
    >(
        input_files.into_iter().map(|(_, packet)| packet),
        output_path,
        buckets_count,
        second_buckets_count,
        threads_count,
        k,
        m,
        (),
        Some(k - 1),
        false,
        chunking_size_threshold,
        target_chunk_size,
        forward_only,
    )
}
