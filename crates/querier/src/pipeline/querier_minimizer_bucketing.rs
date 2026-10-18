use crate::pipeline::parallel_kmers_query::QueryKmersReferenceData;
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use hashes::default::MNHFactory;
use hashes::rolling::batch_minqueue::BatchMinQueue;
use hashes::simd::{HashKernel, SIMD_LANES};
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use hashes::{HashFunction, HashableSequence};
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{SequenceExtraDataTempBufferManagement, TempBuffer};
use io::sequences_stream::SequenceInfo;
use io::sequences_stream::file::FileSequencesStream;
use io::simd::SequenceBuffer;
use minimizer_bucketing::buffer_minimizers::BufferMinimizers;
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
    MinimzerBucketingFilesReaderInputPacket, PushSequenceInfo,
};
use parallel_processor::buckets::{BucketsCount, MultiChunkBucket};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use utils::dna_sequence::{DnaSequence, DnaSequencesFileType};

use super::parallel_kmers_query::RewriteBucketComputeQuery;

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

pub struct ReadTypeBuffered<CX: ColorsManager> {
    colors_buffer: TempBuffer<QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>>,
    read_type: ReadType<CX>,
}

#[derive(Clone)]
pub enum ReadType<CX: ColorsManager> {
    Graph {
        color: MinimizerBucketingSeqColorDataType<CX>,
    },
    Query(NonZeroU64),
}

impl<CX: ColorsManager> Default for ReadTypeBuffered<CX> {
    fn default() -> Self {
        Self {
            colors_buffer:
                QueryKmersReferenceData::<MinimizerBucketingSeqColorDataType<CX>>::new_temp_buffer(),
            read_type: ReadType::Query(NonZeroU64::new(1).unwrap()),
        }
    }
}

pub struct QuerierMinimizerBucketingGlobalData {
    pub queries_count: Arc<AtomicUsize>,
}

pub struct QuerierMinimizerBucketingExecutor<CX: ColorsManager> {
    minimizer_queue: BatchMinQueue<()>,
    /// For whole buffers of input sequences, on windows of a k-mer's m-mers; taken out while
    /// in use.
    buffer_minimizers: Option<BufferMinimizers>,
    global_data: Arc<MinimizerBucketingCommonData<QuerierMinimizerBucketingGlobalData>>,
    _phantom: PhantomData<CX>,
}

pub struct QuerierMinimizerBucketingExecutorFactory<CX: ColorsManager>(PhantomData<CX>);

/// Hands `push_sequence` the super-k-mer of the part of `sequence` from its base `offset` whose
/// k-mers run from `last_index` to `index`, that one included for the last, as the scalar queue's
/// split callback numbers them on that part, with its minimizer's hash `min_hash`.
#[inline(always)]
fn push_superkmer<
    S: MinimizerInputSequence,
    CX: ColorsManager,
    F: FnMut(PushSequenceInfo<S, QuerierMinimizerBucketingExecutorFactory<CX>>),
>(
    k: usize,
    preprocess_info: &ReadTypeBuffered<CX>,
    sequence: S,
    offset: usize,
    (last_index, index): (usize, usize),
    min_hash: u64,
    is_last: bool,
    (used_bits, first_bits, second_bits): (usize, usize, usize),
    push_sequence: &mut F,
) {
    push_sequence(PushSequenceInfo {
        bucket: MNHFactory::get_bucket(used_bits, first_bits, min_hash),
        second_bucket: MNHFactory::get_bucket(used_bits + first_bits, second_bits, min_hash),
        sequence: sequence
            .get_subslice((offset + last_index)..(offset + index + k - !is_last as usize)),
        extra_data: match &preprocess_info.read_type {
            ReadType::Graph { color } => QueryKmersReferenceData::Graph(color.get_subslice(
                (offset + last_index)..(offset + index + is_last as usize),
                false,
            )),

            ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
        },
        temp_buffer: &preprocess_info.colors_buffer,
        minimizer_pos: 0,
        flags: 0,
        rc: false,
    });
}

impl<CX: ColorsManager> MinimizerBucketingExecutorFactory
    for QuerierMinimizerBucketingExecutorFactory<CX>
{
    type GlobalData = QuerierMinimizerBucketingGlobalData;
    type ReadExtraData = QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>;
    type PreprocessInfo = ReadTypeBuffered<CX>;
    type StreamInfo = FileType;

    type RewriteBucketCompute = RewriteBucketComputeQuery;

    type FlagsCount = typenum::U0;

    type ExecutorType = QuerierMinimizerBucketingExecutor<CX>;

    // A query is numbered by its record in the query file, however many sequences it gives.
    const RECORD_INDICES: bool = true;

    fn new(
        global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>,
    ) -> Self::ExecutorType {
        Self::ExecutorType {
            minimizer_queue: BatchMinQueue::new(global_data.k - global_data.m + 1),
            buffer_minimizers: Some(BufferMinimizers::new(
                global_data.k,
                global_data.m,
                global_data.k - global_data.m + 1,
                HashKernel::detect(),
            )),
            global_data: global_data.clone(),
            _phantom: PhantomData,
        }
    }

    fn input_records(global_data: &Self::GlobalData, stream_info: &Self::StreamInfo, records: u64) {
        // Every query, those that give no sequence included, as the counts are kept by index.
        if let FileType::Query = stream_info {
            global_data
                .queries_count
                .store(records as usize, Ordering::Relaxed);
        }
    }
}

impl<CX: ColorsManager> MinimizerBucketingExecutor<QuerierMinimizerBucketingExecutorFactory<CX>>
    for QuerierMinimizerBucketingExecutor<CX>
{
    fn preprocess_dna_sequence(
        &mut self,
        stream_info: &<QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::StreamInfo,
        sequence_info: SequenceInfo,
        read_index: u64,
        sequence: &DnaSequence<'_, CompressedRead<'_>>,
        preprocess_info: &mut <QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        MinimizerBucketingSeqColorDataType::<CX>::clear_temp_buffer(
            &mut preprocess_info.colors_buffer.0,
        );

        preprocess_info.read_type = match stream_info {
            FileType::Graph => {
                let color = MinimizerBucketingSeqColorDataType::<CX>::create(
                    SingleSequenceInfo {
                        static_color: sequence_info.color.unwrap_or(0),
                        sequence_ident: match sequence.format {
                            DnaSequencesFileType::FASTA | DnaSequencesFileType::FASTQ => {
                                SequenceIdent::FASTA(sequence.ident_data)
                            }
                            DnaSequencesFileType::GFA => SequenceIdent::GFA {
                                colors: sequence.ident_data,
                            },
                        },
                    },
                    &mut preprocess_info.colors_buffer.0,
                );

                if CX::COLORS_ENABLED
                    && (color.debug_count() != sequence.seq.bases_count() - self.global_data.k + 1)
                {
                    ggcat_logging::error!(
                        "WARN: Sequence does not have enough colors, please check matching k size:\n{}\n{}",
                        std::str::from_utf8(sequence.ident_data).unwrap(),
                        sequence.seq.debug_to_string()
                    );
                }

                ReadType::Graph { color }
            }
            FileType::Query => ReadType::Query(NonZeroU64::new(read_index + 1).unwrap()),
        }
    }

    // FIXME: Resolve issues
    fn reprocess_sequence(
        &mut self,
        _flags: u8,
        extra_data: &<QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ReadExtraData,
        extra_data_buffer: &TempBuffer<<QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ReadExtraData>,
        preprocess_info: &mut <QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        MinimizerBucketingSeqColorDataType::<CX>::copy_temp_buffer(
            &mut preprocess_info.colors_buffer.0,
            &extra_data_buffer.0,
        );

        preprocess_info.read_type = match extra_data {
            QueryKmersReferenceData::Graph(color) => ReadType::Graph {
                color: color.clone(),
            },
            QueryKmersReferenceData::Query(query) => ReadType::Query(*query),
        }
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(PushSequenceInfo<S, QuerierMinimizerBucketingExecutorFactory<CX>>),
        const SEPARATE_DUPLICATES: bool,
    >(
        &mut self,
        preprocess_info: &<QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        mut push_sequence: F,
    ) {
        let hashes = MNHFactory::new(sequence, self.global_data.m);
        let mut last_index = 0;

        self.minimizer_queue.get_minimizer_splits::<_, false>(
            hashes.iter().map(|x| (x.to_unextendable(), ())),
            0,
            0,
            #[inline(always)]
            |index, min_hash, is_last| {
                push_superkmer(
                    self.global_data.k,
                    preprocess_info,
                    sequence,
                    0,
                    (last_index, index),
                    min_hash.0,
                    is_last,
                    (used_bits, first_bits, second_bits),
                    &mut push_sequence,
                );
                last_index = index;
            },
        );
    }

    /// The input sequences of `buffer` split all at once, by the SIMD minimizer queue on the
    /// whole buffer: each run of k-mers it finds in a part of a sequence is the super-k-mer the
    /// scalar queue's split would give on that part, as the same callback cuts it out, but that it
    /// also splits where the minimizer's position changes. The parts of a sequence in two lanes
    /// overlap by k - 1 bases: each k-mer is in one. Each lane keeps the preprocessing of the
    /// sequence it is in.
    fn process_buffer<
        F: FnMut(PushSequenceInfo<CompressedRead, QuerierMinimizerBucketingExecutorFactory<CX>>),
    >(
        &mut self,
        stream_info: &FileType,
        sequence_info: SequenceInfo,
        buffer: &SequenceBuffer,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        mut push_sequence: F,
    ) {
        let mut minimizers = self
            .buffer_minimizers
            .take()
            .expect("the buffer queue is only taken while in use");
        let k = self.global_data.k;
        let (descriptions, record_indices) = (buffer.descriptions(), buffer.record_indices());
        let mut lane_infos: [ReadTypeBuffered<CX>; SIMD_LANES] = Default::default();
        let mut lane_sequences = [usize::MAX; SIMD_LANES];
        minimizers.runs::<false>(buffer, |run| {
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
            // As the scalar queue's split callback numbers them: the k-mers from the run's first
            // to the next run's, the run's last itself at the part's end.
            let is_last = run.end == run.windows;
            push_superkmer(
                k,
                info,
                sequence.seq,
                run.offset,
                (run.start, run.end - is_last as usize),
                run.value as u64,
                is_last,
                (used_bits, first_bits, second_bits),
                &mut push_sequence,
            );
        });
        self.buffer_minimizers = Some(minimizers);
    }
}

pub fn minimizer_bucketing<CX: ColorsManager>(
    graph_file: PathBuf,
    query_file: PathBuf,
    output_path: &Path,
    buckets_count: BucketsCount,
    second_buckets_count: BucketsCount,
    threads_count: usize,
    k: usize,
    m: usize,
    chunking_size_threshold: Option<u64>,
    target_chunk_size: u64,
) -> (Vec<MultiChunkBucket>, u64) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: graph + query bucketing".to_string());

    let input_files = vec![
        MinimzerBucketingFilesReaderInputPacket {
            sequences: (graph_file, None),
            stream_info: FileType::Graph,
        },
        MinimzerBucketingFilesReaderInputPacket {
            sequences: (query_file, None),
            stream_info: FileType::Query,
        },
    ];

    let queries_count = Arc::new(AtomicUsize::new(0));

    (
        GenericMinimizerBucketing::do_bucketing::<
            QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>,
            QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>,
            QuerierMinimizerBucketingExecutorFactory<CX>,
            FileSequencesStream,
        >(
            input_files.into_iter(),
            output_path,
            buckets_count,
            second_buckets_count,
            threads_count,
            k,
            m,
            QuerierMinimizerBucketingGlobalData {
                queries_count: queries_count.clone(),
            },
            None,
            CX::COLORS_ENABLED,
            chunking_size_threshold,
            target_chunk_size,
            false,
        ),
        queries_count.load(Ordering::Relaxed) as u64,
    )
}
