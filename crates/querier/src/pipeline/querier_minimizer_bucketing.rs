use crate::pipeline::parallel_kmers_query::QueryKmersReferenceData;
use byteorder::ReadBytesExt;
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use config::BucketIndexType;
use hashes::default::MNHFactory;
use hashes::rolling::batch_minqueue::BatchMinQueue;
use hashes::HashFunction;
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_stream::fasta::FastaFileSequencesStream;
use io::sequences_stream::SequenceInfo;
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence, PushSequenceInfo,
};
use parallel_processor::buckets::SingleBucket;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::parallel_kmers_query::RewriteBucketComputeQuery;

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

pub struct ReadTypeBuffered<CX: ColorsManager> {
    colors_buffer: <QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
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

impl HasEmptyExtraBuffer for KmersQueryData {}
impl SequenceExtraData for KmersQueryData {
    #[inline(always)]
    fn decode_extended(_: &mut (), reader: &mut impl Read) -> Option<Self> {
        Some(Self(decode_varint(|| reader.read_u8().ok())?))
    }

    #[inline(always)]
    fn encode_extended(&self, _: &(), writer: &mut impl Write) {
        encode_varint(|b| writer.write_all(b), self.0).unwrap();
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE
    }
}

pub struct QuerierMinimizerBucketingGlobalData {
    pub queries_count: Arc<AtomicUsize>,
}

pub struct QuerierMinimizerBucketingExecutor<CX: ColorsManager> {
    minimizer_queue: BatchMinQueue<()>,
    global_data: Arc<MinimizerBucketingCommonData<QuerierMinimizerBucketingGlobalData>>,
    _phantom: PhantomData<CX>,
}

pub struct QuerierMinimizerBucketingExecutorFactory<CX: ColorsManager>(PhantomData<CX>);

impl<CX: ColorsManager> MinimizerBucketingExecutorFactory
    for QuerierMinimizerBucketingExecutorFactory<CX>
{
    type GlobalData = QuerierMinimizerBucketingGlobalData;
    type ExtraData = QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>;
    type PreprocessInfo = ReadTypeBuffered<CX>;
    type StreamInfo = FileType;

    type ColorsManager = CX;
    type RewriteBucketCompute = RewriteBucketComputeQuery;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;

    type ExecutorType = QuerierMinimizerBucketingExecutor<CX>;

    fn new(
        global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>,
    ) -> Self::ExecutorType {
        Self::ExecutorType {
            minimizer_queue: BatchMinQueue::new(global_data.k - global_data.m + 1),
            global_data: global_data.clone(),
            _phantom: PhantomData,
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
        sequence: &DnaSequence,
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

                ReadType::Graph { color }
            }
            FileType::Query => {
                self.global_data
                    .global_data
                    .queries_count
                    .fetch_add(1, Ordering::Relaxed);
                ReadType::Query(NonZeroU64::new(read_index + 1).unwrap())
            }
        }
    }

    // FIXME: Resolve issues
    fn reprocess_sequence(
        &mut self,
        _flags: u8,
        extra_data: &<QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData,
        extra_data_buffer: &<<QuerierMinimizerBucketingExecutorFactory<CX> as MinimizerBucketingExecutorFactory>::ExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
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
            |index, min_hash, _, _| {
                push_sequence(PushSequenceInfo {
                    bucket: MNHFactory::get_bucket(used_bits, first_bits, min_hash.0),
                    second_bucket: MNHFactory::get_bucket(
                        used_bits + first_bits,
                        second_bits,
                        min_hash.0,
                    ),
                    sequence: sequence.get_subslice(last_index..(index + self.global_data.k)),
                    extra_data: match &preprocess_info.read_type {
                        ReadType::Graph { color } => QueryKmersReferenceData::Graph(
                            color.get_subslice(last_index..index, false),
                        ),

                        ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
                    },
                    temp_buffer: &preprocess_info.colors_buffer,
                    flags: 0,
                    rc: false,
                });

                last_index = index;
            },
        );
    }
}

pub fn minimizer_bucketing<CX: ColorsManager>(
    graph_file: PathBuf,
    query_file: PathBuf,
    output_path: &Path,
    buckets_count: usize,
    threads_count: usize,
    k: usize,
    m: usize,
) -> ((Vec<SingleBucket>, PathBuf), u64) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: graph + query bucketing".to_string());

    let input_files = vec![
        ((graph_file, None), FileType::Graph),
        ((query_file, None), FileType::Query),
    ];

    let queries_count = Arc::new(AtomicUsize::new(0));

    (
        GenericMinimizerBucketing::do_bucketing_no_max_usage::<
            QuerierMinimizerBucketingExecutorFactory<CX>,
            FastaFileSequencesStream,
        >(
            input_files.into_iter(),
            output_path,
            buckets_count,
            threads_count,
            k,
            m,
            QuerierMinimizerBucketingGlobalData {
                queries_count: queries_count.clone(),
            },
            None,
            CX::COLORS_ENABLED,
            0,
        ),
        queries_count.load(Ordering::Relaxed) as u64,
    )
}
