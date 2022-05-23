use crate::colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::colors::default_colors_manager::SingleSequenceInfo;
use crate::config::BucketIndexType;
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::MinimizerHashFunctionFactory;
use crate::io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use crate::io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use crate::pipeline_common::minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
};
use crate::query_pipeline::parallel_kmers_query::QueryKmersReferenceData;
use crate::query_pipeline::QueryPipeline;
use crate::rolling::minqueue::RollingMinQueue;
use crate::FastaSequence;
use byteorder::ReadBytesExt;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    colors_buffer: <MinimizerBucketingSeqColorDataType<CX> as SequenceExtraData>::TempBuffer,
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
            colors_buffer: MinimizerBucketingSeqColorDataType::<CX>::new_temp_buffer(),
            read_type: ReadType::Query(NonZeroU64::new(1).unwrap()),
        }
    }
}

impl SequenceExtraData for KmersQueryData {
    type TempBuffer = ();

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

pub struct QuerierMinimizerBucketingExecutor<H: MinimizerHashFunctionFactory, CX: ColorsManager> {
    minimizer_queue: RollingMinQueue<H>,
    global_data: Arc<MinimizerBucketingCommonData<()>>,
    _phantom: PhantomData<CX>,
}

pub struct QuerierMinimizerBucketingExecutorFactory<
    H: MinimizerHashFunctionFactory,
    CX: ColorsManager,
>(PhantomData<(H, CX)>);

impl<H: MinimizerHashFunctionFactory, CX: ColorsManager> MinimizerBucketingExecutorFactory
    for QuerierMinimizerBucketingExecutorFactory<H, CX>
{
    type GlobalData = ();
    type ExtraData = QueryKmersReferenceData<MinimizerBucketingSeqColorDataType<CX>>;
    type PreprocessInfo = ReadTypeBuffered<CX>;
    type FileInfo = FileType;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U0;

    type ExecutorType = QuerierMinimizerBucketingExecutor<H, CX>;

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

impl<H: MinimizerHashFunctionFactory, CX: ColorsManager>
    MinimizerBucketingExecutor<QuerierMinimizerBucketingExecutorFactory<H, CX>>
    for QuerierMinimizerBucketingExecutor<H, CX>
{
    fn preprocess_fasta(
        &mut self,
        file_info: &<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::FileInfo,
        read_index: u64,
        sequence: &FastaSequence,
        preprocess_info: &mut <QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        MinimizerBucketingSeqColorDataType::<CX>::clear_temp_buffer(
            &mut preprocess_info.color_info_buffer,
        );

        let color = MinimizerBucketingSeqColorDataType::<CX>::create(
            SingleSequenceInfo {
                file_index: 0, // FIXME: Change this to support querying of raw reads
                sequence_ident: sequence.ident,
            },
            &mut preprocess_info.colors_buffer,
        );

        preprocess_info.read_type = match file_info {
            FileType::Graph => ReadType::Graph { color },
            FileType::Query => ReadType::Query(NonZeroU64::new(read_index + 1).unwrap()),
        }
    }

    // FIXME: Implement
    fn reprocess_sequence(
        &mut self,
        _flags: u8,
        _extra_data: &<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData,
        _extra_data_buffer: &<<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData as SequenceExtraData>::TempBuffer,
        _preprocess_info: &mut <QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
    ) {
        todo!()
    }

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(BucketIndexType, BucketIndexType, S, u8, <QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData, &<<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::ExtraData as SequenceExtraData>::TempBuffer),
    >(
        &mut self,
        preprocess_info: &<QuerierMinimizerBucketingExecutorFactory<H, CX> as MinimizerBucketingExecutorFactory>::PreprocessInfo,
        sequence: S,
        _range: Range<usize>,
        mut push_sequence: F,
    ){
        let hashes = H::new(sequence, self.global_data.m);

        let mut rolling_iter = self
            .minimizer_queue
            .make_iter(hashes.iter().map(|x| x.to_unextendable()));

        let mut last_index = 0;
        let mut last_hash = rolling_iter.next().unwrap();

        for (index, min_hash) in rolling_iter.enumerate() {
            if H::get_full_minimizer(min_hash) != H::get_full_minimizer(last_hash) {
                push_sequence(
                    H::get_first_bucket(last_hash) & self.global_data.buckets_count_mask,
                    H::get_second_bucket(last_hash) & self.global_data.buckets_count_mask,
                    sequence.get_subslice(last_index..(index + self.global_data.k)),
                    0,
                    match preprocess_info {
                        ReadType::Graph { color } => QueryKmersReferenceData::Graph(
                            color.get_subslice(last_index..(index + 1)), // FIXME: Check if the subslice is correct,
                        ),

                        ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
                    },
                    &preprocess_info.colors_buffer,
                );

                last_index = index + 1;
                last_hash = min_hash;
            }
        }

        push_sequence(
            H::get_first_bucket(last_hash) & self.global_data.buckets_count_mask,
            H::get_second_bucket(last_hash) & self.global_data.buckets_count_mask,
            sequence.get_subslice(last_index..sequence.seq_len()),
            0,
            match &preprocess_info.read_type {
                ReadType::Graph { color } => {
                    QueryKmersReferenceData::Graph(
                        color.get_subslice(
                            last_index..(sequence.seq_len() + 1 - self.global_data.k),
                        ), // FIXME: Check if the subslice is correct,
                    )
                }

                ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
            },
            &preprocess_info.colors_buffer,
        );
    }
}

impl QueryPipeline {
    pub fn minimizer_bucketing<H: MinimizerHashFunctionFactory, CX: ColorsManager>(
        graph_file: PathBuf,
        query_file: PathBuf,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
    ) -> (Vec<PathBuf>, PathBuf) {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: graph + query bucketing".to_string());

        let input_files = vec![(graph_file, FileType::Graph), (query_file, FileType::Query)];

        GenericMinimizerBucketing::do_bucketing::<QuerierMinimizerBucketingExecutorFactory<H, CX>>(
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
