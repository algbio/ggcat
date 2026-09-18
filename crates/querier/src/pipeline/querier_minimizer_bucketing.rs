use crate::pipeline::parallel_kmers_query::QueryKmersReferenceData;
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use hashes::HashFunction;
use hashes::default::{MNHFactory, MinimizerHashFactory};
use hashes::rolling::batch_minqueue::BatchMinQueue;
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{SequenceExtraDataTempBufferManagement, TempBuffer};
use io::sequences_reader::DnaSequencesFileType;
use io::sequences_stream::SequenceInfo;
use io::sequences_stream::fasta::FastaFileSequencesStream;
use minimizer_bucketing::lane_runs::{LaneRunResolver, SimdScratch, build_valid_lanes};
use minimizer_bucketing::simd_batch::{RecordInfo, SequencesLaneBatch};
use minimizer_bucketing::{
    GenericMinimizerBucketing, MinimizerBucketingCommonData, MinimizerBucketingExecutor,
    MinimizerBucketingExecutorFactory, MinimizerInputSequence,
    MinimzerBucketingFilesReaderInputPacket, PushSequenceInfo,
};
use parallel_processor::buckets::{BucketsCount, MultiChunkBucket};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use simd_accel::hashing::{SIMD_LANES, canonical_hash_items};
use simd_accel::minimizer::SimdBatchMinQueue;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    simd_queue: SimdBatchMinQueue<()>,
    global_data: Arc<MinimizerBucketingCommonData<QuerierMinimizerBucketingGlobalData>>,
    _phantom: PhantomData<CX>,
}

pub struct QuerierMinimizerBucketingExecutorFactory<CX: ColorsManager>(PhantomData<CX>);

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

    fn new(
        global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>,
    ) -> Self::ExecutorType {
        Self::ExecutorType {
            minimizer_queue: BatchMinQueue::new(global_data.k - global_data.m + 1),
            simd_queue: SimdBatchMinQueue::new(global_data.k - global_data.m + 1),
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
        record: &RecordInfo<'_>,
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
                push_sequence(PushSequenceInfo {
                    bucket: MNHFactory::get_bucket(used_bits, first_bits, min_hash.0),
                    second_bucket: MNHFactory::get_bucket(
                        used_bits + first_bits,
                        second_bits,
                        min_hash.0,
                    ),
                    sequence: sequence
                        .get_subslice(last_index..(index + self.global_data.k - !is_last as usize)),
                    extra_data: match &preprocess_info.read_type {
                        ReadType::Graph { color } => QueryKmersReferenceData::Graph(
                            color.get_subslice(last_index..index + is_last as usize, false),
                        ),

                        ReadType::Query(val) => QueryKmersReferenceData::Query(*val),
                    },
                    temp_buffer: &preprocess_info.colors_buffer,
                    minimizer_pos: 0,
                    flags: 0,
                    rc: false,
                });

                last_index = index;
            },
        );
    }

    fn process_simd_batch<F, const SEPARATE_DUPLICATES: bool>(
        &mut self,
        batch: &SequencesLaneBatch,
        preprocess: &[ReadTypeBuffered<CX>],
        scratch: &mut SimdScratch,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        mut push_sequence: F,
    ) where
        F: for<'a, 'b> FnMut(
            PushSequenceInfo<'a, CompressedRead<'b>, QuerierMinimizerBucketingExecutorFactory<CX>>,
        ),
    {
        let (k, m) = (self.global_data.k, self.global_data.m);

        // Here a window is a whole k-mer, so a super-k-mer needs no extra base.
        build_valid_lanes(&mut scratch.valid_lanes, batch, k);
        scratch.destride(batch);
        let scratch = &*scratch;

        let mut resolvers: [LaneRunResolver; SIMD_LANES] =
            std::array::from_fn(|lane| LaneRunResolver::new(batch.fragments(lane)));

        let hashes = canonical_hash_items::<false>(batch.packed_sequence(), m)
            .expect("the lanes are shorter than the minimizer");

        self.simd_queue.get_valid_minimizer_splits::<_, false>(
            hashes,
            &scratch.valid_lanes,
            #[inline(always)]
            |run| {
                let resolved =
                    resolvers[run.lane].resolve(run.start, run.end, run.finished, k, false);
                let preprocess_info = &preprocess[resolved.record_idx as usize];
                let hash = run.hash as u64;
                push_sequence(PushSequenceInfo {
                    bucket: MinimizerHashFactory::get_bucket(used_bits, first_bits, hash),
                    second_bucket: MinimizerHashFactory::get_bucket(
                        used_bits + first_bits,
                        second_bits,
                        hash,
                    ),
                    sequence: scratch
                        .lane_read(run.lane)
                        .sub_slice((resolved.start..resolved.end).into()),
                    extra_data: match &preprocess_info.read_type {
                        ReadType::Graph { color } => QueryKmersReferenceData::Graph(
                            // Colours are indexed by k-mer inside the whole
                            // record, which a lane fragment is only a part of.
                            color.get_subslice(
                                resolved.source_index(resolved.start)
                                    ..resolved.source_index(run.end),
                                false,
                            ),
                        ),
                        ReadType::Query(value) => QueryKmersReferenceData::Query(*value),
                    },
                    temp_buffer: &preprocess_info.colors_buffer,
                    minimizer_pos: 0,
                    flags: 0,
                    rc: false,
                });
            },
        );
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
            FastaFileSequencesStream,
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
            CX::COLORS_ENABLED,
            0,
            chunking_size_threshold,
            target_chunk_size,
            false,
        ),
        queries_count.load(Ordering::Relaxed) as u64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use colors::bundles::graph_querying::ColorBundleGraphQuerying;
    use colors::colors_manager::MinimizerBucketingSeqColorDataIterable;
    use io::compressed_read::CompressedRead;
    use minimizer_bucketing::lane_runs::SimdScratch;
    use minimizer_bucketing::simd_batch::RecordExtra;
    use parallel_processor::buckets::ExtraBuckets;
    use simd_accel::batch::VecBatchSink;
    use simd_accel::packer::LanePacker;
    use std::sync::atomic::AtomicUsize;

    type CX = ColorBundleGraphQuerying;
    type TestFactory = QuerierMinimizerBucketingExecutorFactory<CX>;

    /// One emitted super-k-mer with the colours of each of its k-mers.
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct Emitted {
        bases: String,
        colors: Vec<u32>,
    }

    fn graph_record(k: usize) -> (Vec<u8>, Vec<u8>, Vec<u32>) {
        let mut state = 0x5eed_1234u32;
        let sequence: Vec<u8> = (0..600)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                b"ACGT"[(state >> 29) as usize & 3]
            })
            .collect();
        let kmers = sequence.len() - k + 1;
        // Colour runs of uneven length, as a real graph has.
        let runs = [(3u32, 40usize), (9, 7), (1, 200), (4, 1), (7, kmers)];
        let mut header = format!(">0 LN:i:{}", sequence.len());
        let mut colors = Vec::new();
        for (color, count) in runs {
            if colors.len() >= kmers {
                break;
            }
            let count = count.min(kmers - colors.len());
            header.push_str(&format!(" C:{color}:{count}"));
            colors.extend(std::iter::repeat_n(color, count));
        }
        assert_eq!(colors.len(), kmers);
        (header.into_bytes(), sequence, colors)
    }

    fn common(
        k: usize,
        m: usize,
    ) -> Arc<MinimizerBucketingCommonData<QuerierMinimizerBucketingGlobalData>> {
        Arc::new(MinimizerBucketingCommonData::new(
            k,
            m,
            BucketsCount::new(4, ExtraBuckets::None),
            0,
            BucketsCount::new(3, ExtraBuckets::None),
            QuerierMinimizerBucketingGlobalData {
                queries_count: Arc::new(AtomicUsize::new(0)),
            },
            false,
        ))
    }

    fn colors_of(info: &PushSequenceInfo<'_, CompressedRead<'_>, TestFactory>) -> Vec<u32> {
        match &info.extra_data {
            QueryKmersReferenceData::Graph(color) => {
                MinimizerBucketingSeqColorDataIterable::<'_, u32>::get_iterator(
                    color,
                    &info.temp_buffer.0,
                )
                .collect()
            }
            QueryKmersReferenceData::Query(_) => Vec::new(),
        }
    }

    #[test]
    fn every_graph_kmer_keeps_its_colour() {
        const K: usize = 31;
        const M: usize = 12;
        let global = common(K, M);
        let (header, sequence, expected_colors) = graph_record(K);
        let mut executor = TestFactory::new(&global);

        // Ordinary path, over the whole record at once.
        let mut preprocess = ReadTypeBuffered::<CX>::default();
        executor.preprocess_dna_sequence(
            &FileType::Graph,
            SequenceInfo { color: None },
            0,
            &RecordInfo {
                ident_data: &header,
                format: DnaSequencesFileType::FASTA,
                bases_count: sequence.len(),
            },
            &mut preprocess,
        );
        let mut packed = Vec::new();
        CompressedRead::compress_from_plain(&sequence, |part| packed.extend_from_slice(part));
        let read = CompressedRead::new_from_compressed(&packed, sequence.len());
        let mut ordinary = Vec::new();
        executor.process_sequence::<_, _, true>(
            &preprocess,
            read,
            0..sequence.len(),
            0,
            4,
            3,
            |info| {
                ordinary.push(Emitted {
                    bases: info.sequence.debug_to_string(),
                    colors: colors_of(&info),
                })
            },
        );

        // Vectorized path, over a parsed batch.
        let mut packer = LanePacker::<RecordExtra>::new(K, 1024, 0, true, 1 << 20).unwrap();
        let mut sink = VecBatchSink::new(1024);
        packer.push_record(
            &mut sink,
            (SequenceInfo { color: None }, DnaSequencesFileType::FASTA),
            &header,
            &sequence,
            false,
        );
        packer.finish(&mut sink);
        let mut scratch = SimdScratch::default();
        let mut vectorized = Vec::new();
        for batch in &sink.batches {
            let mut infos: Vec<ReadTypeBuffered<CX>> = (0..batch.records.len())
                .map(|_| Default::default())
                .collect();
            for (index, record) in batch.records.iter().enumerate() {
                executor.preprocess_dna_sequence(
                    &FileType::Graph,
                    record.extra.0,
                    record.read_index,
                    &RecordInfo {
                        ident_data: batch.header(index),
                        format: record.extra.1,
                        bases_count: record.bases_total as usize,
                    },
                    &mut infos[index],
                );
            }
            executor.process_simd_batch::<_, true>(batch, &infos, &mut scratch, 0, 4, 3, |info| {
                vectorized.push(Emitted {
                    bases: info.sequence.debug_to_string(),
                    colors: colors_of(&info),
                })
            });
        }

        // The two paths use different minimizer hashes, so they cut the
        // record in different places. What has to hold either way is that
        // every k-mer is covered exactly once, in order, with the colour the
        // header gave it, and that the bases come from the record.
        let text = String::from_utf8(sequence.clone()).unwrap();
        for (name, emitted) in [("ordinary", &ordinary), ("vectorized", &vectorized)] {
            let mut colors = Vec::new();
            let mut rebuilt = String::new();
            for item in emitted.iter() {
                assert_eq!(
                    item.colors.len(),
                    item.bases.len() - K + 1,
                    "{name}: one colour per k-mer"
                );
                colors.extend(item.colors.iter().copied());
                if rebuilt.is_empty() {
                    rebuilt.push_str(&item.bases);
                } else {
                    assert!(
                        rebuilt.ends_with(&item.bases[..K - 1]),
                        "{name}: super-k-mers do not follow each other"
                    );
                    rebuilt.push_str(&item.bases[K - 1..]);
                }
            }
            assert_eq!(colors, expected_colors, "{name}: colours");
            assert_eq!(rebuilt, text, "{name}: covered bases");
        }
    }
}
