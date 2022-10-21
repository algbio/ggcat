use config::{
    get_memory_mode, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT,
    INTERMEDIATE_COMPRESSION_LEVEL_FAST, INTERMEDIATE_COMPRESSION_LEVEL_SLOW, KEEP_FILES,
};
use hashes::{HashFunctionFactory, HashableSequence, MinimizerHashFunctionFactory};

use crate::pipeline::build_unitigs::write_fasta_entry;
use crate::structs::link_mapping::LinkMapping;
use colors::colors_manager::{color_types, ColorsManager};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use io::concurrent::structured_sequences::fasta::FastaWriterConcurrentBuffer;
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataOwned, SequenceExtraDataTempBufferManagement,
};
use io::get_bucket_index;
use io::reads_writer::ReadsWriter;
use io::structs::unitig_link::UnitigIndex;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::{
    CompressedBinaryWriter, CompressionLevelInfo,
};
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::{Read, Write};
use std::mem::transmute;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ReorganizedReadsExtraData<CX: SequenceExtraData> {
    pub unitig: UnitigIndex,
    pub color: CX,
}

#[repr(transparent)]
pub struct ReorganizedReadsBuffer<CX: SequenceExtraData>(pub CX::TempBuffer);
impl<CX: SequenceExtraData> ReorganizedReadsBuffer<CX> {
    #[allow(dead_code)]
    pub fn from_inner_mut(inner: &mut CX::TempBuffer) -> &mut Self {
        unsafe { transmute(inner) }
    }
    pub fn from_inner(inner: &CX::TempBuffer) -> &Self {
        unsafe { transmute(inner) }
    }
}

impl<CX: SequenceExtraData> SequenceExtraDataTempBufferManagement<ReorganizedReadsBuffer<CX>>
    for ReorganizedReadsExtraData<CX>
{
    #[inline(always)]
    fn new_temp_buffer() -> ReorganizedReadsBuffer<CX> {
        ReorganizedReadsBuffer(CX::new_temp_buffer())
    }

    #[inline(always)]
    fn clear_temp_buffer(buffer: &mut ReorganizedReadsBuffer<CX>) {
        CX::clear_temp_buffer(&mut buffer.0)
    }

    fn copy_temp_buffer(dest: &mut ReorganizedReadsBuffer<CX>, src: &ReorganizedReadsBuffer<CX>) {
        CX::copy_temp_buffer(&mut dest.0, &src.0)
    }

    #[inline(always)]
    fn copy_extra_from(
        extra: Self,
        src: &ReorganizedReadsBuffer<CX>,
        dst: &mut ReorganizedReadsBuffer<CX>,
    ) -> Self {
        let changed_color = CX::copy_extra_from(extra.color, &src.0, &mut dst.0);
        Self {
            unitig: extra.unitig,
            color: changed_color,
        }
    }
}

impl<CX: SequenceExtraData> SequenceExtraData for ReorganizedReadsExtraData<CX> {
    type TempBuffer = ReorganizedReadsBuffer<CX>;

    #[inline(always)]
    fn decode_extended(buffer: &mut Self::TempBuffer, mut reader: &mut impl Read) -> Option<Self> {
        Some(Self {
            unitig: UnitigIndex::decode(&mut reader)?,
            color: CX::decode_extended(&mut buffer.0, &mut reader)?,
        })
    }

    #[inline(always)]
    fn encode_extended(&self, buffer: &Self::TempBuffer, mut writer: &mut impl Write) {
        self.unitig.encode(&mut writer);
        self.color.encode_extended(&buffer.0, &mut writer);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.unitig.max_size() + self.color.max_size()
    }
}

pub fn reorganize_reads<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
>(
    mut reads: Vec<PathBuf>,
    mut mapping_files: Vec<PathBuf>,
    temp_path: &Path,
    out_file: &Mutex<ReadsWriter>,
    buckets_count: usize,
) -> (Vec<PathBuf>, PathBuf) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: reads reorganization".to_string());

    let buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
        buckets_count,
        temp_path.join("reads_bucket"),
        &(
            get_memory_mode(SwapPriority::ReorganizeReads),
            CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            CompressionLevelInfo {
                fast_disk: INTERMEDIATE_COMPRESSION_LEVEL_FAST.load(Ordering::Relaxed),
                slow_disk: INTERMEDIATE_COMPRESSION_LEVEL_SLOW.load(Ordering::Relaxed),
            },
        ),
    ));

    reads.sort();
    mapping_files.sort();

    let inputs: Vec<_> = reads.iter().zip(mapping_files.iter()).collect();

    let reads_thread_buffers = ScopedThreadLocal::new(move || {
        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
    });

    inputs.par_iter().for_each(|(read_file, mapping_file)| {
        let mut buffers = reads_thread_buffers.get();

        let mut tmp_reads_buffer = BucketsThreadDispatcher::new(&buckets, buffers.take());

        let mut tmp_lonely_unitigs_buffer =
            FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE);

        let mut mappings = Vec::new();

        assert_eq!(get_bucket_index(read_file), get_bucket_index(mapping_file));

        let bucket_index = get_bucket_index(read_file);

        LockFreeBinaryReader::new(
            mapping_file,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<LinkMapping, _>((), &mut (), |link, _| {
            mappings.push(link);
        });

        parallel_processor::make_comparer!(Compare, LinkMapping, entry: u64);
        fast_smart_radix_sort::<_, Compare, false>(&mut mappings[..]);

        let mut index = 0;
        let mut map_index = 0;

        let mut decompress_buffer = Vec::new();

        let mut fasta_temp_buffer = Vec::new();

        let mut colors_buffer =
            color_types::PartialUnitigsColorStructure::<H, MH, CX>::new_temp_buffer();

        CompressedBinaryReader::new(
            read_file,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<CompressedReadsBucketHelper<
            color_types::PartialUnitigsColorStructure<H, MH, CX>,
            typenum::U0,
            false,
        >, _>(
            Vec::new(),
            &mut colors_buffer,
            |(_, _, color, seq), color_buffer| {
                if seq.bases_count() > decompress_buffer.len() {
                    decompress_buffer.resize(seq.bases_count(), 0);
                }
                seq.write_unpacked_to_slice(&mut decompress_buffer[..seq.bases_count()]);

                let seq = &decompress_buffer[..seq.bases_count()];

                if map_index < mappings.len() && mappings[map_index].entry == index {
                    // Mapping found
                    tmp_reads_buffer.add_element_extended(
                        mappings[map_index].bucket,
                        &ReorganizedReadsExtraData {
                            unitig: UnitigIndex::new(bucket_index, index as usize, false),
                            color,
                        },
                        ReorganizedReadsBuffer::from_inner(color_buffer),
                        &CompressedReadsBucketHelper::<
                            ReorganizedReadsExtraData<
                                color_types::PartialUnitigsColorStructure<H, MH, CX>,
                            >,
                            typenum::U0,
                            false,
                        >::new(seq, 0, 0),
                    );
                    map_index += 1;
                } else {
                    // No mapping, write unitig to file
                    write_fasta_entry::<H, MH, CX, _>(
                        &mut fasta_temp_buffer,
                        &mut tmp_lonely_unitigs_buffer,
                        color,
                        color_buffer,
                        seq,
                        0,
                    );
                }

                color_types::PartialUnitigsColorStructure::<H, MH, CX>::clear_temp_buffer(
                    color_buffer,
                );

                index += 1;
            },
        );

        buffers.put_back(tmp_reads_buffer.finalize().0);
        tmp_lonely_unitigs_buffer.finalize();

        assert_eq!(map_index, mappings.len())
    });

    (buckets.finalize(), PathBuf::new())
}
