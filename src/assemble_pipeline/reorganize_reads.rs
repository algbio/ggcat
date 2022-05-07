use crate::assemble_pipeline::links_compaction::LinkMapping;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{color_types, ColorsManager, ColorsMergeManager};
use crate::config::{
    SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT,
};
use crate::hashes::{HashFunctionFactory, HashableSequence};

use crate::assemble_pipeline::build_unitigs::write_fasta_entry;
use crate::config::DEFAULT_OUTPUT_BUFFER_SIZE;
use crate::io::concurrent::fasta_writer::FastaWriterConcurrentBuffer;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::reads_writer::ReadsWriter;
use crate::io::structs::unitig_link::UnitigIndex;
use crate::utils::{get_memory_mode, Utils};
use crate::KEEP_FILES;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ReorganizedReadsExtraData<CX: SequenceExtraData> {
    pub unitig: UnitigIndex,
    pub color: CX,
}

impl<CX: SequenceExtraData> SequenceExtraData for ReorganizedReadsExtraData<CX> {
    #[inline(always)]
    fn decode<'a>(mut reader: &'a mut impl Read) -> Option<Self> {
        Some(Self {
            unitig: UnitigIndex::decode(&mut reader)?,
            color: CX::decode(&mut reader)?,
        })
    }

    #[inline(always)]
    fn encode<'a>(&self, mut writer: &'a mut impl Write) {
        self.unitig.encode(&mut writer);
        self.color.encode(&mut writer);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.unitig.max_size() + self.color.max_size()
    }
}

impl AssemblePipeline {
    pub fn reorganize_reads<MH: HashFunctionFactory, CX: ColorsManager>(
        mut reads: Vec<PathBuf>,
        mut mapping_files: Vec<PathBuf>,
        temp_path: &Path,
        out_file: &Mutex<ReadsWriter>,
        buckets_count: usize,
    ) -> (Vec<PathBuf>, PathBuf) {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: reads reorganization".to_string());

        let mut buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            temp_path.join("reads_bucket"),
            &(
                get_memory_mode(SwapPriority::ReorganizeReads),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
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

            assert_eq!(
                Utils::get_bucket_index(read_file),
                Utils::get_bucket_index(mapping_file)
            );

            let bucket_index = Utils::get_bucket_index(read_file);

            LockFreeBinaryReader::new(
                mapping_file,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            )
            .decode_all_bucket_items::<LinkMapping, _>((), |link| {
                mappings.push(link);
            });

            crate::make_comparer!(Compare, LinkMapping, entry: u64);
            fast_smart_radix_sort::<_, Compare, false>(&mut mappings[..]);

            let mut index = 0;
            let mut map_index = 0;

            let mut decompress_buffer = Vec::new();

            let mut fasta_temp_buffer = Vec::new();

            CompressedBinaryReader::new(
                read_file,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            )
            .decode_all_bucket_items::<CompressedReadsBucketHelper<
                color_types::PartialUnitigsColorStructure<MH, CX>,
                typenum::U0,
                false,
                true,
            >, _>(Vec::new(), |(_, _, color, seq)| {
                if seq.bases_count() > decompress_buffer.len() {
                    decompress_buffer.resize(seq.bases_count(), 0);
                }
                seq.write_unpacked_to_slice(&mut decompress_buffer[..seq.bases_count()]);

                let seq = &decompress_buffer[..seq.bases_count()];

                if map_index < mappings.len() && mappings[map_index].entry == index {
                    // Mapping found
                    tmp_reads_buffer.add_element(
                        mappings[map_index].bucket,
                        &ReorganizedReadsExtraData {
                            unitig: UnitigIndex::new(bucket_index, index as usize, false),
                            color,
                        },
                        &CompressedReadsBucketHelper::<
                            ReorganizedReadsExtraData<
                                color_types::PartialUnitigsColorStructure<MH, CX>,
                            >,
                            typenum::U0,
                            false,
                            true,
                        >::new(seq, 0, 0),
                    );
                    map_index += 1;
                } else {
                    // No mapping, write unitig to file
                    write_fasta_entry::<MH, CX, _>(
                        &mut fasta_temp_buffer,
                        &mut tmp_lonely_unitigs_buffer,
                        color,
                        seq,
                        0,
                    );
                }

                color_types::ColorsMergeManagerType::<MH, CX>::clear_deserialized_unitigs_colors();

                index += 1;
            });

            buffers.put_back(tmp_reads_buffer.finalize().0);
            tmp_lonely_unitigs_buffer.finalize();

            assert_eq!(map_index, mappings.len())
        });

        (buckets.finalize(), PathBuf::new())
    }
}
