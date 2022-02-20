use crate::assemble_pipeline::links_compaction::LinkMapping;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{color_types, ColorsManager, ColorsMergeManager};
use crate::config::SwapPriority;
use crate::hashes::{HashFunctionFactory, HashableSequence};

use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::concurrent::temp_reads::reads_reader::IntermediateReadsReader;
use crate::io::concurrent::temp_reads::reads_writer::IntermediateReadsWriter;
use crate::io::concurrent::temp_reads::thread_writer::IntermediateReadsThreadWriter;
use crate::io::structs::unitig_link::UnitigIndex;
use crate::utils::Utils;
use crate::KEEP_FILES;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

#[cfg(not(feature = "build-links"))]
use {
    crate::config::DEFAULT_OUTPUT_BUFFER_SIZE,
    crate::io::concurrent::fasta_writer::FastaWriterConcurrentBuffer,
    crate::io::reads_writer::ReadsWriter, crate::io::sequences_reader::FastaSequence,
    bstr::ByteSlice,
};

#[cfg(feature = "build-links")]
use {
    crate::io::concurrent::temp_reads::single_thread_writer::SingleIntermediateReadsThreadWriter,
    parallel_processor::buckets::bucket_type::BucketType,
};

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

#[derive(Clone, Debug)]
pub struct CompletedReadsExtraData<CX: SequenceExtraData> {
    pub color: CX,
}

impl<CX: SequenceExtraData> SequenceExtraData for CompletedReadsExtraData<CX> {
    #[inline(always)]
    fn decode<'a>(mut reader: &'a mut impl Read) -> Option<Self> {
        Some(Self {
            color: CX::decode(&mut reader)?,
        })
    }

    #[inline(always)]
    fn encode<'a>(&self, mut writer: &'a mut impl Write) {
        self.color.encode(&mut writer);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.color.max_size()
    }
}

impl AssemblePipeline {
    pub fn reorganize_reads<MH: HashFunctionFactory, CX: ColorsManager>(
        mut reads: Vec<PathBuf>,
        mut mapping_files: Vec<PathBuf>,
        temp_path: &Path,
        #[cfg(not(feature = "build-links"))] out_file: &Mutex<ReadsWriter>,
        buckets_count: usize,
    ) -> (Vec<PathBuf>, PathBuf) {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: reads reorganization".to_string());

        let mut buckets = MultiThreadBuckets::<
            IntermediateReadsWriter<
                ReorganizedReadsExtraData<color_types::PartialUnitigsColorStructure<MH, CX>>,
            >,
        >::new(
            buckets_count,
            &(
                SwapPriority::ReorganizeReads,
                temp_path.join("reads_bucket"),
            ),
            None,
        );

        #[cfg(feature = "build-links")]
        let final_unitigs_temp_bucket = Mutex::new(IntermediateReadsWriter::<
            CompletedReadsExtraData<color_types::PartialUnitigsColorStructure<MH, CX>>,
        >::new(
            &(
                SwapPriority::ReorganizeReads,
                temp_path.join("reads_bucket_lonely"),
            ),
            0,
        ));

        reads.sort();
        mapping_files.sort();

        let inputs: Vec<_> = reads.iter().zip(mapping_files.iter()).collect();

        inputs.par_iter().for_each(|(read_file, mapping_file)| {
            let mut tmp_reads_buffer = IntermediateReadsThreadWriter::new(buckets_count, &buckets);
            #[cfg(feature = "build-links")]
            let mut tmp_final_reads_buffer =
                SingleIntermediateReadsThreadWriter::new(&final_unitigs_temp_bucket);

            #[cfg(not(feature = "build-links"))]
            let mut tmp_lonely_unitigs_buffer =
                FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE);

            let mut mappings = Vec::new();

            assert_eq!(
                Utils::get_bucket_index(read_file),
                Utils::get_bucket_index(mapping_file)
            );

            let bucket_index = Utils::get_bucket_index(read_file);

            let mut reader = FileReader::open(&mapping_file).unwrap();

            while let Some(link) = LinkMapping::from_stream(&mut reader) {
                mappings.push(link);
            }

            drop(reader);
            MemoryFs::remove_file(&mapping_file, !KEEP_FILES.load(Ordering::Relaxed)).unwrap();

            struct Compare {}
            impl SortKey<LinkMapping> for Compare {
                type KeyType = u64;
                const KEY_BITS: usize = 64;

                fn compare(left: &LinkMapping, right: &LinkMapping) -> std::cmp::Ordering {
                    left.entry.cmp(&right.entry)
                }

                fn get_shifted(value: &LinkMapping, rhs: u8) -> u8 {
                    (value.entry >> rhs) as u8
                }
            }

            fast_smart_radix_sort::<_, Compare, false>(&mut mappings[..]);

            let mut index = 0;
            let mut map_index = 0;

            let mut decompress_buffer = Vec::new();
            #[cfg(not(feature = "build-links"))]
            let mut ident_buffer = Vec::new();

            IntermediateReadsReader::<color_types::PartialUnitigsColorStructure<MH, CX>>::new(
                read_file,
                !KEEP_FILES.load(Ordering::Relaxed),
            )
            .for_each::<_, typenum::U0>(|_, color, seq| {
                if seq.bases_count() > decompress_buffer.len() {
                    decompress_buffer.resize(seq.bases_count(), 0);
                }
                seq.write_to_slice(&mut decompress_buffer[..seq.bases_count()]);

                let seq = &decompress_buffer[..seq.bases_count()];

                if map_index < mappings.len() && mappings[map_index].entry == index {
                    // Mapping found
                    tmp_reads_buffer.add_read::<typenum::U0>(
                        ReorganizedReadsExtraData {
                            unitig: UnitigIndex::new(bucket_index, index as usize, false),
                            color,
                        },
                        seq,
                        mappings[map_index].bucket,
                        0,
                    );
                    map_index += 1;
                } else {
                    #[cfg(not(feature = "build-links"))]
                    {
                        // No mapping, write unitig to file
                        ident_buffer.clear();
                        write!(ident_buffer, "> {} {}", bucket_index, index).unwrap();
                        CX::ColorsMergeManagerType::<MH>::print_color_data(
                            &color,
                            &mut ident_buffer,
                        );

                        tmp_lonely_unitigs_buffer.add_read(FastaSequence {
                            ident: ident_buffer.as_bytes(),
                            seq,
                            qual: None,
                        });
                    }

                    #[cfg(feature = "build-links")]
                    {
                        tmp_final_reads_buffer.add_read::<typenum::U0>(
                            CompletedReadsExtraData { color },
                            seq,
                            0,
                        )
                    }
                }

                color_types::ColorsMergeManagerType::<MH, CX>::clear_deserialized_unitigs_colors();

                index += 1;
            });
            #[cfg(not(feature = "build-links"))]
            tmp_lonely_unitigs_buffer.finalize();

            #[cfg(feature = "build-links")]
            tmp_final_reads_buffer.finalize();

            assert_eq!(map_index, mappings.len())
        });

        let final_unitigs_temp_path = match () {
            #[cfg(feature = "build-links")]
            () => {
                let final_unitigs_temp_bucket = final_unitigs_temp_bucket.into_inner();
                let path = final_unitigs_temp_bucket.get_path();
                final_unitigs_temp_bucket.finalize();
                path
            }
            #[cfg(not(feature = "build-links"))]
            () => PathBuf::new(),
        };

        (buckets.finalize(), final_unitigs_temp_path)
    }
}
