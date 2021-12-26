use crate::assemble_pipeline::links_compaction::LinkMapping;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::concurrent::fasta_writer::FastaWriterConcurrentBuffer;
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsReader, IntermediateReadsWriter, IntermediateSequencesStorage,
    SequenceExtraData,
};
use crate::io::reads_reader::ReadsReader;
use crate::io::reads_writer::ReadsWriter;

use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use crate::io::structs::unitig_link::UnitigIndex;
use crate::rolling::minqueue::RollingMinQueue;
use crate::utils::Utils;
use crate::{DEFAULT_BUFFER_SIZE, KEEP_FILES};
use bstr::ByteSlice;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use nix::sys::ptrace::cont;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::fs::File;
use std::intrinsics::unlikely;
use std::io::{Cursor, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, Thread};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ReorganizedReadsExtraData<CX: SequenceExtraData> {
    pub unitig: UnitigIndex,
    pub color: CX,
}

impl<CX: SequenceExtraData> SequenceExtraData for ReorganizedReadsExtraData<CX> {
    #[inline(always)]
    fn decode(mut reader: impl Read) -> Option<Self> {
        Some(Self {
            unitig: UnitigIndex::decode(&mut reader)?,
            color: CX::decode(&mut reader)?,
        })
    }

    #[inline(always)]
    fn encode(&self, mut writer: impl Write) {
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
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: reads reorganization".to_string());

        let mut buckets = MultiThreadBuckets::<
            IntermediateReadsWriter<ReorganizedReadsExtraData<<CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                MH,
                CX,
            >>::PartialUnitigsColorStructure>>,
        >::new(buckets_count, &temp_path.join("reads_bucket"), None);

        reads.sort();
        mapping_files.sort();

        let inputs: Vec<_> = reads.iter().zip(mapping_files.iter()).collect();

        inputs.par_iter().for_each(|(read_file, mapping_file)| {
            let mut tmp_reads_buffer = IntermediateSequencesStorage::new(buckets_count, &buckets);
            let mut tmp_lonely_unitigs_buffer =
                FastaWriterConcurrentBuffer::new(out_file, DEFAULT_BUFFER_SIZE);

            let mut mappings = Vec::new();

            assert_eq!(
                Utils::get_bucket_index(read_file),
                Utils::get_bucket_index(mapping_file)
            );

            let bucket_index = Utils::get_bucket_index(read_file);

            let mappings_file = filebuffer::FileBuffer::open(mapping_file).unwrap();
            let mut reader = Cursor::new(mappings_file.deref());
            while let Some(link) = LinkMapping::from_stream(&mut reader) {
                mappings.push(link);
            }

            drop(mappings_file);
            if !KEEP_FILES.load(Ordering::Relaxed) {
                std::fs::remove_file(mapping_file);
            }

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
            let mut ident_buffer = Vec::new();

            IntermediateReadsReader::<<CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                MH,
                CX,
            >>::PartialUnitigsColorStructure>::new(read_file, !KEEP_FILES.load(Ordering::Relaxed))
                .for_each(|color, seq| {

                    if seq.bases_count() > decompress_buffer.len() {
                        decompress_buffer.resize(seq.bases_count(), 0);
                    }
                    seq.write_to_slice(&mut decompress_buffer[..seq.bases_count()]);

                    let seq = &decompress_buffer[..seq.bases_count()];

                    if map_index < mappings.len() && mappings[map_index].entry == index {
                        // Mapping found
                        tmp_reads_buffer.add_read(ReorganizedReadsExtraData {
                            unitig: UnitigIndex::new(bucket_index, index as usize, false),
                            color
                        },
                            seq,
                            mappings[map_index].bucket,
                        );
                        map_index += 1;
                    } else {

                        ident_buffer.clear();
                        write!(ident_buffer, "> {} {}", bucket_index, index);
                        CX::ColorsMergeManagerType::<MH>::print_color_data(&color, &mut ident_buffer);

                        tmp_lonely_unitigs_buffer.add_read(FastaSequence {
                            ident: ident_buffer.as_bytes(),
                            seq,
                            qual: None,
                        });
                        // No mapping, write unitig to file
                    }

                    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                        MH,
                        CX,
                    >>::clear_deserialized_unitigs_colors();

                    index += 1;
                });
            tmp_lonely_unitigs_buffer.finalize();
            assert_eq!(map_index, mappings.len())
        });
        buckets.finalize()
    }
}
