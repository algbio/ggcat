use crate::intermediate_storage::{IntermediateReadsWriter, IntermediateSequencesStorage};
use crate::multi_thread_buckets::MultiThreadBuckets;
use crate::pipeline::links_compaction::LinkMapping;
use crate::pipeline::Pipeline;
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::unitig_link::UnitigIndex;
use crate::utils::Utils;
use crate::NtHashMinTransform;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use nix::sys::ptrace::cont;
use nthash::NtHashIterator;
use object_pool::Pool;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::fs::File;
use std::intrinsics::unlikely;
use std::io::Cursor;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, Thread};
use std::time::{Duration, Instant};

impl Pipeline {
    pub fn build_unitigs(
        mut reads: Vec<PathBuf>,
        mut mapping_files: Vec<PathBuf>,
        output_path: &Path,
        buckets_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        let start_time = Instant::now();
        let mut buckets = MultiThreadBuckets::<IntermediateReadsWriter<UnitigIndex>>::new(
            buckets_count + 1,
            &output_path.join("reads_bucket"),
        );

        reads.sort();
        mapping_files.sort();

        let inputs: Vec<_> = reads.iter().zip(mapping_files.iter()).collect();

        inputs.par_iter().for_each(|(read_file, mapping_file)| {
            let mut tmp_reads_buffer =
                IntermediateSequencesStorage::new(buckets_count + 1, &buckets);

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

            struct Compare {}
            impl SortKey<LinkMapping> for Compare {
                fn get(value: &LinkMapping) -> u64 {
                    value.entry
                }
            }

            smart_radix_sort::<_, Compare, false>(&mut mappings[..], 64 - 8);

            let mut index = 0;
            let mut map_index = 0;

            SequencesReader::process_file_extended(read_file, |seq| {
                if map_index < mappings.len() && mappings[map_index].entry == index {
                    // Mapping found
                    tmp_reads_buffer.add_read(
                        UnitigIndex::new(bucket_index, index as usize),
                        seq.seq,
                        (mappings[map_index].bucket + 1) as usize,
                    );
                    map_index += 1;
                } else {
                    tmp_reads_buffer.add_read(
                        UnitigIndex::new(bucket_index, index as usize),
                        seq.seq,
                        0,
                    );
                    // No mapping, write unitig to file
                }

                index += 1;
            });
            println!("Total reads: {}/{:?}", index, mappings.last().unwrap());
            assert_eq!(map_index, mappings.len())
        });

        buckets.finalize()
    }
}
