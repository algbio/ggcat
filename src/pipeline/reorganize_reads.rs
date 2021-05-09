use crate::intermediate_storage::IntermediateReadsWriter;
use crate::multi_thread_buckets::MultiThreadBuckets;
use crate::pipeline::links_compaction::LinkMapping;
use crate::pipeline::Pipeline;
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
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
    pub fn reorganize_reads(
        mut reads: Vec<PathBuf>,
        mut mapping: Vec<PathBuf>,
        output_path: &Path,
        buckets_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        let start_time = Instant::now();
        let mut buckets = MultiThreadBuckets::<IntermediateReadsWriter>::new(
            buckets_count,
            &output_path.join("reads_bucket"),
        );

        reads.sort();
        mapping.sort();

        let inputs: Vec<_> = reads.iter().zip(mapping.iter()).collect();

        inputs.iter().for_each(|(read, mapping)| {
            let mut mappings = Vec::new();

            let mappings_file = filebuffer::FileBuffer::open(mapping).unwrap();
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

            println!("Mappings: {:?}\n", &mappings[0..100]);

            SequencesReader::process_file_extended(read, |seq| {
                println!(
                    "Entry {}/{} / {} => {}",
                    index,
                    mappings.len(),
                    mappings[map_index].entry,
                    map_index < mappings.len() && mappings[map_index].entry == index
                );
                if map_index < mappings.len() && mappings[map_index].entry == index {
                    // Mapping found
                    println!("Incr!");

                    map_index += 1;
                } else {
                    // No mapping, write unitig to file
                }

                index += 1;
            });
            assert_eq!(map_index, mappings.len())
        });

        buckets.finalize()
    }
}
