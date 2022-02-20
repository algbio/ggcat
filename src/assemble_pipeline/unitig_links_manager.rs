use crate::config::{BucketIndexType, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE};
use crate::hashes::HashFunctionFactory;
use crate::io::structs::hash_entry::{HashCompare, HashEntry};
use crate::io::structs::link_connection::LinkConnection;
use crate::io::structs::link_remap::LinkRemap;
use crate::io::structs::unitig_link::UnitigIndex;
use crate::utils::get_memory_mode;
use crate::KEEP_FILES;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::single::SingleBucketThreadDispatcher;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::fast_smart_radix_sort;
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::MemoryFs;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counters {
    index: AtomicUsize,
}

pub struct UnitigLinksManager {
    #[cfg(feature = "build-links")]
    remap_buckets: Option<MultiThreadBuckets<LockFreeBinaryWriter>>,
    links_data: Vec<Counters>,
    prefix_indexes: Vec<usize>,
    final_unitig_indexes_offset: usize,
}

impl UnitigLinksManager {
    pub fn new(buckets_count: usize, temp_dir: PathBuf) -> Self {
        UnitigLinksManager {
            #[cfg(feature = "build-links")]
            remap_buckets: Some(MultiThreadBuckets::new(
                buckets_count,
                &(
                    temp_dir.join("links-manager"),
                    get_memory_mode(SwapPriority::KmersMergeBuckets),
                ),
                None,
            )),
            links_data: (0..buckets_count)
                .map(|_| Counters {
                    index: AtomicUsize::new(0),
                })
                .collect(),
            prefix_indexes: vec![0; buckets_count + 1],
            final_unitig_indexes_offset: 0,
        }
    }

    pub fn build_links<H: HashFunctionFactory>(
        &self,
        temp_dir: PathBuf,
        buckets_count: usize,
        links_buckets: Vec<PathBuf>,
    ) -> Vec<PathBuf> {
        let mut link_pairs_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            &(
                temp_dir.join("link-pairs"),
                get_memory_mode(SwapPriority::LinkPairs),
            ),
            None,
        );

        links_buckets.into_par_iter().for_each(|input| {
            let mut reader = FileReader::open(&input).unwrap();

            let mut thread_link_pairs =
                BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &link_pairs_buckets);

            let mut vec: Vec<HashEntry<H::HashTypeUnextendable>> = Vec::new();

            while let Ok(value) = bincode::deserialize_from(&mut reader) {
                vec.push(value);
            }

            drop(reader);
            MemoryFs::remove_file(&input, !KEEP_FILES.load(Ordering::Relaxed)).unwrap();

            fast_smart_radix_sort::<_, HashCompare<H>, false>(&mut vec[..]);

            for linked in vec.group_by(|a, b| a.hash == b.hash) {
                for (index, first) in linked.iter().enumerate() {
                    for second in linked.iter().skip(index + 1) {
                        thread_link_pairs.add_element(
                            second.bucket,
                            &(),
                            &LinkConnection {
                                source: UnitigIndex::new(first.bucket, first.entry as usize, false),
                                dest: UnitigIndex::new(second.bucket, second.entry as usize, false),
                            },
                        );
                    }
                }
            }
        });

        link_pairs_buckets.finalize()
    }

    pub fn remap_first_pass(&mut self, count: usize) {
        let _remap_buckets = self.remap_buckets.take().unwrap().finalize();

        for i in 0..self.links_data.len() {
            self.prefix_indexes[i + 1] =
                self.prefix_indexes[i] + self.links_data[i].index.load(Ordering::Relaxed);
        }

        // Set the final unitigs offset
        self.final_unitig_indexes_offset = self.prefix_indexes[self.links_data.len()] + count;

        println!(
            "Total reads count: {}",
            self.links_data
                .iter()
                .map(|c| c.index.load(Ordering::Relaxed))
                .sum::<usize>()
                + count
        );
    }

    pub fn remap_second_pass(&mut self) {}

    #[inline(always)]
    pub fn get_unitig_index(&self, bucket: BucketIndexType, index: usize) -> usize {
        self.prefix_indexes[bucket as usize] + index
    }

    #[inline(always)]
    pub fn get_final_unitig_index(&self, index: usize) -> usize {
        self.final_unitig_indexes_offset + index
    }
}

pub struct ThreadUnitigsLinkManager<'a> {
    links_manager: &'a UnitigLinksManager,
    #[cfg(feature = "build-links")]
    thread_bucket: SingleBucketThreadDispatcher<'a, LockFreeBinaryWriter>,
    bucket_index: BucketIndexType,
    counter: usize,
}

impl<'a> ThreadUnitigsLinkManager<'a> {
    pub fn new(links_manager: &'a UnitigLinksManager, bucket: BucketIndexType) -> Self {
        Self {
            links_manager,
            #[cfg(feature = "build-links")]
            thread_bucket: SingleBucketThreadDispatcher::new(
                DEFAULT_PER_CPU_BUFFER_SIZE,
                bucket,
                links_manager.remap_buckets.as_ref().unwrap(),
            ),
            bucket_index: bucket,
            counter: links_manager.links_data[bucket as usize]
                .index
                .load(Ordering::Relaxed),
        }
    }

    #[inline(always)]
    pub fn notify_add_read(
        &mut self,
        first_original_link: UnitigIndex,
        last_original_link: UnitigIndex,
    ) {
        let new_index = self.counter;
        self.counter += 1;

        #[cfg(feature = "build-links")]
        {
            let remap_first = LinkRemap {
                index: first_original_link.index() as u64,
                new_bucket: self.bucket_index,
                new_index: new_index as u64,
                at_beginning: true,
            };

            let remap_last = LinkRemap {
                index: last_original_link.index() as u64,
                new_bucket: self.bucket_index,
                new_index: new_index as u64,
                at_beginning: false,
            };

            self.thread_bucket.add_element(&(), &remap_first);
            self.thread_bucket.add_element(&(), &remap_last);
        }
    }
}

impl<'a> Drop for ThreadUnitigsLinkManager<'a> {
    fn drop(&mut self) {
        self.links_manager.links_data[self.bucket_index as usize]
            .index
            .store(self.counter, Ordering::Relaxed);
    }
}
