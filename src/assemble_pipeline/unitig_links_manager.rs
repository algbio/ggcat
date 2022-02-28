use crate::assemble_pipeline::reorganize_reads::CompletedReadsExtraData;
use crate::colors::colors_manager::{color_types, ColorsManager};
use crate::config::{BucketIndexType, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE};
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::concurrent::temp_reads::reads_reader::IntermediateReadsReader;
use crate::io::structs::edge_hash_entry::{EdgeHashCompare, EdgeHashEntry};
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::link_connection::LinkConnection;
use crate::io::structs::link_remap::LinkRemap;
use crate::io::structs::unitig_link::UnitigIndex;
use crate::utils::{get_memory_mode, Utils};
use hashbrown::HashMap;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::fast_smart_radix_sort;
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counters {
    index: AtomicUsize,
}

pub struct UnitigLinksManager {
    #[cfg(feature = "build-links")]
    remap_buckets: Option<MultiThreadBuckets<LockFreeBinaryWriter>>,
    remap_buckets_paths: Vec<PathBuf>,
    links_data: Vec<Counters>,
    prefix_indexes: Vec<usize>,
    final_unitig_indexes_offset: usize,
}

impl UnitigLinksManager {
    pub fn new(buckets_count: usize, #[cfg(feature = "build-links")] temp_dir: PathBuf) -> Self {
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
            remap_buckets_paths: vec![],
            links_data: (0..(buckets_count + 1))
                .map(|_| Counters {
                    index: AtomicUsize::new(0),
                })
                .collect(),
            prefix_indexes: vec![0; buckets_count + 2],
            final_unitig_indexes_offset: 0,
        }
    }

    pub fn compute_id_offsets(&mut self) {
        // Build the final ids offsets
        for i in 0..self.links_data.len() {
            self.prefix_indexes[i + 1] =
                self.prefix_indexes[i] + self.links_data[i].index.load(Ordering::Relaxed);
        }

        // Set the final unitigs offset
        self.final_unitig_indexes_offset = self.prefix_indexes[self.links_data.len() - 1];
    }

    pub fn build_links<H: HashFunctionFactory, C: ColorsManager>(
        &self,
        temp_dir: PathBuf,
        completed_unitigs_path: PathBuf,
        buckets_count: usize,
        links_buckets: Vec<PathBuf>,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links building".to_string());

        rayon::scope(|s| {
            // Read the completed unitigs and add the corresponding remappings
            s.spawn(|_| {
                // Map to the last bucket, dedicated to completed unitigs
                let mut thread_links_writer =
                    ThreadUnitigsLinkManager::new(self, buckets_count as BucketIndexType);

                IntermediateReadsReader::<
                    CompletedReadsExtraData<color_types::PartialUnitigsColorStructure<H, C>>,
                >::new(completed_unitigs_path, RemoveFileMode::Keep)
                .for_each::<_, typenum::U0>(|_, data, _seq| {
                    let unitig_index = UnitigIndex::new(data.bucket, data.index, false);
                    thread_links_writer.notify_add_read(unitig_index, unitig_index);
                });
            });

            let mut link_pairs_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
                buckets_count,
                &(
                    temp_dir.join("link-pairs"),
                    get_memory_mode(SwapPriority::LinkPairs),
                ),
                None,
            );

            links_buckets.into_par_iter().for_each(|input| {
                let mut thread_link_pairs =
                    BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &link_pairs_buckets);

                let mut vec: Vec<EdgeHashEntry<H::HashTypeUnextendable>> =
                    Utils::bincode_deserialize_to_vec(input, true);

                fast_smart_radix_sort::<_, EdgeHashCompare<H>, false>(&mut vec[..]);

                for linked in vec.group_by(|a, b| a.hentry.hash == b.hentry.hash) {
                    for (index, first) in linked.iter().enumerate() {
                        for second in linked.iter().skip(index + 1) {
                            if first.hentry.direction != second.hentry.direction {
                                thread_link_pairs.add_element(
                                    second.hentry.bucket,
                                    &(),
                                    &LinkConnection {
                                        source: UnitigIndex::new(
                                            first.hentry.bucket,
                                            first.hentry.entry as usize,
                                            first.orig_dir == Direction::Backward,
                                        ),
                                        dest: UnitigIndex::new(
                                            second.hentry.bucket,
                                            second.hentry.entry as usize,
                                            second.orig_dir == Direction::Forward,
                                        ),
                                    },
                                );
                            }
                        }
                    }
                }
            });

            link_pairs_buckets.finalize()
        })
    }

    fn remap_reads(
        &mut self,
        temp_dir: PathBuf,
        mut link_pairs: Vec<PathBuf>,
        next_buckets_count: usize,
        new_links_prefix: &str,
        get_target_index: impl Fn(&LinkConnection) -> usize + Sync,
        update_remap: impl Fn(BucketIndexType, usize, &mut LinkConnection, bool) + Sync,
        write_to_bucket: impl Fn(
                &mut BucketsThreadDispatcher<LockFreeBinaryWriter, LinkConnection>,
                &mut LinkConnection,
            ) + Sync,
    ) -> Vec<PathBuf> {
        let mut first_link_remapped = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            next_buckets_count,
            &(
                temp_dir.join(new_links_prefix),
                get_memory_mode(SwapPriority::LinkPairs),
            ),
            None,
        );

        link_pairs.sort();
        self.remap_buckets_paths.sort();

        let buckets: Vec<_> = link_pairs
            .into_iter()
            .zip(self.remap_buckets_paths.iter())
            .collect();

        buckets.into_par_iter().for_each(|(pairs, remap)| {
            let mut thread_remapped =
                BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &first_link_remapped);

            let mut remap_hmap = HashMap::new();

            let mut remap_reader = FileReader::open(remap).unwrap();
            while let Ok(pair) = bincode::deserialize_from::<_, LinkRemap>(&mut remap_reader) {
                remap_hmap.insert(
                    pair.index,
                    (pair.new_bucket, pair.new_index, pair.complemented),
                );
            }

            let mut pairs_reader = FileReader::open(pairs).unwrap();
            while let Some(mut pair) = LinkConnection::decode(&mut pairs_reader) {
                let (new_bucket, new_index, needs_rc) =
                    remap_hmap.get(&(get_target_index(&pair) as u64)).unwrap();
                update_remap(*new_bucket, *new_index as usize, &mut pair, *needs_rc);
                write_to_bucket(&mut thread_remapped, &mut pair);
            }

            thread_remapped.finalize();
        });

        first_link_remapped.finalize()
    }

    #[cfg(feature = "build-links")]
    pub fn remap_first_pass(
        &mut self,
        temp_dir: PathBuf,
        link_pairs: Vec<PathBuf>,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links remapping step 1/2".to_string());

        self.remap_buckets_paths = self.remap_buckets.take().unwrap().finalize();

        let buckets_count = link_pairs.len();

        self.remap_reads(
            temp_dir,
            link_pairs,
            buckets_count,
            "link-pairs-fp",
            |pair| pair.dest.index(),
            |new_bucket, new_index, pair, rc| {
                pair.dest = UnitigIndex::new(
                    new_bucket,
                    new_index,
                    pair.dest.is_reverse_complemented() ^ rc,
                );
            },
            |buckets, pair| {
                buckets.add_element(pair.source.bucket(), &(), &pair);
            },
        )
    }

    #[cfg(feature = "build-links")]
    pub fn remap_second_pass(
        &mut self,
        temp_dir: PathBuf,
        link_pairs: Vec<PathBuf>,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links remapping step 2/2".to_string());

        let buckets_count = link_pairs.len();

        self.remap_reads(
            temp_dir,
            link_pairs,
            // Add 1 to the final number of buckets, as now we have the extra bucket for the already complete unitigs
            buckets_count + 1,
            "link-pairs-sp",
            |pair| pair.source.index(),
            |new_bucket, new_index, pair, rc| {
                pair.source = UnitigIndex::new(
                    new_bucket,
                    new_index,
                    pair.source.is_reverse_complemented() ^ rc,
                );
            },
            |buckets, pair| {
                buckets.add_element(pair.source.bucket(), &(), pair);
                // FIXME: Check if both links should be always added
                if pair.source != pair.dest {
                    std::mem::swap(&mut pair.source, &mut pair.dest);
                    // Swap the reverse complement status as the link is reversed
                    pair.source.change_reverse_complemented();
                    pair.dest.change_reverse_complemented();
                    buckets.add_element(pair.source.bucket(), &(), &pair);
                }
            },
        )
    }

    pub fn get_links_hmap(path: PathBuf) -> HashMap<usize, Vec<(bool, UnitigIndex)>> {
        let mut hmap = HashMap::new();

        let mut unitig_links_reader = FileReader::open(path).unwrap();
        while let Some(pair) = LinkConnection::decode(&mut unitig_links_reader) {
            hmap.entry(pair.source.index())
                .or_insert(Vec::new())
                .push((pair.source.is_reverse_complemented(), pair.dest));
        }

        hmap
    }

    #[inline(always)]
    #[track_caller]
    pub fn get_unitig_index(&self, bucket: BucketIndexType, index: usize) -> usize {
        self.prefix_indexes[bucket as usize] + index
    }

    #[inline(always)]
    #[track_caller]
    pub fn get_final_unitig_index(&self, index: usize) -> usize {
        self.final_unitig_indexes_offset + index
    }
}

pub struct ThreadUnitigsLinkManager<'a> {
    links_manager: &'a UnitigLinksManager,
    #[cfg(feature = "build-links")]
    thread_buckets: BucketsThreadDispatcher<'a, LockFreeBinaryWriter, LinkRemap>,
    bucket_index: BucketIndexType,
    counter: usize,
}

impl<'a> ThreadUnitigsLinkManager<'a> {
    pub fn new(links_manager: &'a UnitigLinksManager, bucket: BucketIndexType) -> Self {
        Self {
            links_manager,
            #[cfg(feature = "build-links")]
            thread_buckets: BucketsThreadDispatcher::new(
                DEFAULT_PER_CPU_BUFFER_SIZE,
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
        #[cfg(feature = "build-links")] first_original_link: UnitigIndex,
        #[cfg(feature = "build-links")] last_original_link: UnitigIndex,
    ) {
        #[cfg(feature = "build-links")]
        let new_index = self.counter;

        self.counter += 1;

        #[cfg(feature = "build-links")]
        {
            let remap_first = LinkRemap {
                index: first_original_link.index() as u64,
                new_bucket: self.bucket_index,
                new_index: new_index as u64,
                complemented: first_original_link.is_reverse_complemented(),
            };

            let remap_last = LinkRemap {
                index: last_original_link.index() as u64,
                new_bucket: self.bucket_index,
                new_index: new_index as u64,
                complemented: last_original_link.is_reverse_complemented(),
            };

            self.thread_buckets
                .add_element(first_original_link.bucket(), &(), &remap_first);

            if first_original_link != last_original_link {
                self.thread_buckets
                    .add_element(last_original_link.bucket(), &(), &remap_last);
            }
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
