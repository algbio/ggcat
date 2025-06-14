use crate::structs::link_mapping::{LinkMapping, LinkMappingSerializer};
use config::{
    BucketIndexType, DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    SwapPriority, get_memory_mode,
};
use io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink, UnitigLinkSerializer};
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::async_binary_reader::AllowedCheckpointStrategy;
use parallel_processor::buckets::readers::generic_binary_reader::ChunkReader;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::single::SingleBucketThreadDispatcher;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{BucketsCount, MultiThreadBuckets, SingleBucket};
use parallel_processor::fast_smart_bucket_sort::{SortKey, fast_smart_radix_sort};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use utils::fast_rand_bool::FastRandBool;
use utils::vec_slice::VecSlice;

pub fn links_compaction(
    links_inputs: Vec<SingleBucket>,
    output_dir: impl AsRef<Path>,
    buckets_count: BucketsCount,
    elab_index: usize,
    result_map_buckets: &Arc<MultiThreadBuckets<LockFreeBinaryWriter>>,
    final_buckets: &Arc<MultiThreadBuckets<LockFreeBinaryWriter>>,
    // links_manager: &UnitigLinksManager,
    link_thread_buffers: &ScopedThreadLocal<BucketsThreadBuffer>,
    result_thread_buffers: &ScopedThreadLocal<BucketsThreadBuffer>,
) -> (Vec<SingleBucket>, u64) {
    let totsum = AtomicU64::new(0);

    let links_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
        buckets_count,
        output_dir
            .as_ref()
            .to_path_buf()
            .join(format!("linksi{}", elab_index)),
        None,
        &(
            get_memory_mode(SwapPriority::LinksBuckets),
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        &(),
    ));

    links_inputs.par_iter().for_each(|input| {
        let bucket_index = input.index as BucketIndexType;

        let mut link_buffers = link_thread_buffers.get();
        let mut links_tmp = BucketsThreadDispatcher::<_, UnitigLinkSerializer>::new(
            &links_buckets,
            link_buffers.take(),
            (),
        );
        let mut final_links_tmp = SingleBucketThreadDispatcher::<_, UnitigLinkSerializer>::new(
            DEFAULT_PER_CPU_BUFFER_SIZE,
            bucket_index,
            &final_buckets,
            (),
        );
        // let mut thread_links_manager = ThreadUnitigsLinkManager::new(links_manager, bucket_index);

        let mut result_buffers = result_thread_buffers.get();
        let mut results_tmp = BucketsThreadDispatcher::<_, LinkMappingSerializer>::new(
            &result_map_buckets,
            result_buffers.take(),
            (),
        );

        let mut rand_bool = FastRandBool::<1>::new();

        let file_reader = LockFreeBinaryReader::new(
            &input.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        );

        let mut vec = Vec::new();

        let mut last_unitigs_vec = Vec::new();
        let mut current_unitigs_vec = Vec::new();
        let mut final_unitigs_vec = Vec::new();

        let mut deserializer = UnitigLinkSerializer::new(());

        while let Some(checkpoint) =
            file_reader.get_read_parallel_stream(AllowedCheckpointStrategy::DecompressOnly, None)
        {
            match checkpoint {
                ChunkReader::Reader(mut stream, _) => {
                    while let Some(entry) =
                        deserializer.read_from(&mut stream, &mut last_unitigs_vec, &mut ())
                    {
                        vec.push(entry);
                    }
                }
                ChunkReader::Passtrough { .. } => unreachable!(),
            }
        }

        drop(file_reader);

        struct Compare;
        impl SortKey<UnitigLink> for Compare {
            type KeyType = u32;
            const KEY_BITS: usize = std::mem::size_of::<u32>() * 8;

            #[inline(always)]
            fn compare(left: &UnitigLink, right: &UnitigLink) -> std::cmp::Ordering {
                left.entry().cmp(&right.entry())
            }

            #[inline(always)]
            fn get_shifted(value: &UnitigLink, rhs: u8) -> u8 {
                (value.entry() >> rhs) as u8
            }
        }

        fast_smart_radix_sort::<_, Compare, false>(&mut vec[..]);

        let mut rem_links = 0;

        for x in vec.nq_group_by_mut(|a, b| a.entry() == b.entry()) {
            current_unitigs_vec.clear();

            let (link1, link2) =
                if x.len() == 2 && x[0].entries.len() != 0 && x[1].entries.len() != 0 {
                    // assert_ne!(x[0].flags.is_forward(), x[1].flags.is_forward());
                    if x[0].flags().is_forward() == x[1].flags().is_forward() {
                        // Flip one of the strands
                        x[1].change_flags(|flags| flags.set_forward(!flags.is_forward()));

                        // Reverse complement one of the strands
                        x[1].change_flags(|flags| *flags = flags.reverse_complement());
                        let strand_slice = x[1].entries.get_slice_mut(&mut last_unitigs_vec);
                        for el in strand_slice {
                            el.change_reverse_complemented();
                        }
                    }

                    // To be joined the entries should have the same reverse_complement flag
                    assert_eq!(
                        x[0].flags().is_reverse_complemented(),
                        x[1].flags().is_reverse_complemented()
                    );

                    let flags = UnitigFlags::combine(x[0].flags(), x[1].flags());

                    assert_eq!(x[0].flags().end_sealed(), flags.end_sealed());

                    let should_swap = x[1].flags().end_sealed()
                        || (!x[0].flags().end_sealed() && rand_bool.get_randbool());
                    let (fw, bw, mut flags) = if should_swap {
                        (1, 0, flags.flipped())
                    } else {
                        (0, 1, flags)
                    };

                    assert_eq!(x[fw].flags().end_sealed(), flags.end_sealed());
                    assert_eq!(x[bw].flags().end_sealed(), flags.begin_sealed());
                    assert!(!x[fw].flags().begin_sealed() && !x[bw].flags().begin_sealed());

                    let fw_slice = x[fw].entries.get_slice(&last_unitigs_vec);
                    let bw_slice = x[bw].entries.get_slice(&last_unitigs_vec);

                    let new_entry = bw_slice[bw_slice.len() - 1];
                    let other_entry = fw_slice[fw_slice.len() - 1];

                    // Remove the last entry
                    let bw_slice = &bw_slice[..bw_slice.len() - 1];

                    let concat_slice = VecSlice::new_extend_iter(
                        &mut current_unitigs_vec,
                        bw_slice
                            .iter()
                            .rev()
                            .chain(
                                [UnitigIndex::new(
                                    bucket_index,
                                    x[0].entry() as usize,
                                    x[0].flags().is_reverse_complemented(),
                                )]
                                .iter(),
                            )
                            .chain(fw_slice.iter())
                            .map(|x| *x),
                    );

                    // Update the complemented status to match the one of the new entry
                    flags.set_reverse_complement(new_entry.is_reverse_complemented());

                    // join_links += 1;
                    assert!(flags.end_sealed() || !flags.begin_sealed());

                    (
                        (
                            new_entry.bucket(),
                            UnitigLink::new(new_entry.index() as u64, flags, concat_slice),
                        ),
                        Some((
                            other_entry.bucket(),
                            UnitigLink::new(
                                other_entry.index() as u64,
                                UnitigFlags::new_empty(),
                                VecSlice::EMPTY,
                            ),
                        )),
                    )
                } else {
                    let entry = if x[0].entries.len() != 0 {
                        &x[0]
                    } else if x.len() > 1 && x[1].entries.len() != 0 {
                        &x[1]
                    } else {
                        continue;
                    };

                    let mut flags = entry.flags();

                    let is_lonely = x.len() == 1;

                    assert!(is_lonely || x[0].entries.len() == 0 || x[1].entries.len() == 0);
                    assert!(!flags.begin_sealed() || is_lonely);

                    if is_lonely {
                        // not_links += 1;
                        flags.seal_beginning();

                        if flags.end_sealed() {
                            let linked = entry.entries.get_slice(&last_unitigs_vec);

                            // Write to disk, full unitig!
                            final_unitigs_vec.clear();
                            let entries = VecSlice::new_extend(&mut final_unitigs_vec, linked);

                            // thread_links_manager.notify_add_read();

                            final_links_tmp.add_element(
                                &final_unitigs_vec,
                                &UnitigLink::new(entry.entry(), flags, entries),
                            );

                            results_tmp.add_element(
                                bucket_index,
                                &(),
                                &LinkMapping {
                                    entry: entry.entry() as u64,
                                    bucket: bucket_index,
                                },
                            );

                            for link in linked.iter() {
                                results_tmp.add_element(
                                    link.bucket(),
                                    &(),
                                    &LinkMapping {
                                        entry: link.index() as u64,
                                        bucket: bucket_index,
                                    },
                                );
                            }
                            continue;
                        }
                    }

                    let entries = entry.entries.get_slice(&last_unitigs_vec);

                    let first_entry = UnitigIndex::new(
                        bucket_index,
                        entry.entry() as usize,
                        entry.flags().is_reverse_complemented(),
                    );
                    let last_entry = *entries.last().unwrap();

                    // Circular unitig detected, output it
                    if first_entry == last_entry {
                        // Write to disk, full unitig!
                        let unitig_entries = entry.entries.get_slice(&last_unitigs_vec);

                        final_unitigs_vec.clear();
                        let entries = VecSlice::new_extend(&mut final_unitigs_vec, unitig_entries);

                        // thread_links_manager.notify_add_read();

                        final_links_tmp.add_element(
                            &final_unitigs_vec,
                            &UnitigLink::new(entry.entry(), flags, entries),
                        );

                        for link in unitig_entries.iter() {
                            results_tmp.add_element(
                                link.bucket(),
                                &(),
                                &LinkMapping {
                                    entry: link.index() as u64,
                                    bucket: bucket_index,
                                },
                            );
                        }
                        continue;
                    }

                    let (new_entry, oth_entry, vec_slice, mut flags) = if flags.end_sealed()
                        || (!flags.begin_sealed() && rand_bool.get_randbool())
                    {
                        (
                            first_entry,
                            last_entry,
                            VecSlice::new_extend(&mut current_unitigs_vec, entries),
                            flags,
                        )
                    } else {
                        (
                            last_entry,
                            first_entry,
                            VecSlice::new_extend_iter(
                                &mut current_unitigs_vec,
                                entries
                                    .iter()
                                    .rev()
                                    .skip(1)
                                    .chain(&[first_entry])
                                    .map(|x| *x),
                            ),
                            flags.flipped(),
                        )
                    };

                    assert!(!flags.begin_sealed() || !flags.end_sealed());
                    assert!(flags.end_sealed() || !flags.begin_sealed());
                    flags.set_reverse_complement(new_entry.is_reverse_complemented());

                    (
                        (
                            new_entry.bucket(),
                            UnitigLink::new(new_entry.index() as u64, flags, vec_slice),
                        ),
                        Some((
                            oth_entry.bucket(),
                            UnitigLink::new(
                                oth_entry.index() as u64,
                                UnitigFlags::new_empty(),
                                VecSlice::EMPTY,
                            ),
                        )),
                    )
                };
            rem_links += 1;

            links_tmp.add_element(link1.0, &current_unitigs_vec, &link1.1);
            if let Some(link2) = link2 {
                links_tmp.add_element(link2.0, &current_unitigs_vec, &link2.1);
            }
        }

        totsum.fetch_add(rem_links, Ordering::Relaxed);
        link_buffers.put_back(links_tmp.finalize().0);
        final_links_tmp.finalize();
        result_buffers.put_back(results_tmp.finalize().0);
    });

    (
        links_buckets.finalize_single(),
        totsum.load(Ordering::Relaxed),
    )
}
