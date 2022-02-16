use crate::assemble_pipeline::AssemblePipeline;
use crate::config::{BucketIndexType, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE};
use crate::io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink};
use crate::io::varint::{decode_varint, encode_varint};
use crate::utils::fast_rand_bool::FastRandBool;
use crate::utils::vec_slice::VecSlice;
use crate::utils::{get_memory_mode, Utils};
use crate::KEEP_FILES;
use byteorder::ReadBytesExt;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use parallel_processor::buckets::bucket_writer::BucketWriter;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::buckets::single::SingleBucketThreadDispatcher;

#[derive(Clone, Debug)]
pub struct LinkMapping {
    pub bucket: BucketIndexType,
    pub entry: u64,
}

impl LinkMapping {
    pub fn from_stream(mut reader: impl Read) -> Option<LinkMapping> {
        let bucket = decode_varint(|| reader.read_u8().ok())? as BucketIndexType;
        let entry = decode_varint(|| reader.read_u8().ok())?;
        Some(LinkMapping { bucket, entry })
    }
}

impl BucketWriter for LinkMapping {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        encode_varint(|b| bucket.write_all(b), self.bucket as u64).unwrap();
        encode_varint(|b| bucket.write_all(b), self.entry).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        16
    }
}

impl AssemblePipeline {
    pub fn links_compaction(
        links_inputs: Vec<PathBuf>,
        output_dir: impl AsRef<Path>,
        buckets_count: usize,
        elab_index: usize,
        result_map_buckets: &mut MultiThreadBuckets<LockFreeBinaryWriter>,
        final_buckets: &mut MultiThreadBuckets<LockFreeBinaryWriter>,
    ) -> (Vec<PathBuf>, bool) {
        let totsum = AtomicU64::new(0);

        let mut links_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            &(
                output_dir
                    .as_ref()
                    .to_path_buf()
                    .join(format!("linksi{}", elab_index)),
                get_memory_mode(SwapPriority::LinksBuckets as usize),
            ),
            None,
        );

        links_inputs.par_iter().for_each(|input| {
            let bucket_index = Utils::get_bucket_index(input);

            let mut links_tmp =
                BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &links_buckets);
            let mut final_links_tmp =
                SingleBucketThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, bucket_index, &final_buckets);
            let mut results_tmp =
                BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &result_map_buckets);


            let mut rand_bool = FastRandBool::new();

            let mut reader = FileReader::open(&input).unwrap();
            let mut vec = Vec::new();

            let mut last_unitigs_vec = Vec::new();
            let mut current_unitigs_vec = Vec::new();
            let mut final_unitigs_vec = Vec::new();

            while let Some(entry) = UnitigLink::read_from(&mut reader, &mut last_unitigs_vec) {
                vec.push(entry);
            }

            drop(reader);
            MemoryFs::remove_file(&input, !KEEP_FILES.load(Ordering::Relaxed)).unwrap();

            struct Compare {}
            impl SortKey<UnitigLink> for Compare {
                type KeyType = u64;
                const KEY_BITS: usize = 64;

                fn compare(left: &UnitigLink, right: &UnitigLink) -> std::cmp::Ordering {
                    left.entry.cmp(&right.entry)
                }

                fn get_shifted(value: &UnitigLink, rhs: u8) -> u8 {
                    (value.entry >> rhs) as u8
                }
            }

            fast_smart_radix_sort::<_, Compare, false>(&mut vec[..]);

            let mut rem_links = 0;

            for x in vec.group_by_mut(|a, b| a.entry == b.entry) {
                let (link1, link2) =
                    if x.len() == 2 && x[0].entries.len() != 0 && x[1].entries.len() != 0 {
                        // assert_ne!(x[0].flags.is_forward(), x[1].flags.is_forward());
                        if x[0].flags.is_forward() == x[1].flags.is_forward() {
                            // Flip one of the strands
                            x[1].flags.set_forward(!x[1].flags.is_forward());

                            // Reverse complement one of the strands
                            x[1].flags = x[1].flags.reverse_complement();
                            let strand_slice = x[1].entries.get_slice_mut(&mut last_unitigs_vec);
                            for el in strand_slice {
                                el.change_reverse_complemented();
                            }
                        }

                        // To be joined the entries should have the same reverse_complement flag
                        assert_eq!(
                            x[0].flags.is_reverse_complemented(),
                            x[1].flags.is_reverse_complemented()
                        );

                        let flags = UnitigFlags::combine(x[0].flags, x[1].flags);

                        assert_eq!(x[0].flags.end_sealed(), flags.end_sealed());

                        let should_swap = x[1].flags.end_sealed()
                            || (!x[0].flags.end_sealed() && rand_bool.get_randbool());
                        let (fw, bw, mut flags) = if should_swap {
                            (1, 0, flags.flipped())
                        } else {
                            (0, 1, flags)
                        };

                        assert_eq!(x[fw].flags.end_sealed(), flags.end_sealed());
                        assert_eq!(x[bw].flags.end_sealed(), flags.begin_sealed());
                        assert!(!x[fw].flags.begin_sealed() && !x[bw].flags.begin_sealed());

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
                                        x[0].entry as usize,
                                        x[0].flags.is_reverse_complemented(),
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
                                UnitigLink {
                                    entry: new_entry.index() as u64,
                                    flags,
                                    entries: concat_slice,
                                },
                            ),
                            Some((
                                other_entry.bucket(),
                                UnitigLink {
                                    entry: other_entry.index() as u64,
                                    flags: UnitigFlags::new_empty(),
                                    entries: VecSlice::EMPTY,
                                },
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

                        let mut flags = entry.flags;

                        let is_lonely = x.len() == 1;

                        assert!(is_lonely || x[0].entries.len() == 0 || x[1].entries.len() == 0);
                        assert!(!flags.begin_sealed() || is_lonely);

                        if is_lonely {
                            // not_links += 1;
                            flags.seal_beginning();

                            if flags.end_sealed() {
                                let linked = entry.entries.get_slice(&last_unitigs_vec);

                                // Write to disk, full unitig!
                                let entries = VecSlice::new_extend(&mut final_unitigs_vec, linked);

                                final_links_tmp.add_element(
                                    &final_unitigs_vec,
                                    &UnitigLink {
                                        entry: entry.entry,
                                        flags,
                                        entries,
                                    },
                                );

                                results_tmp.add_element(
                                    bucket_index,
                                    &(),
                                    &LinkMapping {
                                        entry: entry.entry as u64,
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
                            entry.entry as usize,
                            entry.flags.is_reverse_complemented(),
                        );
                        let last_entry = *entries.last().unwrap();

                        // Circular unitig detected, output it
                        if first_entry == last_entry {
                            // Write to disk, full unitig!
                            let unitig_entries = entry.entries.get_slice(&last_unitigs_vec);

                            let entries =
                                VecSlice::new_extend(&mut final_unitigs_vec, unitig_entries);

                            final_links_tmp.add_element(
                                bucket_index,
                                &final_unitigs_vec,
                                &UnitigLink {
                                    entry: entry.entry,
                                    flags,
                                    entries,
                                },
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
                                UnitigLink {
                                    entry: new_entry.index() as u64,
                                    flags,
                                    entries: vec_slice,
                                },
                            ),
                            Some((
                                oth_entry.bucket(),
                                UnitigLink {
                                    entry: oth_entry.index() as u64,
                                    flags: UnitigFlags::new_empty(),
                                    entries: VecSlice::EMPTY,
                                },
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
            links_tmp.finalize();
            final_links_tmp.finalize();
            results_tmp.finalize();
        });

        println!(
            "Remaining: {} {}",
            totsum.load(Ordering::Relaxed),
            PHASES_TIMES_MONITOR
                .read()
                .get_formatted_counter_without_memory()
        );
        (
            links_buckets.finalize(),
            totsum.load(Ordering::Relaxed) == 0,
        )
    }
}
