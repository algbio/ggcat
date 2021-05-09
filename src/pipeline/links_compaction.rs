use std::io::{Cursor, Read};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use itertools::Itertools;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;

use crate::binary_writer::{BinaryWriter, StorageMode};
use crate::fast_rand_bool::FastRandBool;
use crate::hash_entry::Direction;
use crate::multi_thread_buckets::{BucketWriter, BucketsThreadDispatcher, MultiThreadBuckets};
use crate::pipeline::Pipeline;
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink};
use crate::utils::Utils;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;
use byteorder::ReadBytesExt;
use serde::{Deserialize, Serialize};
use std::process::exit;

#[derive(Clone, Debug)]
pub struct LinkMapping {
    pub bucket: u64,
    pub entry: u64,
}

impl LinkMapping {
    pub fn from_stream(mut reader: impl Read) -> Option<LinkMapping> {
        let bucket = decode_varint(|| reader.read_u8().ok())?;
        let entry = decode_varint(|| reader.read_u8().ok())?;
        Some(LinkMapping { bucket, entry })
    }
}

impl BucketWriter for LinkMapping {
    type BucketType = BinaryWriter;
    type ExtraData = ();

    fn write_to(&self, bucket: &mut Self::BucketType, extra_data: &Self::ExtraData) {
        encode_varint(|b| bucket.get_writer().write(b), self.bucket);
        encode_varint(|b| bucket.get_writer().write(b), self.entry);
    }
}

impl Pipeline {
    pub fn links_compaction(
        links_inputs: Vec<PathBuf>,
        output_dir: impl AsRef<Path>,
        buckets_count: usize,
        elab_index: usize,
    ) -> (Vec<PathBuf>, Option<(Vec<PathBuf>, Vec<PathBuf>)>) {
        let totsum = AtomicU64::new(0);

        let mut links_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                output_dir
                    .as_ref()
                    .to_path_buf()
                    .join(format!("linksi{}", elab_index)),
                StorageMode::Plain,
            ),
        );

        let mut result_map_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                output_dir.as_ref().to_path_buf().join("results_map"),
                StorageMode::AppendOrCreate,
            ),
        );

        let mut final_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                output_dir.as_ref().to_path_buf().join("unitigs_map"),
                StorageMode::AppendOrCreate,
            ),
        );

        links_inputs
            .par_iter()
            .enumerate()
            .for_each(|(index, input)| {
                let mut links_tmp = BucketsThreadDispatcher::new(65536, &links_buckets);
                let mut final_links_tmp = BucketsThreadDispatcher::new(16384, &final_buckets);
                let mut results_tmp = BucketsThreadDispatcher::new(16384, &result_map_buckets);

                let bucket_index = Utils::get_bucket_index(input);

                let mut rand_bool = FastRandBool::new();

                let file = filebuffer::FileBuffer::open(input).unwrap();
                let mut vec = Vec::new();

                let mut reader = Cursor::new(file.deref());
                let mut last_unitigs_vec = Vec::new();
                let mut current_unitigs_vec = Vec::new();
                let mut final_unitigs_vec = Vec::new();

                while let Some(entry) = UnitigLink::read_from(&mut reader, &mut last_unitigs_vec) {
                    vec.push(entry);
                }

                struct Compare {}
                impl SortKey<UnitigLink> for Compare {
                    fn get(value: &UnitigLink) -> u64 {
                        value.entry
                    }
                }

                smart_radix_sort::<_, Compare, false>(&mut vec[..], 64 - 8);

                let mut rem_links = 0;
                let mut join_links = 0;
                let mut not_links = 0;

                for x in vec.group_by(|a, b| a.entry == b.entry) {
                    let (link1, link2) = if x.len() == 2
                        && x[0].entries.len() != 0
                        && x[1].entries.len() != 0
                    {
                        assert_ne!(x[0].flags.is_forward(), x[1].flags.is_forward());

                        let flags = UnitigFlags::combine(x[0].flags, x[1].flags);

                        let should_swap = x[0].flags.end_sealed() || rand_bool.get_randbool();
                        let (fw, bw, flags) = if should_swap {
                            (1, 0, flags.reversed())
                        } else {
                            (0, 1, flags)
                        };

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
                                .chain([UnitigIndex::new(bucket_index, x[0].entry as usize)].iter())
                                .chain(fw_slice.iter())
                                .map(|x| *x),
                        );

                        join_links += 1;

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
                                    flags: flags.reversed(),
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
                        if is_lonely {
                            not_links += 1;
                            flags.seal_beginning();

                            if flags.end_sealed() {
                                let linked = entry.entries.get_slice(&last_unitigs_vec);

                                // Write to disk, full unitig!
                                let entries = VecSlice::new_extend(&mut final_unitigs_vec, linked);

                                final_links_tmp.add_element(
                                    bucket_index,
                                    &final_unitigs_vec,
                                    UnitigLink {
                                        entry: entry.entry,
                                        flags,
                                        entries,
                                    },
                                );

                                results_tmp.add_element(
                                    bucket_index,
                                    &(),
                                    LinkMapping {
                                        entry: entry.entry as u64,
                                        bucket: bucket_index as u64,
                                    },
                                );

                                for link in linked {
                                    results_tmp.add_element(
                                        link.bucket(),
                                        &(),
                                        LinkMapping {
                                            entry: link.index() as u64,
                                            bucket: bucket_index as u64,
                                        },
                                    );
                                }

                                continue;
                            }
                        }

                        let entries = entry.entries.get_slice(&last_unitigs_vec);

                        let first_entry = UnitigIndex::new(bucket_index, entry.entry as usize);
                        let last_entry = *entries.first().unwrap();

                        // Circular unitig detected, output it
                        if first_entry == last_entry {
                            // Write to disk, full unitig!
                            let unitig_entries = entry.entries.get_slice(&last_unitigs_vec);

                            let entries = VecSlice::new_extend(
                                &mut final_unitigs_vec,
                                &unitig_entries[..unitig_entries.len() - 1],
                            );

                            final_links_tmp.add_element(
                                0,
                                &final_unitigs_vec,
                                UnitigLink {
                                    entry: entry.entry,
                                    flags,
                                    entries,
                                },
                            );
                            continue;
                        }

                        let (new_entry, oth_entry, vec_slice, flags) =
                            if flags.end_sealed() || rand_bool.get_randbool() {
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
                                    flags.reversed(),
                                )
                            };

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
                                    flags: flags.reversed(),
                                    entries: VecSlice::EMPTY,
                                },
                            )),
                        )
                    };
                    rem_links += 1;
                    links_tmp.add_element(link1.0, &current_unitigs_vec, link1.1);
                    if let Some(link2) = link2 {
                        links_tmp.add_element(link2.0, &current_unitigs_vec, link2.1);
                    }
                }
                println!(
                    "Done {} {}/{} [JOINED: {}]!",
                    index, rem_links, not_links, join_links
                );
                totsum.fetch_add(rem_links, Ordering::Relaxed);
                links_tmp.finalize(&current_unitigs_vec);
                final_links_tmp.finalize(&final_unitigs_vec);
                results_tmp.finalize(&());
            });

        let final_buckets = final_buckets.finalize();
        let result_map_buckets = result_map_buckets.finalize();

        println!("Remaining: {}", totsum.load(Ordering::Relaxed));
        (
            links_buckets.finalize(),
            match totsum.load(Ordering::Relaxed) {
                0 => Some((final_buckets, result_map_buckets)),
                _ => None,
            },
        )
    }
}
