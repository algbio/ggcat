use crate::binary_writer::{BinaryWriter, StorageMode};
use crate::hash_entry::HashEntry;
use crate::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use crate::pipeline::Pipeline;
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::unitig_link::{Direction, UnitigIndex, UnitigLink};
use crate::vec_slice::VecSlice;
use rand::{thread_rng, RngCore};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::Cursor;
use std::ops::Deref;
use std::path::{Path, PathBuf};

impl Pipeline {
    pub fn hashes_sorting(
        file_hashes_inputs: Vec<PathBuf>,
        output_dir: impl AsRef<Path>,
        buckets_count: usize,
    ) -> Vec<PathBuf> {
        let mut links_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(output_dir.as_ref().join("links"), StorageMode::Plain),
        );

        file_hashes_inputs
            .par_iter()
            .enumerate()
            .for_each(|(index, input)| {
                let mut random = thread_rng();
                let mut randidx = 64;
                let mut randval = random.next_u64();

                let mut get_randbool = || {
                    if randidx == 0 {
                        randval = random.next_u64();
                        randidx = 64;
                    }
                    randidx -= 1;
                    let result = (randval & 0x1) == 1;
                    randval >>= 1;
                    result
                };

                let mut links_tmp = BucketsThreadDispatcher::new(65536, &links_buckets);

                let file = filebuffer::FileBuffer::open(input).unwrap();

                let mut reader = Cursor::new(file.deref());
                let mut vec: Vec<HashEntry> = Vec::new();

                while let Ok(value) = bincode::deserialize_from(&mut reader) {
                    vec.push(value);
                }

                struct Compare {}
                impl SortKey<HashEntry> for Compare {
                    fn get(value: &HashEntry) -> u64 {
                        value.hash
                    }
                }

                // vec.sort_unstable_by_key(|e| e.hash);
                smart_radix_sort::<_, Compare, false>(&mut vec[..], 64 - 8);

                let mut unitigs_vec = Vec::new();

                for x in vec.group_by(|a, b| a.hash == b.hash) {
                    if x.len() == 2 && x[0].direction != x[1].direction {
                        let (fw, bw) = match x[0].direction {
                            Direction::Forward => (0, 1),
                            Direction::Backward => (1, 0),
                        };

                        if get_randbool() {
                            unitigs_vec.push(UnitigIndex::new(
                                x[bw].bucket as usize,
                                x[bw].entry as usize,
                            ));
                            links_tmp.add_element(
                                x[fw].bucket as usize,
                                &unitigs_vec,
                                UnitigLink {
                                    entry: 0,
                                    direction: Direction::Forward,
                                    entries: VecSlice::new(unitigs_vec.len() - 1, 1),
                                },
                            );
                        } else {
                            unitigs_vec.push(UnitigIndex::new(
                                x[fw].bucket as usize,
                                x[fw].entry as usize,
                            ));
                            links_tmp.add_element(
                                x[bw].bucket as usize,
                                &unitigs_vec,
                                UnitigLink {
                                    entry: 0,
                                    direction: Direction::Backward,
                                    entries: VecSlice::new(unitigs_vec.len() - 1, 1),
                                },
                            );
                        }
                        // println!(
                        //     "A: [{}]/{} B: [{}]{}",
                        //     x[0].bucket, x[0].entry, x[1].bucket, x[1].entry
                        // );
                    }
                }
                links_tmp.finalize(&unitigs_vec);
                println!("Done {}!", index);
            });
        links_buckets.finalize()
    }
}
