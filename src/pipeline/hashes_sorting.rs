use std::io::Cursor;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::fast_rand_bool::FastRandBool;
use crate::hash::{ExtendableHashTraitType, HashFunctionFactory};
use crate::hash_entry::{Direction, HashEntry};
use crate::pipeline::Pipeline;
use crate::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink};
use crate::vec_slice::VecSlice;
use crate::{DEFAULT_BUFFER_SIZE, KEEP_FILES};
use parallel_processor::binary_writer::{BinaryWriter, StorageMode};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::smart_bucket_sort::{smart_radix_sort, SortKey};
use rand::{thread_rng, RngCore};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

impl Pipeline {
    pub fn hashes_sorting<H: HashFunctionFactory, P: AsRef<Path>>(
        file_hashes_inputs: Vec<PathBuf>,
        output_dir: P,
        buckets_count: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: hashes sorting".to_string());

        let mut links_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                output_dir.as_ref().join("links"),
                StorageMode::Plain {
                    buffer_size: DEFAULT_BUFFER_SIZE,
                },
            ),
            None,
        );

        file_hashes_inputs
            .par_iter()
            .enumerate()
            .for_each(|(index, input)| {
                let mut links_tmp = BucketsThreadDispatcher::new(
                    MemoryDataSize::from_kibioctets(64.0),
                    &links_buckets,
                );

                let mut rand_bool = FastRandBool::new();

                let file = filebuffer::FileBuffer::open(input).unwrap();

                let mut reader = Cursor::new(file.deref());
                let mut vec: Vec<HashEntry<H::HashTypeUnextendable>> = Vec::new();

                while let Ok(value) = bincode::deserialize_from(&mut reader) {
                    vec.push(value);
                }

                drop(file);
                if !KEEP_FILES.load(Ordering::Relaxed) {
                    std::fs::remove_file(&input);
                }

                struct Compare<H: HashFunctionFactory> {
                    _phantom: PhantomData<H>,
                }
                impl<H: HashFunctionFactory> SortKey<HashEntry<H::HashTypeUnextendable>> for Compare<H> {
                    type KeyType = H::HashTypeUnextendable;
                    const KEY_BITS: usize = 64;

                    #[inline(always)]
                    fn get(value: &HashEntry<H::HashTypeUnextendable>) -> H::HashTypeUnextendable {
                        value.hash
                    }

                    #[inline(always)]
                    fn get_shifted(value: &HashEntry<H::HashTypeUnextendable>, rhs: u8) -> u8 {
                        H::get_shifted(value.hash, rhs) as u8
                    }
                }

                // vec.sort_unstable_by_key(|e| e.hash);
                smart_radix_sort::<_, Compare<H>, false>(&mut vec[..]);

                let mut unitigs_vec = Vec::new();

                for x in vec.group_by(|a, b| a.hash == b.hash) {
                    match x.len() {
                        2 => {
                            let mut reverse_complemented = [false, false];

                            // Can happen with canonical kmers, we should reverse-complement one of the strands
                            // the direction reverse is implicit as x[1] is treated as if it had the opposite of the x[0] direction
                            if x[0].direction == x[1].direction {
                                reverse_complemented[1] = true;
                            }

                            let (fw, bw) = match x[0].direction {
                                Direction::Forward => (0, 1),
                                Direction::Backward => (1, 0),
                            };

                            let (slice_fw, slice_bw) = if rand_bool.get_randbool() {
                                unitigs_vec.push(UnitigIndex::new(x[bw].bucket, x[bw].entry as usize, reverse_complemented[bw]));
                                (VecSlice::new(unitigs_vec.len() - 1, 1), VecSlice::EMPTY)
                            } else {
                                unitigs_vec.push(UnitigIndex::new(x[fw].bucket, x[fw].entry as usize, reverse_complemented[fw]));
                                (VecSlice::EMPTY, VecSlice::new(unitigs_vec.len() - 1, 1))
                            };

                            links_tmp.add_element(
                                x[fw].bucket,
                                &unitigs_vec,
                                UnitigLink {
                                    entry: x[fw].entry,
                                    flags: UnitigFlags::new_direction(true, reverse_complemented[fw]),
                                    entries: slice_fw,
                                },
                            );

                            links_tmp.add_element(
                                x[bw].bucket,
                                &unitigs_vec,
                                UnitigLink {
                                    entry: x[bw].entry,
                                    flags: UnitigFlags::new_direction(false, reverse_complemented[bw]),
                                    entries: slice_bw,
                                },
                            );
                        },
                        1 => {
                            println!("Warning spurious hash detected with index {}, this is a bug or a collision in the KmersMerge phase!", x[0].entry);
                        }
                        _ => {
                            println!("More than 2 equal hashes found in hashes sorting phase, this indicates an hash collision!");
                        }
                    }
                }
                links_tmp.finalize();
            });
        links_buckets.finalize()
    }
}
