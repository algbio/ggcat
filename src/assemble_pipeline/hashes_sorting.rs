use std::path::{Path, PathBuf};

use crate::assemble_pipeline::AssemblePipeline;
use crate::config::SwapPriority;
use crate::hashes::HashFunctionFactory;
use crate::io::structs::hash_entry::{Direction, HashCompare, HashEntry};
use crate::io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink};
use crate::utils::fast_rand_bool::FastRandBool;
use crate::utils::vec_slice::VecSlice;
use crate::utils::{get_memory_mode, Utils};
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::fast_smart_radix_sort;
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;

impl AssemblePipeline {
    pub fn hashes_sorting<H: HashFunctionFactory, P: AsRef<Path>>(
        file_hashes_inputs: Vec<PathBuf>,
        output_dir: P,
        buckets_count: usize,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: hashes sorting".to_string());

        let mut links_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            &(
                output_dir.as_ref().join("links"),
                get_memory_mode(SwapPriority::LinksBuckets as usize),
            ),
            None,
        );

        file_hashes_inputs
            .par_iter()
            .for_each(|input| {
                let mut links_tmp = BucketsThreadDispatcher::new(
                    MemoryDataSize::from_kibioctets(4),
                    &links_buckets,
                );

                let mut rand_bool = FastRandBool::new();

                let mut hashes_vec: Vec<HashEntry<H::HashTypeUnextendable>> = Utils::bincode_deserialize_to_vec(input, true);
                fast_smart_radix_sort::<_, HashCompare<H>, false>(&mut hashes_vec[..]);

                let mut unitigs_vec = Vec::new();

                for x in hashes_vec.group_by(|a, b| a.hash == b.hash) {
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
                                &UnitigLink {
                                    entry: x[fw].entry,
                                    flags: UnitigFlags::new_direction(true, reverse_complemented[fw]),
                                    entries: slice_fw,
                                },
                            );

                            links_tmp.add_element(
                                x[bw].bucket,
                                &unitigs_vec,
                                &UnitigLink {
                                    entry: x[bw].entry,
                                    flags: UnitigFlags::new_direction(false, reverse_complemented[bw]),
                                    entries: slice_bw,
                                },
                            );
                        },
                        1 => {
                            println!("Warning spurious hash detected ({:?}) with index {}, this is a bug or a collision in the KmersMerge phase!", x[0].hash, x[0].entry);
                        }
                        _ => {
                            println!("More than 2 equal hashes found in hashes sorting phase, this indicates an hash ({}) collision!",  x[0].hash);
                        }
                    }
                }
                links_tmp.finalize();
            });
        links_buckets.finalize()
    }
}
