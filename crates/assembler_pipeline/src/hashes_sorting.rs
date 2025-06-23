use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ::dynamic_dispatch::dynamic_dispatch;
use config::{
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, SwapPriority, get_memory_mode,
};
use hashes::HashFunctionFactory;
use io::structs::hash_entry::{Direction, HashCompare, HashEntrySerializer};
use io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink, UnitigLinkSerializer};
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::readers::typed_binary_reader::TypedStreamReader;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{BucketsCount, MultiThreadBuckets, SingleBucket};
use parallel_processor::fast_smart_bucket_sort::fast_smart_radix_sort;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use utils::fast_rand_bool::FastRandBool;
use utils::vec_slice::VecSlice;

#[dynamic_dispatch(H = [
    #[cfg(all(feature = "hash-forward", feature = "hash-16bit"))] hashes::fw_seqhash::u16::ForwardSeqHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-32bit"))] hashes::fw_seqhash::u32::ForwardSeqHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-64bit"))] hashes::fw_seqhash::u64::ForwardSeqHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-128bit"))] hashes::fw_seqhash::u128::ForwardSeqHashFactory,
    #[cfg(feature = "hash-16bit")] hashes::cn_seqhash::u16::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-32bit")] hashes::cn_seqhash::u32::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-64bit")] hashes::cn_seqhash::u64::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-128bit")] hashes::cn_seqhash::u128::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-rkarp")] hashes::cn_rkhash::u128::CanonicalRabinKarpHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-rkarp"))] hashes::fw_rkhash::u128::ForwardRabinKarpHashFactory,
])]
pub fn hashes_sorting<H: HashFunctionFactory>(
    file_hashes_inputs: Vec<SingleBucket>,
    output_dir: &Path,
    buckets_count: BucketsCount,
) -> Vec<SingleBucket> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: hashes sorting".to_string());

    let links_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
        buckets_count,
        output_dir.join("links"),
        None,
        &(
            get_memory_mode(SwapPriority::LinksBuckets),
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        &(),
    ));

    let buckets_thread_buffers = ScopedThreadLocal::new(move || {
        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &buckets_count)
    });

    file_hashes_inputs
        .par_iter()
        .for_each(|input| {

            let mut buffers = buckets_thread_buffers.get();
            let mut links_tmp = BucketsThreadDispatcher::<_, UnitigLinkSerializer>::new(
                &links_buckets,
                buffers.take(),
                ()
            );

            let mut rand_bool = FastRandBool::<1>::new();

            let mut hashes_vec = Vec::new();

            let file_index = ChunkedBinaryReaderIndex::from_file(&input.path, RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed)
            }, DEFAULT_PREFETCH_AMOUNT);

            TypedStreamReader::get_items::<HashEntrySerializer<H::HashTypeUnextendable>>(
                None,
                (),
                file_index.into_chunks(),
                |h, _| {
                    hashes_vec.push(h);
                }
            );

            fast_smart_radix_sort::<_, HashCompare<H>, false>(&mut hashes_vec[..]);

            let mut unitigs_vec = Vec::new();

            for x in hashes_vec.nq_group_by(|a, b| a.hash == b.hash) {
                match x.len() {
                    2 => {
                        let mut reverse_complemented = [false, false];

                        // Can happen with canonical kmers, we should reverse-complement one of the strands
                        // the direction reverse is implicit as x[1] is treated as if it had the opposite of the x[0] direction
                        if x[0].direction() == x[1].direction() {
                            reverse_complemented[1] = true;
                        }

                        let (fw, bw) = match x[0].direction() {
                            Direction::Forward => (0, 1),
                            Direction::Backward => (1, 0),
                        };

                        let (slice_fw, slice_bw) = if rand_bool.get_randbool() {
                            unitigs_vec.push(UnitigIndex::new(x[bw].bucket(), x[bw].entry() as usize, reverse_complemented[bw]));
                            (VecSlice::new(unitigs_vec.len() - 1, 1), VecSlice::EMPTY)
                        } else {
                            unitigs_vec.push(UnitigIndex::new(x[fw].bucket(), x[fw].entry() as usize, reverse_complemented[fw]));
                            (VecSlice::EMPTY, VecSlice::new(unitigs_vec.len() - 1, 1))
                        };

                        links_tmp.add_element(
                            x[fw].bucket(),
                            &unitigs_vec,
                            &UnitigLink::new(
                                x[fw].entry(),
                                UnitigFlags::new_direction(true, reverse_complemented[fw]),
                                slice_fw,
                            ),
                        );

                        links_tmp.add_element(
                            x[bw].bucket(),
                            &unitigs_vec,
                            &UnitigLink::new(
                                x[bw].entry(),
                                UnitigFlags::new_direction(false, reverse_complemented[bw]),
                                slice_bw,
                            ),
                        );
                    },
                    1 => {
                        ggcat_logging::error!("Warning spurious hash detected ({:?}) with index {}, this is a bug or a collision in the KmersMerge phase!", x[0].hash, x[0].entry());
                    }
                    _ => {
                        ggcat_logging::error!("More than 2 equal hashes found in hashes sorting phase, this indicates an hash ({}) collision!",  x[0].hash);
                    }
                }
            }
            buffers.put_back(links_tmp.finalize().0);
        });
    links_buckets.finalize_single()
}
