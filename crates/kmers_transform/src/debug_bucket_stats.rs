use config::{
    BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, READ_FLAG_INCL_END,
};
use hashes::default::MNHFactory;
use hashes::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, CompressedReadsBucketDataSerializer, DeserializedRead,
    NoMultiplicity, ReadsCheckpointData, WithSecondBucket,
};
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::readers::typed_binary_reader::{
    AsyncReaderThread, TypedStreamReader,
};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::memory_fs::RemoveFileMode;
use std::collections::HashSet;
use std::path::PathBuf;

fn get_sequence_bucket<C>(
    k: usize,
    m: usize,
    seq_data: &DeserializedRead<'_, C>,
    used_hash_bits: usize,
    bucket_bits_count: usize,
) -> BucketIndexType {
    let decr_val =
        ((seq_data.read.bases_count() == k) && (seq_data.flags & READ_FLAG_INCL_END) == 0) as usize;

    let hashes = MNHFactory::new(seq_data.read.sub_slice((1 - decr_val)..(k - decr_val)), m);

    let minimizer = hashes.iter().min_by_key(|k| k.to_unextendable()).unwrap();

    MNHFactory::get_bucket(
        used_hash_bits,
        bucket_bits_count,
        minimizer.to_unextendable(),
    )
}

pub fn compute_stats_for_bucket<MH: HashFunctionFactory>(
    bucket: PathBuf,
    bucket_index: usize,
    buckets_count: usize,
    second_buckets_log: usize,
    k: usize,
    m: usize,
) {
    let bucket_file_index = ChunkedBinaryReaderIndex::from_file(
        &bucket,
        RemoveFileMode::Remove { remove_fs: false },
        DEFAULT_PREFETCH_AMOUNT,
    );

    let file_size = bucket_file_index.get_file_size();

    let second_buckets_count = 1 << second_buckets_log;

    let mut hash_maps = (0..second_buckets_count)
        .map(|_| HashSet::new())
        .collect::<Vec<_>>();

    let mut total_counters = vec![0; second_buckets_count];

    let mut checkpoints_iterator =
        TypedStreamReader::get_items::<
            CompressedReadsBucketDataSerializer<
                (),
                WithSecondBucket,
                NoMultiplicity,
                AssemblerMinimizerPosition,
                typenum::U2,
            >,
        >(None, k, bucket_file_index.into_chunks(), |read_info, _| {
            let orig_bucket = get_sequence_bucket::<()>(
                k,
                m,
                &read_info,
                buckets_count.ilog2() as usize,
                second_buckets_log,
            ) as usize;

            let hashes = MH::new(read_info.read, k);

            for hash in hashes.iter() {
                total_counters[orig_bucket] += 1;
                hash_maps[orig_bucket].insert(hash.to_unextendable());
            }
        });

    let counters_string = hash_maps
        .iter()
        .zip(total_counters.iter())
        .map(|(h, t)| format!("({}/{})", h.len(), t))
        .collect::<Vec<String>>()
        .join(";");

    let tot_seqs = total_counters.iter().sum::<usize>();
    let uniq_seqs = hash_maps.iter().map(|h| h.len()).sum::<usize>();

    ggcat_logging::info!("Stats for bucket: {}", bucket_index);
    ggcat_logging::info!(
        "FSIZE: {} SEQUENCES: {}/{} UNIQUE_RATIO: {} COMPR_RATIO: {} ",
        file_size,
        tot_seqs,
        uniq_seqs,
        (tot_seqs as f64 / uniq_seqs as f64),
        (file_size as f64 / tot_seqs as f64)
    );
    ggcat_logging::info!("Results: {}", counters_string);
}
