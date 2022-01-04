use super::structs::ReadRef;
use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager};
use crate::hashes::HashFunction;
use crate::hashes::{ExtendableHashTraitType, HashFunctionFactory, HashableSequence};
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::io::structs::hash_entry::{Direction, HashEntry};
use crate::io::varint::decode_varint_flags;
use crate::pipeline_common::kmers_transform::{
    KmersTransformExecutor, KmersTransformExecutorFactory,
};
use crate::types::BucketIndexType;
use crate::utils::async_vec::AsyncVec;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::debug_utils::debug_increase;
use crate::utils::Utils;
use hashbrown::HashMap;
use parallel_processor::binary_writer::BinaryWriter;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use std::mem::size_of;
use std::slice::from_raw_parts;

pub fn process_subbucket<'a, F: KmersTransformExecutorFactory>(
    global_data: &F::GlobalExtraData<'a>,
    cmp_read: &mut AsyncVec<ReadRef>,
    executor: &mut F::ExecutorType<'a>,
) {
    let cmp_read_slice = cmp_read.as_slice_mut();

    struct Compare {};
    impl SortKey<ReadRef> for Compare {
        type KeyType = u64;
        const KEY_BITS: usize = size_of::<u64>() * 8;

        #[inline(always)]
        fn compare(left: &ReadRef, right: &ReadRef) -> std::cmp::Ordering {
            left.hash.cmp(&right.hash)
        }

        #[inline(always)]
        fn get_shifted(value: &ReadRef, rhs: u8) -> u8 {
            (value.hash >> rhs) as u8
        }
    }

    fast_smart_radix_sort::<_, Compare, false>(cmp_read_slice);

    for slice in cmp_read_slice.group_by(|a, b| a.hash == b.hash) {
        executor.process_group(global_data, slice);
    }
}
