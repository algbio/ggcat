use super::structs::ReadRef;
use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager};
use crate::hashes::HashFunction;
use crate::hashes::{ExtendableHashTraitType, HashFunctionFactory, HashableSequence};
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::io::structs::hash_entry::{Direction, HashEntry};
use crate::io::varint::{decode_varint, decode_varint_flags};
use crate::io::DataReader;
use crate::pipeline_common::kmers_transform::{
    KmersTransformExecutor, KmersTransformExecutorFactory,
};
use crate::config::BucketIndexType;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::debug_utils::debug_increase;
use crate::utils::Utils;
use byteorder::{LittleEndian, ReadBytesExt};
use hashbrown::HashMap;
use parallel_processor::binary_writer::BinaryWriter;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use std::io::Read;
use std::mem::size_of;
use std::slice::from_raw_parts;

pub fn process_subbucket<'a, F: KmersTransformExecutorFactory, R: DataReader>(
    global_data: &F::GlobalExtraData<'a>,
    mut bucket_stream: &mut R,
    tmp_ref_vec: &mut Vec<ReadRef>,
    tmp_data_vec: &mut Vec<u8>,
    executor: &mut F::ExecutorType<'a>,
) {

    while let Ok(hash) = bucket_stream.read_u16::<LittleEndian>() {

        let size = decode_varint(|| bucket_stream.read_u8().ok()).unwrap() as usize;

        tmp_data_vec.reserve(size);
        let len = tmp_data_vec.len();
        unsafe {
            tmp_data_vec.set_len(len + size);
        }
        bucket_stream
            .read_exact(&mut tmp_data_vec[len..(len + size)])
            .unwrap();

        tmp_ref_vec.push(ReadRef {
            read_start: len,
            hash,
        })
    }

    struct Compare {};
    impl SortKey<ReadRef> for Compare {
        type KeyType = u16;
        const KEY_BITS: usize = size_of::<u16>() * 8;

        #[inline(always)]
        fn compare(left: &ReadRef, right: &ReadRef) -> std::cmp::Ordering {
            left.hash.cmp(&right.hash)
        }

        #[inline(always)]
        fn get_shifted(value: &ReadRef, rhs: u8) -> u8 {
            (value.hash >> rhs) as u8
        }
    }

    fast_smart_radix_sort::<_, Compare, false>(tmp_ref_vec);

    for slice in tmp_ref_vec.group_by(|a, b| a.hash == b.hash) {
        executor.process_group(global_data, slice, tmp_data_vec);
    }
    tmp_ref_vec.clear();
    tmp_data_vec.clear();
}
