use super::structs::ReadRef;
use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager};
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_MINIMIZER_MASK, FIRST_BUCKET_BITS,
    RESPLIT_MINIMIZER_MASK,
};
use crate::hashes::HashFunction;
use crate::hashes::{ExtendableHashTraitType, HashFunctionFactory, HashableSequence};
use crate::io::concurrent::intermediate_storage::{IntermediateReadsWriter, SequenceExtraData};
use crate::io::structs::hash_entry::{Direction, HashEntry};
use crate::io::varint::{decode_varint, decode_varint_flags};
use crate::pipeline_common::kmers_transform::{
    KmersTransformExecutor, KmersTransformExecutorFactory,
};
use crate::pipeline_common::minimizer_bucketing::{
    MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory,
};
use crate::utils::compressed_read::CompressedRead;
use crate::utils::debug_utils::debug_increase;
use crate::utils::Utils;
use byteorder::{LittleEndian, ReadBytesExt};
use crossbeam::queue::SegQueue;
use hashbrown::HashMap;
use parallel_processor::binary_writer::BinaryWriter;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::file::writer::FileWriter;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::max;
use std::io::{Read, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use typenum::Unsigned;

pub fn process_subbucket<'a, F: KmersTransformExecutorFactory>(
    global_data: &F::GlobalExtraData<'a>,
    mut bucket_stream: FileReader,
    tmp_ref_vec: &mut Vec<ReadRef>,
    tmp_data_vec: &mut Vec<u8>,
    executor: &mut F::ExecutorType<'a>,
    file_path: &Path,
    vecs_process_queue: &Arc<SegQueue<(PathBuf, bool)>>,
    can_resplit: bool,
    is_outlier: bool,
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

        tmp_ref_vec.push(ReadRef::new(len, hash))
    }

    drop(bucket_stream);
    MemoryFs::remove_file(file_path, true);

    struct Compare {};
    impl SortKey<ReadRef> for Compare {
        type KeyType = u16;
        const KEY_BITS: usize = size_of::<u16>() * 8;

        #[inline(always)]
        fn compare(left: &ReadRef, right: &ReadRef) -> std::cmp::Ordering {
            left.get_hash().cmp(&right.get_hash())
        }

        #[inline(always)]
        fn get_shifted(value: &ReadRef, rhs: u8) -> u8 {
            (value.get_hash() >> rhs) as u8
        }
    }

    fast_smart_radix_sort::<_, Compare, false>(tmp_ref_vec);

    let mut paths: Vec<_> = Vec::new();

    for slice in tmp_ref_vec.group_by(|a, b| a.get_hash() == b.get_hash()) {
        if can_resplit
            && is_outlier
            && slice.len() > 1000000
            && slice.len() > (tmp_ref_vec.len() / 4)
        {
            let mut splitter = F::new_resplitter(global_data);
            let mut temp_mem = Vec::with_capacity(max(ReadRef::TMP_DATA_OFFSET, 256));

            let mut preproc_info = <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::PreprocessInfo::default();

            let buckets_log2 = FIRST_BUCKET_BITS;
            let buckets_count = (1 << buckets_log2);

            let mut sub_buckets = Vec::with_capacity(buckets_count);

            for i in 0..buckets_count {
                sub_buckets.push(FileWriter::create(
                    {
                        let mut name = file_path.file_name().unwrap().to_os_string();
                        name.push(format!("-{}", i));
                        file_path.parent().unwrap().join(name)
                    },
                    MemoryFileMode::PreferMemory {
                        swap_priority: SwapPriority::KmersMergeBuckets,
                    },
                ));
            }

            for read in slice {
                let (flags, bases, extra) =
                    read.unpack::<F::AssociatedExtraData, F::FLAGS_COUNT>(tmp_data_vec);

                splitter.reprocess_sequence(flags, &extra, &mut preproc_info);
                splitter.process_sequence::<_, _, { RESPLIT_MINIMIZER_MASK }>(
                    &preproc_info,
                    bases,
                    0..bases.bases_count(),
                    |bucket, seq, flags, extra, sorting_hash| {
                        let data = ReadRef::pack::<_, F::FLAGS_COUNT>(
                            sorting_hash,
                            flags,
                            seq,
                            &extra,
                            &mut temp_mem,
                        );
                        sub_buckets[bucket as usize].write(data);
                    },
                );
            }

            paths.extend(sub_buckets.into_iter().map(|x| x.get_path()));
        } else {
            executor.process_group(global_data, slice, tmp_data_vec);
        }
    }

    if paths.len() > 0 {
        *tmp_ref_vec = Vec::new();
        *tmp_data_vec = Vec::new();
        for path in paths {
            vecs_process_queue.push((path, false));
        }
    }
}
