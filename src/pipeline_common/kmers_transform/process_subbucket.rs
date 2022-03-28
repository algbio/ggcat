use super::structs::ReadRef;
use crate::config::{SwapPriority, FIRST_BUCKET_BITS, RESPLIT_MINIMIZER_MASK};
use crate::hashes::HashableSequence;
use crate::pipeline_common::kmers_transform::structs::ProcessQueueItem;
use crate::pipeline_common::kmers_transform::{
    KmersTransformExecutor, KmersTransformExecutorFactory,
};
use crate::pipeline_common::minimizer_bucketing::{
    MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory,
};
use crossbeam::queue::SegQueue;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::file::writer::FileWriter;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn process_subbucket<'a, 'b: 'a, F: KmersTransformExecutorFactory>(
    global_data: &F::GlobalExtraData<'b>,
    mut bucket_reader: LockFreeBinaryReader,
    executor: &mut F::ExecutorType<'a>,
    file_path: &Path,
    can_resplit: bool,
    is_outlier: bool,
) {
    // let mut paths: Vec<_> = Vec::new();

    if true {
        // !is_outlier || !can_resplit || bucket_reader.total_file_size() < 1000000 {
        executor.process_group(global_data, bucket_reader);
    }
    // else {
    //     println!("Resplitting bucket {}", file_path.display());
    //     // executor.process_group(global_data, bucket_reader);
    //     // return;
    //     let buckets_log2 = FIRST_BUCKET_BITS;
    //     let buckets_count = 1 << buckets_log2;
    //
    //     let mut sub_buckets = Vec::with_capacity(buckets_count);
    //
    //     for i in 0..buckets_count {
    //         sub_buckets.push(FileWriter::create(
    //             {
    //                 let mut name = file_path.file_name().unwrap().to_os_string();
    //                 name.push(format!("-{}", i));
    //                 file_path.parent().unwrap().join(name)
    //             },
    //             MemoryFileMode::PreferMemory {
    //                 swap_priority: SwapPriority::KmersMergeBuckets,
    //             },
    //         ));
    //     }
    //
    //     let mut splitter = F::new_resplitter(global_data);
    //     let mut temp_mem_decode = Vec::with_capacity(256);
    //     let mut temp_mem_encode = Vec::with_capacity(256);
    //     let mut preproc_info = <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::PreprocessInfo::default();
    //
    //     while let Some((flags, bases, extra)) = ReadRef::unpack::<
    //         F::AssociatedExtraData,
    //         _,
    //         F::FLAGS_COUNT,
    //     >(&mut bucket_reader, &mut temp_mem_decode)
    //     {
    //         splitter.reprocess_sequence(flags, &extra, &mut preproc_info);
    //         splitter.process_sequence::<_, _, { RESPLIT_MINIMIZER_MASK }>(
    //             &preproc_info,
    //             bases,
    //             0..bases.bases_count(),
    //             |bucket, seq, flags, extra| {
    //                 let data = ReadRef::pack::<_, F::FLAGS_COUNT>(
    //                     flags,
    //                     seq,
    //                     &extra,
    //                     &mut temp_mem_encode,
    //                 );
    //                 sub_buckets[bucket as usize].write(data).unwrap();
    //             },
    //         );
    //     }
    //
    //     paths.extend(sub_buckets.into_iter().map(|x| x.get_path()));
    //     bucket_reader.close_and_remove(true);
    // }

    // if paths.len() > 0 {
    //     for path in paths {
    //         process_queue.push(ProcessQueueItem {
    //             path,
    //             can_resplit: false,
    //             buffers_counter: Arc::new(())
    //         });
    //     }
    // }
}
