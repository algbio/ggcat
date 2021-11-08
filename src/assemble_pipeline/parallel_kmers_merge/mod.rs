mod process_subbucket;
pub mod structs;

use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use std::cmp::min;
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{stdout, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use std::ops::{Index, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rayon::prelude::*;

use crate::assemble_pipeline::current_kmers_merge::process_subbucket::{
    GlobalSubBucketContext, ResultsBucketType, SubBucketThreadContext, READ_FLAG_INCL_END,
};
use crate::assemble_pipeline::current_kmers_merge::structs::{BucketProcessData, ReadRef};
use crate::assemble_pipeline::parallel_kmers_merge::structs::RetType;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::hashes::{ExtendableHashTraitType, HashFunction};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsReader, IntermediateReadsWriter, SequenceExtraData,
};
use crate::io::concurrent::intermediate_storage_single::IntermediateSequencesStorageSingleBucket;
use crate::io::reads_reader::ReadsReader;
use crate::io::sequences_reader::FastaSequence;
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::hash_entry::HashEntry;
use crate::io::varint::{decode_varint, decode_varint_flags, encode_varint_flags};
use crate::rolling::minqueue::RollingMinQueue;
use crate::types::BucketIndexType;
use crate::utils::chunked_vector::{ChunkedVector, ChunkedVectorPool};
use crate::utils::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::utils::debug_utils::debug_increase;
use crate::utils::flexible_pool::FlexiblePool;
use crate::utils::Utils;
use crate::{DEFAULT_BUFFER_SIZE, KEEP_FILES};
use bitvec::ptr::Mut;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crossbeam::queue::*;
use hashbrown::HashMap;
use parallel_processor::binary_writer::{BinaryWriter, StorageMode};
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::multi_thread_buckets::{
    BucketType, BucketsThreadDispatcher, MultiThreadBuckets,
};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadChainObject, ThreadPoolDefinition, ThreadPoolsChain,
};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::process::exit;
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct KmersFlags<CX: MinimizerBucketingSeqColorData>(pub u8, pub CX);

impl<CX: MinimizerBucketingSeqColorData> SequenceExtraData for KmersFlags<CX> {
    #[inline(always)]
    fn decode(mut reader: impl Read) -> Option<Self> {
        Some(Self(reader.read_u8().ok()?, CX::decode(reader)?))
    }

    #[inline(always)]
    fn encode(&self, mut writer: impl Write) {
        writer.write_u8(self.0).unwrap();
        self.1.encode(writer);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.1.max_size() + 1
    }
}

pub const MERGE_BUCKETS_COUNT: usize = 256;
const BUFFER_CHUNK_SIZE: usize = 1024 * 128;

impl AssemblePipeline {
    pub fn parallel_kmers_merge<
        H: HashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
        P: AsRef<Path> + std::marker::Sync,
    >(
        file_inputs: Vec<PathBuf>,
        colors_global_table: &CX::GlobalColorsTable,
        buckets_count: usize,
        min_multiplicity: usize,
        out_directory: P,
        k: usize,
        m: usize,
        threads_count: usize,
    ) -> RetType {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: kmers merge".to_string());

        static CURRENT_BUCKETS_COUNT: AtomicU64 = AtomicU64::new(0);

        const NONE: Option<Mutex<BufWriter<File>>> = None;
        let mut hashes_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                out_directory.as_ref().join("hashes"),
                StorageMode::Plain {
                    buffer_size: DEFAULT_BUFFER_SIZE,
                },
            ),
            None,
        );

        let mut sequences = Vec::new();

        let mut reads_buckets =
            MultiThreadBuckets::<
                IntermediateReadsWriter<
                    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                        MH,
                        CX,
                    >>::PartialUnitigsColorStructure,
                >,
            >::new(buckets_count, &out_directory.as_ref().join("result"), None);

        let files_queue = ArrayQueue::new(file_inputs.len());
        file_inputs
            .into_iter()
            .for_each(|f| files_queue.push(f).unwrap());

        let vecs_pool = FlexiblePool::new(8192);
        let vecs_process_queue = Arc::new(SegQueue::new());

        let mut last_info_log = Mutex::new(Instant::now());

        const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(15);

        let open_bucket = || {
            let file = files_queue.pop()?;

            let mut last_info_log = last_info_log.lock();
            if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
                println!(
                    "Processing bucket {} of {} {}",
                    buckets_count - files_queue.len(),
                    buckets_count,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
                *last_info_log = Instant::now();
            }

            Some(Arc::new(BucketProcessData::<CX>::new(
                file,
                vecs_pool.clone(),
                vecs_process_queue.clone(),
            )))
        };

        let current_bucket = RwLock::new(open_bucket());
        let chunked_pool = ChunkedVectorPool::new(BUFFER_CHUNK_SIZE);
        let reading_finished = AtomicBool::new(false);
        const MAX_HASHES_FOR_FLUSH: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);
        const MAX_TEMP_SEQUENCES_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);

        let output_results_buckets = SegQueue::new();
        for bucket_index in 0..buckets_count {
            let bucket_read = ResultsBucketType::<MH, CX> {
                read_index: 0,
                bucket_ref: IntermediateSequencesStorageSingleBucket::new(
                    bucket_index as BucketIndexType,
                    &reads_buckets,
                ),
            };

            sequences.push(bucket_read.bucket_ref.get_path());
            output_results_buckets.push(bucket_read);
        }

        let subbucket_context = GlobalSubBucketContext::<CX> {
            k,
            min_multiplicity,
            colors_map: colors_global_table,
            buckets_count,
        };

        crossbeam::thread::scope(|s| {
            for i in 0..min(buckets_count, threads_count) {
                s.spawn(|_| {
                    let mut subbucket_thread_context = SubBucketThreadContext::<MH, CX> {
                        gctx: &subbucket_context,
                        current_bucket: output_results_buckets.pop().unwrap(),
                        hashes_tmp: BucketsThreadDispatcher::new(
                            MAX_HASHES_FOR_FLUSH,
                            &hashes_buckets,
                        ),
                    };

                    let mut results_buckets_counter = MERGE_BUCKETS_COUNT;
                    let mut buckets: Vec<ChunkedVector<u8>> = vec![];

                    let mut process_pending_reads = || {
                        while let Some((seqs, memory_ref)) = vecs_process_queue.pop() {
                            if results_buckets_counter == 0 {
                                results_buckets_counter = MERGE_BUCKETS_COUNT;
                                replace_with::replace_with_or_abort(
                                    &mut subbucket_thread_context.current_bucket,
                                    |results_bucket| {
                                        output_results_buckets.push(results_bucket);
                                        output_results_buckets.pop().unwrap()
                                    },
                                );
                            }

                            process_subbucket::process_subbucket::<MH, CX>(
                                &mut subbucket_thread_context,
                                seqs,
                            );

                            results_buckets_counter -= 1;

                            drop(memory_ref);
                        }
                    };

                    'outer_loop: loop {
                        if reading_finished.load(Ordering::Relaxed) {
                            process_pending_reads();
                            break 'outer_loop;
                        }

                        buckets.clear();
                        buckets.resize_with(MERGE_BUCKETS_COUNT, || {
                            ChunkedVector::new(chunked_pool.clone())
                        });

                        let mut bucket = match current_bucket.read().clone() {
                            None => continue,
                            Some(val) => val,
                        };
                        let mut cmp_reads =
                            BucketsThreadDispatcher::new(MAX_TEMP_SEQUENCES_SIZE, &bucket.buckets);

                        let mut continue_read = true;

                        while continue_read {
                            process_pending_reads();

                            continue_read = bucket.reader.read_parallel(|flags, x| {
                                let decr_val = ((x.bases_count() == k)
                                    && (flags.0 & READ_FLAG_INCL_END) == 0)
                                    as usize;

                                let hashes = H::new(x.sub_slice((1 - decr_val)..(k - decr_val)), m);

                                let minimizer = hashes
                                    .iter()
                                    .min_by_key(|k| H::get_minimizer(k.to_unextendable()))
                                    .unwrap();

                                let bucket_index =
                                    H::get_second_bucket(minimizer.to_unextendable())
                                        % (MERGE_BUCKETS_COUNT as BucketIndexType);

                                let bases_slice = x.get_compr_slice();

                                let pointer = buckets[bucket_index as usize]
                                    .ensure_reserve(10 + bases_slice.len() + flags.1.max_size());

                                encode_varint_flags::<_, _, 2>(
                                    |slice| {
                                        buckets[bucket_index as usize].push_contiguous_slice(slice)
                                    },
                                    x.bases_count() as u64,
                                    flags.0,
                                );
                                buckets[bucket_index as usize].push_contiguous_slice(bases_slice);
                                flags.1.encode(&mut buckets[bucket_index as usize]);

                                cmp_reads.add_element(
                                    bucket_index,
                                    &(),
                                    ReadRef {
                                        read_start: pointer,
                                        hash: H::get_u64(minimizer.to_unextendable()),
                                    },
                                );
                            });
                            break;
                        }

                        bucket.add_chunks_refs(&mut buckets);
                        cmp_reads.finalize();

                        drop(bucket);

                        let mut wbucket = current_bucket.write();
                        if wbucket
                            .as_ref()
                            .map(|x| x.reader.is_finished())
                            .unwrap_or(false)
                        {
                            if let Some(bucket) = open_bucket() {
                                *wbucket = Some(bucket);
                            } else {
                                wbucket.take();
                                reading_finished.store(true, Ordering::Relaxed);
                            }
                        }
                    }

                    subbucket_thread_context.hashes_tmp.finalize()
                });
            }
        });

        // Close all the results buckets
        drop(output_results_buckets);

        // file_inputs.par_iter().for_each(|input| {
        //     println!("Processing {}", input.display());
        //
        //     let bucket_index = Utils::get_bucket_index(&input);
        //
        //     let mut temp_bucket_writer =
        //         IntermediateSequencesStorageSingleBucket::new(bucket_index, &reads_buckets);
        //
        //     let incr_bucket_index_val = incr_bucket_index.fetch_add(1, Ordering::Relaxed);

        //
        // });

        RetType {
            sequences,
            hashes: hashes_buckets.finalize(),
        }
    }
}
