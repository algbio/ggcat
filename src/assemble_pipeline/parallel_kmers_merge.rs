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

use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::config::{BucketIndexType, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE};
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
use crate::pipeline_common::kmers_transform::structs::ReadRef;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory, ReadDispatchInfo,
    MERGE_BUCKETS_COUNT,
};
use crate::rolling::minqueue::RollingMinQueue;
use crate::utils::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::utils::debug_utils::debug_increase;
use crate::utils::{get_memory_mode, Utils};
use crate::KEEP_FILES;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crossbeam::queue::*;
use hashbrown::HashMap;
use parallel_processor::binary_writer::{BinaryWriter, StorageMode};
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::mem_tracker::tracked_vec::TrackedVec;
#[cfg(feature = "mem-analysis")]
use parallel_processor::mem_tracker::MemoryInfo;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
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
use std::slice::from_raw_parts;
use std::sync::Arc;
use structs::*;

pub const READ_FLAG_INCL_BEGIN: u8 = (1 << 0);
pub const READ_FLAG_INCL_END: u8 = (1 << 1);

pub mod structs {
    use crate::config::BucketIndexType;
    use crate::io::concurrent::intermediate_storage::SequenceExtraData;
    use crate::io::concurrent::intermediate_storage_single::IntermediateSequencesStorageSingleBucket;
    use std::io::Write;
    use std::path::PathBuf;

    pub struct MapEntry<CHI> {
        pub count: usize,
        pub ignored: bool,
        pub color_index: CHI,
    }

    pub struct ResultsBucket<'a, X: SequenceExtraData> {
        pub read_index: u64,
        pub bucket_ref: IntermediateSequencesStorageSingleBucket<'a, X>,
    }

    impl<'a, X: SequenceExtraData> ResultsBucket<'a, X> {
        pub fn add_read(&mut self, el: X, read: &[u8]) -> u64 {
            self.bucket_ref.add_read(el, read);
            let read_index = self.read_index;
            self.read_index += 1;
            read_index
        }

        pub fn get_bucket_index(&self) -> BucketIndexType {
            self.bucket_ref.get_bucket_index()
        }
    }

    pub struct RetType {
        pub sequences: Vec<PathBuf>,
        pub hashes: Vec<PathBuf>,
    }
}

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

struct GlobalMergeData<'a, MH: HashFunctionFactory, CX: ColorsManager> {
    k: usize,
    m: usize,
    buckets_count: usize,
    min_multiplicity: usize,
    colors_global_table: &'a CX::GlobalColorsTable,
    output_results_buckets: SegQueue<ResultsBucketType<'a, MH, CX>>,
    hashes_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
}

pub type ResultsBucketType<'a, MH: HashFunctionFactory, CX: ColorsManager> = ResultsBucket<
    'a,
    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::PartialUnitigsColorStructure,
>;

struct ParallelKmersMergeFactory<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, MH, CX)>,
);

impl<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersMergeFactory<H, MH, CX>
{
    type GlobalExtraData<'a> = GlobalMergeData<'a, MH, CX>;
    type InputBucketExtraData = KmersFlags<CX::MinimizerBucketingSeqColorDataType>;
    type IntermediateExtraData = CX::MinimizerBucketingSeqColorDataType;
    type ExecutorType<'a> = ParallelKmersMerge<'a, H, MH, CX>;

    const FLAGS_COUNT: usize = 2;

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
        #[cfg(feature = "mem-analysis")]
        let info = parallel_processor::mem_tracker::create_hashmap_entry(
            std::panic::Location::caller(),
            "HashMap",
        );

        Self::ExecutorType::<'a> {
            results_buckets_counter: MERGE_BUCKETS_COUNT,
            current_bucket: global_data.output_results_buckets.pop().unwrap(),
            hashes_tmp: BucketsThreadDispatcher::new(
                DEFAULT_PER_CPU_BUFFER_SIZE,
                &global_data.hashes_buckets,
            ),
            forward_seq: Vec::with_capacity(global_data.k),
            backward_seq: Vec::with_capacity(global_data.k),
            temp_colors: CX::ColorsMergeManagerType::<MH>::allocate_temp_buffer_structure(),
            unitigs_temp_colors: CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure(),
            rcorrect_reads: TrackedVec::new(),
            rhash_map: HashMap::with_capacity(4096),
            #[cfg(feature = "mem-analysis")]
            hmap_meminfo: info,
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersMerge<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> {
    results_buckets_counter: usize,
    current_bucket: ResultsBucketType<'x, MH, CX>,
    hashes_tmp:
        BucketsThreadDispatcher<'x, LockFreeBinaryWriter, HashEntry<MH::HashTypeUnextendable>>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    temp_colors:
        <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::ColorsBufferTempStructure,
    unitigs_temp_colors:
        <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::TempUnitigColorStructure,

    rcorrect_reads: TrackedVec<(MH::HashTypeExtendable, *const u8, u8, bool, bool)>,
    rhash_map: HashMap<
        MH::HashTypeUnextendable,
        MapEntry<
            <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::HashMapTempColorIndex,
        >,
    >,
    #[cfg(feature = "mem-analysis")]
    hmap_meminfo: Arc<MemoryInfo>,
    _phantom: PhantomData<H>,
}

impl<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutor<'x, ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMerge<'x, H, MH, CX>
{
    fn preprocess_bucket(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
        input_extra_data: <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::InputBucketExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<
        <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::IntermediateExtraData,
    >{
        let decr_val = ((read.bases_count() == global_data.k)
            && (input_extra_data.0 & READ_FLAG_INCL_END) == 0) as usize;

        let hashes = H::new(
            read.sub_slice((1 - decr_val)..(global_data.k - decr_val)),
            global_data.m,
        );

        let minimizer = hashes
            .iter()
            .min_by_key(|k| H::get_full_minimizer(k.to_unextendable()))
            .unwrap();

        let bucket = H::get_second_bucket(minimizer.to_unextendable())
            % (MERGE_BUCKETS_COUNT as BucketIndexType);

        ReadDispatchInfo {
            bucket,
            hash: H::get_sorting_hash(minimizer.to_unextendable()),
            flags: input_extra_data.0,
            extra_data: input_extra_data.1,
        }
    }

    fn maybe_swap_bucket(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
    ) {
        if self.results_buckets_counter == 0 {
            self.results_buckets_counter = MERGE_BUCKETS_COUNT;
            replace_with::replace_with_or_abort(&mut self.current_bucket, |results_bucket| {
                global_data.output_results_buckets.push(results_bucket);
                global_data.output_results_buckets.pop().unwrap()
            });
        }
        self.results_buckets_counter -= 1;
    }

    fn process_group(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
        reads: &[ReadRef],
        memory: &[u8],
    ) {
        let k = global_data.k;
        let bucket_index = self.current_bucket.get_bucket_index();
        let buckets_count = global_data.buckets_count;

        {
            CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut self.temp_colors);
            self.rhash_map.clear();
            self.rcorrect_reads.clear();

            let mut tot_reads = 0;
            let mut tot_chars = 0;

            for packed_read in reads {
                let (kmer_flags, read, color) = packed_read
                    .unpack(memory, <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT);

                let hashes = MH::new(read, k);

                let last_hash_pos = read.bases_count() - k;
                let mut did_max = false;

                for (idx, hash) in hashes.iter_enumerate() {
                    let position_ptr = unsafe { read.as_ptr().add(idx / 4) };
                    let begin_ignored = kmer_flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                    let end_ignored = kmer_flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;
                    assert!(idx <= last_hash_pos);
                    did_max |= idx == last_hash_pos;

                    let entry = self
                        .rhash_map
                        .entry(hash.to_unextendable())
                        .or_insert(MapEntry {
                            ignored: begin_ignored || end_ignored,
                            count: 0,
                            color_index: CX::ColorsMergeManagerType::<MH>::new_color_index(),
                        });

                    entry.count += 1;
                    CX::ColorsMergeManagerType::<MH>::add_temp_buffer_structure_el(
                        &mut self.temp_colors,
                        &color,
                        (idx, hash.to_unextendable()),
                    );

                    if entry.count == global_data.min_multiplicity {
                        self.rcorrect_reads.push((
                            hash,
                            position_ptr,
                            (idx % 4) as u8,
                            begin_ignored,
                            end_ignored,
                        ));

                        #[cfg(feature = "mem-analysis")]
                        {
                            let len = self.rcorrect_reads.len();
                            self.rcorrect_reads.update_maximum_usage(len);
                        }
                    }
                }
                assert!(did_max);
                tot_reads += 1;
                tot_chars += read.bases_count();
            }

            if CX::COLORS_ENABLED {
                CX::ColorsMergeManagerType::<MH>::process_colors(
                    &global_data.colors_global_table,
                    &mut self.temp_colors,
                    &mut self.rhash_map,
                    global_data.min_multiplicity,
                );
            }

            for (hash, read_bases_start, reads_offset, begin_ignored, end_ignored) in
                self.rcorrect_reads.drain(..)
            {
                let reads_slice = unsafe {
                    from_raw_parts(read_bases_start, (k + reads_offset as usize + 3) / 4)
                };

                let cread =
                    CompressedRead::from_compressed_reads(reads_slice, reads_offset as usize, k);

                let rhentry = self.rhash_map.get_mut(&hash.to_unextendable()).unwrap();
                if rhentry.count == usize::MAX {
                    continue;
                }
                rhentry.count = usize::MAX;

                CX::ColorsMergeManagerType::<MH>::reset_unitig_color_structure(
                    &mut self.unitigs_temp_colors,
                );

                unsafe {
                    self.forward_seq.set_len(k);
                    self.backward_seq.set_len(k);
                }

                cread.write_to_slice(&mut self.forward_seq[..]);
                self.backward_seq[..].copy_from_slice(&self.forward_seq[..]);
                self.backward_seq.reverse();

                CX::ColorsMergeManagerType::<MH>::extend_forward(
                    &mut self.unitigs_temp_colors,
                    rhentry,
                );

                let mut try_extend_function =
                    |output: &mut Vec<u8>,
                     compute_hash_fw: fn(
                         hash: MH::HashTypeExtendable,
                         klen: usize,
                         out_b: u8,
                         in_b: u8,
                     )
                         -> MH::HashTypeExtendable,
                     compute_hash_bw: fn(
                         hash: MH::HashTypeExtendable,
                         klen: usize,
                         out_b: u8,
                         in_b: u8,
                     )
                         -> MH::HashTypeExtendable,
                     colors_function: fn(
                         ts: &mut  <CX::ColorsMergeManagerType::<MH> as ColorsMergeManager<MH, CX>>::TempUnitigColorStructure,
                         entry: &MapEntry< <CX::ColorsMergeManagerType::<MH> as ColorsMergeManager<MH, CX>>::HashMapTempColorIndex>,
                     )| {
                        let mut temp_data = (hash, 0);
                        let mut current_hash;

                        return 'ext_loop: loop {
                            let mut count = 0;
                            current_hash = temp_data.0;
                            for idx in 0..4 {
                                let new_hash = compute_hash_fw(
                                    current_hash,
                                    k,
                                    Utils::compress_base(output[output.len() - k]),
                                    idx,
                                );
                                if let Some(hash) =
                                self.rhash_map.get(&new_hash.to_unextendable())
                                {
                                    if hash.count >= global_data.min_multiplicity {
                                        // println!("Forward match extend read {:x?}!", new_hash);
                                        count += 1;
                                        temp_data = (new_hash, idx);
                                    }
                                }
                            }

                            if count == 1 {
                                // Test for backward branches
                                {
                                    let mut ocount = 0;
                                    let new_hash = temp_data.0;
                                    for idx in 0..4 {
                                        let bw_hash =
                                            compute_hash_bw(new_hash, k, temp_data.1, idx);
                                        if let Some(hash) =
                                        self.rhash_map.get(&bw_hash.to_unextendable())
                                        {
                                            if hash.count >= global_data.min_multiplicity {
                                                // println!("Backward match extend read {:x?}!", bw_hash);
                                                if ocount > 0 {
                                                    break 'ext_loop (current_hash, false);
                                                }
                                                ocount += 1;
                                            }
                                        }
                                    }
                                    assert_eq!(ocount, 1);
                                }

                                let entryref = self.rhash_map
                                    .get_mut(&temp_data.0.to_unextendable())
                                    .unwrap();

                                let already_used = entryref.count == usize::MAX;

                                // Found a cycle unitig
                                if already_used {
                                    break (temp_data.0, false);
                                }

                                // Flag the entry as already used
                                entryref.count = usize::MAX;

                                if CX::COLORS_ENABLED {
                                    colors_function(&mut self.unitigs_temp_colors, entryref);
                                }

                                output.push(Utils::decompress_base(temp_data.1));

                                // Found a continuation into another bucket
                                let contig_break = entryref.ignored;
                                if contig_break {
                                    break (temp_data.0, contig_break);
                                }
                            } else {
                                break (temp_data.0, false);
                            }
                        };
                    };

                let fw_hash = {
                    if end_ignored {
                        Some(hash)
                    } else {
                        let (fw_hash, end_ignored) = try_extend_function(
                            &mut self.forward_seq,
                            MH::manual_roll_forward,
                            MH::manual_roll_reverse,
                            CX::ColorsMergeManagerType::<MH>::extend_forward,
                        );
                        match end_ignored {
                            true => Some(fw_hash),
                            false => None,
                        }
                    }
                };

                let bw_hash = {
                    if begin_ignored {
                        Some(hash)
                    } else {
                        let (bw_hash, begin_ignored) = try_extend_function(
                            &mut self.backward_seq,
                            MH::manual_roll_reverse,
                            MH::manual_roll_forward,
                            CX::ColorsMergeManagerType::<MH>::extend_backward,
                        );
                        match begin_ignored {
                            true => Some(bw_hash),
                            false => None,
                        }
                    }
                };

                let out_seq = {
                    self.backward_seq.reverse();
                    self.backward_seq.extend_from_slice(&self.forward_seq[k..]);
                    &self.backward_seq[..]
                };

                // <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                //     MH,
                //     CX,
                // >>::debug_tucs(&unitigs_temp_colors, out_seq);

                let read_index = self.current_bucket.add_read(<CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                    MH,
                    CX,
                >>::encode_part_unitigs_colors(&mut self.unitigs_temp_colors), out_seq);

                if let Some(fw_hash) = fw_hash {
                    let fw_hash = fw_hash.to_unextendable();
                    let fw_hash_sr = HashEntry {
                        hash: fw_hash,
                        bucket: bucket_index,
                        entry: read_index,
                        direction: Direction::Forward,
                    };
                    let fw_bucket_index =
                        MH::get_first_bucket(fw_hash) % (buckets_count as BucketIndexType);
                    self.hashes_tmp
                        .add_element(fw_bucket_index, &(), &fw_hash_sr);
                }

                if let Some(bw_hash) = bw_hash {
                    let bw_hash = bw_hash.to_unextendable();

                    let bw_hash_sr = HashEntry {
                        hash: bw_hash,
                        bucket: bucket_index,
                        entry: read_index,
                        direction: Direction::Backward,
                    };
                    let bw_bucket_index =
                        MH::get_first_bucket(bw_hash) % (buckets_count as BucketIndexType);
                    self.hashes_tmp
                        .add_element(bw_bucket_index, &(), &bw_hash_sr);
                }
            }

            #[cfg(feature = "mem-analysis")]
            self.hmap_meminfo.bytes.store(self.rhash_map.capacity() * (
                std::mem::size_of::<MH::HashTypeUnextendable>() +
                std::mem::size_of::<MapEntry<
                    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::HashMapTempColorIndex,
                >>() + 8
                ), Ordering::Relaxed)
        }
    }

    fn finalize(
        self,
        _global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
    ) {
        self.hashes_tmp.finalize();
    }
}

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
        save_memory: bool,
    ) -> RetType {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: kmers merge".to_string());

        let mut hashes_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            &(
                out_directory.as_ref().join("hashes"),
                get_memory_mode(SwapPriority::HashBuckets),
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

        let global_data = GlobalMergeData::<MH, CX> {
            k,
            m,
            buckets_count,
            min_multiplicity,
            colors_global_table,
            output_results_buckets,
            hashes_buckets: &hashes_buckets,
        };

        KmersTransform::parallel_kmers_transform::<ParallelKmersMergeFactory<H, MH, CX>>(
            file_inputs,
            buckets_count,
            threads_count,
            global_data,
            save_memory,
        );

        RetType {
            sequences,
            hashes: hashes_buckets.finalize(),
        }
    }
}
