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

use crate::assemble_pipeline::current_kmers_merge::structs::ResultsBucket;
use crate::assemble_pipeline::parallel_kmers_merge::structs::{MapEntry, RetType};
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
use crate::pipeline_common::kmers_transform::structs::ReadRef;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory, ReadDispatchInfo,
    MERGE_BUCKETS_COUNT,
};
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
use std::slice::from_raw_parts;
use std::sync::Arc;

pub const READ_FLAG_INCL_BEGIN: u8 = (1 << 0);
pub const READ_FLAG_INCL_END: u8 = (1 << 1);

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
    hashes_buckets: &'a MultiThreadBuckets<BinaryWriter>,
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
        Self::ExecutorType::<'a> {
            results_buckets_counter: MERGE_BUCKETS_COUNT,
            current_bucket: global_data.output_results_buckets.pop().unwrap(),
            hashes_tmp: BucketsThreadDispatcher::new(
                MAX_HASHES_FOR_FLUSH,
                &global_data.hashes_buckets,
            ),
            forward_seq: Vec::with_capacity(global_data.k),
            backward_seq: Vec::with_capacity(global_data.k),
            temp_colors: CX::ColorsMergeManagerType::<MH>::allocate_temp_buffer_structure(),
            unitigs_temp_colors: CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure(),
            rcorrect_reads: vec![],
            rhash_map: HashMap::with_capacity(4096),
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersMerge<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> {
    results_buckets_counter: usize,
    current_bucket: ResultsBucketType<'x, MH, CX>,
    hashes_tmp: BucketsThreadDispatcher<'x, BinaryWriter, HashEntry<MH::HashTypeUnextendable>>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    temp_colors:
        <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::ColorsBufferTempStructure,
    unitigs_temp_colors:
        <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::TempUnitigColorStructure,

    rcorrect_reads: Vec<(MH::HashTypeExtendable, *const u8, u8, bool, bool)>,
    rhash_map: HashMap<
        MH::HashTypeUnextendable,
        MapEntry<
            <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::HashMapTempColorIndex,
        >,
    >,
    _phantom: PhantomData<H>,
}

const MAX_HASHES_FOR_FLUSH: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);
const MAX_TEMP_SEQUENCES_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);

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
            .min_by_key(|k| H::get_minimizer(k.to_unextendable()))
            .unwrap();

        let bucket = H::get_second_bucket(minimizer.to_unextendable())
            % (MERGE_BUCKETS_COUNT as BucketIndexType);

        ReadDispatchInfo {
            bucket,
            hash: H::get_u64(minimizer.to_unextendable()),
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
        reads: &[crate::pipeline_common::kmers_transform::structs::ReadRef],
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

            for &ReadRef { mut read_start, .. } in reads {
                let (read_len, kmer_flags) = decode_varint_flags::<_>(
                    || {
                        let x = unsafe { *read_start };
                        read_start = unsafe { read_start.add(1) };
                        Some(x)
                    },
                    <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT,
                )
                .unwrap();

                let read_bases_start = read_start;
                let read_len = read_len as usize;
                let read_len_bytes = (read_len + 3) / 4;

                let read_slice = unsafe { from_raw_parts(read_bases_start, read_len_bytes) };

                let read = CompressedRead::new_from_compressed(read_slice, read_len);

                let color = unsafe {
                    let color_slice_ptr = read_bases_start.add(read_len_bytes);
                    CX::MinimizerBucketingSeqColorDataType::decode_from_pointer(color_slice_ptr)
                        .unwrap()
                };

                let hashes = MH::new(read, k);

                let last_hash_pos = read_len - k;
                let mut did_max = false;

                for (idx, hash) in hashes.iter_enumerate() {
                    let position_ptr = unsafe { read_bases_start.add(idx / 4) };
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
                        MH::get_bucket(fw_hash) % (buckets_count as BucketIndexType);
                    self.hashes_tmp
                        .add_element(fw_bucket_index, &(), fw_hash_sr);
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
                        MH::get_bucket(bw_hash) % (buckets_count as BucketIndexType);
                    self.hashes_tmp
                        .add_element(bw_bucket_index, &(), bw_hash_sr);
                }
            }
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
        'a,
        H: HashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
        P: AsRef<Path> + std::marker::Sync,
    >(
        file_inputs: Vec<PathBuf>,
        colors_global_table: &'a CX::GlobalColorsTable,
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
        );

        RetType {
            sequences,
            hashes: hashes_buckets.finalize(),
        }
    }
}
