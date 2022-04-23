use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{color_types, ColorsManager};
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
};
use crate::hashes::HashFunction;
use crate::hashes::{ExtendableHashTraitType, HashableHashFunctionFactory};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::hash_entry::HashEntry;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory,
};
use crate::utils::get_memory_mode;
use crate::utils::owned_drop::OwnedDrop;
use crate::CompressedRead;
use crossbeam::queue::*;
use hashbrown::hash_map::Entry::{Occupied, Vacant};
use hashbrown::HashMap;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{LockFreeBucket, MultiThreadBuckets};
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
#[cfg(feature = "mem-analysis")]
use parallel_processor::mem_tracker::MemoryInfo;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use structs::*;

pub const READ_FLAG_INCL_BEGIN: u8 = 1 << 0;
pub const READ_FLAG_INCL_END: u8 = 1 << 1;

pub mod structs {
    use crate::config::BucketIndexType;
    use crate::hashes::ExtendableHashTraitType;
    use crate::hashes::HashFunction;
    use crate::hashes::HashableHashFunctionFactory;
    use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
    use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
    use crate::utils::owned_drop::OwnedDrop;
    use crate::CompressedRead;
    use parallel_processor::buckets::bucket_writer::BucketItem;
    use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
    use parallel_processor::buckets::LockFreeBucket;
    use std::cell::{Cell, RefCell};
    use std::hash::{Hash, Hasher};
    use std::marker::PhantomData;
    use std::path::PathBuf;

    pub struct ResultsBucket<X: SequenceExtraData> {
        pub read_index: u64,
        pub reads_writer: OwnedDrop<CompressedBinaryWriter>,
        pub temp_buffer: Vec<u8>,
        pub bucket_index: BucketIndexType,
        pub _phantom: PhantomData<X>,
    }

    impl<X: SequenceExtraData> ResultsBucket<X> {
        pub fn add_read(&mut self, el: X, read: &[u8]) -> u64 {
            self.temp_buffer.clear();
            CompressedReadsBucketHelper::<X, typenum::U0, false>::new(read, 0, 0)
                .write_to(&mut self.temp_buffer, &el);
            self.reads_writer.write_data(self.temp_buffer.as_slice());

            let read_index = self.read_index;
            self.read_index += 1;
            read_index
        }

        pub fn get_bucket_index(&self) -> BucketIndexType {
            self.bucket_index
        }
    }

    impl<X: SequenceExtraData> Drop for ResultsBucket<X> {
        fn drop(&mut self) {
            unsafe { self.reads_writer.take().finalize() }
        }
    }

    pub struct RetType {
        pub sequences: Vec<PathBuf>,
        pub hashes: Vec<PathBuf>,
    }

    thread_local! {
        pub static HMAP_CONFIG: RefCell<HMapConfig> = RefCell::new(HMapConfig {
            reads_vec: Vec::new(),
            hashes_cache: vec![(u64::MAX, u128::MAX); 8192],
            k_value: 0,
            pending_hash: 0
        });
    }

    const TAKE_LAST_ADDRESS: usize = u32::MAX as usize;
    const HASHES_CACHE_SIZE: usize = 8192;

    pub struct HMapConfig {
        pub reads_vec: Vec<u8>,
        hashes_cache: Vec<(u64, u128)>,
        pending_hash: u128,
        pub k_value: usize,
    }

    impl HMapConfig {
        pub fn reset(&mut self, k: usize) {
            self.k_value = k;
            self.reads_vec.clear();
            self.hashes_cache.fill((u64::MAX, u128::MAX));
        }
    }

    pub struct HMapKey<MH: HashableHashFunctionFactory> {
        addr: Cell<u32>,
        _phantom: PhantomData<MH>,
    }

    impl<MH: HashableHashFunctionFactory> Hash for HMapKey<MH> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            MH::write_to_hasher_u128(self.get_hash_u128(), state);
        }
    }

    impl<MH: HashableHashFunctionFactory> HMapKey<MH> {
        pub fn build_temporary(hash: MH::HashTypeUnextendable) -> Self {
            HMAP_CONFIG.with(|cfg| {
                cfg.borrow_mut().pending_hash = MH::get_u128(hash);
            });
            Self {
                addr: Cell::new(TAKE_LAST_ADDRESS as u32),
                _phantom: Default::default(),
            }
        }

        pub fn settle_temporaru(&self, addr: usize) {
            assert!(addr < u32::MAX as usize);
            self.addr.set(addr as u32);
            HMAP_CONFIG.with(|cfg| {
                let mut cfg_borrow = cfg.borrow_mut();
                cfg_borrow.hashes_cache[addr as usize % HASHES_CACHE_SIZE] =
                    (addr as u64, cfg_borrow.pending_hash);
            });
        }

        fn get_hash_u128(&self) -> u128 {
            HMAP_CONFIG.with(|cfg| {
                let mut cfg_borrow = cfg.borrow_mut();
                let addr = self.addr.get() as usize;
                if addr == TAKE_LAST_ADDRESS {
                    return cfg_borrow.pending_hash;
                } else {
                    let cache = &cfg_borrow.hashes_cache[addr as usize % HASHES_CACHE_SIZE];
                    if cache.0 == addr as u64 {
                        return cache.1;
                    } else {
                        let read = CompressedRead::from_compressed_reads(
                            &cfg_borrow.reads_vec,
                            addr,
                            cfg_borrow.k_value,
                        );
                        let hash_iter = MH::new(read, cfg_borrow.k_value);
                        let hash_value =
                            MH::get_u128(hash_iter.iter().next().unwrap().to_unextendable()); // TODO: Compute directly instead of building iters
                        cfg_borrow.hashes_cache[addr as usize % HASHES_CACHE_SIZE] =
                            (addr as u64, hash_value);
                        hash_value
                    }
                }
            })
        }
    }

    impl<MH: HashableHashFunctionFactory> PartialEq<Self> for HMapKey<MH> {
        fn eq(&self, other: &Self) -> bool {
            self.get_hash_u128() == other.get_hash_u128()
        }
    }

    impl<MH: HashableHashFunctionFactory> Eq for HMapKey<MH> {}
}

struct GlobalMergeData<'a, MH: HashableHashFunctionFactory, CX: ColorsManager> {
    k: usize,
    m: usize,
    buckets_count: usize,
    min_multiplicity: usize,
    colors_global_table: &'a CX::GlobalColorsTable,
    output_results_buckets:
        ArrayQueue<ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>>,
    hashes_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
}

struct ParallelKmersMergeFactory<MH: HashableHashFunctionFactory, CX: ColorsManager>(
    PhantomData<(MH, CX)>,
);

impl<MH: HashableHashFunctionFactory, CX: ColorsManager> KmersTransformExecutorFactory
    for ParallelKmersMergeFactory<MH, CX>
{
    type GlobalExtraData<'a> = GlobalMergeData<'a, MH, CX>;
    type AssociatedExtraData = CX::MinimizerBucketingSeqColorDataType;
    type ExecutorType<'a> = ParallelKmersMerge<'a, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
        #[cfg(feature = "mem-analysis")]
        let info = parallel_processor::mem_tracker::create_hashmap_entry(
            std::panic::Location::caller(),
            "HashMap",
        );

        let mut hashes_buffer = Box::new(BucketsThreadBuffer::new(
            DEFAULT_PER_CPU_BUFFER_SIZE,
            global_data.buckets_count,
        ));

        let buffers = unsafe { &mut *(hashes_buffer.deref_mut() as *mut BucketsThreadBuffer) };

        Self::ExecutorType::<'a> {
            hashes_tmp: BucketsThreadDispatcher::new(&global_data.hashes_buckets, buffers),
            hashes_buffer,
            forward_seq: Vec::with_capacity(global_data.k),
            backward_seq: Vec::with_capacity(global_data.k),
            temp_colors: CX::ColorsMergeManagerType::<MH>::allocate_temp_buffer_structure(),
            unitigs_temp_colors: CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure(),
            rhash_map: HashMap::with_capacity(4096 * 128),
            #[cfg(feature = "mem-analysis")]
            hmap_meminfo: info,
        }
    }
}

struct ParallelKmersMerge<'x, MH: HashableHashFunctionFactory, CX: ColorsManager> {
    hashes_tmp: BucketsThreadDispatcher<'x, LockFreeBinaryWriter>,
    // This field has to appear after hashes_tmp so that it's dropped only when not used anymore
    hashes_buffer: Box<BucketsThreadBuffer>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    temp_colors: color_types::ColorsBufferTempStructure<MH, CX>,
    unitigs_temp_colors: color_types::TempUnitigColorStructure<MH, CX>,
    rhash_map: HashMap<HMapKey<MH>, color_types::HashMapTempColorIndex<MH, CX>>,
    #[cfg(feature = "mem-analysis")]
    hmap_meminfo: Arc<MemoryInfo>,
}

fn get_kmer_multiplicity<CHI>(entry: &CHI) -> usize {
    // If the current set has both the partial sequences endings, we should divide the counter by 2,
    // as all the kmers are counted exactly two times
    // entry.count >> ((entry.ignored == (READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END)) as u8)
    todo!()
}

impl<'x, MH: HashableHashFunctionFactory, CX: ColorsManager> ParallelKmersMerge<'x, MH, CX> {
    #[inline(always)]
    fn write_hashes(
        hashes_tmp: &mut BucketsThreadDispatcher<LockFreeBinaryWriter>,
        hash: MH::HashTypeUnextendable,
        bucket: BucketIndexType,
        entry: u64,
        do_merge: bool,
        direction: Direction,
        buckets_count_mask: BucketIndexType,
    ) {
        if do_merge {
            hashes_tmp.add_element(
                MH::get_first_bucket(hash) & buckets_count_mask,
                &(),
                &HashEntry {
                    hash,
                    bucket,
                    entry,
                    direction,
                },
            );
        }
    }
}

#[inline]
fn clear_hashmap<K, V>(hashmap: &mut HashMap<K, V>) {
    // if hashmap.capacity() < 8192 * 32 {
    //     hashmap.clear();
    // } else {
    //     // Reset the hashmap if it gets too big
    //     *hashmap = HashMap::with_capacity(4096);
    // }
    hashmap.clear();
}

impl<'x, MH: HashableHashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutor<'x, ParallelKmersMergeFactory<MH, CX>>
    for ParallelKmersMerge<'x, MH, CX>
{
    fn process_group<'y: 'x, R: BucketReader>(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
        reader: R,
    ) {
        HMAP_CONFIG.with(|cfg| {
            let mut cfg = cfg.borrow_mut();
            cfg.reset(global_data.k);
        });

        let k = global_data.k;

        let mut current_bucket = global_data.output_results_buckets.pop().unwrap();

        let bucket_index = current_bucket.get_bucket_index();
        let buckets_count = global_data.buckets_count;

        CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut self.temp_colors);
        clear_hashmap(&mut self.rhash_map);

        // let mut saved_reads = Vec::with_capacity(256);

        reader.decode_all_bucket_items::<CompressedReadsBucketHelper<
            _,
            <ParallelKmersMergeFactory<MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT,
            true,
        >, _>(Vec::new(), |(flags, _second_bucket, color, read)| {
            let hashes = MH::new(read, k);

            let last_hash_pos = read.bases_count() - k;

            let mut saved_reads_offset = None;
            let reads_buffer_start_position = HMAP_CONFIG.with(|x| x.borrow().reads_vec.len()) * 4;
            let mut copy_start = 0;

            for (idx, hash) in hashes.iter_enumerate() {
                // let begin_ignored = flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                // let end_ignored = flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;
                //
                // let is_forward = hash.is_forward();

                let mut entry = self
                    .rhash_map
                    .entry(HMapKey::build_temporary(hash.to_unextendable()));

                let entry = match entry {
                    Vacant(_) => {
                        let occupied_entry =
                            entry.insert(CX::ColorsMergeManagerType::<MH>::new_color_index());
                        match &mut saved_reads_offset {
                            None => {
                                assert_eq!(read.get_offset(), 0);

                                let k_bytes_count = (k + 3 + (idx % 4)) / 4;
                                let copy_start_byte = idx / 4;
                                HMAP_CONFIG.with(|x| {
                                    x.borrow_mut().reads_vec.extend_from_slice(
                                        &read.get_packed_slice()
                                            [copy_start_byte..copy_start_byte + k_bytes_count],
                                    )
                                });
                                saved_reads_offset = Some(copy_start_byte + k_bytes_count);
                                copy_start = copy_start_byte * 4;
                            }
                            Some(position) => {
                                let mut difference = (*position as isize) * 4 - (idx + k) as isize;
                                if difference == -1 {
                                    HMAP_CONFIG.with(|x| {
                                        x.borrow_mut()
                                            .reads_vec
                                            .push(read.get_packed_slice()[*position])
                                    });
                                    *position += 1;
                                } else if difference < 0 {
                                    // TODO: Optimize for big gaps
                                    let remaining = ((-difference as usize) + 3) / 4;
                                    HMAP_CONFIG.with(|x| {
                                        x.borrow_mut().reads_vec.extend_from_slice(
                                            &read.get_packed_slice()
                                                [*position..(*position + remaining)],
                                        )
                                    });
                                    *position += remaining;
                                }
                            }
                        }

                        occupied_entry
                            .key()
                            .settle_temporaru(reads_buffer_start_position + idx - copy_start);

                        occupied_entry
                    }
                    Occupied(entry) => entry,
                };

                // entry.ignored |= ((begin_ignored as u8) << ((!is_forward) as u8))
                //     | ((end_ignored as u8) << (is_forward as u8));
                //
                // entry.count += 1;
                //
                CX::ColorsMergeManagerType::<MH>::add_temp_buffer_structure_el(
                    &mut self.temp_colors,
                    &color,
                    (idx, hash.to_unextendable()),
                );

                // if entry.count == global_data.min_multiplicity {
                //     if saved_read_offset.is_none() {
                //         saved_read_offset = Some(saved_reads.len());
                //         saved_reads.extend_from_slice(read.get_packed_slice())
                //     }
                //     // self.rcorrect_reads.push((
                //     //     hash,
                //     //     saved_read_offset.unwrap() + (idx / 4),
                //     //     (idx % 4) as u8,
                //     //     is_forward,
                //     // ));
                // }
            }

            #[cfg(feature = "mem-analysis")]
            {
                let len = self.rcorrect_reads.len();
                self.rcorrect_reads.update_maximum_usage(len);
            }
        });

        {
            static COUNTER_KMERS_MAX: AtomicCounter<MaxMode> =
                declare_counter_i64!("kmers_cardinality_max", MaxMode, false);
            static COUNTER_READS_MAX: AtomicCounter<MaxMode> =
                declare_counter_i64!("correct_reads_max", MaxMode, false);
            static COUNTER_READS_MAX_LAST: AtomicCounter<MaxMode> =
                declare_counter_i64!("correct_reads_max_last", MaxMode, true);
            static COUNTER_READS_AVG: AtomicCounter<AvgMode> =
                declare_avg_counter_i64!("correct_reads_avg", false);

            // let len = self.rcorrect_reads.len() as i64;
            // COUNTER_KMERS_MAX.max(self.rhash_map.len() as i64);
            // COUNTER_READS_MAX.max(len);
            // COUNTER_READS_MAX_LAST.max(len);
            // COUNTER_READS_AVG.add_value(len);
        }

        if CX::COLORS_ENABLED {
            CX::ColorsMergeManagerType::<MH>::process_colors(
                &global_data.colors_global_table,
                &mut self.temp_colors,
                &mut self.rhash_map,
                global_data.min_multiplicity,
            );
        }

        // for (hash, read_bases_start, reads_offset, is_forward) in self.rcorrect_reads.drain(..) {
        //     let reads_slice = unsafe {
        //         from_raw_parts(
        //             saved_reads.as_ptr().add(read_bases_start),
        //             (k + reads_offset as usize + 3) / 4,
        //         )
        //     };
        //
        //     let cread =
        //         CompressedRead::from_compressed_reads(reads_slice, reads_offset as usize, k);
        //
        //     let rhentry = self.rhash_map.get_mut(&hash.to_unextendable()).unwrap();
        //
        //     // If the current set has both the partial sequences endings, we should divide the counter by 2,
        //     // as all the kmers are counted exactly two times
        //     let count = get_kmer_multiplicity(&rhentry);
        //
        //     if count < global_data.min_multiplicity {
        //         continue;
        //     }
        //
        //     if rhentry.count == usize::MAX {
        //         continue;
        //     }
        //     rhentry.count = usize::MAX;
        //
        //     let (begin_ignored, end_ignored) = if is_forward {
        //         (
        //             rhentry.ignored == READ_FLAG_INCL_BEGIN,
        //             rhentry.ignored == READ_FLAG_INCL_END,
        //         )
        //     } else {
        //         (
        //             rhentry.ignored == READ_FLAG_INCL_END,
        //             rhentry.ignored == READ_FLAG_INCL_BEGIN,
        //         )
        //     };
        //
        //     CX::ColorsMergeManagerType::<MH>::reset_unitig_color_structure(
        //         &mut self.unitigs_temp_colors,
        //     );
        //
        //     unsafe {
        //         self.forward_seq.set_len(k);
        //         self.backward_seq.set_len(k);
        //     }
        //
        //     cread.write_unpacked_to_slice(&mut self.forward_seq[..]);
        //     self.backward_seq[..].copy_from_slice(&self.forward_seq[..]);
        //     self.backward_seq.reverse();
        //
        //     CX::ColorsMergeManagerType::<MH>::extend_forward(
        //         &mut self.unitigs_temp_colors,
        //         rhentry,
        //     );
        //
        //     let mut try_extend_function = |output: &mut Vec<u8>,
        //                                    compute_hash_fw: fn(
        //         hash: MH::HashTypeExtendable,
        //         klen: usize,
        //         out_b: u8,
        //         in_b: u8,
        //     )
        //         -> MH::HashTypeExtendable,
        //                                    compute_hash_bw: fn(
        //         hash: MH::HashTypeExtendable,
        //         klen: usize,
        //         out_b: u8,
        //         in_b: u8,
        //     )
        //         -> MH::HashTypeExtendable,
        //                                    colors_function: fn(
        //         ts: &mut color_types::TempUnitigColorStructure<MH, CX>,
        //         entry: &color_types::HashMapTempColorIndex<MH, CX>,
        //     )| {
        //         let mut temp_data = (hash, 0);
        //         let mut current_hash;
        //
        //         return 'ext_loop: loop {
        //             let mut count = 0;
        //             current_hash = temp_data.0;
        //             for idx in 0..4 {
        //                 let new_hash = compute_hash_fw(
        //                     current_hash,
        //                     k,
        //                     Utils::compress_base(output[output.len() - k]),
        //                     idx,
        //                 );
        //                 if let Some(hash) = self.rhash_map.get(&new_hash.to_unextendable()) {
        //                     if get_kmer_multiplicity(&hash) >= global_data.min_multiplicity {
        //                         // println!("Forward match extend read {:x?}!", new_hash);
        //                         count += 1;
        //                         temp_data = (new_hash, idx);
        //                     }
        //                 }
        //             }
        //
        //             if count == 1 {
        //                 // Test for backward branches
        //                 {
        //                     let mut ocount = 0;
        //                     let new_hash = temp_data.0;
        //                     for idx in 0..4 {
        //                         let bw_hash = compute_hash_bw(new_hash, k, temp_data.1, idx);
        //                         if let Some(hash) = self.rhash_map.get(&bw_hash.to_unextendable()) {
        //                             if get_kmer_multiplicity(&hash) >= global_data.min_multiplicity
        //                             {
        //                                 if ocount > 0 {
        //                                     break 'ext_loop (current_hash, false);
        //                                 }
        //                                 ocount += 1;
        //                             }
        //                         }
        //                     }
        //                     assert_eq!(ocount, 1);
        //                 }
        //
        //                 let entryref = self
        //                     .rhash_map
        //                     .get_mut(&temp_data.0.to_unextendable())
        //                     .unwrap();
        //
        //                 let already_used = entryref.count == usize::MAX;
        //
        //                 // Found a cycle unitig
        //                 if already_used {
        //                     break (temp_data.0, false);
        //                 }
        //
        //                 // Flag the entry as already used
        //                 entryref.count = usize::MAX;
        //
        //                 if CX::COLORS_ENABLED {
        //                     colors_function(&mut self.unitigs_temp_colors, entryref);
        //                 }
        //
        //                 output.push(Utils::decompress_base(temp_data.1));
        //
        //                 // Found a continuation into another bucket
        //                 let contig_break = (entryref.ignored == READ_FLAG_INCL_BEGIN)
        //                     || (entryref.ignored == READ_FLAG_INCL_END);
        //                 if contig_break {
        //                     break (temp_data.0, true);
        //                 }
        //             } else {
        //                 break (current_hash, false);
        //             }
        //         };
        //     };
        //
        //     let (fw_hash, fw_merge) = {
        //         if end_ignored {
        //             (hash, true)
        //         } else {
        //             let (fw_hash, end_ignored) = try_extend_function(
        //                 &mut self.forward_seq,
        //                 MH::manual_roll_forward,
        //                 MH::manual_roll_reverse,
        //                 CX::ColorsMergeManagerType::<MH>::extend_forward,
        //             );
        //             (fw_hash, end_ignored)
        //         }
        //     };
        //
        //     let (bw_hash, bw_merge) = {
        //         if begin_ignored {
        //             (hash, true)
        //         } else {
        //             let (bw_hash, begin_ignored) = try_extend_function(
        //                 &mut self.backward_seq,
        //                 MH::manual_roll_reverse,
        //                 MH::manual_roll_forward,
        //                 CX::ColorsMergeManagerType::<MH>::extend_backward,
        //             );
        //             (bw_hash, begin_ignored)
        //         }
        //     };
        //
        //     let out_seq = {
        //         self.backward_seq.reverse();
        //         self.backward_seq.extend_from_slice(&self.forward_seq[k..]);
        //         &self.backward_seq[..]
        //     };
        //
        //     let read_index = current_bucket.add_read(
        //         color_types::ColorsMergeManagerType::<MH, CX>::encode_part_unitigs_colors(
        //             &mut self.unitigs_temp_colors,
        //         ),
        //         out_seq,
        //     );
        //
        //     Self::write_hashes(
        //         &mut self.hashes_tmp,
        //         fw_hash.to_unextendable(),
        //         bucket_index,
        //         read_index,
        //         fw_merge,
        //         Direction::Forward,
        //         (buckets_count - 1) as BucketIndexType,
        //     );
        //
        //     Self::write_hashes(
        //         &mut self.hashes_tmp,
        //         bw_hash.to_unextendable(),
        //         bucket_index,
        //         read_index,
        //         bw_merge,
        //         Direction::Backward,
        //         (buckets_count - 1) as BucketIndexType,
        //     );
        // }

        #[cfg(feature = "mem-analysis")]
        self.hmap_meminfo.bytes.store(
            self.rhash_map.capacity()
                * (std::mem::size_of::<MH::HashTypeUnextendable>()
                    + std::mem::size_of::<MapEntry<color_types::HashMapTempColorIndex<MH, CX>>>()
                    + 8),
            Ordering::Relaxed,
        )
    }

    fn finalize<'y: 'x>(
        self,
        _global_data: &<ParallelKmersMergeFactory<MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
        self.hashes_tmp.finalize();
    }
}

impl AssemblePipeline {
    pub fn parallel_kmers_merge<
        MH: HashableHashFunctionFactory,
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

        let mut hashes_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            out_directory.as_ref().join("hashes"),
            &(
                get_memory_mode(SwapPriority::HashBuckets),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        );

        let mut sequences = Vec::new();

        let reads_buckets = MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            out_directory.as_ref().join("result"),
            &(
                get_memory_mode(SwapPriority::ResultBuckets),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        );

        let output_results_buckets = ArrayQueue::new(reads_buckets.count());
        for (index, bucket) in reads_buckets.into_buckets().enumerate() {
            let bucket_read = ResultsBucket::<color_types::PartialUnitigsColorStructure<MH, CX>> {
                read_index: 0,
                reads_writer: OwnedDrop::new(bucket),
                temp_buffer: Vec::with_capacity(256),
                bucket_index: index as BucketIndexType,
                _phantom: PhantomData,
            };
            sequences.push(bucket_read.reads_writer.get_path());
            let res = output_results_buckets.push(bucket_read).is_ok();
            assert!(res);
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

        KmersTransform::<ParallelKmersMergeFactory<MH, CX>>::new(
            file_inputs,
            buckets_count,
            global_data,
        )
        .parallel_kmers_transform(threads_count);

        RetType {
            sequences,
            hashes: hashes_buckets.finalize(),
        }
    }
}
