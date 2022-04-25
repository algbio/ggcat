use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{color_types, ColorsManager};
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
};
use crate::hashes::{ExtendableHashTraitType, HashFunction, MinimizerHashFunctionFactory};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::hash_entry::HashEntry;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory,
    KmersTransformPreprocessor,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::owned_drop::OwnedDrop;
use crate::utils::{get_memory_mode, Utils};
use crate::CompressedRead;
use core::slice::from_raw_parts;
use crossbeam::queue::*;
use hashbrown::HashMap;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{LockFreeBucket, MultiThreadBuckets};
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
use parallel_processor::mem_tracker::tracked_vec::TrackedVec;
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
    use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
    use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
    use crate::utils::owned_drop::OwnedDrop;
    use parallel_processor::buckets::bucket_writer::BucketItem;
    use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
    use parallel_processor::buckets::LockFreeBucket;
    use std::marker::PhantomData;
    use std::path::PathBuf;

    pub struct MapEntry<CHI> {
        pub count: usize,
        pub ignored: u8,
        pub color_index: CHI,
    }

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
            CompressedReadsBucketHelper::<X, typenum::U0, false, true>::new(read, 0, 0)
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
}

struct GlobalMergeData<'a, MH: HashFunctionFactory, CX: ColorsManager> {
    k: usize,
    m: usize,
    buckets_count: usize,
    min_multiplicity: usize,
    colors_global_table: &'a CX::GlobalColorsTable,
    output_results_buckets:
        ArrayQueue<ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>>,
    hashes_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
}

struct ParallelKmersMergeFactory<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
>(PhantomData<(H, MH, CX)>);

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersMergeFactory<H, MH, CX>
{
    type GlobalExtraData<'a> = GlobalMergeData<'a, MH, CX>;
    type AssociatedExtraData = CX::MinimizerBucketingSeqColorDataType;
    type ExecutorType<'a> = ParallelKmersMergeExecutor<'a, H, MH, CX>;
    type PreprocessorType<'a> = ParallelKmersMergePreprocessor<'a, H, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;

    fn new_preprocessor<'a>(
        _global_data: &Self::GlobalExtraData<'a>,
    ) -> Self::PreprocessorType<'a> {
        ParallelKmersMergePreprocessor {
            _phantom: Default::default(),
        }
    }

    fn new_executor<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
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
            saved_reads: Vec::with_capacity(256),
            current_bucket: None,
            rcorrect_reads: TrackedVec::new(),
            rhash_map: HashMap::with_capacity(4096),
            #[cfg(feature = "mem-analysis")]
            hmap_meminfo: info,
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersMergePreprocessor<
    'x,
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    _phantom: PhantomData<&'x (H, MH, CX)>,
}

impl<'x, H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformPreprocessor<'x, ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMergePreprocessor<'x, H, MH, CX>
{
    fn get_sequence_bucket<'y: 'x, 'a, C>(
        &self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
        seq_data: &(u8, u8, C, CompressedRead),
    ) -> BucketIndexType {
        let read = &seq_data.3;
        let flags = seq_data.0;
        let decr_val =
            ((read.bases_count() == global_data.k) && (flags & READ_FLAG_INCL_END) == 0) as usize;

        let hashes = H::new(
            read.sub_slice((1 - decr_val)..(global_data.k - decr_val)),
            global_data.m,
        );

        let minimizer = hashes
            .iter()
            .min_by_key(|k| H::get_full_minimizer(k.to_unextendable()))
            .unwrap();

        H::get_second_bucket(minimizer.to_unextendable())
    }
}

struct ParallelKmersMergeExecutor<
    'x,
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    hashes_tmp: BucketsThreadDispatcher<'x, LockFreeBinaryWriter>,
    // This field has to appear after hashes_tmp so that it's dropped only when not used anymore
    hashes_buffer: Box<BucketsThreadBuffer>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    temp_colors: color_types::ColorsBufferTempStructure<MH, CX>,
    unitigs_temp_colors: color_types::TempUnitigColorStructure<MH, CX>,
    saved_reads: Vec<u8>,
    current_bucket: Option<ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>>,
    rcorrect_reads: TrackedVec<(MH::HashTypeExtendable, usize, u8, bool)>,
    rhash_map:
        HashMap<MH::HashTypeUnextendable, MapEntry<color_types::HashMapTempColorIndex<MH, CX>>>,
    #[cfg(feature = "mem-analysis")]
    hmap_meminfo: Arc<MemoryInfo>,
    _phantom: PhantomData<H>,
}

fn get_kmer_multiplicity<CHI>(entry: &MapEntry<CHI>) -> usize {
    // If the current set has both the partial sequences endings, we should divide the counter by 2,
    // as all the kmers are counted exactly two times
    entry.count >> ((entry.ignored == (READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END)) as u8)
}

impl<'x, H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    ParallelKmersMergeExecutor<'x, H, MH, CX>
{
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
    if hashmap.capacity() < 8192 {
        hashmap.clear();
    } else {
        // Reset the hashmap if it gets too big
        *hashmap = HashMap::with_capacity(4096);
    }
}

impl<'x, H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutor<'x, ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMergeExecutor<'x, H, MH, CX>
{
    fn process_group_start<'y: 'x>(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
        self.saved_reads.clear();
        CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut self.temp_colors);
        clear_hashmap(&mut self.rhash_map);
        self.rcorrect_reads.clear();
        self.current_bucket = Some(global_data.output_results_buckets.pop().unwrap());
    }

    fn process_group_batch_sequences<'y: 'x>(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
        batch: &Vec<(
            u8,
            CX::MinimizerBucketingSeqColorDataType,
            CompressedReadIndipendent,
        )>,
        ref_sequences: &Vec<u8>,
    ) {
        let k = global_data.k;

        for (flags, color, read) in batch.iter() {
            let read = read.as_reference(ref_sequences);

            let hashes = MH::new(read, k);

            let last_hash_pos = read.bases_count() - k;
            let mut saved_read_offset = None;

            for (idx, hash) in hashes.iter_enumerate() {
                let begin_ignored = flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                let end_ignored = flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;

                let is_forward = hash.is_forward();

                let entry = self
                    .rhash_map
                    .entry(hash.to_unextendable())
                    .or_insert(MapEntry {
                        ignored: 0,
                        count: 0,
                        color_index: CX::ColorsMergeManagerType::<MH>::new_color_index(),
                    });

                entry.ignored |= ((begin_ignored as u8) << ((!is_forward) as u8))
                    | ((end_ignored as u8) << (is_forward as u8));

                entry.count += 1;

                CX::ColorsMergeManagerType::<MH>::add_temp_buffer_structure_el(
                    &mut self.temp_colors,
                    &color,
                    (idx, hash.to_unextendable()),
                );

                if entry.count == global_data.min_multiplicity {
                    if saved_read_offset.is_none() {
                        saved_read_offset = Some(self.saved_reads.len());
                        self.saved_reads.extend_from_slice(read.get_packed_slice())
                    }
                    self.rcorrect_reads.push((
                        hash,
                        saved_read_offset.unwrap() + (idx / 4),
                        (idx % 4) as u8,
                        is_forward,
                    ));
                }
            }

            #[cfg(feature = "mem-analysis")]
            {
                let len = self.rcorrect_reads.len();
                self.rcorrect_reads.update_maximum_usage(len);
            }
        }
    }

    fn process_group_finalize<'y: 'x>(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
        let k = global_data.k;
        let buckets_count = global_data.buckets_count;
        let current_bucket = self.current_bucket.as_mut().unwrap();
        let bucket_index = current_bucket.get_bucket_index();

        static COUNTER_KMERS_MAX: AtomicCounter<MaxMode> =
            declare_counter_i64!("kmers_cardinality_max", MaxMode, false);
        static COUNTER_READS_MAX: AtomicCounter<MaxMode> =
            declare_counter_i64!("correct_reads_max", MaxMode, false);
        static COUNTER_READS_MAX_LAST: AtomicCounter<MaxMode> =
            declare_counter_i64!("correct_reads_max_last", MaxMode, true);
        static COUNTER_READS_AVG: AtomicCounter<AvgMode> =
            declare_avg_counter_i64!("correct_reads_avg", false);

        let len = self.rcorrect_reads.len() as i64;
        COUNTER_KMERS_MAX.max(self.rhash_map.len() as i64);
        COUNTER_READS_MAX.max(len);
        COUNTER_READS_MAX_LAST.max(len);
        COUNTER_READS_AVG.add_value(len);

        if CX::COLORS_ENABLED {
            CX::ColorsMergeManagerType::<MH>::process_colors(
                &global_data.colors_global_table,
                &mut self.temp_colors,
                &mut self.rhash_map,
                global_data.min_multiplicity,
            );
        }

        for (hash, read_bases_start, reads_offset, is_forward) in self.rcorrect_reads.drain(..) {
            let reads_slice = unsafe {
                from_raw_parts(
                    self.saved_reads.as_ptr().add(read_bases_start),
                    (k + reads_offset as usize + 3) / 4,
                )
            };

            let cread =
                CompressedRead::from_compressed_reads(reads_slice, reads_offset as usize, k);

            let rhentry = self.rhash_map.get_mut(&hash.to_unextendable()).unwrap();

            // If the current set has both the partial sequences endings, we should divide the counter by 2,
            // as all the kmers are counted exactly two times
            let count = get_kmer_multiplicity(&rhentry);

            if count < global_data.min_multiplicity {
                continue;
            }

            if rhentry.count == usize::MAX {
                continue;
            }
            rhentry.count = usize::MAX;

            let (begin_ignored, end_ignored) = if is_forward {
                (
                    rhentry.ignored == READ_FLAG_INCL_BEGIN,
                    rhentry.ignored == READ_FLAG_INCL_END,
                )
            } else {
                (
                    rhentry.ignored == READ_FLAG_INCL_END,
                    rhentry.ignored == READ_FLAG_INCL_BEGIN,
                )
            };

            CX::ColorsMergeManagerType::<MH>::reset_unitig_color_structure(
                &mut self.unitigs_temp_colors,
            );

            unsafe {
                self.forward_seq.set_len(k);
                self.backward_seq.set_len(k);
            }

            cread.write_unpacked_to_slice(&mut self.forward_seq[..]);
            self.backward_seq[..].copy_from_slice(&self.forward_seq[..]);
            self.backward_seq.reverse();

            CX::ColorsMergeManagerType::<MH>::extend_forward(
                &mut self.unitigs_temp_colors,
                rhentry,
            );

            let mut try_extend_function = |output: &mut Vec<u8>,
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
                ts: &mut color_types::TempUnitigColorStructure<MH, CX>,
                entry: &MapEntry<color_types::HashMapTempColorIndex<MH, CX>>,
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
                        if let Some(hash) = self.rhash_map.get(&new_hash.to_unextendable()) {
                            if get_kmer_multiplicity(&hash) >= global_data.min_multiplicity {
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
                                let bw_hash = compute_hash_bw(new_hash, k, temp_data.1, idx);
                                if let Some(hash) = self.rhash_map.get(&bw_hash.to_unextendable()) {
                                    if get_kmer_multiplicity(&hash) >= global_data.min_multiplicity
                                    {
                                        if ocount > 0 {
                                            break 'ext_loop (current_hash, false);
                                        }
                                        ocount += 1;
                                    }
                                }
                            }
                            assert_eq!(ocount, 1);
                        }

                        let entryref = self
                            .rhash_map
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
                        let contig_break = (entryref.ignored == READ_FLAG_INCL_BEGIN)
                            || (entryref.ignored == READ_FLAG_INCL_END);
                        if contig_break {
                            break (temp_data.0, true);
                        }
                    } else {
                        break (current_hash, false);
                    }
                };
            };

            let (fw_hash, fw_merge) = {
                if end_ignored {
                    (hash, true)
                } else {
                    let (fw_hash, end_ignored) = try_extend_function(
                        &mut self.forward_seq,
                        MH::manual_roll_forward,
                        MH::manual_roll_reverse,
                        CX::ColorsMergeManagerType::<MH>::extend_forward,
                    );
                    (fw_hash, end_ignored)
                }
            };

            let (bw_hash, bw_merge) = {
                if begin_ignored {
                    (hash, true)
                } else {
                    let (bw_hash, begin_ignored) = try_extend_function(
                        &mut self.backward_seq,
                        MH::manual_roll_reverse,
                        MH::manual_roll_forward,
                        CX::ColorsMergeManagerType::<MH>::extend_backward,
                    );
                    (bw_hash, begin_ignored)
                }
            };

            let out_seq = {
                self.backward_seq.reverse();
                self.backward_seq.extend_from_slice(&self.forward_seq[k..]);
                &self.backward_seq[..]
            };

            let read_index = current_bucket.add_read(
                color_types::ColorsMergeManagerType::<MH, CX>::encode_part_unitigs_colors(
                    &mut self.unitigs_temp_colors,
                ),
                out_seq,
            );

            Self::write_hashes(
                &mut self.hashes_tmp,
                fw_hash.to_unextendable(),
                bucket_index,
                read_index,
                fw_merge,
                Direction::Forward,
                (buckets_count - 1) as BucketIndexType,
            );

            Self::write_hashes(
                &mut self.hashes_tmp,
                bw_hash.to_unextendable(),
                bucket_index,
                read_index,
                bw_merge,
                Direction::Backward,
                (buckets_count - 1) as BucketIndexType,
            );
        }

        #[cfg(feature = "mem-analysis")]
        self.hmap_meminfo.bytes.store(
            self.rhash_map.capacity()
                * (std::mem::size_of::<MH::HashTypeUnextendable>()
                    + std::mem::size_of::<MapEntry<color_types::HashMapTempColorIndex<MH, CX>>>()
                    + 8),
            Ordering::Relaxed,
        );

        global_data
            .output_results_buckets
            .push(self.current_bucket.take().unwrap());
    }

    fn finalize<'y: 'x>(
        self,
        _global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'y>,
    ) {
        self.hashes_tmp.finalize();
    }
}

impl AssemblePipeline {
    pub fn parallel_kmers_merge<
        H: MinimizerHashFunctionFactory,
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

        KmersTransform::<ParallelKmersMergeFactory<H, MH, CX>>::new(
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
