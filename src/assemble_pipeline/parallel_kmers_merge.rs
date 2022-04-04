use crate::assemble_pipeline::assembler_minimizer_bucketing::AssemblerMinimizerBucketingExecutorFactory;
use crate::assemble_pipeline::node_state::NodeState;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{color_types, ColorsManager};
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_MINIMIZER_MASK, DEFAULT_PER_CPU_BUFFER_SIZE,
    MERGE_RESULTS_BUCKETS_COUNT, RESPLITTING_MAX_K_M_DIFFERENCE,
};
use crate::hashes::{ExtendableHashTraitType, HashFunction};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::concurrent::temp_reads::reads_writer::IntermediateReadsWriter;
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::hash_entry::HashEntry;
use crate::pipeline_common::kmers_transform::structs::ReadRef;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutor, KmersTransformExecutorFactory, ReadDispatchInfo,
};
use crate::pipeline_common::minimizer_bucketing::{
    MinimizerBucketingCommonData, MinimizerBucketingExecutorFactory,
};
use crate::utils::owned_drop::OwnedDrop;
use crate::utils::{get_memory_mode, Utils};
use crate::CompressedRead;
use core::slice::from_raw_parts;
use crossbeam::queue::*;
use hashbrown::HashMap;
use parallel_processor::buckets::bucket_type::BucketType;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::mem_tracker::tracked_vec::TrackedVec;
#[cfg(feature = "mem-analysis")]
use parallel_processor::mem_tracker::MemoryInfo;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::min;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use structs::*;

pub const READ_FLAG_INCL_BEGIN: u8 = 1 << 0;
pub const READ_FLAG_INCL_END: u8 = 1 << 1;

pub mod structs {
    use crate::config::BucketIndexType;
    use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
    use crate::io::concurrent::temp_reads::reads_writer::IntermediateReadsWriter;
    use crate::utils::owned_drop::OwnedDrop;
    use parallel_processor::buckets::bucket_type::BucketType;
    use std::path::PathBuf;

    pub struct MapEntry<CHI> {
        pub count: usize,
        pub ignored: u8,
        pub color_index: CHI,
    }

    pub struct ResultsBucket<X: SequenceExtraData> {
        pub read_index: u64,
        pub reads_writer: OwnedDrop<IntermediateReadsWriter<X>>,
        pub bucket_index: BucketIndexType,
    }

    impl<X: SequenceExtraData> ResultsBucket<X> {
        pub fn add_read(&mut self, el: X, read: &[u8]) -> u64 {
            self.reads_writer.add_read::<typenum::U0>(el, read, 0);
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
        SegQueue<ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>>,
    hashes_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
    global_resplit_data: MinimizerBucketingCommonData<()>,
}

struct ParallelKmersMergeFactory<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>(
    PhantomData<(H, MH, CX)>,
);

impl<H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersMergeFactory<H, MH, CX>
{
    type SequencesResplitterFactory = AssemblerMinimizerBucketingExecutorFactory<H, CX>;
    type GlobalExtraData<'a> = GlobalMergeData<'a, MH, CX>;
    type AssociatedExtraData = CX::MinimizerBucketingSeqColorDataType;
    type ExecutorType<'a> = ParallelKmersMerge<'a, H, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;

    fn new_resplitter<'a, 'b: 'a>(
        global_data: &'a Self::GlobalExtraData<'b>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType<'a>
    {
        AssemblerMinimizerBucketingExecutorFactory::new(&global_data.global_resplit_data)
    }

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a> {
        #[cfg(feature = "mem-analysis")]
        let info = parallel_processor::mem_tracker::create_hashmap_entry(
            std::panic::Location::caller(),
            "HashMap",
        );

        Self::ExecutorType::<'a> {
            results_buckets_counter: MERGE_RESULTS_BUCKETS_COUNT,
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
            nhash_map: HashMap::with_capacity(4096),
            #[cfg(feature = "mem-analysis")]
            hmap_meminfo: info,
            _phantom: PhantomData,
        }
    }
}

struct ParallelKmersMerge<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> {
    results_buckets_counter: usize,
    current_bucket: ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>,
    hashes_tmp:
        BucketsThreadDispatcher<'x, LockFreeBinaryWriter, HashEntry<MH::HashTypeUnextendable>>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    temp_colors: color_types::ColorsBufferTempStructure<MH, CX>,
    unitigs_temp_colors: color_types::TempUnitigColorStructure<MH, CX>,

    rcorrect_reads: TrackedVec<(MH::HashTypeExtendable, usize, u8, bool)>,
    rhash_map:
        HashMap<MH::HashTypeUnextendable, MapEntry<color_types::HashMapTempColorIndex<MH, CX>>>,
    nhash_map: HashMap<MH::HashTypeUnextendable, NodeState>,
    #[cfg(feature = "mem-analysis")]
    hmap_meminfo: Arc<MemoryInfo>,
    _phantom: PhantomData<H>,
}

fn get_kmer_multiplicity<CHI>(entry: &MapEntry<CHI>) -> usize {
    // If the current set has both the partial sequences endings, we should divide the counter by 2,
    // as all the kmers are counted exactly two times
    entry.count >> ((entry.ignored == (READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END)) as u8)
}

impl<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    ParallelKmersMerge<'x, H, MH, CX>
{
    #[inline(always)]
    fn write_hashes(
        hashes_tmp: &mut BucketsThreadDispatcher<
            'x,
            LockFreeBinaryWriter,
            HashEntry<MH::HashTypeUnextendable>,
        >,
        hash: MH::HashTypeUnextendable,
        bucket: BucketIndexType,
        entry: u64,
        do_merge: bool,
        direction: Direction,
        buckets_count: usize,
    ) {
        if do_merge {
            hashes_tmp.add_element(
                MH::get_first_bucket(hash) % (buckets_count as BucketIndexType),
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

impl<'x, H: HashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutor<'x, ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMerge<'x, H, MH, CX>
{
    fn preprocess_bucket(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
        flags: u8,
        input_extra_data: <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::AssociatedExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<
        <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::AssociatedExtraData,
    >{
        let decr_val =
            ((read.bases_count() == global_data.k) && (flags & READ_FLAG_INCL_END) == 0) as usize;

        let hashes = H::new(
            read.sub_slice((1 - decr_val)..(global_data.k - decr_val)),
            global_data.m,
        );

        let minimizer = hashes
            .iter()
            .min_by_key(|k| {
                H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(k.to_unextendable())
            })
            .unwrap();

        let bucket = H::get_second_bucket(minimizer.to_unextendable());

        ReadDispatchInfo {
            bucket,
            hash: H::get_sorting_hash(minimizer.to_unextendable()),
            flags,
            extra_data: input_extra_data,
        }
    }

    fn maybe_swap_bucket(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData<'x>,
    ) {
        if self.results_buckets_counter == 0 {
            self.results_buckets_counter = MERGE_RESULTS_BUCKETS_COUNT;
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
        mut stream: FileReader,
    ) {
        let k = global_data.k;
        let bucket_index = self.current_bucket.get_bucket_index();
        let buckets_count = global_data.buckets_count;

        CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut self.temp_colors);
        clear_hashmap(&mut self.rhash_map);
        clear_hashmap(&mut self.nhash_map);
        self.rcorrect_reads.clear();

        if cfg!(feature = "kmerge-read-only-reading") {
            // use std::io::Read;
            // let mut data = [0; 1024];
            // while stream.read(&mut data).map(|x| x > 0).unwrap_or(false) {
            //     continue;
            // }
            stream.close_and_remove(true);
            return;
        }

        let mut tmp_read = Vec::with_capacity(256);
        let mut saved_reads = Vec::with_capacity(256);

        while let Some((kmer_flags, read, color)) = ReadRef::unpack::<
            _,
            _,
            <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT,
        >(&mut stream, &mut tmp_read)
        {
            if cfg!(feature = "kmerge-read-only-deserializing") {
                continue;
            }

            let hashes = MH::new(read, k);

            let last_hash_pos = read.bases_count() - k;
            let mut saved_read_offset = None;

            for (idx, hash) in hashes.iter_enumerate() {
                let begin_ignored = kmer_flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                let end_ignored = kmer_flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;

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
                        saved_read_offset = Some(saved_reads.len());
                        saved_reads.extend_from_slice(read.get_compr_slice())
                    }
                    self.rcorrect_reads.push((
                        hash,
                        saved_read_offset.unwrap() + (idx / 4),
                        (idx % 4) as u8,
                        is_forward,
                    ));

                    let first_base = unsafe { read.get_base_unchecked(idx) };
                    let fwd_key = MH::manual_remove_only_forward(hash, k, first_base);

                    let last_base = unsafe { read.get_base_unchecked(idx + k - 1) };
                    let bwd_key = MH::manual_remove_only_reverse(hash, k, last_base);

                    // println!(
                    //     "Processing kmer: {} first: {}/{} last: {}/{}",
                    //     read.sub_slice(idx..(idx + k)).to_string(),
                    //     first_base,
                    //     Utils::decompress_base(first_base) as char,
                    //     last_base,
                    //     Utils::decompress_base(last_base) as char
                    // );

                    self.nhash_map
                        .entry(fwd_key.to_unextendable())
                        .or_insert(NodeState::new())
                        .update(
                            fwd_key.is_forward(),
                            Utils::conditional_rc_base(first_base, !fwd_key.is_forward()),
                        );

                    self.nhash_map
                        .entry(bwd_key.to_unextendable())
                        .or_insert(NodeState::new())
                        .update(
                            !bwd_key.is_forward(),
                            Utils::conditional_rc_base(last_base, !bwd_key.is_forward()),
                        );
                }
            }

            #[cfg(feature = "mem-analysis")]
            {
                let len = self.rcorrect_reads.len();
                self.rcorrect_reads.update_maximum_usage(len);
            }
        }

        {
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
        }

        drop(tmp_read);
        stream.close_and_remove(true);

        if cfg!(feature = "kmerge-kmers-extension-disable") {
            return;
        }

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
                    saved_reads.as_ptr().add(read_bases_start),
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

            cread.write_to_slice(&mut self.forward_seq[..]);
            self.backward_seq[..].copy_from_slice(&self.forward_seq[..]);
            self.backward_seq.reverse();

            CX::ColorsMergeManagerType::<MH>::extend_forward(
                &mut self.unitigs_temp_colors,
                rhentry,
            );

            let mut try_extend_function = |mut nhash: MH::HashTypeExtendable,
                                           mut ehash: MH::HashTypeExtendable,
                                           output: &mut Vec<u8>,
                                           is_forward: bool,
                                           compute_hash_fw: fn(
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
                return loop {
                    if let Some(kmer) = self.nhash_map.get(&nhash.to_unextendable()) {
                        if kmer.is_extendable() {
                            let new_base = Utils::conditional_rc_base(
                                kmer.get_base(nhash.is_forward() ^ is_forward),
                                !nhash.is_forward(),
                            );

                            let new_ehash = compute_hash_fw(
                                ehash,
                                k,
                                Utils::compress_base(output[output.len() - k]),
                                new_base,
                            );

                            // println!(
                            //     "Extending sequence: {} fwd:{} with base {} and state: {:?}",
                            //     std::str::from_utf8(&output).unwrap(),
                            //     is_forward,
                            //     Utils::decompress_base(new_base) as char,
                            //     kmer
                            // );

                            let entry_ref = self
                                .rhash_map
                                .get_mut(&new_ehash.to_unextendable())
                                .unwrap();

                            let already_used = entry_ref.count == usize::MAX;

                            // Found a cycle unitig
                            if already_used {
                                break (ehash, false);
                            }

                            // Flag the entry as already used
                            entry_ref.count = usize::MAX;

                            if CX::COLORS_ENABLED {
                                colors_function(&mut self.unitigs_temp_colors, entry_ref);
                            }

                            output.push(Utils::decompress_base(new_base));
                            ehash = new_ehash;

                            // Found a continuation into another bucket
                            let contig_break = (entry_ref.ignored == READ_FLAG_INCL_BEGIN)
                                || (entry_ref.ignored == READ_FLAG_INCL_END);
                            if contig_break {
                                break (ehash, true);
                            }

                            nhash = compute_hash_fw(
                                nhash,
                                k - 1,
                                Utils::compress_base(output[output.len() - (k - 1)]),
                                new_base,
                            );
                        } else {
                            return (ehash, false);
                        }
                    }

                    // if count == 1 {
                    //     // Test for backward branches
                    //
                    // } else {
                    //     break (current_hash, false); // count > 0);
                    // }
                };
            };

            let (fw_hash, fw_merge) = {
                if end_ignored {
                    (hash, true)
                } else {
                    let (fw_hash, end_ignored) = try_extend_function(
                        MH::manual_remove_only_forward(hash, k, self.forward_seq[0]),
                        hash,
                        &mut self.forward_seq,
                        true,
                        MH::manual_roll_forward,
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
                        MH::manual_remove_only_reverse(hash, k, self.backward_seq[0]),
                        hash,
                        &mut self.backward_seq,
                        false,
                        MH::manual_roll_reverse,
                        CX::ColorsMergeManagerType::<MH>::extend_backward,
                    );
                    (bw_hash, begin_ignored)
                }
            };

            if cfg!(feature = "kmerge-kmers-writing-disable") {
                return;
            }

            let out_seq = {
                self.backward_seq.reverse();
                self.backward_seq.extend_from_slice(&self.forward_seq[k..]);
                &self.backward_seq[..]
            };

            let read_index = self.current_bucket.add_read(
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
                buckets_count,
            );

            Self::write_hashes(
                &mut self.hashes_tmp,
                bw_hash.to_unextendable(),
                bucket_index,
                read_index,
                bw_merge,
                Direction::Backward,
                buckets_count,
            );
        }

        #[cfg(feature = "mem-analysis")]
        self.hmap_meminfo.bytes.store(
            self.rhash_map.capacity()
                * (std::mem::size_of::<MH::HashTypeUnextendable>()
                    + std::mem::size_of::<MapEntry<color_types::HashMapTempColorIndex<MH, CX>>>()
                    + 8),
            Ordering::Relaxed,
        )
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

        let reads_buckets = MultiThreadBuckets::<
            IntermediateReadsWriter<color_types::PartialUnitigsColorStructure<MH, CX>>,
        >::new(
            buckets_count,
            &(
                SwapPriority::ResultBuckets,
                out_directory.as_ref().join("result"),
            ),
            None,
        );

        let output_results_buckets = SegQueue::new();
        for (index, bucket) in reads_buckets.into_buckets().enumerate() {
            let bucket_read = ResultsBucket::<color_types::PartialUnitigsColorStructure<MH, CX>> {
                read_index: 0,
                reads_writer: OwnedDrop::new(bucket),
                bucket_index: index as BucketIndexType,
            };

            sequences.push(bucket_read.reads_writer.get_path());
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
            global_resplit_data: MinimizerBucketingCommonData {
                k,
                m: if k > RESPLITTING_MAX_K_M_DIFFERENCE + 1 {
                    k - RESPLITTING_MAX_K_M_DIFFERENCE
                } else {
                    min(m, 2)
                },
                buckets_count,
                global_data: (),
            },
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
