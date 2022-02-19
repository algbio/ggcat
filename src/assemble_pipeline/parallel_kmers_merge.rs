use crate::assemble_pipeline::assembler_minimizer_bucketing::AssemblerMinimizerBucketingExecutorFactory;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsManager;
use crate::colors::colors_manager::ColorsMergeManager;
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
    output_results_buckets: SegQueue<ResultsBucketType<MH, CX>>,
    hashes_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
    #[cfg(feature = "build-links")]
    links_hashes_buckets: &'a MultiThreadBuckets<LockFreeBinaryWriter>,
    global_resplit_data: MinimizerBucketingCommonData<()>,
}

#[allow(type_alias_bounds)]
pub type ResultsBucketType<MH, CX: ColorsManager> = ResultsBucket<
    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::PartialUnitigsColorStructure,
>;

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
            #[cfg(feature = "build-links")]
            link_hashes_tmp: BucketsThreadDispatcher::new(
                DEFAULT_PER_CPU_BUFFER_SIZE,
                &global_data.links_hashes_buckets,
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
    current_bucket: ResultsBucketType<MH, CX>,
    hashes_tmp:
        BucketsThreadDispatcher<'x, LockFreeBinaryWriter, HashEntry<MH::HashTypeUnextendable>>,
    #[cfg(feature = "build-links")]
    link_hashes_tmp:
        BucketsThreadDispatcher<'x, LockFreeBinaryWriter, HashEntry<MH::HashTypeUnextendable>>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    temp_colors:
        <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::ColorsBufferTempStructure,
    unitigs_temp_colors:
        <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::TempUnitigColorStructure,

    rcorrect_reads: TrackedVec<(MH::HashTypeExtendable, usize, u8, bool)>,
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

fn get_kmer_multiplicity<CHI>(entry: &MapEntry<CHI>) -> usize {
    // If the current set has both the partial sequences endings, we should divide the counter by 2,
    // as all the kmers are counted exactly two times
    entry.count >> ((entry.ignored == (READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END)) as u8)
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
        if self.rhash_map.capacity() < 8192 {
            self.rhash_map.clear();
        } else {
            // Reset the hashmap if it gets too big
            self.rhash_map = HashMap::with_capacity(4096);
        }
        self.rcorrect_reads.clear();

        let mut tmp_read = Vec::with_capacity(256);
        let mut saved_reads = Vec::with_capacity(256);

        while let Some((kmer_flags, read, color)) = ReadRef::unpack::<
            _,
            _,
            <ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::FLAGS_COUNT,
        >(&mut stream, &mut tmp_read)
        {
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
                }
            }

            #[cfg(feature = "mem-analysis")]
            {
                let len = self.rcorrect_reads.len();
                self.rcorrect_reads.update_maximum_usage(len);
            }
        }

        drop(tmp_read);
        stream.close_and_remove(true);

        // static MAX_RHS: AtomicUsize = AtomicUsize::new(0);
        // static MAX_CRR: AtomicUsize = AtomicUsize::new(0);
        //
        // if MAX_RHS.fetch_max(self.rhash_map.len(), Ordering::Relaxed) < self.rhash_map.len() {
        //     println!("Max rhs: {}", self.rhash_map.len());
        // }
        //
        // if MAX_CRR.fetch_max(self.rcorrect_reads.len(), Ordering::Relaxed)
        //     < self.rcorrect_reads.len()
        // {
        //     println!("Max crr: {}", self.rcorrect_reads.len());
        // }

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
                                        let bw_hash =
                                            compute_hash_bw(new_hash, k, temp_data.1, idx);
                                        if let Some(hash) =
                                        self.rhash_map.get(&bw_hash.to_unextendable())
                                        {
                                            if get_kmer_multiplicity(&hash) >= global_data.min_multiplicity {
                                                // println!("Backward match extend read {:x?}!", bw_hash);
                                                if ocount > 0 {
                                                    break 'ext_loop (current_hash, false, true);
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
                                    break (temp_data.0, false, true);
                                }

                                // Flag the entry as already used
                                entryref.count = usize::MAX;

                                if CX::COLORS_ENABLED {
                                    colors_function(&mut self.unitigs_temp_colors, entryref);
                                }

                                output.push(Utils::decompress_base(temp_data.1));

                                // Found a continuation into another bucket
                                let contig_break = (entryref.ignored == READ_FLAG_INCL_BEGIN) || (entryref.ignored == READ_FLAG_INCL_END);
                                if contig_break {
                                    break (temp_data.0, true, false);
                                }
                            } else {
                                break (temp_data.0, false, count > 0);
                            }
                        };
                    };

            let (fw_hash, fw_merge, fw_has_edges) = {
                if end_ignored {
                    (hash, true, false)
                } else {
                    let (fw_hash, end_ignored, has_edges) = try_extend_function(
                        &mut self.forward_seq,
                        MH::manual_roll_forward,
                        MH::manual_roll_reverse,
                        CX::ColorsMergeManagerType::<MH>::extend_forward,
                    );
                    (fw_hash, end_ignored, has_edges)
                }
            };

            let (bw_hash, bw_merge, bw_has_edges) = {
                if begin_ignored {
                    (hash, true, false)
                } else {
                    let (bw_hash, begin_ignored, has_edges) = try_extend_function(
                        &mut self.backward_seq,
                        MH::manual_roll_reverse,
                        MH::manual_roll_forward,
                        CX::ColorsMergeManagerType::<MH>::extend_backward,
                    );
                    (bw_hash, begin_ignored, has_edges)
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

            if fw_merge || (cfg!(feature = "build-links") && fw_has_edges) {
                let fw_hash = fw_hash.to_unextendable();
                let fw_hash_sr = HashEntry {
                    hash: fw_hash,
                    bucket: bucket_index,
                    entry: read_index,
                    direction: Direction::Forward,
                };
                let fw_bucket_index =
                    MH::get_first_bucket(fw_hash) % (buckets_count as BucketIndexType);

                if fw_merge {
                    self.hashes_tmp
                        .add_element(fw_bucket_index, &(), &fw_hash_sr);
                } else {
                    #[cfg(feature = "build-links")]
                    self.link_hashes_tmp
                        .add_element(fw_bucket_index, &(), &fw_hash_sr);
                }
            }

            if bw_merge || (cfg!(feature = "build-links") && bw_has_edges) {
                let bw_hash = bw_hash.to_unextendable();

                let bw_hash_sr = HashEntry {
                    hash: bw_hash,
                    bucket: bucket_index,
                    entry: read_index,
                    direction: Direction::Backward,
                };
                let bw_bucket_index =
                    MH::get_first_bucket(bw_hash) % (buckets_count as BucketIndexType);

                if bw_merge {
                    self.hashes_tmp
                        .add_element(bw_bucket_index, &(), &bw_hash_sr);
                } else {
                    #[cfg(feature = "build-links")]
                    self.link_hashes_tmp
                        .add_element(bw_bucket_index, &(), &bw_hash_sr);
                }
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

        #[cfg(feature = "build-links")]
        let mut links_hashes_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            &(
                out_directory.as_ref().join("links_hashes"),
                get_memory_mode(SwapPriority::HashBuckets),
            ),
            None,
        );

        let mut sequences = Vec::new();

        let reads_buckets =
            MultiThreadBuckets::<
                IntermediateReadsWriter<
                    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                        MH,
                        CX,
                    >>::PartialUnitigsColorStructure,
                >,
            >::new(buckets_count, &(SwapPriority::ResultBuckets, out_directory.as_ref().join("result")), None);

        let output_results_buckets = SegQueue::new();
        for (index, bucket) in reads_buckets.into_buckets().enumerate() {
            let bucket_read = ResultsBucketType::<MH, CX> {
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
            #[cfg(feature = "build-links")]
            links_hashes_buckets: &links_hashes_buckets,
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

        #[cfg(feature = "build-links")]
        links_hashes_buckets.finalize();

        RetType {
            sequences,
            hashes: hashes_buckets.finalize(),
        }
    }
}
