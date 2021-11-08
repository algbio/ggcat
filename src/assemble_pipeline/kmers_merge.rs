use parking_lot::Mutex;
use std::cmp::min;
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{stdout, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use std::ops::{Index, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use rayon::prelude::*;

use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::hashes::{ExtendableHashTraitType, HashFunction};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::concurrent::fasta_writer::FastaWriterConcurrentBuffer;
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsReader, IntermediateReadsWriter, SequenceExtraData,
};
use crate::io::concurrent::intermediate_storage_single::IntermediateSequencesStorageSingleBucket;
use crate::io::reads_reader::ReadsReader;
use crate::io::reads_writer::ReadsWriter;
use crate::io::sequences_reader::FastaSequence;
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::hash_entry::HashEntry;
use crate::io::varint::{decode_varint, decode_varint_flags, encode_varint_flags};
use crate::rolling::minqueue::RollingMinQueue;
use crate::types::BucketIndexType;
use crate::utils::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::utils::Utils;
use crate::{DEFAULT_BUFFER_SIZE, KEEP_FILES};
use bitvec::ptr::Mut;
use byteorder::{ReadBytesExt, WriteBytesExt};
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

pub mod structs {
    use std::path::PathBuf;

    pub struct RetType {
        pub sequences: Vec<PathBuf>,
        pub hashes: Vec<PathBuf>,
    }

    pub struct MapEntry<CHI> {
        pub count: usize,
        pub ignored: bool,
        pub color_index: CHI,
    }

    #[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
    pub struct ReadRef {
        pub read_start: usize,
        pub hash: u64,
    }
}

use crate::utils::debug_utils::debug_increase;
use structs::*;

pub const MERGE_BUCKETS_COUNT: usize = 256;

impl AssemblePipeline {
    pub fn kmers_merge<
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

        let sequences = Mutex::new(Vec::new());

        let incr_bucket_index = AtomicUsize::new(0);

        let mut reads_buckets =
            MultiThreadBuckets::<
                IntermediateReadsWriter<
                    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                        MH,
                        CX,
                    >>::PartialUnitigsColorStructure,
                >,
            >::new(buckets_count, &out_directory.as_ref().join("result"), None);

        file_inputs.par_iter().for_each(|input| {
            println!("Processing {}", input.display());
            const MAX_HASHES_FOR_FLUSH: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);
            let mut hashes_tmp =
                BucketsThreadDispatcher::new(MAX_HASHES_FOR_FLUSH, &hashes_buckets);

            let bucket_index = Utils::get_bucket_index(&input);

            let mut temp_bucket_writer =
                IntermediateSequencesStorageSingleBucket::new(bucket_index, &reads_buckets);

            let incr_bucket_index_val = incr_bucket_index.fetch_add(1, Ordering::Relaxed);
            if incr_bucket_index_val % (buckets_count / 8) == 0 {
                println!(
                    "Processing bucket {} of {} {}",
                    incr_bucket_index_val,
                    buckets_count,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
            }

            sequences.lock().push(temp_bucket_writer.get_path());

            let mut read_index = 0;

            let mut buckets: Vec<Vec<u8>> = vec![Vec::new(); MERGE_BUCKETS_COUNT];
            let mut cmp_reads: Vec<Vec<ReadRef>> = vec![Vec::new(); MERGE_BUCKETS_COUNT];
            let mut buckets = &mut buckets[..];
            let mut cmp_reads = &mut cmp_reads[..];

            IntermediateReadsReader::<KmersFlags<CX::MinimizerBucketingSeqColorDataType>>::new(
                input.clone(),
                !KEEP_FILES.load(Ordering::Relaxed),
            )
            .for_each(|flags, x| {
                let decr_val =
                    ((x.bases_count() == k) && (flags.0 & READ_FLAG_INCL_END) == 0) as usize;

                let hashes = H::new(x.sub_slice((1 - decr_val)..(k - decr_val)), m);

                let minimizer = hashes
                    .iter()
                    .min_by_key(|k| H::get_minimizer(k.to_unextendable()))
                    .unwrap();

                let bucket = H::get_second_bucket(minimizer.to_unextendable())
                    % (MERGE_BUCKETS_COUNT as BucketIndexType);

                let st_len = buckets[bucket as usize].len();

                encode_varint_flags::<_, _, 2>(
                    |slice| buckets[bucket as usize].extend_from_slice(slice),
                    x.bases_count() as u64,
                    flags.0
                );
                buckets[bucket as usize].extend_from_slice(x.get_compr_slice());
                flags.1.encode(&mut buckets[bucket as usize]);

                cmp_reads[bucket as usize].push(ReadRef {
                    read_start: st_len,
                    // read_len: x.bases_count(),
                    hash: H::get_u64(minimizer.to_unextendable()),
                    // flags,
                });
            });

            for b in 0..MERGE_BUCKETS_COUNT {
                let mut rcorrect_reads: Vec<(MH::HashTypeExtendable, usize, bool, bool)> =
                    Vec::new();
                let mut rhash_map = HashMap::with_capacity(4096);
                // let mut xhash_map = hashbrown::HashMap::with_capacity(4096);

                let mut colors_struct = CX::ColorsMergeManagerType::<MH>::allocate_temp_buffer_structure();

                let mut backward_seq = Vec::new();
                let mut forward_seq = Vec::new();

                forward_seq.reserve(k);
                backward_seq.reserve(k);

                struct Compare {
                };
                impl SortKey<ReadRef> for Compare {
                    type KeyType = u64;
                    const KEY_BITS: usize = size_of::<u64>() * 8;

                    #[inline(always)]
                    fn compare(left: &ReadRef, right: &ReadRef) -> std::cmp::Ordering {
                        left.hash
                            .cmp(&right.hash)
                    }

                    #[inline(always)]
                    fn get_shifted(value: &ReadRef, rhs: u8) -> u8 {
                        (value.hash >> rhs) as u8
                    }
                }

                fast_smart_radix_sort::<_, Compare, false>(&mut cmp_reads[b]);

                let mut temp_unitig_colors = CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure();

                for slice in cmp_reads[b]
                    .group_by(|a, b| a.hash == b.hash)
                {
                    CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut colors_struct);
                    rhash_map.clear();
                    rcorrect_reads.clear();

                    let mut tot_reads = 0;
                    let mut tot_chars = 0;

                    let mut do_debug = false;

                    for &ReadRef {
                        mut read_start,
                        ..
                    } in slice
                    {
                        let (read_len, kmer_flags) = decode_varint_flags::<_, 2>(|| {
                            let x = buckets[b][read_start];
                            read_start += 1;
                            Some(x)
                        }).unwrap();

                        let read_bases_start = read_start;
                        let read_len = read_len as usize;
                        let read_len_bytes = (read_len + 3) / 4;

                        let read = CompressedRead::new_from_compressed(
                            &buckets[b][read_bases_start..read_bases_start + read_len_bytes],
                            read_len,
                        );

                        let color = CX::MinimizerBucketingSeqColorDataType::decode_from_slice(
                            &buckets[b][read_bases_start + read_len_bytes..]
                        ).unwrap();

                        let hashes = MH::new(read, k);

                        struct ExtensionEntry {}

                        let last_hash_pos = read_len - k;
                        let mut did_max = false;

                        for (idx, hash) in hashes.iter_enumerate() {
                            let position = (read_bases_start * 4 + idx);
                            let begin_ignored = kmer_flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                            let end_ignored =
                                kmer_flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;
                            assert!(idx <= last_hash_pos);
                            did_max |= idx == last_hash_pos;

                            let entry =
                                rhash_map.entry(hash.to_unextendable()).or_insert(MapEntry {
                                    ignored: begin_ignored || end_ignored,
                                    count: 0,
                                    color_index: CX::ColorsMergeManagerType::<MH>::new_color_index()
                                });

                            entry.count += 1;
                            CX::ColorsMergeManagerType::<MH>::add_temp_buffer_structure_el(&mut colors_struct, &color, (idx, hash.to_unextendable()));

                            if entry.count == min_multiplicity {
                                rcorrect_reads.push((hash, position, begin_ignored, end_ignored));
                            }
                        }
                        assert!(did_max);
                        tot_reads += 1;
                        tot_chars += read.bases_count();
                    }

                    if CX::COLORS_ENABLED {
                        CX::ColorsMergeManagerType::<MH>::process_colors(colors_global_table, &mut colors_struct, &mut rhash_map, min_multiplicity);
                    }

                    for (hash, read_bases_start, begin_ignored, end_ignored) in rcorrect_reads.iter() {
                        let cread =
                            CompressedRead::from_compressed_reads(&buckets[b][..], *read_bases_start, k);

                        let rhentry = rhash_map.get_mut(&hash.to_unextendable()).unwrap();
                        if rhentry.count == usize::MAX {
                            continue;
                        }
                        rhentry.count = usize::MAX;

                        CX::ColorsMergeManagerType::<MH>::reset_unitig_color_structure(&mut temp_unitig_colors);

                        unsafe {
                            forward_seq.set_len(k);
                            backward_seq.set_len(k);
                        }

                        cread.write_to_slice(&mut forward_seq[..]);
                        backward_seq[..].copy_from_slice(&forward_seq[..]);
                        backward_seq.reverse();

                        CX::ColorsMergeManagerType::<MH>::extend_forward(&mut temp_unitig_colors, rhentry);

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
                                let mut temp_data = (*hash, 0);
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
                                            rhash_map.get(&new_hash.to_unextendable())
                                        {
                                            if hash.count >= min_multiplicity {
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
                                                    rhash_map.get(&bw_hash.to_unextendable())
                                                {
                                                    if hash.count >= min_multiplicity {
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

                                        let entryref = rhash_map
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
                                            colors_function(&mut temp_unitig_colors, entryref);
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
                            if *end_ignored {
                                Some(*hash)
                            } else {
                                let (fw_hash, end_ignored) = try_extend_function(
                                    &mut forward_seq,
                                    MH::manual_roll_forward,
                                    MH::manual_roll_reverse,
                                    CX::ColorsMergeManagerType::<MH>::extend_forward
                                );
                                match end_ignored {
                                    true => Some(fw_hash),
                                    false => None,
                                }
                            }
                        };

                        let bw_hash = {
                            if *begin_ignored {
                                Some(*hash)
                            } else {
                                let (bw_hash, begin_ignored) = try_extend_function(
                                    &mut backward_seq,
                                    MH::manual_roll_reverse,
                                    MH::manual_roll_forward,
                                    CX::ColorsMergeManagerType::<MH>::extend_backward
                                );
                                match begin_ignored {
                                    true => Some(bw_hash),
                                    false => None,
                                }
                            }
                        };

                        let out_seq = {
                            backward_seq.reverse();
                            backward_seq.extend_from_slice(&forward_seq[k..]);
                            &backward_seq[..]
                        };

                        // <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                        //     MH,
                        //     CX,
                        // >>::debug_tucs(&temp_unitig_colors, out_seq);

                        temp_bucket_writer.add_read(<CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                            MH,
                            CX,
                        >>::encode_part_unitigs_colors(&mut temp_unitig_colors), out_seq);

                        if let Some(fw_hash) = fw_hash {
                            let fw_hash = fw_hash.to_unextendable();
                            let fw_hash_sr = HashEntry {
                                hash: fw_hash,
                                bucket: bucket_index as u32,
                                entry: read_index,
                                direction: Direction::Forward,
                            };
                            let fw_bucket_index =
                                MH::get_bucket(fw_hash) % (buckets_count as BucketIndexType);
                            hashes_tmp.add_element(fw_bucket_index, &(), fw_hash_sr);
                        }

                        if let Some(bw_hash) = bw_hash {
                            let bw_hash = bw_hash.to_unextendable();

                            let bw_hash_sr = HashEntry {
                                hash: bw_hash,
                                bucket: bucket_index as u32,
                                entry: read_index,
                                direction: Direction::Backward,
                            };
                            let bw_bucket_index =
                                MH::get_bucket(bw_hash) % (buckets_count as BucketIndexType);
                            hashes_tmp.add_element(bw_bucket_index, &(), bw_hash_sr);
                        }

                        read_index += 1;
                    }
                }
            }

            temp_bucket_writer.finalize();
            hashes_tmp.finalize()
        });

        RetType {
            sequences: sequences.into_inner(),
            hashes: hashes_buckets.finalize(),
        }
    }
}
