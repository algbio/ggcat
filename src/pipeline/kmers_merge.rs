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

use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::hash::{ExtendableHashTraitType, HashFunction};
use crate::hash::{HashFunctionFactory, HashableSequence};
use crate::hash_entry::Direction;
use crate::hash_entry::HashEntry;
use crate::intermediate_storage::{
    IntermediateReadsReader, IntermediateReadsWriter, SequenceExtraData,
};
use crate::intermediate_storage_single::IntermediateSequencesStorageSingleBucket;
use crate::pipeline::Pipeline;
use crate::reads_storage::{FastaWriterConcurrentBuffer, ReadsStorage, ReadsWriter};
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::FastaSequence;
use crate::types::BucketIndexType;
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
pub struct KmersFlags(pub u8);

impl SequenceExtraData for KmersFlags {
    fn decode(mut reader: impl Read) -> Option<Self> {
        reader.read_u8().ok().map(|v| Self(v))
    }

    fn encode(&self, mut writer: impl Write) {
        writer.write_u8(self.0).unwrap();
    }
}

pub struct RetType {
    pub sequences: Vec<PathBuf>,
    pub hashes: Vec<PathBuf>,
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
struct ReadRef<H: HashFunctionFactory + Clone> {
    read_start: usize,
    read_len: usize,
    hash: H::HashTypeExtendable,
    flags: KmersFlags,
}

const MERGE_BUCKETS_COUNT: usize = 256;

impl Pipeline {
    pub fn kmers_merge<
        H: HashFunctionFactory,
        MH: HashFunctionFactory,
        P: AsRef<Path> + std::marker::Sync,
    >(
        file_inputs: Vec<PathBuf>,
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

        let mut reads_buckets = MultiThreadBuckets::<IntermediateReadsWriter<()>>::new(
            buckets_count,
            &out_directory.as_ref().join("result"),
            None,
        );

        file_inputs.par_iter().for_each(|input| {
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
            let mut cmp_reads: Vec<Vec<ReadRef<H>>> = vec![Vec::new(); MERGE_BUCKETS_COUNT];
            let mut buckets = &mut buckets[..];
            let mut cmp_reads = &mut cmp_reads[..];

            IntermediateReadsReader::<KmersFlags>::new(
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

                let slen = buckets[bucket as usize].len();
                buckets[bucket as usize].extend_from_slice(x.get_compr_slice());

                cmp_reads[bucket as usize].push(ReadRef {
                    read_start: slen,
                    read_len: x.bases_count(),
                    hash: minimizer,
                    flags,
                });
            });

            for b in 0..MERGE_BUCKETS_COUNT {
                let mut rcorrect_reads: Vec<(MH::HashTypeExtendable, usize, bool, bool)> =
                    Vec::new();
                let mut rhash_map = hashbrown::HashMap::with_capacity(4096);

                let mut backward_seq = Vec::new();
                let mut forward_seq = Vec::new();

                forward_seq.reserve(k);

                struct Compare<H> {
                    _phantom: PhantomData<H>,
                };
                impl<H: HashFunctionFactory> SortKey<ReadRef<H>> for Compare<H> {
                    type KeyType = H::HashTypeUnextendable;
                    const KEY_BITS: usize = size_of::<H::HashTypeUnextendable>() * 8;

                    #[inline(always)]
                    fn compare(left: &ReadRef<H>, right: &ReadRef<H>) -> std::cmp::Ordering {
                        left.hash
                            .to_unextendable()
                            .cmp(&right.hash.to_unextendable())
                    }

                    #[inline(always)]
                    fn get_shifted(value: &ReadRef<H>, rhs: u8) -> u8 {
                        H::get_shifted(value.hash.to_unextendable(), rhs)
                    }
                }

                fast_smart_radix_sort::<_, Compare<H>, false>(&mut cmp_reads[b]);

                for slice in cmp_reads[b]
                    .group_by(|a, b| a.hash.to_unextendable() == b.hash.to_unextendable())
                {
                    rhash_map.clear();
                    rcorrect_reads.clear();

                    let mut tot_reads = 0;
                    let mut tot_chars = 0;

                    let mut do_debug = false; //false;

                    for &ReadRef {
                        read_start,
                        read_len,
                        flags,
                        ..
                    } in slice
                    {
                        let read = CompressedRead::new_from_compressed(
                            &buckets[b][read_start..read_start + ((read_len + 3) / 4)],
                            read_len,
                        );

                        let hashes = MH::new(read, k);

                        struct MapEntry {
                            count: usize,
                            position: u32,
                            begin_ignored: bool,
                            end_ignored: bool,
                        }

                        let last_hash_pos = read_len - k;
                        let mut did_max = false;

                        for (idx, hash) in hashes.iter_enumerate() {
                            let position = (read_start * 4 + idx);
                            let begin_ignored = flags.0 & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                            let end_ignored =
                                flags.0 & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;
                            assert!(idx <= last_hash_pos);
                            did_max |= idx == last_hash_pos;

                            let entry =
                                rhash_map.entry(hash.to_unextendable()).or_insert(MapEntry {
                                    position: position as u32,
                                    begin_ignored,
                                    count: 0,
                                    end_ignored,
                                });

                            entry.count += 1;

                            if entry.count == min_multiplicity {
                                rcorrect_reads.push((hash, position, begin_ignored, end_ignored));
                            }
                        }
                        assert!(did_max);
                        tot_reads += 1;
                        tot_chars += read.bases_count();
                    }

                    for (hash, read_start, begin_ignored, end_ignored) in rcorrect_reads.iter() {
                        let mut read =
                            CompressedRead::from_compressed_reads(&buckets[b][..], *read_start, k);

                        let rhentry = rhash_map.get_mut(&hash.to_unextendable()).unwrap();
                        if rhentry.count == usize::MAX {
                            continue;
                        }
                        rhentry.count = usize::MAX;

                        backward_seq.clear();
                        unsafe {
                            forward_seq.set_len(k);
                        }

                        read.write_to_slice(&mut forward_seq[..]);

                        let mut try_extend_function =
                            |output: &mut Vec<u8>,
                             compute_hash_fw: fn(
                                hash: MH::HashTypeExtendable,
                                klen: usize,
                                out_b: u8,
                                in_b: u8,
                            )
                                -> MH::HashTypeExtendable,
                             out_base_index_fw: usize,
                             compute_hash_bw: fn(
                                hash: MH::HashTypeExtendable,
                                klen: usize,
                                out_b: u8,
                                in_b: u8,
                            )
                                -> MH::HashTypeExtendable,
                             out_base_index_bw: usize| {
                                let mut temp_data = (*hash, 0, 0);
                                let mut current_hash;

                                return 'ext_loop: loop {
                                    let mut count = 0;
                                    current_hash = temp_data.0;
                                    for idx in 0..4 {
                                        let new_hash = compute_hash_fw(
                                            current_hash,
                                            k,
                                            unsafe { read.get_base_unchecked(out_base_index_fw) },
                                            idx,
                                        );
                                        if let Some(hash) =
                                            rhash_map.get(&new_hash.to_unextendable())
                                        {
                                            if hash.count >= min_multiplicity {
                                                // println!("Forward match extend read {:x?}!", new_hash);
                                                count += 1;
                                                temp_data = (new_hash, idx, hash.position);
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

                                        output.push(Utils::decompress_base(temp_data.1));

                                        // Found a continuation into another bucket
                                        let contig_break =
                                            entryref.begin_ignored || entryref.end_ignored;
                                        if contig_break {
                                            break (temp_data.0, contig_break);
                                        }

                                        read = CompressedRead::from_compressed_reads(
                                            &buckets[b][..],
                                            temp_data.2 as usize,
                                            k,
                                        );
                                        // println!("Again!");
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
                                    0,
                                    MH::manual_roll_reverse,
                                    k - 1,
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
                                    k - 1,
                                    MH::manual_roll_forward,
                                    0,
                                );
                                match begin_ignored {
                                    true => Some(bw_hash),
                                    false => None,
                                }
                            }
                        };

                        let out_seq = if backward_seq.len() > 0 {
                            backward_seq.reverse();
                            backward_seq.extend_from_slice(&forward_seq[..]);
                            &backward_seq[..]
                        } else {
                            &forward_seq[..]
                        };

                        temp_bucket_writer.add_read((), out_seq);

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
