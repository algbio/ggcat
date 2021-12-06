use super::structs::{MapEntry, ReadRef, ResultsBucket};
use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager};
use crate::hashes::HashFunction;
use crate::hashes::{ExtendableHashTraitType, HashFunctionFactory, HashableSequence};
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::io::structs::hash_entry::{Direction, HashEntry};
use crate::io::varint::decode_varint_flags;
use crate::types::BucketIndexType;
use crate::utils::async_vec::AsyncVec;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::debug_utils::debug_increase;
use crate::utils::Utils;
use hashbrown::HashMap;
use parallel_processor::binary_writer::BinaryWriter;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use std::mem::size_of;
use std::slice::from_raw_parts;

pub const READ_FLAG_INCL_BEGIN: u8 = (1 << 0);
pub const READ_FLAG_INCL_END: u8 = (1 << 1);

pub type ResultsBucketType<'a, MH: HashFunctionFactory, CX: ColorsManager> = ResultsBucket<
    'a,
    <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::PartialUnitigsColorStructure,
>;

pub struct GlobalSubBucketContext<'a, CX: ColorsManager> {
    pub k: usize,
    pub min_multiplicity: usize,
    pub colors_map: &'a CX::GlobalColorsTable,
    pub buckets_count: usize,
}

pub struct SubBucketThreadContext<'a, MH: HashFunctionFactory, CX: ColorsManager> {
    pub gctx: &'a GlobalSubBucketContext<'a, CX>,
    pub current_bucket: ResultsBucketType<'a, MH, CX>,
    pub hashes_tmp: BucketsThreadDispatcher<'a, BinaryWriter, HashEntry<MH::HashTypeUnextendable>>,
}

pub fn process_subbucket<MH: HashFunctionFactory, CX: ColorsManager>(
    context: &mut SubBucketThreadContext<MH, CX>,
    cmp_read: AsyncVec<ReadRef>,
) {
    let cmp_read_slice = cmp_read.as_slice_mut();

    let mut rcorrect_reads: Vec<(MH::HashTypeExtendable, *const u8, u8, bool, bool)> = Vec::new();
    let mut rhash_map = HashMap::with_capacity(4096);

    let mut colors_struct = CX::ColorsMergeManagerType::<MH>::allocate_temp_buffer_structure();

    let mut backward_seq = Vec::new();
    let mut forward_seq = Vec::new();

    let k = context.gctx.k;
    let min_multiplicity = context.gctx.min_multiplicity;
    let global_colors_table = &context.gctx.colors_map;
    let current_bucket = &mut context.current_bucket;
    let bucket_index = current_bucket.get_bucket_index();
    let hashes_tmp = &mut context.hashes_tmp;
    let buckets_count = context.gctx.buckets_count;

    forward_seq.reserve(k);
    backward_seq.reserve(k);

    struct Compare {};
    impl SortKey<ReadRef> for Compare {
        type KeyType = u64;
        const KEY_BITS: usize = size_of::<u64>() * 8;

        #[inline(always)]
        fn compare(left: &ReadRef, right: &ReadRef) -> std::cmp::Ordering {
            left.hash.cmp(&right.hash)
        }

        #[inline(always)]
        fn get_shifted(value: &ReadRef, rhs: u8) -> u8 {
            (value.hash >> rhs) as u8
        }
    }

    fast_smart_radix_sort::<_, Compare, false>(cmp_read_slice);

    let mut temp_unitig_colors = CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure();

    for slice in cmp_read_slice.group_by(|a, b| a.hash == b.hash) {
        CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut colors_struct);
        rhash_map.clear();
        rcorrect_reads.clear();

        let mut tot_reads = 0;
        let mut tot_chars = 0;

        for &ReadRef { mut read_start, .. } in slice {
            let (read_len, kmer_flags) = decode_varint_flags::<_, 2>(|| {
                let x = unsafe { *read_start };
                read_start = unsafe { read_start.add(1) };
                Some(x)
            })
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

                let entry = rhash_map.entry(hash.to_unextendable()).or_insert(MapEntry {
                    ignored: begin_ignored || end_ignored,
                    count: 0,
                    color_index: CX::ColorsMergeManagerType::<MH>::new_color_index(),
                });

                entry.count += 1;
                CX::ColorsMergeManagerType::<MH>::add_temp_buffer_structure_el(
                    &mut colors_struct,
                    &color,
                    (idx, hash.to_unextendable()),
                );

                if entry.count == min_multiplicity {
                    rcorrect_reads.push((
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
                global_colors_table,
                &mut colors_struct,
                &mut rhash_map,
                min_multiplicity,
            );
        }

        for (hash, read_bases_start, reads_offset, begin_ignored, end_ignored) in
            rcorrect_reads.drain(..)
        {
            let reads_slice =
                unsafe { from_raw_parts(read_bases_start, (k + reads_offset as usize + 3) / 4) };

            let cread =
                CompressedRead::from_compressed_reads(reads_slice, reads_offset as usize, k);

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
                if end_ignored {
                    Some(hash)
                } else {
                    let (fw_hash, end_ignored) = try_extend_function(
                        &mut forward_seq,
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
                        &mut backward_seq,
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
                backward_seq.reverse();
                backward_seq.extend_from_slice(&forward_seq[k..]);
                &backward_seq[..]
            };

            // <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
            //     MH,
            //     CX,
            // >>::debug_tucs(&temp_unitig_colors, out_seq);

            let read_index = current_bucket.add_read(<CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                MH,
                CX,
            >>::encode_part_unitigs_colors(&mut temp_unitig_colors), out_seq);

            if let Some(fw_hash) = fw_hash {
                let fw_hash = fw_hash.to_unextendable();
                let fw_hash_sr = HashEntry {
                    hash: fw_hash,
                    bucket: bucket_index,
                    entry: read_index,
                    direction: Direction::Forward,
                };
                let fw_bucket_index = MH::get_bucket(fw_hash) % (buckets_count as BucketIndexType);
                hashes_tmp.add_element(fw_bucket_index, &(), fw_hash_sr);
            }

            if let Some(bw_hash) = bw_hash {
                let bw_hash = bw_hash.to_unextendable();

                let bw_hash_sr = HashEntry {
                    hash: bw_hash,
                    bucket: bucket_index,
                    entry: read_index,
                    direction: Direction::Backward,
                };
                let bw_bucket_index = MH::get_bucket(bw_hash) % (buckets_count as BucketIndexType);
                hashes_tmp.add_element(bw_bucket_index, &(), bw_hash_sr);
            }
        }
    }
}
