use std::{
    cmp::{max, min},
    ops::Deref,
    slice::from_raw_parts,
};

use colors::colors_manager::{
    ColorsManager, ColorsMergeManager, MinimizerBucketingSeqColorData,
    color_types::{self, MinimizerBucketingMultipleSeqColorDataType},
};
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use hashes::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
    varint::{decode_varint, encode_varint},
};
use kmers_transform::{GroupProcessStats, reads_buffer::DeserializedReadIndependent};
use rustc_hash::{FxBuildHasher, FxHashMap};
use structs::map_entry::MapEntry;
use utils::Utils;

use crate::KMERGE_TEMP_DIR;

use super::{GlobalExtenderParams, UnitigExtensionColorsData, UnitigsExtenderTrait};

pub struct HashMapUnitigsExtender<MH: HashFunctionFactory, CX: ColorsManager> {
    params: GlobalExtenderParams,
    rhash_map:
        FxHashMap<MH::HashTypeUnextendable, MapEntry<color_types::HashMapTempColorIndex<CX>>>,
    saved_reads: Vec<u8>,
    encoded_saved_reads_indexes: Vec<u8>,
    temp_colors: color_types::ColorsBufferTempStructure<CX>,
    suggested_hasmap_size: u64,
    suggested_sequences_size: u64,
    kmers_count: u64,
    unique_kmers_count: u64,
    last_saved_len: usize,
    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
}

#[inline]
fn clear_hashmap<K, V>(hashmap: &mut FxHashMap<K, V>, suggested_size: usize) {
    let suggested_capacity = (suggested_size / 2).next_power_of_two();

    if hashmap.capacity() < suggested_capacity {
        hashmap.clear();
    } else {
        // Reset the hashmap if it gets too big
        *hashmap = FxHashMap::with_capacity_and_hasher(suggested_capacity, FxBuildHasher);
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> HashMapUnitigsExtender<MH, CX> {
    fn get_kmers(
        &self,
        mut callback: impl FnMut(
            MH::HashTypeExtendable,
            CompressedRead,
            &MapEntry<color_types::HashMapTempColorIndex<CX>>,
        ),
    ) {
        let k = self.params.k;

        if MH::INVERTIBLE {
            for (hash, rhentry) in self.rhash_map.iter() {
                let count = rhentry.get_kmer_multiplicity();
                if count < self.params.min_multiplicity {
                    continue;
                }

                if rhentry.is_used() {
                    continue;
                }

                let cread_bases = MH::invert(*hash);
                let cread = CompressedRead::new_from_compressed(cread_bases.as_ref(), k);
                let hash = MH::new(cread, k).iter().next().unwrap();

                callback(hash, cread, rhentry);
            }
        } else {
            let mut cursor = 0;

            let mut last_saved_index = decode_varint(|| {
                let value = self.encoded_saved_reads_indexes.get(cursor).copied();
                cursor += 1;
                value
            })
            .unwrap() as usize;
            let mut is_last = false;

            while !is_last {
                let read_start = last_saved_index as usize;
                let read_end = last_saved_index
                    + decode_varint(|| {
                        let value = self.encoded_saved_reads_indexes.get(cursor).copied();
                        cursor += 1;
                        value
                    })
                    .unwrap_or_else(|| {
                        is_last = true;
                        (self.saved_reads.len() - last_saved_index) as u64
                    }) as usize;
                last_saved_index = read_end;

                let hashes_iter = MH::new(
                    CompressedRead::from_compressed_reads(
                        &self.saved_reads[read_start..read_end],
                        0,
                        (read_end - read_start) * 4,
                    ),
                    k,
                );

                for (base_index, hash) in hashes_iter.iter_enumerate() {
                    let rhentry = match self.rhash_map.get(&hash.to_unextendable()) {
                        Some(entry) => entry,
                        None => {
                            continue;
                        }
                    };

                    let count = rhentry.get_kmer_multiplicity();
                    if count < self.params.min_multiplicity {
                        continue;
                    }

                    if rhentry.is_used() {
                        continue;
                    }

                    let base_index = read_start * 4 + base_index;
                    let read_bases_start = base_index / 4;
                    let reads_offset = base_index % 4;

                    let reads_slice = unsafe {
                        from_raw_parts(
                            self.saved_reads.as_ptr().add(read_bases_start),
                            (k + reads_offset + 3) / 4,
                        )
                    };

                    let cread = CompressedRead::from_compressed_reads(reads_slice, reads_offset, k);
                    callback(hash, cread, rhentry);
                }
            }
        }
    }

    #[inline(always)]
    fn try_extend_function<const COMPUTE_SIMPLITIGS: bool>(
        &self,
        colors_data: &mut UnitigExtensionColorsData<CX>,
        hash: MH::HashTypeExtendable,
        output: &mut Vec<u8>,
        compute_hash_fw: fn(
            hash: MH::HashTypeExtendable,
            kmer_length: usize,
            out_b: u8,
            in_b: u8,
        ) -> MH::HashTypeExtendable,
        compute_hash_bw: fn(
            hash: MH::HashTypeExtendable,
            kmer_length: usize,
            out_b: u8,
            in_b: u8,
        ) -> MH::HashTypeExtendable,
        colors_function: fn(
            ts: &mut color_types::TempUnitigColorStructure<CX>,
            entry: &MapEntry<color_types::HashMapTempColorIndex<CX>>,
        ),
        #[cfg(feature = "support_kmer_counters")] is_forward: bool,
    ) -> Option<MH::HashTypeUnextendable> {
        let mut temp_data = (hash, 0);
        let mut current_hash;

        return 'ext_loop: loop {
            let mut count = 0;
            current_hash = temp_data.0;
            #[cfg(feature = "support_kmer_counters")]
            let mut multiplicity = 0;
            for idx in 0..4 {
                let new_hash = compute_hash_fw(
                    current_hash,
                    self.params.k,
                    Utils::compress_base(output[output.len() - self.params.k]),
                    idx,
                );
                if let Some(hash) = self.rhash_map.get(&new_hash.to_unextendable()) {
                    if hash.get_kmer_multiplicity() >= self.params.min_multiplicity {
                        // ggcat_logging::info!("Forward match extend read {:x?}!", new_hash);
                        #[cfg(feature = "support_kmer_counters")]
                        {
                            multiplicity = hash.get_kmer_multiplicity() as u64;
                        }
                        count += 1;
                        temp_data = (new_hash, idx);

                        if COMPUTE_SIMPLITIGS {
                            // If we are computing simplitigs, it can be that multiple outgoing edges could be valid,
                            // but some of them are already used. So we must check that the chosen branch is not used yet before selecting it
                            let entryref =
                                self.rhash_map.get(&temp_data.0.to_unextendable()).unwrap();

                            let already_used = entryref.is_used();
                            if !already_used {
                                break;
                            }
                        }
                    }
                }
            }

            let should_extend = if COMPUTE_SIMPLITIGS {
                // Simplitigs can be always extended if there is an outgoing edge
                count > 0
            } else {
                // Unitigs can be extended only if they're not branching
                count == 1
            };

            if should_extend {
                // Test for backward branches
                if !COMPUTE_SIMPLITIGS {
                    let mut ocount = 0;
                    let new_hash = temp_data.0;
                    for idx in 0..4 {
                        let bw_hash = compute_hash_bw(new_hash, self.params.k, temp_data.1, idx);
                        if let Some(hash) = self.rhash_map.get(&bw_hash.to_unextendable()) {
                            if hash.get_kmer_multiplicity() >= self.params.min_multiplicity {
                                if ocount > 0 {
                                    break 'ext_loop None;
                                }
                                ocount += 1;
                            }
                        }
                    }
                    assert_eq!(ocount, 1);
                }

                let entryref = self.rhash_map.get(&temp_data.0.to_unextendable()).unwrap();

                let already_used = entryref.is_used();

                // Found a cycle unitig
                if already_used {
                    break None;
                }

                if CX::COLORS_ENABLED {
                    colors_function(&mut colors_data.unitigs_temp_colors, entryref);
                }

                // Flag the entry as already used
                entryref.set_used();

                output.push(Utils::decompress_base(temp_data.1));

                #[cfg(feature = "support_kmer_counters")]
                {
                    counters.sum += multiplicity;
                    if is_forward {
                        counters.last = multiplicity;
                    } else {
                        counters.first = multiplicity;
                    }
                }

                // Found a continuation into another bucket
                let contig_break = (entryref.get_flags() == READ_FLAG_INCL_BEGIN)
                    || (entryref.get_flags() == READ_FLAG_INCL_END);
                if contig_break {
                    break Some(temp_data.0.to_unextendable());
                }
            } else {
                break None;
            }
        };
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> UnitigsExtenderTrait<MH, CX>
    for HashMapUnitigsExtender<MH, CX>
{
    #[inline]
    fn new(params: &GlobalExtenderParams) -> Self {
        Self {
            params: *params,
            rhash_map: FxHashMap::with_capacity_and_hasher(4096, FxBuildHasher),
            saved_reads: vec![],
            encoded_saved_reads_indexes: vec![],
            temp_colors: CX::ColorsMergeManagerType::allocate_temp_buffer_structure(
                KMERGE_TEMP_DIR.read().deref().as_ref().unwrap(),
            ),
            suggested_hasmap_size: 0,
            suggested_sequences_size: 0,
            kmers_count: 0,
            unique_kmers_count: 0,
            last_saved_len: 0,
            forward_seq: Vec::with_capacity(params.k * 2),
            backward_seq: Vec::with_capacity(params.k * 2),
        }
    }

    fn reset(&mut self) {
        clear_hashmap(
            &mut self.rhash_map,
            max(8192, self.suggested_hasmap_size as usize),
        );

        let saved_reads_suggested_size =
            (self.suggested_sequences_size).next_power_of_two() as usize;

        if self.saved_reads.capacity() < saved_reads_suggested_size {
            self.saved_reads.clear();
        } else {
            self.saved_reads = Vec::with_capacity(saved_reads_suggested_size)
        }

        if self.encoded_saved_reads_indexes.capacity() < saved_reads_suggested_size {
            self.encoded_saved_reads_indexes.clear();
        } else {
            self.encoded_saved_reads_indexes = Vec::with_capacity(saved_reads_suggested_size)
        }

        self.kmers_count = 0;
        self.unique_kmers_count = 0;
        self.last_saved_len = 0;

        CX::ColorsMergeManagerType::reinit_temp_buffer_structure(&mut self.temp_colors);
    }

    fn get_memory_usage(&self) -> usize {
        self.rhash_map.len()
            * (size_of::<(
                MH::HashTypeUnextendable,
                MapEntry<color_types::HashMapTempColorIndex<CX>>,
            )>() + 1)
            + self.saved_reads.len()
    }

    fn add_sequence(
        &mut self,
        sequences_data: &[u8],
        extra_buffer: &<MinimizerBucketingMultipleSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        sequence: DeserializedReadIndependent<MinimizerBucketingMultipleSeqColorDataType<CX>>,
    ) {
        let read = sequence.read.as_reference(sequences_data);

        let hashes = MH::new(read, self.params.k);

        self.kmers_count += (read.bases_count() - self.params.k + 1) as u64;

        let last_hash_pos = read.bases_count() - self.params.k;
        let mut min_idx = usize::MAX;
        let mut max_idx = 0;

        for ((idx, hash), kmer_color) in hashes
            .iter_enumerate()
            .zip(sequence.extra.get_iterator(extra_buffer))
        {
            let begin_ignored = sequence.flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
            let end_ignored = sequence.flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;

            let is_forward = hash.is_forward();

            let entry = self
                .rhash_map
                .entry(hash.to_unextendable())
                .or_insert_with(|| {
                    self.unique_kmers_count += 1;
                    MapEntry::new(CX::ColorsMergeManagerType::new_color_index())
                });

            entry.update_flags(
                ((begin_ignored as u8) << ((!is_forward) as u8))
                    | ((end_ignored as u8) << (is_forward as u8)),
            );

            let crossed_min_abundance =
                entry.incr_by_and_check(sequence.multiplicity, self.params.min_multiplicity);

            CX::ColorsMergeManagerType::add_temp_buffer_structure_el::<MH>(
                &mut self.temp_colors,
                kmer_color,
                (idx, hash.to_unextendable()),
                entry,
            );

            // Update the valid indexes to allow saving reads that cross the min abundance threshold
            if !MH::INVERTIBLE && crossed_min_abundance {
                min_idx = min(min_idx, idx / 4);
                max_idx = max(max_idx, idx);
            }
        }

        CX::ColorsMergeManagerType::add_temp_buffer_sequence(
            &mut self.temp_colors,
            read,
            self.params.k,
            self.params.m,
            sequence.flags,
        );

        if !MH::INVERTIBLE {
            if min_idx != usize::MAX {
                encode_varint(
                    |b| {
                        self.encoded_saved_reads_indexes.extend_from_slice(b);
                    },
                    (self.saved_reads.len() - self.last_saved_len) as u64,
                );
                self.last_saved_len = self.saved_reads.len();
                self.saved_reads.extend_from_slice(
                    &read.get_packed_slice()[min_idx..((max_idx + self.params.k + 3) / 4)],
                );
            }
        }
    }

    fn get_stats(&self) -> GroupProcessStats {
        GroupProcessStats {
            total_kmers: self.kmers_count,
            unique_kmers: self.unique_kmers_count,
            saved_read_bytes: self.saved_reads.len() as u64,
        }
    }

    fn compute_unitigs<const COMPUTE_SIMPLITIGS: bool>(
        &mut self,
        colors_data: &mut UnitigExtensionColorsData<CX>,
        mut output_unitig: impl FnMut(
            &mut UnitigExtensionColorsData<CX>,
            &[u8],
            Option<MH::HashTypeUnextendable>,
            Option<MH::HashTypeUnextendable>,
        ),
    ) {
        if CX::COLORS_ENABLED {
            CX::ColorsMergeManagerType::process_colors::<MH>(
                &colors_data.colors_global_table,
                &mut self.temp_colors,
                &mut self.rhash_map,
                self.params.k,
                self.params.min_multiplicity,
            );
        }

        let bases_count = self.saved_reads.len() * 4;
        if !MH::INVERTIBLE && bases_count < self.params.k {
            return;
        }

        let forward_seq = unsafe { &mut *(&mut self.forward_seq as *mut Vec<u8>) };
        let backward_seq = unsafe { &mut *(&mut self.backward_seq as *mut Vec<u8>) };

        self.get_kmers(|hash, cread, rhentry| {
            let ignored_status = rhentry.get_flags();

            let (begin_ignored, end_ignored) = if hash.is_forward() {
                (
                    ignored_status == READ_FLAG_INCL_BEGIN,
                    ignored_status == READ_FLAG_INCL_END,
                )
            } else {
                (
                    ignored_status == READ_FLAG_INCL_END,
                    ignored_status == READ_FLAG_INCL_BEGIN,
                )
            };

            CX::ColorsMergeManagerType::reset_unitig_color_structure(
                &mut colors_data.unitigs_temp_colors,
            );

            unsafe {
                forward_seq.set_len(self.params.k);
                backward_seq.set_len(self.params.k);
            }

            cread.write_unpacked_to_slice(&mut forward_seq[..]);
            backward_seq[..].copy_from_slice(&forward_seq[..]);
            backward_seq.reverse();

            CX::ColorsMergeManagerType::extend_forward(
                &mut colors_data.unitigs_temp_colors,
                rhentry,
            );
            rhentry.set_used();

            #[cfg(feature = "support_kmer_counters")]
            let first_count = rhentry.get_kmer_multiplicity() as u64;

            #[cfg(feature = "support_kmer_counters")]
            let mut counters = UnitigsCounters {
                first: first_count,
                sum: first_count,
                last: first_count,
            };

            let fw_hash = {
                if end_ignored {
                    Some(hash.to_unextendable())
                } else {
                    self.try_extend_function::<COMPUTE_SIMPLITIGS>(
                        colors_data,
                        hash,
                        forward_seq,
                        MH::manual_roll_forward,
                        MH::manual_roll_reverse,
                        CX::ColorsMergeManagerType::extend_forward,
                        #[cfg(feature = "support_kmer_counters")]
                        true,
                    )
                }
            };

            let bw_hash = {
                if begin_ignored {
                    Some(hash.to_unextendable())
                } else {
                    self.try_extend_function::<COMPUTE_SIMPLITIGS>(
                        colors_data,
                        hash,
                        backward_seq,
                        MH::manual_roll_reverse,
                        MH::manual_roll_forward,
                        CX::ColorsMergeManagerType::extend_backward,
                        #[cfg(feature = "support_kmer_counters")]
                        false,
                    )
                }
            };

            let out_seq = {
                backward_seq.reverse();
                backward_seq.extend_from_slice(&forward_seq[self.params.k..]);
                &backward_seq[..]
            };

            output_unitig(colors_data, out_seq, fw_hash, bw_hash)
        });
    }

    fn set_suggested_sizes(&mut self, hashmap_size: u64, sequences_size: u64) {
        self.suggested_hasmap_size = hashmap_size;
        self.suggested_sequences_size = sequences_size;
    }
}
