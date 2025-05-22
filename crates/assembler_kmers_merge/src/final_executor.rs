use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::structs::PartialUnitigExtraData;
use crate::{GlobalMergeData, ParallelKmersMergeFactory, ResultsBucket};
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::{color_types, ColorsManager};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use core::slice::from_raw_parts;
use ggcat_logging::stats;
use hashes::HashFunction;
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use io::varint::decode_varint;
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::ops::DerefMut;
use structs::map_entry::MapEntry;
#[cfg(feature = "support_kmer_counters")]
use structs::unitigs_counters::UnitigsCounters;
use utils::Utils;

local_setup_instrumenter!();

pub struct ParallelKmersMergeFinalExecutor<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    const COMPUTE_SIMPLITIGS: bool,
> {
    hashes_tmp: BucketsThreadDispatcher<
        LockFreeBinaryWriter,
        HashEntrySerializer<MH::HashTypeUnextendable>,
    >,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    unitigs_temp_colors: color_types::TempUnitigColorStructure<CX>,
    current_bucket: Option<ResultsBucket<color_types::PartialUnitigsColorStructure<CX>>>,
    temp_color_buffer:
        <color_types::PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    bucket_counter: usize,
    bucket_change_threshold: usize,
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    pub fn new(global_data: &GlobalMergeData<CX>) -> Self {
        let hashes_buffer =
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, global_data.buckets_count);

        Self {
            hashes_tmp: BucketsThreadDispatcher::new(
                &global_data.hashes_buckets,
                hashes_buffer,
                (),
            ),
            forward_seq: Vec::with_capacity(global_data.k),
            backward_seq: Vec::with_capacity(global_data.k),
            unitigs_temp_colors: CX::ColorsMergeManagerType::alloc_unitig_color_structure(),
            current_bucket: None,
            temp_color_buffer: color_types::PartialUnitigsColorStructure::<CX>::new_temp_buffer(),
            bucket_counter: 0,
            bucket_change_threshold: 16, // TODO: Parametrize
        }
    }

    fn get_kmers(
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        map_struct: &ParallelKmersMergeMapPacket<MH, CX>,
        mut callback: impl FnMut(
            MH::HashTypeExtendable,
            CompressedRead,
            &MapEntry<color_types::HashMapTempColorIndex<CX>>,
        ),
    ) {
        let k = global_data.k;

        if MH::INVERTIBLE {
            for (hash, rhentry) in map_struct.rhash_map.iter() {
                let count = rhentry.get_kmer_multiplicity();
                if count < global_data.min_multiplicity {
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
                let value = map_struct.encoded_saved_reads_indexes.get(cursor).copied();
                cursor += 1;
                value
            })
            .unwrap() as usize;
            let mut is_last = false;

            while !is_last {
                let read_start = last_saved_index as usize;
                let read_end = last_saved_index
                    + decode_varint(|| {
                        let value = map_struct.encoded_saved_reads_indexes.get(cursor).copied();
                        cursor += 1;
                        value
                    })
                    .unwrap_or_else(|| {
                        is_last = true;
                        (map_struct.saved_reads.len() - last_saved_index) as u64
                    }) as usize;
                last_saved_index = read_end;

                let hashes_iter = MH::new(
                    CompressedRead::from_compressed_reads(
                        &map_struct.saved_reads[read_start..read_end],
                        0,
                        (read_end - read_start) * 4,
                    ),
                    k,
                );

                for (base_index, hash) in hashes_iter.iter_enumerate() {
                    let rhentry = match map_struct.rhash_map.get(&hash.to_unextendable()) {
                        Some(entry) => entry,
                        None => {
                            continue;
                        }
                    };

                    let count = rhentry.get_kmer_multiplicity();
                    if count < global_data.min_multiplicity {
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
                            map_struct.saved_reads.as_ptr().add(read_bases_start),
                            (k + reads_offset + 3) / 4,
                        )
                    };

                    let cread = CompressedRead::from_compressed_reads(reads_slice, reads_offset, k);
                    callback(hash, cread, rhentry);
                }
            }
        }
    }
}

// static DEBUG_MAPS_HOLDER: Mutex<Vec<Box<dyn Any + Sync + Send>>> = const_mutex(Vec::new());

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    KmersTransformFinalExecutor<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>>
    for ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX>;

    #[instrumenter::track(fields(map_capacity = map_struct_packet.rhash_map.capacity(), map_size = map_struct_packet.rhash_map.len()))]
    fn process_map(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        mut map_struct_packet: Packet<Self::MapStruct>,
    ) -> Packet<Self::MapStruct> {
        if self.current_bucket.is_none() {
            self.current_bucket = Some(global_data.output_results_buckets.pop().unwrap());
        }

        stats!(
            map_struct_packet.detailed_stats.start_finalize_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            let mut stat_output_kmers_count = 0;
        );

        let map_struct = map_struct_packet.deref_mut();

        let k = global_data.k;
        let buckets_count = global_data.buckets_count;
        let buckets_count_bits = buckets_count.ilog2() as usize;

        let current_bucket = self.current_bucket.as_mut().unwrap();
        let bucket_index = current_bucket.get_bucket_index();

        if CX::COLORS_ENABLED {
            CX::ColorsMergeManagerType::process_colors::<MH>(
                &global_data.colors_global_table,
                &mut map_struct.temp_colors,
                &mut map_struct.rhash_map,
                global_data.k,
                global_data.min_multiplicity,
            );
        }

        let bases_count = map_struct.saved_reads.len() * 4;
        if !MH::INVERTIBLE && bases_count < k {
            return map_struct_packet;
        }

        Self::get_kmers(global_data, map_struct, |hash, cread, rhentry| {
            stats!(
                stat_output_kmers_count += 1;
            );

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

            CX::ColorsMergeManagerType::reset_unitig_color_structure(&mut self.unitigs_temp_colors);

            unsafe {
                self.forward_seq.set_len(k);
                self.backward_seq.set_len(k);
            }

            cread.write_unpacked_to_slice(&mut self.forward_seq[..]);
            self.backward_seq[..].copy_from_slice(&self.forward_seq[..]);
            self.backward_seq.reverse();

            CX::ColorsMergeManagerType::extend_forward(&mut self.unitigs_temp_colors, rhentry);
            rhentry.set_used();

            #[cfg(feature = "support_kmer_counters")]
            let first_count = rhentry.get_kmer_multiplicity() as u64;

            #[cfg(feature = "support_kmer_counters")]
            let mut counters = UnitigsCounters {
                first: first_count,
                sum: first_count,
                last: first_count,
            };

            let mut try_extend_function =
                |output: &mut Vec<u8>,
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
                 #[cfg(feature = "support_kmer_counters")] is_forward: bool| {
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
                                k,
                                Utils::compress_base(output[output.len() - k]),
                                idx,
                            );
                            if let Some(hash) =
                                map_struct.rhash_map.get(&new_hash.to_unextendable())
                            {
                                if hash.get_kmer_multiplicity() >= global_data.min_multiplicity {
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
                                        let entryref = map_struct
                                            .rhash_map
                                            .get(&temp_data.0.to_unextendable())
                                            .unwrap();

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
                                    let bw_hash = compute_hash_bw(new_hash, k, temp_data.1, idx);
                                    if let Some(hash) =
                                        map_struct.rhash_map.get(&bw_hash.to_unextendable())
                                    {
                                        if hash.get_kmer_multiplicity()
                                            >= global_data.min_multiplicity
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

                            let entryref = map_struct
                                .rhash_map
                                .get(&temp_data.0.to_unextendable())
                                .unwrap();

                            let already_used = entryref.is_used();

                            // Found a cycle unitig
                            if already_used {
                                break (temp_data.0, false);
                            }

                            if CX::COLORS_ENABLED {
                                colors_function(&mut self.unitigs_temp_colors, entryref);
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
                        CX::ColorsMergeManagerType::extend_forward,
                        #[cfg(feature = "support_kmer_counters")]
                        true,
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
                        CX::ColorsMergeManagerType::extend_backward,
                        #[cfg(feature = "support_kmer_counters")]
                        false,
                    );
                    (bw_hash, begin_ignored)
                }
            };

            let out_seq = {
                self.backward_seq.reverse();
                self.backward_seq.extend_from_slice(&self.forward_seq[k..]);
                &self.backward_seq[..]
            };

            let colors = color_types::ColorsMergeManagerType::<CX>::encode_part_unitigs_colors(
                &mut self.unitigs_temp_colors,
                &mut self.temp_color_buffer,
            );

            let extra_data = PartialUnitigExtraData {
                colors,

                #[cfg(feature = "support_kmer_counters")]
                counters,
            };

            let read_index = current_bucket.add_read(extra_data, out_seq, &self.temp_color_buffer);

            color_types::PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                &mut self.temp_color_buffer,
            );

            Self::write_hashes(
                &mut self.hashes_tmp,
                fw_hash.to_unextendable(),
                bucket_index,
                read_index,
                fw_merge,
                Direction::Forward,
                buckets_count_bits,
            );

            Self::write_hashes(
                &mut self.hashes_tmp,
                bw_hash.to_unextendable(),
                bucket_index,
                read_index,
                bw_merge,
                Direction::Backward,
                buckets_count_bits,
            );
        });

        self.bucket_counter += 1;
        if self.bucket_counter >= self.bucket_change_threshold {
            self.bucket_counter = 0;
            let _ = global_data
                .output_results_buckets
                .push(self.current_bucket.take().unwrap());
        }

        stats!(
            map_struct_packet.detailed_stats.end_finalize_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            map_struct_packet.detailed_stats.output_kmers_count = stat_output_kmers_count;
        );

        stats!(stats.assembler.kmers_merge_stats.push(map_struct_packet.detailed_stats.clone()););

        // DEBUG_MAPS_HOLDER.lock().push(Box::new(map_struct_packet));
        map_struct_packet
    }

    fn finalize(
        self,
        _global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        self.hashes_tmp.finalize();
    }
}
