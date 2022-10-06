use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::{GlobalMergeData, ParallelKmersMergeFactory, ResultsBucket};
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::{color_types, ColorsManager};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use core::slice::from_raw_parts;
use hashes::HashFunction;
use hashes::{ExtendableHashTraitType, HashFunctionFactory, MinimizerHashFunctionFactory};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::structs::hash_entry::Direction;
use io::varint::decode_varint;
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::marker::PhantomData;
use std::ops::DerefMut;
use structs::map_entry::MapEntry;
use utils::Utils;

local_setup_instrumenter!();

pub struct ParallelKmersMergeFinalExecutor<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    hashes_tmp: BucketsThreadDispatcher<LockFreeBinaryWriter>,

    forward_seq: Vec<u8>,
    backward_seq: Vec<u8>,
    unitigs_temp_colors: color_types::TempUnitigColorStructure<MH, CX>,
    current_bucket: Option<ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>>,
    temp_color_buffer:
        <color_types::PartialUnitigsColorStructure<MH, CX> as SequenceExtraData>::TempBuffer,
    bucket_counter: usize,
    bucket_change_threshold: usize,
    _phantom: PhantomData<H>,
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    ParallelKmersMergeFinalExecutor<H, MH, CX>
{
    pub fn new(global_data: &GlobalMergeData<MH, CX>) -> Self {
        let hashes_buffer =
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, global_data.buckets_count);

        Self {
            hashes_tmp: BucketsThreadDispatcher::new(&global_data.hashes_buckets, hashes_buffer),
            forward_seq: Vec::with_capacity(global_data.k),
            backward_seq: Vec::with_capacity(global_data.k),
            unitigs_temp_colors: CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure(),
            current_bucket: None,
            temp_color_buffer: color_types::PartialUnitigsColorStructure::<MH, CX>::new_temp_buffer(
            ),
            bucket_counter: 0,
            bucket_change_threshold: 16, // TODO: Parametrize
            _phantom: PhantomData,
        }
    }
}

// static DEBUG_MAPS_HOLDER: Mutex<Vec<Box<dyn Any + Sync + Send>>> = const_mutex(Vec::new());

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformFinalExecutor<ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMergeFinalExecutor<H, MH, CX>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX>;

    #[instrumenter::track(fields(map_capacity = map_struct_packet.rhash_map.capacity(), map_size = map_struct_packet.rhash_map.len()))]
    fn process_map(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
        mut map_struct_packet: Packet<Self::MapStruct>,
    ) -> Packet<Self::MapStruct> {
        if self.current_bucket.is_none() {
            self.current_bucket = Some(global_data.output_results_buckets.pop().unwrap());
        }

        let map_struct = map_struct_packet.deref_mut();

        let k = global_data.k;
        let buckets_count = global_data.buckets_count;
        let buckets_count_bits = buckets_count.ilog2() as usize;

        let current_bucket = self.current_bucket.as_mut().unwrap();
        let bucket_index = current_bucket.get_bucket_index();

        if CX::COLORS_ENABLED {
            CX::ColorsMergeManagerType::<MH>::process_colors(
                &global_data.colors_global_table,
                &mut map_struct.temp_colors,
                &mut map_struct.rhash_map,
                global_data.k,
                global_data.min_multiplicity,
            );
        }

        let bases_count = map_struct.saved_reads.len() * 4;
        if bases_count < k {
            return map_struct_packet;
        }

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
                let rhentry = match map_struct.rhash_map.get_mut(&hash.to_unextendable()) {
                    Some(entry) => entry,
                    None => {
                        continue;
                    }
                };

                // If the current set has both the partial sequences endings, we should divide the counter by 2,
                // as all the kmers are counted exactly two times
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
                rhentry.set_used();

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
                            if let Some(hash) =
                                map_struct.rhash_map.get(&new_hash.to_unextendable())
                            {
                                if hash.get_kmer_multiplicity() >= global_data.min_multiplicity {
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
                                .get_mut(&temp_data.0.to_unextendable())
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

                let colors =
                    color_types::ColorsMergeManagerType::<MH, CX>::encode_part_unitigs_colors(
                        &mut self.unitigs_temp_colors,
                        &mut self.temp_color_buffer,
                    );

                let read_index = current_bucket.add_read(colors, out_seq, &self.temp_color_buffer);

                color_types::PartialUnitigsColorStructure::<MH, CX>::clear_temp_buffer(
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
            }
        }

        self.bucket_counter += 1;
        if self.bucket_counter >= self.bucket_change_threshold {
            self.bucket_counter = 0;
            let _ = global_data
                .output_results_buckets
                .push(self.current_bucket.take().unwrap());
        }
        // DEBUG_MAPS_HOLDER.lock().push(Box::new(map_struct_packet));
        map_struct_packet
    }

    fn finalize(
        self,
        _global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        self.hashes_tmp.finalize();
    }
}
