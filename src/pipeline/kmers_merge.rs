use std::cmp::min;
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufWriter, Write, stdout, Read};
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use std::ops::{Index, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use parallel_processor::binary_writer::{BinaryWriter, StorageMode};
use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::hash::HashFunction;
use crate::hash::{HashFunctionFactory, HashableSequence};
use crate::hash_entry::Direction;
use crate::hash_entry::HashEntry;
use crate::intermediate_storage::{IntermediateReadsReader, SequenceExtraData};
use parallel_processor::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use crate::sequences_reader::FastaSequence;
use parallel_processor::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::types::BucketIndexType;
use crate::utils::Utils;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crate::DEFAULT_BUFFER_SIZE;
use hashbrown::HashMap;
use std::process::exit;
use parallel_processor::memory_data_size::MemoryDataSize;


pub const READ_FLAG_INCL_BEGIN: u8 = (1 << 0);
pub const READ_FLAG_INCL_END: u8 = (1 << 1);

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone)]
struct ReadRef<H: HashFunctionFactory + Clone> {
    read_start: usize,
    read_len: usize,
    hash: H::HashType,
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
        let start_time = Instant::now();
        static CURRENT_BUCKETS_COUNT: AtomicU64 = AtomicU64::new(0);

        const NONE: Option<Mutex<BufWriter<File>>> = None;
        let mut hashes_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(out_directory.as_ref().join("hashes"), StorageMode::Plain {
                buffer_size: DEFAULT_BUFFER_SIZE
            }),
            None
        );

        let sequences = Mutex::new(Vec::new());

        file_inputs.par_iter().for_each(|input| {
            // let mut hashmap = HashMap::new();

            const MAX_HASHES_FOR_FLUSH: MemoryDataSize = MemoryDataSize::from_kibioctets(64.0);
            let mut hashes_tmp = BucketsThreadDispatcher::new(MAX_HASHES_FOR_FLUSH, &hashes_buckets);


            let bucket_index = Utils::get_bucket_index(&input);
            // if bucket_index != 448 {
            //     hashes_tmp.finalize(&());
            //     return;
            // }

            println!("Processing file {:?}", input.file_name().unwrap());
            println!("Processing bucket {}...", bucket_index);


            let mut kmers_cnt = 0;
            let mut kmers_unique = 0;

            let mut writer = ReadsFreezer::optfile_splitted_compressed_lz4(format!("{}/result.{}", out_directory.as_ref().display(), bucket_index));
            sequences.lock().unwrap().push(writer.get_path());

            let mut read_index = 0;

            let mut buckets: Vec<Vec<u8>> = vec![Vec::new(); MERGE_BUCKETS_COUNT];
            let mut cmp_reads: Vec<Vec<ReadRef<H>>> = vec![Vec::new(); MERGE_BUCKETS_COUNT];
            let mut buckets = &mut buckets[..];
            let mut cmp_reads = &mut cmp_reads[..];

            IntermediateReadsReader::<KmersFlags>::new(input.clone()).for_each(|flags, x| {

                let decr_val = ((x.bases_count() == k) && (flags.0 & READ_FLAG_INCL_END) == 0) as usize;

                // let do_debug = x.to_string().contains("TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

                let hashes = H::new(x.sub_slice((1 - decr_val)..(k - decr_val)), m);

                let minimizer = hashes.iter().min_by_key(|k| H::get_minimizer(*k)).unwrap();

                // if do_debug {
                //
                //     // if !hashmap.contains_key(&minimizer) {
                //         let hashes1 = H::new(x.sub_slice(1..x.bases_count() - 1), m);
                //         let minimizer2 = hashes1.iter().min_by_key(|k| H::get_minimizer(*k)).unwrap();
                //
                //         let hashes2 = H::new(x.sub_slice(0..x.bases_count() - 1), m);
                //         let minimizer3 = hashes2.iter().min_by_key(|k| H::get_minimizer(*k)).unwrap();
                //
                //
                //         println!("ABCFlags: {} => {} M: {}/{}/{} // {}", flags.0, x.to_string(), minimizer, minimizer2, minimizer3, decr_val);
                //
                //         stdout().lock().flush().unwrap();
                //     // }
                //     // hashmap.insert(minimizer, ());
                //     // assert!(minimizer == H::HashType::from(13777055726464398864) || minimizer == H::HashType::from(12838026436787689768));
                // }

                let bucket = H::get_second_bucket(minimizer) % (MERGE_BUCKETS_COUNT as BucketIndexType);

                let slen = buckets[bucket as usize].len();
                buckets[bucket as usize].extend_from_slice(x.get_compr_slice());

                cmp_reads[bucket as usize].push(ReadRef { read_start: slen, read_len: x.bases_count(), hash: minimizer, flags });
            });

            let mut m5 = 0;

            for b in 0..MERGE_BUCKETS_COUNT {
                #[derive(Copy, Clone)]
                struct SimpleHash<HF: HashFunctionFactory> {
                    value: u64,
                    _phantom: PhantomData<HF>
                }
                impl<HF: HashFunctionFactory> Hasher for SimpleHash<HF> {
                    fn finish(&self) -> u64 {
                        self.value
                    }

                    #[inline(always)]
                    fn write(&mut self, bytes: &[u8]) {
                        self.value = match size_of::<HF::HashType>() {
                            0 => 0,
                            1 => unsafe { *(bytes.as_ptr() as *const u8) as u64 }
                            2 => unsafe { *(bytes.as_ptr() as *const u16) as u64 }
                            4 => unsafe { *(bytes.as_ptr() as *const u32) as u64 }
                            _ => unsafe { *(bytes.as_ptr() as *const u64) }
                        }
                    }
                }
                struct SimpleHashBuilder<HF: HashFunctionFactory> { _phantom: PhantomData<HF> };
                impl<HF: HashFunctionFactory> BuildHasher for SimpleHashBuilder<HF> {
                    type Hasher = SimpleHash<HF>;
                    fn build_hasher(&self) -> Self::Hasher {
                        SimpleHash {
                            value: unsafe {MaybeUninit::uninit().assume_init()},
                            _phantom: PhantomData
                        }
                    }
                }

                let mut rcorrect_reads: Vec<(MH::HashType, usize, bool, bool)> = Vec::new();
                let mut rhash_map = hashbrown::HashMap::with_capacity(4096); // , SimpleHashBuilder::<MH> { _phantom: PhantomData });

                let mut backward_seq = Vec::new();
                let mut forward_seq = Vec::new();

                // let mut dbg_seq = Vec::new();

                forward_seq.reserve(k);

                let mut idx_str: Vec<u8> = Vec::new();

                struct Compare<H> { _phantom: PhantomData<H> };
                impl<H: HashFunctionFactory> SortKey<ReadRef<H>> for Compare<H> {
                    type KeyType = H::HashType;
                    const KEY_BITS: usize = size_of::<H::HashType>();

                    #[inline(always)]
                    fn get(value: &ReadRef<H>) -> H::HashType {
                        value.hash
                    }

                    #[inline(always)]
                    fn get_shifted(value: &ReadRef<H>, rhs: u8) -> u8 {
                        H::get_shifted(value.hash, rhs)
                    }
                }

                smart_radix_sort::<_, Compare<H>, false>(&mut cmp_reads[b]);

                for slice in cmp_reads[b].group_by(|a, b| a.hash == b.hash) {

                    rhash_map.clear();
                    rcorrect_reads.clear();

                    let mut tot_reads = 0;
                    let mut tot_chars = 0;

                    let mut do_debug = false; //false;

                    for &ReadRef { read_start, read_len, flags, .. } in slice {

                        kmers_cnt += read_len - k + 1;

                        let read = CompressedRead::new_from_compressed(
                            &buckets[b][read_start..read_start + ((read_len + 3) / 4)],
                            read_len,
                        );

                        // let tgtstr = read.to_string().contains("TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");;
                        //
                        // do_debug |= tgtstr;

                        // if tgtstr {
                        //     println!("Processing string {}", read.to_string());
                        // }

                        let hashes = MH::new(read, k);

                        struct MapEntry {
                            count: usize,
                            position: u32,
                            begin_ignored: bool,
                            end_ignored: bool,
                        }

                        let last_hash_pos = read_len - k;
                        let mut did_max = false;

                        for (idx, hash) in hashes.iter().enumerate() {
                            let position = (read_start * 4 + idx);
                            let begin_ignored = flags.0 & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                            let end_ignored = flags.0 & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;
                            assert!(idx <= last_hash_pos);
                            did_max |= idx == last_hash_pos;

                            let entry = rhash_map.entry(hash).or_insert(MapEntry { position: position as u32, begin_ignored, count: 0, end_ignored });

                            // if (entry.begin_ignored !=  begin_ignored) ||
                            //     (entry.end_ignored != end_ignored) {
                            //     // println!("Bug found on hash {} in bucket {}", hash, input.clone().display());
                            //     // exit(0);
                            // }

                            entry.count += 1;

                            if entry.count == min_multiplicity {
                                rcorrect_reads.push((hash, position, begin_ignored, end_ignored));
                            }
                        }
                        assert!(did_max);
                        tot_reads += 1;
                        tot_chars += read.bases_count();
                    }

                    // if do_debug {
                    //     println!("ABC Processing new SEQUENCE!");
                    // }
                    for (hash, read_start, begin_ignored, end_ignored) in rcorrect_reads.iter() {

                        let mut read = CompressedRead::from_compressed_reads(
                            &buckets[b][..],
                            *read_start,
                            k,
                        );

                        // if do_debug {
                        //     println!("ABC Processing new hash seq {}!", read.to_string());
                        // }
                        // // // TTTTCTTTTTTTTTTTTTTTAATTTTGAGACAGAGTCTCACTCTATCACCCAGGCTGGAGTGCG
                        // //   TTCTTTTTTTTTTTTTTTAATTTTGAGACAGAGTCTCACTCTATCACCCAGGCTGGAGTGCAG
                        // let mut debug = false;
                        // if read.to_string().as_str().contains("TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
                        //     println!("Bucketing works!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! {} {} {}", read.to_string(), begin_ignored, end_ignored);
                        //     // do_debug = true;
                        // }

                        let rhentry = rhash_map.get_mut(hash).unwrap();
                        if rhentry.count == usize::MAX {
                            continue;
                        }
                        rhentry.count = usize::MAX;

                        // if do_debug {
                        //     println!("ABCMerging: {} => {}", read.to_string(), rhentry.ignored);
                        // }

                        backward_seq.clear();
                        unsafe {
                            forward_seq.set_len(k);
                        }

                        read.write_to_slice(&mut forward_seq[..]);

                        let mut try_extend_function = |
                            output: &mut Vec<u8>,
                            compute_hash_fw: fn(hash: MH::HashType, klen: usize, out_b: u8, in_b: u8) -> MH::HashType,
                            out_base_index_fw: usize,
                            compute_hash_bw: fn(hash: MH::HashType, klen: usize, out_b: u8, in_b: u8) -> MH::HashType,
                            out_base_index_bw: usize
                        | {
                            let mut temp_data = (*hash, 0, 0);
                            let mut current_hash;
                            
                            // let mut lastxread = read;
                            // let mut lastxread1 = read;

                            // if out_base_index_fw == 0 {
                            //     assert_eq!(MH::new(read, k).iter().next().unwrap(), *hash);
                            // }
                            return 'ext_loop: loop {
                                let mut count = 0;
                                current_hash = temp_data.0;
                                let mut ocount = 0;
                                let tmp_hash = compute_hash_fw(current_hash, k, unsafe { read.get_base_unchecked(out_base_index_fw) }, 0);
                                for idx in 0..4 {
                                    let bw_hash = compute_hash_bw(tmp_hash, k, unsafe { read.get_base_unchecked(out_base_index_bw) }, idx);
                                    if let Some(hash) = rhash_map.get(&bw_hash) {
                                        if hash.count >= min_multiplicity {
                                            if ocount > 0 {
                                                break 'ext_loop (temp_data.0, false);
                                            }
                                            ocount += 1;
                                        }
                                    }
                                }

                                for idx in 0..4 {
                                    let new_hash = compute_hash_fw(current_hash, k, unsafe { read.get_base_unchecked(out_base_index_fw) }, idx);
                                    if let Some(hash) = rhash_map.get(&new_hash) {
                                        if hash.count >= min_multiplicity {
                                            count += 1;
                                            temp_data = (new_hash, idx, hash.position);
                                        }
                                    }
                                }

                                if count == 1 {

                                    let entryref = rhash_map.get_mut(&temp_data.0).unwrap();

                                    let already_used = entryref.count == usize::MAX;

                                    // Found a cycle unitig
                                    if already_used {
                                        break (temp_data.0, false);
                                    }

                                    // Flag the entry as already used
                                    entryref.count = usize::MAX;

                                    output.push(Utils::decompress_base(temp_data.1));

                                    // Found a continuation into another bucket
                                    let contig_break = entryref.begin_ignored || entryref.end_ignored;
                                    if contig_break {
                                        break (temp_data.0, contig_break);
                                    }

                                    read = CompressedRead::from_compressed_reads(
                                        &buckets[b][..],
                                        temp_data.2 as usize,
                                        k,
                                    );

                                } else {
                                    break (temp_data.0, false);
                                }
                            };
                        };

                        let (mut fw_hash, fwd_ignored) = try_extend_function(&mut forward_seq, MH::manual_roll_forward, 0, MH::manual_roll_reverse, k - 1);
                        let (mut bw_hash, bwd_ignored) = try_extend_function(&mut backward_seq, MH::manual_roll_reverse, k - 1, MH::manual_roll_forward, 0);

                        let fwd_ignored = if forward_seq.len() == k { *end_ignored } else { fwd_ignored };
                        let bwd_ignored = if backward_seq.len() == 0 { *begin_ignored } else { bwd_ignored };

                        let out_seq = if backward_seq.len() > 0 {
                            backward_seq.reverse();
                            backward_seq.extend_from_slice(&forward_seq[..]);
                            &backward_seq[..]
                        }
                        else {
                            &forward_seq[..]
                        };

                        // if std::str::from_utf8(out_seq).unwrap().contains("TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
                        //     println!("Bucketing after hashing works!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! {}", read.to_string());
                        //     // do_debug = true;
                        // }

                        // fw_hash = MH::manual_remove_only_forward(fw_hash, k, Utils::compress_base(out_seq[out_seq.len() - k]));
                        // bw_hash = MH::manual_remove_only_reverse(bw_hash, k, Utils::compress_base(out_seq[k - 1]));

                        idx_str.clear();
                        idx_str.write_fmt(format_args!("{}", read_index));

                        writer.add_read(FastaSequence {
                            ident: &idx_str[..],
                            seq: out_seq,
                            qual: None
                        });

                        if fwd_ignored {
                            let fw_hash_sr = HashEntry {
                                hash: fw_hash,
                                bucket: bucket_index as u32,
                                entry: read_index,
                                direction: Direction::Forward
                            };
                            let fw_bucket_index = MH::get_bucket(fw_hash) % (buckets_count as BucketIndexType);
                            hashes_tmp.add_element(fw_bucket_index, &(), fw_hash_sr);

                        }

                        if bwd_ignored {
                            let bw_hash_sr = HashEntry {
                                hash: bw_hash,
                                bucket: bucket_index as u32,
                                entry: read_index,
                                direction: Direction::Backward
                            };
                            let bw_bucket_index = MH::get_bucket(bw_hash) % (buckets_count as BucketIndexType);
                            hashes_tmp.add_element(bw_bucket_index, &(), bw_hash_sr);
                        }

                        read_index += 1;
                    }
                }
            }
            println!(
                "[{}/{}]Kmers {}, unique: {}, ratio: {:.2}% ~~ m5: {} ratio: {:.2}% [{:?}] Time: {:?}",
                CURRENT_BUCKETS_COUNT.fetch_add(1, Ordering::Relaxed) + 1,
                buckets_count,
                kmers_cnt,
                kmers_unique,
                (kmers_unique as f32) / (kmers_cnt as f32) * 100.0,
                m5,
                (m5 as f32) / (kmers_unique as f32) * 100.0,
                buckets.iter().map(|x| x.len()).sum::<usize>() / MERGE_BUCKETS_COUNT, // set
                start_time.elapsed()
            );
            writer.finalize();
            hashes_tmp.finalize()
        });

        RetType {
            sequences: sequences.into_inner().unwrap(),
            hashes: hashes_buckets.finalize(),
        }
    }
}
