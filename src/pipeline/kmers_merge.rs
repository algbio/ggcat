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

use crate::binary_writer::{BinaryWriter, StorageMode};
use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::hash::HashFunction;
use crate::hash::{HashFunctionFactory, HashableSequence};
use crate::hash_entry::Direction;
use crate::hash_entry::HashEntry;
use crate::intermediate_storage::{IntermediateReadsReader, SequenceExtraData};
use crate::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use crate::sequences_reader::FastaSequence;
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::types::BucketIndexType;
use crate::utils::Utils;
use byteorder::{ReadBytesExt, WriteBytesExt};

pub const READ_FLAG_INCL_BEGIN: u8 = (1 << 0);
pub const READ_FLAG_INCL_END: u8 = (1 << 1);

#[derive(Copy, Clone)]
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
            &(out_directory.as_ref().join("hashes"), StorageMode::Plain),
        );

        let sequences = Mutex::new(Vec::new());

        file_inputs.par_iter().for_each(|input| {

            const MAX_HASHES_FOR_FLUSH: usize = 1024 * 64;
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

            let mut buckets: Vec<Vec<u8>> = vec![Vec::new(); 256];
            let mut cmp_reads: Vec<Vec<ReadRef<H>>> = vec![Vec::new(); 256];
            let mut buckets = &mut buckets[..];
            let mut cmp_reads = &mut cmp_reads[..];

            IntermediateReadsReader::<KmersFlags>::new(input.clone()).for_each(|flags, x| {

                let decr_val = ((x.bases_count() == k) && (flags.0 & READ_FLAG_INCL_END) == 0) as usize;

                let do_debug = false; //x.to_string().contains("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC");

                let hashes = H::new(x.sub_slice((1 - decr_val)..(k - decr_val)), m);

                let minimizer = hashes.iter().min_by_key(|k| H::get_minimizer(*k)).unwrap();

                if do_debug {
                    let hashes1 = H::new(x.sub_slice(1..x.bases_count() - 1), m);
                    let minimizer2 = hashes1.iter().min_by_key(|k| H::get_minimizer(*k)).unwrap();

                    let hashes2 = H::new(x.sub_slice(0..x.bases_count() - 1), m);
                    let minimizer3 = hashes2.iter().min_by_key(|k| H::get_minimizer(*k)).unwrap();


                    println!("ABCFlags: {} => {} M: {}/{}/{} // {}", flags.0, x.to_string(), minimizer, minimizer2, minimizer3, decr_val);

                    stdout().lock().flush().unwrap();
                    // assert!(minimizer == H::HashType::from(13777055726464398864) || minimizer == H::HashType::from(12838026436787689768));
                }

                let bucket = H::get_second_bucket(minimizer) % 256;

                let slen = buckets[bucket as usize].len();
                buckets[bucket as usize].extend_from_slice(x.get_compr_slice());

                cmp_reads[bucket as usize].push(ReadRef { read_start: slen, read_len: x.bases_count(), hash: minimizer, flags });
            });

            let mut m5 = 0;

            for b in 0..256 {
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

                let mut rcorrect_reads: Vec<(MH::HashType, usize)> = Vec::new();
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

                        // let tgtstr = read.to_string().contains("ATCTGTAGAAGGCATCTGATTAAACACCAGGT");;
                        //
                        // do_debug |= tgtstr;

                        // if tgtstr {
                        //     println!("Processing string {}", read.to_string());
                        // }

                        let hashes = MH::new(read, k);

                        struct MapEntry {
                            count: usize,
                            position: u32,
                            ignored: bool
                        }

                        let last_hash_pos = read_len - k;
                        let mut did_max = false;

                        for (idx, hash) in hashes.iter().enumerate() {
                            let position = (read_start * 4 + idx);
                            let ignored = (flags.0 & READ_FLAG_INCL_BEGIN == 0 && idx == 0) ||
                                (flags.0 & READ_FLAG_INCL_END == 0 && idx == last_hash_pos);
                            assert!(idx <= last_hash_pos);
                            did_max |= idx == last_hash_pos;

                            let entry = rhash_map.entry(hash).or_insert(MapEntry { position: position as u32, count: 0, ignored });
                            entry.count += 1;
                            if entry.count == min_multiplicity && !ignored {
                                rcorrect_reads.push((hash, position));
                            }
                        }
                        assert!(did_max);
                        tot_reads += 1;
                        tot_chars += read.bases_count();
                    }

                    if do_debug {
                        println!("ABC Processing new SEQUENCE!");
                    }
                    for (hash, read_start) in rcorrect_reads.iter() {

                        let mut read = CompressedRead::from_compressed_reads(
                            &buckets[b][..],
                            *read_start,
                            k,
                        );

                        if do_debug {
                            println!("ABC Processing new hash seq {}!", read.to_string());
                        }
                        // // TTTTCTTTTTTTTTTTTTTTAATTTTGAGACAGAGTCTCACTCTATCACCCAGGCTGGAGTGCG
                        // //   TTCTTTTTTTTTTTTTTTAATTTTGAGACAGAGTCTCACTCTATCACCCAGGCTGGAGTGCAG
                        // let mut debug = false;
                        // if read.to_string().as_str().contains("CATTGTCATGCTATTTTGCCTAGCCCTGTTTATCACATGGGACTCATACACATGTAATGAATC") {
                        //     println!("Bucketing works!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! {}", *hash);
                        //     debug = true;
                        // }

                        let rhentry = rhash_map.get_mut(hash).unwrap();
                        if rhentry.count == usize::MAX {
                            continue;
                        }
                        rhentry.count = usize::MAX;

                        if do_debug {
                            println!("ABCMerging: {} => {}", read.to_string(), rhentry.ignored);
                        }

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
                            'ext_loop: loop {
                                let mut count = 0;
                                current_hash = temp_data.0;

                                // if out_base_index_fw == 0 {
                                //     assert_eq!(MH::new(read, k).iter().next().unwrap(), current_hash);
                                // }
                                //
                                // if format!("{}", current_hash).as_str() == "6941243556408711692" {
                                //     println!("AAA")
                                // }
                                let mut ocount = 0;
                                let tmp_hash = compute_hash_fw(current_hash, k, unsafe { read.get_base_unchecked(out_base_index_fw) }, 0);
                                for idx in 0..4 {
                                    let bw_hash = compute_hash_bw(tmp_hash, k, unsafe { read.get_base_unchecked(out_base_index_bw) }, idx);
                                    if let Some(hash) = rhash_map.get(&bw_hash) {
                                        if hash.count >= min_multiplicity {
                                            if ocount > 0 {
                                                // println!("ABCSkip multiple found!");
                                                // Multiple backward letters, cannot extend forward
                                                break 'ext_loop;
                                            }
                                            ocount += 1;
                                        }
                                    }
                                }


                                let mut debug = true;
                                // if read.to_string().contains("ATTCTCTCAGGCCTTGTCTTTGATTCTAATGGCGGTGTCCCTTTCTTTTCTCATCTTGTGTT") {
                                //     println!("ERROR!!!!");
                                //     debug = false;
                                // }

                                for idx in 0..4 {
                                    let new_hash = compute_hash_fw(current_hash, k, unsafe { read.get_base_unchecked(out_base_index_fw) }, idx);
                                    // if out_base_index_fw == 0 {
                                    //     assert_eq!(MH::new(read, k).iter().next().unwrap(), current_hash);
                                    // }
                                    if let Some(hash) = rhash_map.get(&new_hash) {
                                        if hash.count >= min_multiplicity {
                                            // println!("ABCFound entry!!");

                                            count += 1;
                                            temp_data = (new_hash, idx, hash.position);
                                        }

                                        // if out_base_index_fw == 0 {
                                        //     let xnew_read = CompressedRead::from_compressed_reads(
                                        //         &buckets[b][..],
                                        //         hash.0,
                                        //         k,
                                        //     );
                                        //
                                        //     // let xnew_read1 = CompressedRead::from_compressed_reads(
                                        //     //     &buckets[b][..],
                                        //     //     hash.0+1,
                                        //     //     k-1,
                                        //     // );
                                        //     lastxread1 = xnew_read;
                                        //
                                        //     if debug {
                                        //         // println!("XXX {} === {} / {}", xnew_read.sub_slice(1..k).to_string(), &xnew_read1.sub_slice(0..k-1).to_string(), hash.0);
                                        //         let a = lastxread.sub_slice(1..k).to_string();
                                        //         let b = xnew_read.sub_slice(0..k - 1).to_string();
                                        //
                                        //         if a != b { //new_hash == 17583997638664642746 {
                                        //             let h0 = MH::new(read, k).iter().next().unwrap();
                                        //             let h1 = MH::new(lastxread, k).iter().next().unwrap();
                                        //             let h2 = MH::new(xnew_read, k).iter().next().unwrap();
                                        //             let w: u64 = unsafe { core::ptr::read(&h2 as *const _ as *const u64) };
                                        //             let bytes = w.to_le_bytes();
                                        //
                                        //             println!("H0: {} H1: {} H2: {} CH: {} NH: {}", h0, h1, h2, current_hash,
                                        //                      compute_hash_fw(current_hash, k, unsafe { read.get_base_unchecked(out_base_index_fw) }, idx));
                                        //
                                        //             for i in 0..10 {
                                        //                 println!("{} === {} / {} ==> {} // {} // {}", a, b, hash.0, output.len(), count, idx);
                                        //             }
                                        //             assert_eq!(xnew_read.to_string().as_bytes().iter().rev().map(|x| *x as char).collect::<String>(), CompressedRead::from_compressed_reads(&bytes[..], 0, k).to_string());
                                        //             // assert!(count > 2);
                                        //             // let x = H::new(a.as_bytes(), k-1).unwrap();
                                        //             // let y = H::new(b.as_bytes(), k-1).unwrap();
                                        //
                                        //             // println!("Added letter {} {} / {} {}  [{}/{}/{}]", H_INV_LETTERS[start_index.1], count, std::str::from_utf8(output).unwrap(), out_base_index, x.iter().next().unwrap(), y.iter().next().unwrap(), new_hash);
                                        //         }
                                        //     }
                                        //     // else {
                                        //     //     println!("AAA");
                                        //     // }
                                        // }
                                    }
                                }
                                // lastxread = lastxread1;
                                if count == 1 {

                                    let entryref = rhash_map.get_mut(&temp_data.0).unwrap();

                                    let already_used = entryref.count == usize::MAX;
                                    let contig_break = entryref.ignored;

                                    // Found a cycle unitig or a copy from another bucket
                                    if already_used || contig_break {
                                        if do_debug {
                                            println!("ABC Skipping!!");
                                        }
                                        break;
                                    }

                                    // Flag the entry as already used
                                    entryref.count = usize::MAX;

                                    // if (output.len() % (1024 * 1024) == 0) {
                                    //     println!("Adding: {} / {}", start_index.1, output.len());
                                    // }
                                    read = CompressedRead::from_compressed_reads(
                                        &buckets[b][..],
                                        temp_data.2 as usize,
                                        k,
                                    );
                                    if do_debug {
                                        println!("ABCExtending {} / {}!", read.to_string(), temp_data.1);
                                    }
                                    output.push(Utils::decompress_base(temp_data.1));

                                } else {
                                    break;
                                }
                            }
                            current_hash
                        };

                        let mut fw_hash = try_extend_function(&mut forward_seq, MH::manual_roll_forward, 0, MH::manual_roll_reverse, k - 1);
                        let mut bw_hash = try_extend_function(&mut backward_seq, MH::manual_roll_reverse, k - 1, MH::manual_roll_forward, 0);

                        let out_seq = if backward_seq.len() > 0 {
                            backward_seq.reverse();
                            backward_seq.extend_from_slice(&forward_seq[..]);
                            &backward_seq[..]
                        }
                        else {
                            &forward_seq[..]
                        };

                        fw_hash = MH::manual_remove_only_forward(fw_hash, k, Utils::compress_base(out_seq[out_seq.len() - k]));
                        bw_hash = MH::manual_remove_only_reverse(bw_hash, k, Utils::compress_base(out_seq[k - 1]));

                        // if fw_hash == 225543591449317574 {
                        //
                        // }

                        // dbg_seq.clear();
                        // dbg_seq.extend(out_seq.iter().map(|x| Utils::compress_base(*x)));
                        //
                        // assert_eq!(fw_hash, MH::new(&dbg_seq[dbg_seq.len() - k+1..dbg_seq.len()], k-1).iter().next().unwrap());
                        // assert_eq!(bw_hash, MH::new(&dbg_seq[0..k-1], k-1).iter().next().unwrap());

                        idx_str.clear();
                        idx_str.write_fmt(format_args!("{}", read_index));

                        writer.add_read(FastaSequence {
                            ident: &idx_str[..],
                            seq: out_seq,
                            qual: None
                        });

                        if do_debug {
                            println!("ABC Output sequence: {} // F{} B{} HASH: {}", std::str::from_utf8(out_seq).unwrap(), fw_hash, bw_hash, hash);
                        }

                        if std::str::from_utf8(out_seq).unwrap().contains("CATTGTCATGCTATTTTGCCTAGCCCTGTTTATCACATGGGACTCATACACATGTAATGAATC") {
                            println!("Reading works!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                        }


                        let fw_hash_sr = HashEntry {
                            hash: fw_hash,
                            bucket: bucket_index as u32,
                            entry: read_index,
                            direction: Direction::Forward
                        };
                        let fw_bucket_index = MH::get_bucket(fw_hash) % (buckets_count as BucketIndexType);
                        hashes_tmp.add_element(fw_bucket_index, &(), fw_hash_sr);
                        // fw_hash_sr.serialize_to_file(&mut hashes_buckets[]);

                        let bw_hash_sr = HashEntry {
                            hash: bw_hash,
                            bucket: bucket_index as u32,
                            entry: read_index,
                            direction: Direction::Backward
                        };
                        let bw_bucket_index = MH::get_bucket(bw_hash) % (buckets_count as BucketIndexType);
                        hashes_tmp.add_element(bw_bucket_index, &(), bw_hash_sr);

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
                buckets.iter().map(|x| x.len()).sum::<usize>() / 256, // set
                start_time.elapsed()
            );
            writer.finalize();
            hashes_tmp.finalize(&())
        });

        RetType {
            sequences: sequences.into_inner().unwrap(),
            hashes: hashes_buckets.finalize(),
        }
    }
}
