#![feature(new_uninit, core_intrinsics)]
#![feature(is_sorted)]
#![feature(slice_group_by)]
#![feature(llvm_asm)]
#![feature(option_result_unwrap_unchecked)]
#![allow(warnings)]

use crate::bloom_filter::SET_BIT;
use crate::bloom_processing::{HierarchicalBloomFilter, HierarcicalBloomFilterFactory};
use crate::cache_bucket::CacheBucket;
use crate::compressed_read::{CompressedRead, H_INV_LETTERS, H_LOOKUP};
use crate::gzip_fasta_reader::{FastaSequence, GzipFastaReader};
use crate::intermediate_storage::{
    IntermediateReadsReader, IntermediateReadsWriter, IntermediateStorage,
};
use crate::kmer_paths::KmerPaths;
use crate::nthash::RollingNtHashIterator;
use crate::pipeline::Pipeline;
use crate::progress::Progress;
use crate::reads_freezer::{ReadsFreezer, ReadsWriter};
use crate::rolling_kseq_iterator::{RollingKseqImpl, RollingKseqIterator};
use crate::rolling_minqueue::{GenericsFunctional, RollingMinQueue};
use crate::rolling_quality_check::RollingQualityCheck;
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::utils::{cast_static, Utils};
use ::nthash::nt_manual_roll;
use ::nthash::nt_manual_roll_rev;
use ::nthash::NtHashIterator;
use ::nthash::NtSequence;
use bio::alphabets::SymbolRanks;
use bitvec::vec::BitVec;
use bstr::ByteSlice;
use itertools::Itertools;
use nix::unistd::PathconfVar::PIPE_BUF;
use object_pool::Pool;
use pad::{Alignment, PadStr};
use rayon::iter::*;
use rayon::ThreadPoolBuilder;
use std::cell::UnsafeCell;
use std::cmp::{max, min, min_by_key};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::intrinsics::unlikely;
use std::io::{stdin, BufRead, Write};
use std::mem::MaybeUninit;
use std::net::Shutdown::Read;
use std::ops::{Index, Range};
use std::path::Path;
use std::ptr;
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering;
use std::sync::atomic::*;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use structopt::clap::ArgGroup;
use structopt::StructOpt;

mod bloom_filter;
mod bloom_processing;
mod cache_bucket;
mod compressed_read;
mod gzip_fasta_reader;
mod intermediate_storage;
mod kmer_paths;
pub mod libdeflate;
mod nthash;
mod pipeline;
mod progress;
mod reads_freezer;
mod rolling_kseq_iterator;
mod rolling_minqueue;
mod rolling_quality_check;
mod seq_aligner;
mod smart_bucket_sort;
mod utils;

#[derive(StructOpt)]
enum Mode {
    Flat,
    Preprocess,
}

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

#[derive(StructOpt, Debug)]
#[structopt(group = outputs_arg_group())]
struct Cli {
    /// The input files
    inputs: Vec<String>,

    /// The output file
    #[structopt(short, long, group = "outputs")]
    output: Option<String>,

    /// Enables processing from gzipped fasta
    #[structopt(short)]
    gzip_fasta: bool,

    /// Removes N and splits reads accordingly
    #[structopt(short, requires = "klen")]
    nsplit: bool,

    /// Specifies the k-mers length, mandatory with -nbl
    #[structopt(short)]
    klen: Option<usize>,

    /// Bloom filter elaboration
    #[structopt(short, group = "outputs", requires = "klen")]
    elabbloom: bool,

    /// Enables buckets processing with the specified number of buckets
    #[structopt(short, group = "outputs", requires = "klen")]
    bucketing: Option<usize>,

    /// Tests built bloom filter against this file for coverage tests
    #[structopt(short = "f", requires = "elabbloom")]
    coverage: Option<String>,

    /// Decimate bloom filter
    #[structopt(short, requires = "elabbloom")]
    decimated: bool,

    /// Writes out all minimizer k-mers
    #[structopt(short, requires = "klen")]
    minimizers: bool,

    /// Enables output compression
    #[structopt(short, requires = "output")]
    compress: bool,

    /// Processes a bucket
    #[structopt(short, requires = "output", requires = "klen")]
    process_bucket: bool,
}

#[derive(StructOpt, Debug)]
struct Cli2 {
    /// The input files
    input: Vec<String>,

    /// Specifies the k-mers length
    #[structopt(short)]
    klen: usize,

    /// Specifies the m-mers (minimizers) length, defaults to min(12, ceil(K / 2))
    #[structopt(short)]
    mlen: Option<usize>,

    /// Process the sequence to find bucket alignments
    #[structopt(short)]
    alignment: bool,

    /// Minimum molteplicity required to keep a kmer
    #[structopt(short = "s", long = "min-molteplicity")]
    min_molteplicity: Option<usize>,
}

struct NtHashMinTransform;
impl GenericsFunctional<u64, u32> for NtHashMinTransform {
    fn func(value: u64) -> u32 {
        (value >> 32) as u32
    }
}

fn assert_reads(read: &[u8], bucket: u64) {
    // Test ***************************
    let K: usize = 32;

    if read.len() == 33 {
        let hashes = NtHashIterator::new(&read[0..K], M).unwrap();
        let minimizer = hashes.iter().min_by_key(|read| *read >> 32).unwrap();

        let hashes1 = NtHashIterator::new(&read[1..K + 1], M).unwrap();
        let minimizer1 = hashes1.iter().min_by_key(|read| *read >> 32).unwrap();

        assert!(minimizer % 512 == bucket || minimizer1 % 512 == bucket);
        println!("{} / {}", minimizer, minimizer1);
    }

    if read.len() < 34 {
        return;
    }

    let x = &read[1..read.len() - 1];

    const M: usize = 12;

    let hashes = NtHashIterator::new(&x[0..K], M).unwrap();
    let minimizer = hashes.iter().min_by_key(|x| *x >> 32).unwrap();

    assert!(minimizer % 512 == bucket);

    if x.len() > K {
        let hashes2 = NtHashIterator::new(&x[..], M).unwrap();
        let minimizer2 = hashes2.iter().min_by_key(|x| *x >> 32).unwrap();

        if minimizer != minimizer2 {
            let vec: Vec<_> = NtHashIterator::new(&x[..], M)
                .unwrap()
                .iter()
                .map(|x| x >> 32)
                .collect();

            println!("Kmers {}", std::str::from_utf8(x).unwrap());
            println!("Hashes {:?}", vec);
            panic!("AA {} {}", minimizer, minimizer2);
        }
    }
    // Test ***************************
}

fn main() {
    let args = Cli2::from_args();

    const BUCKETS_COUNT: usize = 512;
    static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);

    ThreadPoolBuilder::new().num_threads(16).build_global();

    let k: usize = args.klen;
    let m: usize = args.mlen.unwrap_or(min(12, (k + 2) / 3));

    let start_time = Instant::now();

    if !args.alignment {
        const NONE: Option<Mutex<IntermediateReadsWriter>> = None;
        let mut buckets = [NONE; BUCKETS_COUNT];

        for i in 0..BUCKETS_COUNT {
            buckets[i] = Some(Mutex::new(IntermediateStorage::new_writer(format!(
                "buckets/bucket{}",
                i
            ))));
        }

        let mut input: Vec<String> = args.input.into();

        input.sort_by_cached_key(|file| std::fs::metadata(file).unwrap().len());
        input.reverse();

        input.par_iter().for_each(|input| {
            // let mut vec: [u64; BUCKETS_COUNT + 1] = [0; BUCKETS_COUNT + 1];

            // let mut bfilter = pool.try_pull().unwrap();

            // let mut total = 0;
            // let mut correct = 0;
            //
            // let mut nthash = RollingNtHashIterator::new();
            // let mut qcheck = RollingQualityCheck::new();
            //
            // let mut hashvec: Vec<u64> = Vec::new();
            //
            // const BFILTER_SIZE: usize = 1024 * 1024 * 1024;

            // let mut bfilter = ;
            //
            // let mut address = 0;

            const VEC: Vec<u8> = Vec::new();
            const VEC_USZ: Vec<usize> = Vec::new();
            const ALLOWED_LEN: usize = 1024 * 1024;
            let mut temp_buffers = [VEC; BUCKETS_COUNT];
            let mut temp_indexes = [VEC_USZ; BUCKETS_COUNT];

            let mut flush_buffer = |temp_indexes: &mut [Vec<usize>; BUCKETS_COUNT],
                                    temp_buffers: &mut [Vec<u8>; BUCKETS_COUNT],
                                    bucket: usize| {
                // println!(
                //     "Flush buffer {} {:?}",
                //     temp_buffers[bucket].len(),
                //     temp_indexes[bucket][0]
                // );

                let mut reader = buckets[bucket].as_ref().unwrap().lock().unwrap();

                if temp_indexes.len() == 0 {
                    return;
                }

                temp_indexes[bucket].push(temp_buffers[bucket].len());
                let mut last_index = 0;
                for index in temp_indexes[bucket].iter().skip(1) {
                    reader.add_acgt_read(&temp_buffers[bucket][last_index..*index]);
                    last_index = *index;
                }
                temp_buffers[bucket].clear();
                temp_indexes[bucket].clear();
            };
            let mut minimizer_queue =
                RollingMinQueue::<u64, u32, NtHashMinTransform>::new(k - m + 1);

            GzipFastaReader::process_file_extended(input.to_string(), |x| {
                if x.seq.len() < k {
                    return;
                }

                let mut start = 0;
                let mut end = 0;

                let mut get_sequence = || {
                    while end < x.seq.len() {
                        start = end;
                        // Skip all not recognized characters
                        while start < x.seq.len() && x.seq[start] == b'N' {
                            start += 1;
                        }
                        end = start;
                        // Find the last valid character in this sequence
                        while end < x.seq.len() && x.seq[end] != b'N' {
                            end += 1;
                        }
                        // If the length of the read is long enough, return it
                        if end - start >= k {
                            return Some(&x.seq[start..end]);
                        }
                    }
                    None
                };

                while let Some(seq) = get_sequence() {
                    let hashes = NtHashIterator::new(seq, m).unwrap();

                    let mut rolling_iter = minimizer_queue.make_iter(hashes.iter());

                    let mut last_index = 0;

                    let mut add_buffer = |index: usize, bucket: usize| {
                        temp_indexes[bucket].push(temp_buffers[bucket].len());
                        temp_buffers[bucket].extend_from_slice(&seq[last_index..(index + k)]);
                        // assert_reads(
                        //     &seq[(max(1, last_index) - 1)..min(seq.len(), index + k + 1)],
                        //     bucket as u64,
                        // );

                        if temp_buffers[bucket].len() > ALLOWED_LEN {
                            flush_buffer(&mut temp_indexes, &mut temp_buffers, bucket);
                        }

                        last_index = index + 1;
                    };

                    let mut last_hash = rolling_iter.next().unwrap();
                    for (index, min_hash) in rolling_iter.enumerate() {
                        if min_hash != last_hash {
                            let bucket = (last_hash as usize) % BUCKETS_COUNT;
                            add_buffer(index, bucket);
                            last_hash = min_hash;
                        }
                    }
                    add_buffer(seq.len() - k, (last_hash as usize) % BUCKETS_COUNT);
                }
                //                 let mut rolling_iter = RollingKseqIterator::new(seq, 32);
                //                 let mut rolling_qiter = RollingKseqIterator::new(x.qual, 32);
                // //        let mut ntiter = NtHashIterator::new(seq, 32);
                //
                //
                //                 let threshold: f64 = 0.995;
                //                 let threshold_log = (-threshold.log10() * 1048576.0) as u32;
                //
                //                 // hashvec.clear();
                //
                //                 for (hash, quality_log) in rolling_iter.iter(&mut nthash).zip(rolling_qiter.iter(&mut qcheck)) {
                //                     let filtered = if quality_log > threshold_log { 0 } else { hash };
                //                     // hashvec.push(filtered);
                //
                //                     const ACCESSED_BLOCK_SIZE: usize = BFILTER_SIZE; //1024 * 8;
                //                     // const DIVIDER: usize = 16;
                //
                //                     // if hash as usize % (ACCESSED_BLOCK_SIZE / DIVIDER) == 0 {
                //                     //     address = (((hash >> 32) as usize % BFILTER_SIZE) as usize & !(ACCESSED_BLOCK_SIZE - 1)) as usize;
                //                     // }
                //
                //                     bfilter[address + (hash % (ACCESSED_BLOCK_SIZE as u64)) as usize] += 1;
                //                     // bfilter[address + ((hash.rotate_left(32)) % (ACCESSED_BLOCK_SIZE as u64)) as usize] += 1;
                //                 }

                if SEQ_COUNT.fetch_add(1, Ordering::Relaxed) % 10000000 == 0 {
                    println!(
                        "Elaborated {} sequences! Set bits: {} [{:?}]",
                        SEQ_COUNT.load(Ordering::Relaxed),
                        SET_BIT.load(Ordering::Relaxed),
                        start_time.elapsed()
                    );
                }

                //            println!("{}", String::from_utf8_lossy(seq));
                //            println!("{}", String::from_utf8_lossy(x.qual));
                //            println!("{:?}", hashvec);
                //
                //                 let mut rolling_minqiter = RollingKseqIterator::new(hashvec.as_slice(), 8);
                //
                //                 let mut minmax_value = rolling_minqiter.iter(&mut minqueue).max().unwrap_or(0);
                //
                //                 let bucket = if minmax_value == 0 { 0 } else { (minmax_value >> 1) % (BUCKETS_COUNT as u64) + 1 };
                //                 vec[bucket as usize] += 1;
                //
                //                 // let ident = format!("@SeqId {}", minmax_value);
                //
                //                 // buckets[bucket as usize].lock().unwrap().add_read(FastaSequence {
                //                 //     ident: ident.as_bytes(),
                //                 //     seq: seq,
                //                 //     qual: x.qual
                //                 // });
                //
                //                 let mut rolling_qiter1 = RollingKseqIterator::new(x.qual, x.qual.len());
                //                 let mut prob_log = rolling_qiter1.iter(&mut qcheck).min().unwrap_or(std::u32::MAX);
                //
                //                 total += 1;
                //
                //                 let threshold1: f64 = 0.9;
                //                 let threshold1_log = (-threshold1.log10() * 1048576.0) as u32;
                //
                //
                // //        println!("{} < {}", prob_log, threshold_log);
                //
                //                 if prob_log < threshold1_log {
                //                     correct += 1;
                //                     if correct % 100000 == 0 {
                //                         println!("Prob: {:.2}% correct => LEN: {} {}", (10.0 as f64).powf(-(prob_log as f64) / 1048576.0) * 100.0, seq.len(), bucket);
                //                         println!("{}", String::from_utf8_lossy(seq));
                //                         println!("{}", String::from_utf8_lossy(x.qual));
                //                         println!("Correct/Total = {}/{} ===> {:.2}%", correct, total, (correct as f64) / (total as f64) * 100.0);
                //                     }
                //                 }
            });
            //             total_at.fetch_add(total as u64, Ordering::Relaxed);
            //             correct_at.fetch_add(correct as u64, Ordering::Relaxed);
            // println!("Frequencies: {:?}", &vec[..]);
            for i in 0..BUCKETS_COUNT {
                flush_buffer(&mut temp_indexes, &mut temp_buffers, i);
            }
        });

        for bucket in buckets.iter_mut() {
            let bucket = bucket.take().unwrap().into_inner().unwrap();
            bucket.finalize()
        }
    } else {
        static CURRENT_BUCKETS_COUNT: AtomicU64 = AtomicU64::new(0);

        let min_molteplicity = args.min_molteplicity.unwrap_or(4);

        args.input.par_iter().enumerate().for_each(|(index, input)| {
            let mut kmers_cnt = 0;
            let mut kmers_unique = 0;

            let mut writer = ReadsFreezer::optfile_splitted_compressed_lz4(format!("output/result{}", index));

            struct X {}

            impl Index<usize> for X {
                type Output = usize;

                fn index(&self, index: usize) -> &Self::Output {
                    &0
                }
            }

            impl Index<Range<usize>> for X {
                type Output = usize;

                fn index(&self, index: Range<usize>) -> &Self::Output {
                    &0
                }
            }

            const CVEC: Vec<u8> = Vec::new();
            const CREAD: Vec<(usize, usize, usize, u64)> = Vec::new();
            let mut buckets = [CVEC; 256];
            let mut cmp_reads = [CREAD; 256];

            // FIXME: Embed in file!
            let file_name = Path::new(&input).file_name().unwrap().to_string_lossy().to_string();
            let bucket_string: Vec<u8> = (&file_name[0..file_name.rfind(".").unwrap()]).as_bytes().iter()
                .map(|x| *x)
                .filter(|x| (*x as char).is_digit(10)).collect();

            let bucket_index: u64 = String::from_utf8(bucket_string).unwrap().parse().unwrap();
            println!("Processing bucket {}...", bucket_index);

            IntermediateStorage::new_reader(input.clone()).for_each(|x| {

                let hashes = NtHashIterator::new(x.sub_slice(0..min(x.bases_count(), k)), m).unwrap();

                let (minpos, minimizer) = hashes.iter_enumerate().min_by_key(|(i, k)| *k).unwrap();

                let bucket = (minimizer >> 12) % 256;

                let slen = buckets[bucket as usize].len();
                buckets[bucket as usize].extend_from_slice(x.cmp_slice());

                let mut sort_key: u64 = minimizer;
                cmp_reads[bucket as usize].push((slen, x.bases_count(), minpos, sort_key))
            });

                let mut m5 = 0;

                for b in 0..256 {

                    // let mut correct_reads: Vec<(usize, usize)> = Vec::new();
                    #[derive(Copy, Clone)]
                    struct TestHash {
                        value: u64
                    }
                    impl Hasher for TestHash {
                        fn finish(&self) -> u64 {
                            self.value
                        }

                        fn write(&mut self, bytes: &[u8]) {
                            self.value = unsafe { *(bytes.as_ptr() as *const u64) };
                        }
                    }
                    impl BuildHasher for TestHash {
                        type Hasher = Self;

                        fn build_hasher(&self) -> Self::Hasher {
                            *self
                        }
                    }

                    let mut rcorrect_reads: Vec<(u64, usize)> = Vec::new();
                    let mut rhash_map = hashbrown::HashMap::with_capacity_and_hasher(4096, TestHash { value: 0 });

                    let mut backward_seq = Vec::new();
                    let mut forward_seq = Vec::new();
                    forward_seq.reserve(k);

                    struct Compare {}
                    impl SortKey<(usize, usize, usize, u64)> for Compare {
                        fn get(value: &(usize, usize, usize, u64)) -> u64 {
                            value.3
                        }
                    }

                    smart_radix_sort::<_, Compare, false>(&mut cmp_reads[b], 64 - 8);

                    for slice in cmp_reads[b].group_by(|a, b| a.3 == b.3) {

                        rhash_map.clear();
                        // rhash_map.shrink_to(slice.len() * k);
                        rcorrect_reads.clear();

                        // let mut last_read: Option<(CompressedRead, usize)> = None;
                        let mut tot_reads = 0;
                        let mut tot_chars = 0;

                        // println!("New minimizer!");
                        for (read_idx, count, pos, minimizer) in slice {
                            kmers_cnt += count - k + 1;

                            let read = CompressedRead::new(
                                &buckets[b][*read_idx..*read_idx + ((*count + 3) / 4)],
                                *count,
                            );

                            let hashes = NtHashIterator::new(read, k).unwrap();

                            for (idx, hash) in hashes.iter().enumerate() {
                                // kmers_cnt += hash as usize;
                                // counters[hash as usize % hlen] = min(254, counters[hash as usize % hlen] + 1);
                                let entry = rhash_map.entry(hash).or_insert((read_idx * 4 + idx, 0));
                                entry.1 += 1;
                                if entry.1 == min_molteplicity {
                                    rcorrect_reads.push((hash, entry.0));
                                }
                                // hmap[hash as usize % hlen] += 1;
                                // *hmap.entry(hash).or_insert(0) += 1;
                            }

                            // println!("Read {}", read.to_string().pad_to_width_with_alignment(read.bases_count() + 32 - *pos, Alignment::Right));
                            // stdin().read_line(&mut String::new());

                            tot_reads += 1;
                            tot_chars += read.bases_count();

                            // path.add_kmer(read, *pos, *pos + m);
                            // if let Some((last_read, last_pos)) = last_read {
                            //     let mut correct = true;
                            //     for i in 0..m {
                            //         unsafe {
                            //             correct &=
                            //                 (read.get_base_unchecked(pos + i) ==
                            //                 last_read.get_base_unchecked(last_pos + i));
                            //         }
                            //     }
                            //     if !correct {
                            //         println!("{} vs\n{}\n\n\n\n => {} {}", read.to_string(), last_read.to_string(), pos, last_pos);
                            //     }
                            // }

                            // last_read = Some((read, *pos));

                            // let hashes = NtHashIterator::new(
                            //     CompressedRead::new(
                            //         &buckets[b][*read_idx..*read_idx + ((*count + 3) / 4)],
                            //         *count,
                            //     ),
                            //     K,
                            // )
                            //     .unwrap();

                            // reads.extend(hashes.iter());
                        }

                        for (hash, read_start) in rcorrect_reads.iter() {

                            let mut read = CompressedRead::from_compressed_reads(
                                &buckets[b][..],
                                *read_start,
                                k,
                            );

                            if rhash_map.remove_entry(hash).is_none() {
                                continue;
                            }

                            backward_seq.clear();
                            unsafe {
                                forward_seq.set_len(k);
                            }

                            read.write_to_slice(&mut forward_seq[..]);

                            let mut try_extend_function = |
                                output: &mut Vec<u8>,
                                compute_hash: fn(hash: u64, klen: usize, out_h: u64, in_h: u64) -> u64,
                                out_base_index: usize
                            | {
                                let mut start_index = (*hash, 0, 0);
                                loop {
                                    let mut count = 0;
                                    for idx in 0..4 {
                                        let new_hash = compute_hash(start_index.0, k, unsafe { read.get_h_unchecked(0) }, H_LOOKUP[idx]);
                                        if let Some(hash) = rhash_map.remove(&new_hash) {
                                            if hash.1 >= min_molteplicity {
                                                count += 1;
                                                start_index = (new_hash, idx, hash.0);
                                            }
                                        }
                                    }
                                    if count == 1 {
                                        read = CompressedRead::from_compressed_reads(
                                            &buckets[b][..],
                                            start_index.2,
                                            k,
                                        );
                                        output.push(H_INV_LETTERS[start_index.1]);
                                    } else {
                                        break;
                                    }
                                }
                            };

                            try_extend_function(&mut forward_seq, nt_manual_roll, 0);
                            try_extend_function(&mut backward_seq, nt_manual_roll_rev, k - 1);

                            backward_seq.reverse();
                            backward_seq.extend_from_slice(&forward_seq[..]);

                            writer.add_read(FastaSequence {
                                ident: &[],
                                seq: &backward_seq[..],
                                qual: None
                            });
                        }

                        // for (read_idx, count, pos, minimizer) in slice {
                        // 
                        //     let read = CompressedRead::new(
                        //         &buckets[b][*read_idx..*read_idx + ((*count + 3) / 4)],
                        //         *count,
                        //     );
                        // 
                        //     let hashes = NtHashIterator::new(read, k).unwrap();
                        // 
                        //     let hlen = counters.len();
                        // 
                        //     for (index, hash) in hashes.iter().enumerate() {
                        //         // if counters[hash as usize % hlen] >= 20 {
                        //         //     correct_reads.push((index, 32));
                        //         // }
                        //         if *rhash_map.get(&hash).unwrap_or(&0) >= 20 {
                        //             rcorrect_reads.push((index, 32));
                        //         }
                        //     }
                        // }

                        // if tot_reads > 50 {
                        //     // println!("Correct reads: {} / {}, [{}] AAAA///{}", correct_reads.len(), (kmers_cnt - skmer_cnt), counters.len(), debug_AAA);
                        //     println!("RCorrect reads: {} / {} [{}/{}]", rcorrect_reads.len(), (kmers_cnt - skmer_cnt), 
                        //              rhash_map.iter().filter(|x| *x.1 >= 20).count(),
                        //     rhash_map.capacity());
                        // }
                        // let fwd = path.iterate(&mut writer, true);
                        // let bkw = path.iterate(&mut writer, false);
                        // if tot_reads > 50 {
                        //     println!(
                        //         "B[{:.2}%] Size: {} / {} || R: {} B: {}  R[{:.2}%] | FWD: {} / BKW: {} ",
                        //         (path.nodes_forward.len() + path.nodes_backward.len()) as f32
                        //             / tot_chars as f32
                        //             * 100.0,
                        //         path.nodes_forward.len(),
                        //         path.nodes_backward.len(),
                        //         tot_reads,
                        //         tot_chars,
                        //         (path.nodes_forward.len() + path.nodes_backward.len()) as f32
                        //             / tot_reads as f32
                        //             * 100.0,
                        //         fwd,
                        //         bkw
                        //     );
                        // }
                    }
                }
                //
                //     if reads.len() == 0 {
                //         continue;
                //     }
                //
                //     // smart_radix_sort::<false>(reads.as_mut_slice(), 64 - 8);
                //     reads.sort_unstable();
                //
                //     let mut last = reads[0];
                //     let mut count = 1;
                //     kmers_unique += 1;
                //     for read in reads.iter() {
                //         if last != *read {
                //             kmers_unique += 1;
                //
                //             if count >= 3 {
                //                 m5 += 1;
                //             }
                //
                //             last = *read;
                //             count = 1;
                //         } else {
                //             count += 1;
                //         }
                //     }
                //
                //     if count >= 3 {
                //         m5 += 1;
                //     }
                // }

                // vec.sort_unstable();

                /**
             * A kmer can be considered non branching if
             */

                println!(
                    "[{}/{}]Kmers {}, unique: {}, ratio: {:.2}% ~~ m5: {} ratio: {:.2}% [{:?}] Time: {:?}",
                    CURRENT_BUCKETS_COUNT.fetch_add(1, Ordering::Relaxed) + 1,
                    BUCKETS_COUNT,
                    kmers_cnt,
                    kmers_unique,
                    (kmers_unique as f32) / (kmers_cnt as f32) * 100.0,
                    m5,
                    (m5 as f32) / (kmers_unique as f32) * 100.0,
                    buckets.iter().map(|x| x.len()).sum::<usize>() / 256, // set
                    start_time.elapsed()
                );
            writer.finalize();
        });
    }

    // let mut filters = Vec::new();
    //
    // while let Some(mut filter) = pool.try_pull() {
    //     filter.finalize();
    //     filters.push(filter);
    // }

    return;

    //    let mut progress = Progress::new();
    //
    ////    ctrlc::set_handler(move || {
    ////        println!("received Ctrl+C!");
    ////    });
    //
    //    let args = Cli::from_args();
    //
    //    let mut current: &ReadsFreezer;
    //
    //    let reads;
    //    let cut_n;
    //    let minim;
    //    let mut merge: Vec<Box<ReadsFreezer>> = Vec::new();
    //
    //    reads = if args.gzip_fasta {
    //        Pipeline::fasta_gzip_to_reads(args.inputs.as_slice())
    //    }
    //    else {
    //        Pipeline::file_freezers_to_reads(args.inputs.as_slice())
    //    };
    //    current = cast_static(&reads);
    //
    //    if args.nsplit {
    //        cut_n = Pipeline::cut_n(current, args.klen.unwrap());
    //        current = cast_static(&cut_n);
    //    }
    //
    //    if args.process_bucket {
    //        let kvalue = args.klen.unwrap();
    ////        for i in 1..1 {
    //            merge.push(Box::new(Pipeline::merge_bucket(current, kvalue, 256, 0)));
    //            current = cast_static(&merge.last().unwrap());
    ////        }
    //    }
    //
    //    if args.minimizers {
    //        minim = Pipeline::save_minimals(current, args.klen.unwrap());
    //        current = cast_static(&minim);
    //    }
    //
    //    if args.elabbloom {
    //        let mut filter = Pipeline::bloom_filter(current, args.klen.unwrap(), args.decimated);
    //        if let Some(ctest) = args.coverage {
    ////            Pipeline::bloom_check_coverage(ReadsFreezer::from_file(ctest.clone()), &mut filter);
    //            Pipeline::bloom_check_coverage(ReadsFreezer::from_file(ctest), &mut filter);
    //        }
    //    }
    //    else if let Some(bnum) = args.bucketing {
    //        Pipeline::make_buckets(current, args.klen.unwrap(), bnum, "buckets/bucket-");
    //    }
    //    else {
    //        current.freeze(args.output.unwrap(), args.compress);
    //    }
    //
    //    Utils::join_all();
    //    println!("Finished elab {}, elapsed {:.2} seconds", args.klen.unwrap_or_else(|| 0), progress.elapsed());
}
