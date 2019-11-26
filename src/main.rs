#![allow(warnings)]
#[macro_use]
extern crate lazy_static;

use crate::binary_serializer::BinarySerializer;
use crate::rolling_hash::RollingHash;
use std::env::args;
use crate::bloom_filter::BloomFilter;
use stopwatch::Stopwatch;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use crate::cache_bucket::{CacheBucketsFirst, CacheBucketsSecond, CacheBucketsThird};
use std::cmp::min;
use std::mem::MaybeUninit;
use std::alloc::Layout;
use crate::reads_freezer::ReadsFreezer;
use std::io::BufRead;
use std::net::Shutdown::Read;

mod binary_serializer;
mod single_dna_read;
mod single_dna_read_half;
mod utils;
mod bloom_filter;
mod rolling_hash;
mod reads_freezer;
mod cache_bucket;

pub const TOTAL_BLOOM_COUNTERS_EXP: usize = 35;
pub const BLOOM_FIELDS_EXP_PER_BYTE: usize = 3;

pub const TOTAL_MEM_EXP_IN_BYTES: usize = TOTAL_BLOOM_COUNTERS_EXP - BLOOM_FIELDS_EXP_PER_BYTE;


pub const TOTAL_MEM_EXP_FIRST: usize = TOTAL_MEM_EXP_IN_BYTES + BLOOM_FIELDS_EXP_PER_BYTE;
pub const MAP_SIZE_EXP_FIRST: usize = 9;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_FIRST: usize = 1048576*4;
pub const LINE_SIZE_EXP_FIRST: usize = TOTAL_MEM_EXP_FIRST - MAP_SIZE_EXP_FIRST;

pub const TOTAL_MEM_EXP_SECOND: usize = LINE_SIZE_EXP_FIRST;
pub const MAP_SIZE_EXP_SECOND: usize = 8;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_SECOND: usize = 8192 * 2;
pub const LINE_SIZE_EXP_SECOND: usize = TOTAL_MEM_EXP_SECOND - MAP_SIZE_EXP_SECOND;

pub const TOTAL_MEM_EXP_THIRD: usize = LINE_SIZE_EXP_SECOND;
pub const MAP_SIZE_EXP_THIRD: usize = 8;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_THIRD: usize = 128;
pub const LINE_SIZE_EXP_THIRD: usize = TOTAL_MEM_EXP_THIRD - MAP_SIZE_EXP_THIRD;

static mut TOTAL_FILL_FIRST: usize = 0;
static mut TOTAL_FILL_SECOND: usize = 0;
static mut TOTAL_FILL_THIRD: usize = 0;

type BucketValueFirst = u32;
type BucketValueSecond = u32;
type BucketValueThird = u16;

type SecondLevelCacheArray = [CacheBucketsSecond<BucketValueSecond>; 1 << MAP_SIZE_EXP_FIRST];
type ThirdLevelCacheArray = [[CacheBucketsThird<BucketValueThird>; 1 << MAP_SIZE_EXP_SECOND]; 1 << MAP_SIZE_EXP_FIRST];

static mut BLOOM: Option<&mut BloomFilter> = None;
static mut SECOND_LEVEL_CACHES: Option<&mut SecondLevelCacheArray> = None;
static mut THIRD_LEVEL_CACHES: Option<&mut ThirdLevelCacheArray> = None;
static mut COLLISIONS: u64 = 0;
static mut TOTAL: u64 = 0;
static mut FIRST_LEVEL_FLUSHES: u64 = 0;
static mut SECOND_LEVEL_FLUSHES: u64 = 0;
static mut THIRD_LEVEL_FLUSHES: u64 = 0;


pub fn process_first_cache_flush(bucket_first: usize, addresses_first: &[BucketValueFirst], full: bool) {

    unsafe { FIRST_LEVEL_FLUSHES += 1; }
    return;
    let second_cache = unsafe { SECOND_LEVEL_CACHES.as_mut().unwrap().get_unchecked_mut(bucket_first) };


    let second_lambda_process = |bucket_second: usize, addresses_second: &[BucketValueSecond]| {
        unsafe { SECOND_LEVEL_FLUSHES += 1; }
        let third_cache = unsafe { THIRD_LEVEL_CACHES.as_mut().unwrap().get_unchecked_mut(bucket_first).get_unchecked_mut(bucket_second) };


        let third_lambda_process = |bucket_third: usize, addresses_third: &[BucketValueThird]| {
            unsafe { THIRD_LEVEL_FLUSHES += 1; }
            let base_first = bucket_first << LINE_SIZE_EXP_FIRST;
            let base_second = bucket_second << LINE_SIZE_EXP_SECOND;
            let base_third = bucket_third << LINE_SIZE_EXP_THIRD;
            unsafe { TOTAL_FILL_THIRD -= addresses_third.len(); }
            for addr in addresses_third {
                unsafe {
                    let final_address = base_first | base_second | base_third | *addr as usize;
//                println!("Base address: {}", base_first | base_second | base_third);
                    if BLOOM.as_mut().unwrap().increment_cell(final_address) {
                        COLLISIONS += 1;// * ((hash != 0) as u64);
                    }
                    TOTAL += 1
                }
            }
        };

        unsafe { TOTAL_FILL_SECOND -= addresses_second.len(); }
        for address in addresses_second {
            let (major_third, minor_third, base_third) = CacheBucketsThird::<BucketValueThird>::parameters(*address as usize);
            unsafe { TOTAL_FILL_THIRD += 1; }
            third_cache.push(major_third, minor_third as BucketValueThird, third_lambda_process);
        }

        if full {
            for i in 0..(1 << MAP_SIZE_EXP_THIRD) {
                third_cache.flush(i, third_lambda_process);
            }
        }
    };

    unsafe { TOTAL_FILL_FIRST -= addresses_first.len(); }
    for address in addresses_first {
        let (major_second, minor_second, base_second) = CacheBucketsSecond::<BucketValueSecond>::parameters(*address as usize);
        unsafe { TOTAL_FILL_SECOND += 1; }
        second_cache.push(major_second, minor_second as BucketValueSecond, second_lambda_process);
    }

    if full {
        for i in 0..(1 << MAP_SIZE_EXP_SECOND) {
            second_cache.flush(i, second_lambda_process);
        }
    }
}



fn main() {

    println!("{} {} {}", LINE_SIZE_EXP_FIRST, LINE_SIZE_EXP_SECOND, LINE_SIZE_EXP_THIRD);
    assert!(LINE_SIZE_EXP_SECOND <= 32, "Second level address does not fit 32 bit!!");
    assert!(LINE_SIZE_EXP_THIRD <= 16, "Third level address does not fit 16 bit!!");

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let mut records = 0u64;
    let mut real_total = 0u64;
    let gb = 0u64;
    let k = 31;

//    let freqs:[u64; 4] = [0; 4];


    let mut bloom = BloomFilter::new(1 << TOTAL_MEM_EXP_IN_BYTES);
    unsafe { BLOOM = Some(std::mem::transmute(&mut bloom)); }


    let cache: &mut CacheBucketsFirst<BucketValueFirst> = unsafe {
        &mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<CacheBucketsFirst::<BucketValueFirst>>(), 32).unwrap()) as *mut CacheBucketsFirst::<BucketValueFirst>)
    };
    println!("Allocated");

//    unsafe {
//        SECOND_LEVEL_CACHES = Some(&mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<SecondLevelCacheArray>(), 32).unwrap()) as *mut SecondLevelCacheArray));
//        THIRD_LEVEL_CACHES = Some(&mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<ThirdLevelCacheArray>(), 32).unwrap()) as *mut ThirdLevelCacheArray));
//    };

    let mut freqs = [0u64; 256];

//    let mut result = vec![];
    for file in args().skip(1) {
        let mut freezers: [Option<ReadsFreezer>; 256] = unsafe { MaybeUninit::uninit().assume_init() };//
        for (idx, freezer) in freezers.iter_mut().enumerate() {
            let mut tmp = Some(ReadsFreezer::create(format!("bucket-{}.freeze", idx)));
            std::mem::swap(freezer, &mut tmp);
            std::mem::forget(tmp);
        }
        println!("Reading {}", file);

//        BinarySerializer::process_file(file, |record| {
        ReadsFreezer::open(file).for_each(|record| {
//            for subread in record.split(|x| *x == b'N') {
//                if subread.len() < k {
//                    continue;
//                }
//                freezer.add_read(subread);
//            }

//            freezer.add_read(record);
            real_total += record.len() as u64;
//            for c in record {
//                freqs[*c as usize] += 1;
//            }

//            if record.len() <= k {
//                return;
//            }
            let mut hashes = nthash::NtHashIterator::new(record, k).unwrap();

            let mut minv = (std::u64::MAX, 0);

            for (idx, hash) in record[k..].iter().map(|x| hashes.optim()).enumerate() {
                if  minv > (hash, idx) { // hash < std::u64::MAX / 4 && 
                    minv = (hash, idx);
                }
            }

//            result.push(Vec::from(&record[minv.1..minv.1+k]));

            let crt_hash = minv.0;
            freezers[(crt_hash % 256) as usize].as_mut().unwrap().add_read(record);
//            for hash_seed in record[k..].iter().map(|x| hashes.optim()) {
//                freqs[(hash_seed % 256) as usize] += 1;
//
//                for hash in &[hash_seed]
////                    hash_seed.rotate_right((TOTAL_BLOOM_COUNTERS_EXP/2) as u32),
////                    hash_seed.rotate_right(TOTAL_BLOOM_COUNTERS_EXP as u32)]
//                {
//                    let address = (*hash as usize) % (1 << TOTAL_MEM_EXP_FIRST);
////                    if address > (1 << TOTAL_MEM_EXP_FIRST) / 8 {
////                        continue;
////                    }
//                if bloom.increment_cell(address) {
//                    unsafe { COLLISIONS += 1; }
//                }
//                unsafe {
//                    TOTAL += 1;
//                }
//                continue;
////
//                    let (major_first, minor_first, base_first) = CacheBucketsFirst::<BucketValueFirst>::parameters(address);
////
//                    unsafe { TOTAL_FILL_FIRST += 1; }
//                    cache.push(major_first, minor_first as BucketValueFirst, |a, b| process_first_cache_flush(a, b, false));
//////                    let address = (cache_line * (line_size as u64) + (crc as u64) % (line_size as u64))  as u64;
//////                    println!("{}", address);
//                }
//            }
            records += 1;
            if records % 1000000 == 0 {
//                collisions += aaa.iter().sum::<i32>() as i32;
//                for (major, bucket) in cache.iter_mut().enumerate() {
//
//                    let base = major << LINE_SIZE_EXP;
//                    bucket.flush(|address| {
//                        if bloom.increment_cell(base | (address as usize)) {
//                            collisions += 1;
//                        }
//                    });
//                }
                if records % 50000000 == 0 {
                    println!("Flushing!!");

                    for i in 0..(1 << MAP_SIZE_EXP_FIRST) {
                        cache.flush(i, |a, b| process_first_cache_flush(a, b, true));
                    }
                }
                println!("Rate {:.1}M bases/sec {{{}, {}, {}, {}}} F{{{:.2}, {:.2}, {:.2}}} records [{}] {}/{} => {:.2}% | {:.2}%",
                         real_total as f64 / stopwatch.elapsed().as_secs_f64() / 1000000.0,
                         unsafe { FIRST_LEVEL_FLUSHES },
                         unsafe { SECOND_LEVEL_FLUSHES },
                         unsafe { THIRD_LEVEL_FLUSHES },
                         unsafe { THIRD_LEVEL_FLUSHES * (BULK_SIZE_THIRD as u64) },

                         unsafe { TOTAL_FILL_FIRST as f64 / ((1 << MAP_SIZE_EXP_FIRST) * BULK_SIZE_FIRST) as f64 * 100.0 },
                         unsafe { TOTAL_FILL_SECOND as f64 / ((1 << MAP_SIZE_EXP_FIRST) * (1 << MAP_SIZE_EXP_SECOND) * BULK_SIZE_SECOND) as f64 * 100.0 },
                         unsafe {
                             TOTAL_FILL_THIRD as f64 /
                                 ((1 << MAP_SIZE_EXP_FIRST) * (1 << MAP_SIZE_EXP_SECOND) * (1 << MAP_SIZE_EXP_THIRD)* BULK_SIZE_THIRD) as f64 * 100.0 },

                         records,
                         unsafe { COLLISIONS },
                         unsafe { TOTAL },
                         unsafe { COLLISIONS as f64 / TOTAL as f64 * 100.0 },
                         unsafe { (TOTAL - COLLISIONS) as f64 / ((1u64 << TOTAL_MEM_EXP_FIRST) as f64) * 100.0 });
                unsafe {
                    FIRST_LEVEL_FLUSHES = 0;
                    SECOND_LEVEL_FLUSHES = 0;
                    THIRD_LEVEL_FLUSHES = 0;
                }
                real_total = 0;
                stopwatch.restart();
            }
        });
//        records += BinarySerializer::count_entries(file.clone());//, format!("{}.xbin", file));
//        BinarySerializer::serialize_file(file.clone(), format!("{}.xbin", file));

//        let dna = BinarySerializer::deserialize_file(format!("{}.bin", file));

//        println!("Compressed to {}", file);
//        gb += File::open(file.as_str()).unwrap().metadata().unwrap().len();
    }


//    result.sort();
//    for r in result {
//        println!("{}", std::str::from_utf8(r.as_slice()).unwrap());
//    }

    println!("Final flushing!!");

    for i in 0..(1 << MAP_SIZE_EXP_FIRST) {
        cache.flush(i, |a, b| process_first_cache_flush(a, b, true));
    }


    println!("Rate {}M bases/sec records [{}] {}/{} => {:.2}% | {:.2}%",
             real_total as f64 / stopwatch.elapsed().as_secs_f64() / 1000000.0,
             records,
             unsafe { COLLISIONS },
             unsafe { TOTAL },
             unsafe { COLLISIONS as f64 / TOTAL as f64 * 100.0 },
             unsafe { (TOTAL - COLLISIONS) as f64 / ((1u64 << TOTAL_MEM_EXP_FIRST) as f64) * 100.0 });


    stopwatch.stop();
    println!("Elapsed {} seconds, read {} records and {} gb!", stopwatch.elapsed().as_secs_f64(), records, (gb as f64) / 1024.0 / 1024.0 / 1024.0);
    println!("Freqs A: {} C: {} G: {} T: {} N: {}", freqs[b'A' as usize], freqs[b'C' as usize], freqs[b'G' as usize], freqs[b'T' as usize], freqs[b'N' as usize]);
}
