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

mod binary_serializer;
mod single_dna_read;
mod single_dna_read_half;
mod utils;
mod bloom_filter;
mod rolling_hash;
mod cache_bucket;

pub const TOTAL_MEM_EXP_FIRST: usize = 34;
pub const MAP_SIZE_EXP_FIRST: usize = 9;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_FIRST: usize = 4096;
pub const LINE_SIZE_EXP_FIRST: usize = TOTAL_MEM_EXP_FIRST - MAP_SIZE_EXP_FIRST;

pub const TOTAL_MEM_EXP_SECOND: usize = LINE_SIZE_EXP_FIRST;
pub const MAP_SIZE_EXP_SECOND: usize = 9;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_SECOND: usize = 256;
pub const LINE_SIZE_EXP_SECOND: usize = TOTAL_MEM_EXP_SECOND - MAP_SIZE_EXP_SECOND;

pub const TOTAL_MEM_EXP_THIRD: usize = LINE_SIZE_EXP_SECOND;
pub const MAP_SIZE_EXP_THIRD: usize = 8;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_THIRD: usize = 64;
pub const LINE_SIZE_EXP_THIRD: usize = TOTAL_MEM_EXP_THIRD - MAP_SIZE_EXP_THIRD;

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


pub fn process_first_cache_flush(bucket_first: usize, addresses_first: &[BucketValueFirst], full: bool) {

    let second_cache = unsafe { SECOND_LEVEL_CACHES.as_mut().unwrap().get_unchecked_mut(bucket_first) };


    let second_lambda_process = |bucket_second: usize, addresses_second: &[BucketValueSecond]| {

        let third_cache = unsafe { THIRD_LEVEL_CACHES.as_mut().unwrap().get_unchecked_mut(bucket_first).get_unchecked_mut(bucket_second) };


        let third_lambda_process = |bucket_third: usize, addresses_third: &[BucketValueThird]| {
            let base_first = bucket_first << LINE_SIZE_EXP_FIRST;
            let base_second = bucket_second << LINE_SIZE_EXP_SECOND;
            let base_third = bucket_third << LINE_SIZE_EXP_THIRD;
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

        for address in addresses_second {
            let (major_third, minor_third, base_third) = CacheBucketsThird::<BucketValueThird>::parameters(*address as usize);
            third_cache.push(major_third, minor_third as BucketValueThird, third_lambda_process);
        }

        if full {
            for i in 0..(1 << MAP_SIZE_EXP_THIRD) {
                third_cache.flush(i, third_lambda_process);
            }
        }
    };

    for address in addresses_first {
        let (major_second, minor_second, base_second) = CacheBucketsSecond::<BucketValueSecond>::parameters(*address as usize);
        second_cache.push(major_second, minor_second as BucketValueSecond, second_lambda_process);
    }

    if full {
        for i in 0..(1 << MAP_SIZE_EXP_SECOND) {
            second_cache.flush(i, second_lambda_process);
        }
    }
}

fn main() {

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let records = 0u64;
    let gb = 0u64;
    let k = 31;

    let freqs:[u64; 4] = [0; 4];


    let mut bloom = BloomFilter::new(1 << TOTAL_MEM_EXP_FIRST);
    unsafe { BLOOM = Some(std::mem::transmute(&mut bloom)); }


    for file in args().skip(1) {
        println!("Reading {}", file);

        let mut records = 0u64;


        let cache: &mut CacheBucketsFirst<BucketValueFirst> = unsafe {
            &mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<CacheBucketsFirst::<BucketValueFirst>>(), 32).unwrap()) as *mut CacheBucketsFirst::<BucketValueFirst>)
        };

        unsafe {
            SECOND_LEVEL_CACHES = Some(&mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<SecondLevelCacheArray>(), 32).unwrap()) as *mut SecondLevelCacheArray));
            THIRD_LEVEL_CACHES = Some(&mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<ThirdLevelCacheArray>(), 32).unwrap()) as *mut ThirdLevelCacheArray));
        };

        BinarySerializer::process_file(file, |record| {

            let mut hashes = nthash::NtHashIterator::new(record, k).unwrap();
            for hash in hashes {
                let address = (hash as usize) % (1 << TOTAL_MEM_EXP_FIRST);
//                if bloom.increment_cell(address) {
//                    unsafe { COLLISIONS += 1; }
//                }
//                unsafe {
//                    TOTAL += 1;
//                }
//                continue;

                let (major_first, minor_first, base_first) = CacheBucketsFirst::<BucketValueFirst>::parameters(address);

                cache.push(major_first, minor_first as BucketValueFirst, |a, b| process_first_cache_flush(a, b, false));
//                    let address = (cache_line * (line_size as u64) + (crc as u64) % (line_size as u64))  as u64;
//                    println!("{}", address);
            }
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
                if records == 50000000 {
                    println!("Flushing!!");

                    for i in 0..(1 << MAP_SIZE_EXP_FIRST) {
                        cache.flush(i, |a, b| process_first_cache_flush(a, b, true));
                    }
                }

                println!("Processed {} records [{}] {}/{} => {:.2}%", (records as f64) / stopwatch.elapsed().as_secs_f64(),
                         records,
                         unsafe { COLLISIONS },
                         unsafe { TOTAL },
                         unsafe { COLLISIONS as f64 / TOTAL as f64 * 100.0 });
            }
        });

//        records += BinarySerializer::count_entries(file.clone());//, format!("{}.xbin", file));
//        BinarySerializer::serialize_file(file.clone(), format!("{}.xbin", file));

//        let dna = BinarySerializer::deserialize_file(format!("{}.bin", file));

//        println!("Compressed to {}", file);
//        gb += File::open(file.as_str()).unwrap().metadata().unwrap().len();
    }



    stopwatch.stop();
    println!("Elapsed {} seconds, read {} records and {} gb! => freqs {:?}", stopwatch.elapsed().as_secs_f64(), records, (gb as f64) / 1024.0 / 1024.0 / 1024.0, freqs);
}
