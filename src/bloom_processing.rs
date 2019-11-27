use crate::cache_bucket::{CacheBucketsFirst, CacheBucketsSecond, CacheBucketsThird};
use crate::progress::Progress;
use crate::bloom_filter::BloomFilter;
use std::alloc::Layout;
use crate::reads_freezer::ReadsFreezer;

pub const TOTAL_BLOOM_COUNTERS_EXP: usize = 37;
pub const BLOOM_FIELDS_EXP_PER_BYTE: usize = 3;

pub const TOTAL_MEM_EXP_IN_BYTES: usize = TOTAL_BLOOM_COUNTERS_EXP - BLOOM_FIELDS_EXP_PER_BYTE;


pub const TOTAL_MEM_EXP_FIRST: usize = TOTAL_MEM_EXP_IN_BYTES + BLOOM_FIELDS_EXP_PER_BYTE;
pub const MAP_SIZE_EXP_FIRST: usize = 9;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_FIRST: usize = 1048576*2;
pub const LINE_SIZE_EXP_FIRST: usize = TOTAL_MEM_EXP_FIRST - MAP_SIZE_EXP_FIRST;

pub const TOTAL_MEM_EXP_SECOND: usize = LINE_SIZE_EXP_FIRST;
pub const MAP_SIZE_EXP_SECOND: usize = 8;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_SECOND: usize = 8192;
pub const LINE_SIZE_EXP_SECOND: usize = TOTAL_MEM_EXP_SECOND - MAP_SIZE_EXP_SECOND;

pub const TOTAL_MEM_EXP_THIRD: usize = LINE_SIZE_EXP_SECOND;
pub const MAP_SIZE_EXP_THIRD: usize = 8;//1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_THIRD: usize = 64;
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
//    return;
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


pub fn bloom(freezer: &'static ReadsFreezer, k: usize) {

    println!("{} {} {}", LINE_SIZE_EXP_FIRST, LINE_SIZE_EXP_SECOND, LINE_SIZE_EXP_THIRD);
    assert!(LINE_SIZE_EXP_SECOND <= 32, "Second level address does not fit 32 bit!!");
    assert!(LINE_SIZE_EXP_THIRD <= 16, "Third level address does not fit 16 bit!!");

    let mut progress = Progress::new();
    let mut total_progress = Progress::new();
    let mut real_total = 0u64;
    let gb = 0u64;

    let mut bloom = BloomFilter::new(1 << TOTAL_MEM_EXP_IN_BYTES);
    unsafe { BLOOM = Some(std::mem::transmute(&mut bloom)); }

    let cache: &mut CacheBucketsFirst<BucketValueFirst> = unsafe {
        &mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<CacheBucketsFirst::<BucketValueFirst>>(), 32).unwrap()) as *mut CacheBucketsFirst::<BucketValueFirst>)
    };

    unsafe {
        SECOND_LEVEL_CACHES = Some(&mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<SecondLevelCacheArray>(), 32).unwrap()) as *mut SecondLevelCacheArray));
        THIRD_LEVEL_CACHES = Some(&mut *(std::alloc::alloc_zeroed(Layout::from_size_align(std::mem::size_of::<ThirdLevelCacheArray>(), 32).unwrap()) as *mut ThirdLevelCacheArray));
    };
    println!("Allocated bloom filter structures");


    freezer.for_each(|record| {
        real_total += record.len() as u64;

        if record.len() <= k { return; }

        let mut hashes = nthash::NtHashIterator::new(record, k).unwrap();

        for hash_seed in hashes.iter() {
            for hash in &[hash_seed] {
                let address = (*hash as usize) % (1 << TOTAL_MEM_EXP_FIRST);
//                if bloom.increment_cell(address) {
//                    unsafe { COLLISIONS += 1; }
//                }
//                unsafe {
//                    TOTAL += 1;
//                }
//                continue;
                let (major_first, minor_first, base_first) = CacheBucketsFirst::<BucketValueFirst>::parameters(address);
                unsafe { TOTAL_FILL_FIRST += 1; }
                cache.push(major_first, minor_first as BucketValueFirst, |a, b| process_first_cache_flush(a, b, false));
            }
        }
        progress.event(|t, p| p >= 1000000, |t, p, r, e| {
            if t % 50000000 == 0 {
                println!("Flushing!!");
                for i in 0..(1 << MAP_SIZE_EXP_FIRST) {
                    cache.flush(i, |a, b| process_first_cache_flush(a, b, true));
                }
            }
            println!("Rate {:.1}M bases/sec {{{}, {}, {}, {}}} F{{{:.2}, {:.2}, {:.2}}} records [{}] {}/{} => {:.2}% | {:.2}%",
                     real_total as f64 / e / 1000000.0,
                     unsafe { FIRST_LEVEL_FLUSHES },
                     unsafe { SECOND_LEVEL_FLUSHES },
                     unsafe { THIRD_LEVEL_FLUSHES },
                     unsafe { THIRD_LEVEL_FLUSHES * (BULK_SIZE_THIRD as u64) },
                     unsafe { TOTAL_FILL_FIRST as f64 / ((1 << MAP_SIZE_EXP_FIRST) * BULK_SIZE_FIRST) as f64 * 100.0 },
                     unsafe { TOTAL_FILL_SECOND as f64 / ((1 << MAP_SIZE_EXP_FIRST) * (1 << MAP_SIZE_EXP_SECOND) * BULK_SIZE_SECOND) as f64 * 100.0 },
                     unsafe {
                         TOTAL_FILL_THIRD as f64 /
                             ((1 << MAP_SIZE_EXP_FIRST) * (1 << MAP_SIZE_EXP_SECOND) * (1 << MAP_SIZE_EXP_THIRD) * BULK_SIZE_THIRD) as f64 * 100.0
                     },
                     t,
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
//            progress.restart();
        });
    });
//        records += BinarySerializer::count_entries(file.clone());//, format!("{}.xbin", file));
//        BinarySerializer::serialize_file(file.clone(), format!("{}.xbin", file));

//        let dna = BinarySerializer::deserialize_file(format!("{}.bin", file));

//        println!("Compressed to {}", file);
//        gb += File::open(file.as_str()).unwrap().metadata().unwrap().len();


//    result.sort();
//    for r in result {
//        println!("{}", std::str::from_utf8(r.as_slice()).unwrap());
//    }

    println!("Final flushing!!");
    for i in 0..(1 << MAP_SIZE_EXP_FIRST) {
        cache.flush(i, |a, b| process_first_cache_flush(a, b, true));
    }

    progress.event(|_,_| true, |t, p, r, e| {
        println!("Rate {}M bases/sec records [{}] {}/{} => {:.2}% | {:.2}%",
                 real_total as f64 / e / 1000000.0,
                 t,
                 unsafe { COLLISIONS },
                 unsafe { TOTAL },
                 unsafe { COLLISIONS as f64 / TOTAL as f64 * 100.0 },
                 unsafe { (TOTAL - COLLISIONS) as f64 / ((1u64 << TOTAL_MEM_EXP_FIRST) as f64) * 100.0 });
        println!("Elapsed {} seconds, read {} records and {} gb!", total_progress.elapsed(), t, (gb as f64) / 1024.0 / 1024.0 / 1024.0);
    });
}
