use crate::bloom_filter::{BloomFilter, SET_BIT};
use crate::cache_bucket::CacheBucket;
use crate::gzip_fasta_reader::FastaSequence;
use crate::pipeline::MINIMIZER_THRESHOLD_VALUE;
use crate::progress::Progress;
use crate::reads_freezer::{ReadsFreezer, ReadsWriter};
use itertools::all;
use std::alloc::Layout;
use std::io::Write;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub const TOTAL_BLOOM_COUNTERS_EXP: usize = 35;
/// 38;
pub const BLOOM_FIELDS_EXP_PER_BYTE: usize = 3;

pub const TOTAL_MEM_EXP_IN_BYTES: usize = TOTAL_BLOOM_COUNTERS_EXP - BLOOM_FIELDS_EXP_PER_BYTE;

pub const TOTAL_MEM_EXP_FIRST: usize = TOTAL_MEM_EXP_IN_BYTES + BLOOM_FIELDS_EXP_PER_BYTE;
pub const MAP_SIZE_EXP_FIRST: usize = 8; //1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_FIRST: usize = 524288;
pub const LINE_SIZE_EXP_FIRST: usize = TOTAL_MEM_EXP_FIRST - MAP_SIZE_EXP_FIRST;

pub const TOTAL_MEM_EXP_SECOND: usize = LINE_SIZE_EXP_FIRST;
pub const MAP_SIZE_EXP_SECOND: usize = 8; //1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_SECOND: usize = 8192;
pub const LINE_SIZE_EXP_SECOND: usize = TOTAL_MEM_EXP_SECOND - MAP_SIZE_EXP_SECOND;

pub const TOTAL_MEM_EXP_THIRD: usize = LINE_SIZE_EXP_SECOND;
pub const MAP_SIZE_EXP_THIRD: usize = 7; //1024 * 1024 * 1024 * 4 * 4 / 4096;
pub const BULK_SIZE_THIRD: usize = 2048;
pub const LINE_SIZE_EXP_THIRD: usize = TOTAL_MEM_EXP_THIRD - MAP_SIZE_EXP_THIRD;

const L1_BUCKETS_COUNT: usize = (1 << MAP_SIZE_EXP_FIRST);
const L2_BUCKETS_COUNT: usize = (1 << MAP_SIZE_EXP_SECOND);
const L3_BUCKETS_COUNT: usize = (1 << MAP_SIZE_EXP_THIRD);

type BucketValueFirst = u32;
type BucketValueSecond = u32;
type BucketValueThird = u16;

pub trait FlushableBucket<T> {
    fn initialize(&mut self, parent_value: usize);
    fn add_element(&mut self, val: T);
    fn finalize(&mut self);
}

struct BucketsCache {}

pub struct HierarchicalBloomFilter<
    F1: FnMut(usize, usize, &mut [u32]),
    F2: FnMut(usize, usize, &mut [u32]),
    F3: FnMut(usize, usize, &mut [u32]),
    const USE_CACHE: bool,
> {
    // filter: Box<BloomFilter>,
    l1_cache: Box<CacheBucket<F1, BULK_SIZE_FIRST, L1_BUCKETS_COUNT, LINE_SIZE_EXP_FIRST>>,
    l2_cache: Box<[CacheBucket<F2, BULK_SIZE_SECOND, L2_BUCKETS_COUNT, LINE_SIZE_EXP_SECOND>]>,
    l3_cache: Box<CacheBucket<F3, BULK_SIZE_THIRD, L3_BUCKETS_COUNT, LINE_SIZE_EXP_THIRD>>,
    collisions: u64,
    total: u64,
    l1_flushes: u64,
    l2_flushes: u64,
    l3_flushes: u64,

    progress: Progress,
    total_progress: Progress,
    real_total: u64,
    global_total: u64,
    gb: u64,
    k: usize,

    freeze: ReadsWriter,
}

pub struct HierarcicalBloomFilterFactory {}

pub unsafe fn lifetime_extend_mut<'a, 'b, T: ?Sized>(value: &'a mut T) -> &'b mut T {
    core::mem::transmute(value)
}

impl HierarcicalBloomFilterFactory {
    pub fn new<const USE_CACHE: bool>(
        k: usize,
    ) -> HierarchicalBloomFilter<
        impl FnMut(usize, usize, &mut [u32]),
        impl FnMut(usize, usize, &mut [u32]),
        impl FnMut(usize, usize, &mut [u32]),
        USE_CACHE,
    > {
        println!(
            "{} {} {}",
            LINE_SIZE_EXP_FIRST, LINE_SIZE_EXP_SECOND, LINE_SIZE_EXP_THIRD
        );
        assert!(
            LINE_SIZE_EXP_SECOND <= 32,
            "Second level address does not fit 32 bit!!"
        );
        assert!(
            LINE_SIZE_EXP_THIRD <= 32,
            "Third level address does not fit 32 bit!!"
        );

        // let mut filter: Box<BloomFilter> =
        // Box::new(BloomFilter::new(1 << TOTAL_MEM_EXP_IN_BYTES, k));

        // let test = AtomicU64::new(0);
        // let inc = AtomicU64::new(0);

        // let filter_ref = unsafe { lifetime_extend_mut(filter.as_mut()) };
        let mut l3_cache =
            CacheBucket::<_, BULK_SIZE_THIRD, L3_BUCKETS_COUNT, LINE_SIZE_EXP_THIRD>::new(
                move |addr, bucket, data| {
                    // test.fetch_add(data.len() as u64, Ordering::Relaxed);
                    // inc.fetch_add(1, Ordering::Relaxed);
                    // println!(
                    //     "Size {} {} / {}bytes",
                    //     data.len(),
                    //     test.load(Ordering::Relaxed) as f32 / inc.load(Ordering::Relaxed) as f32,
                    //     (1 << LINE_SIZE_EXP_THIRD)
                    // );
                    // for el in data {
                    //     filter_ref
                    //         .increment_cell(addr + (bucket << LINE_SIZE_EXP_THIRD) + *el as usize);
                    // }
                },
            );

        // let l3_cache_ref = unsafe { lifetime_extend_mut(l3_cache.as_mut()) };
        let mut l2_cache =
            CacheBucket::<_, BULK_SIZE_SECOND, L2_BUCKETS_COUNT, LINE_SIZE_EXP_SECOND>::new_slice(
                move |addr, bucket, data| {
                    data.sort_unstable();

                    let mut diffs = 1;

                    for i in 0..data.len() - 1 {
                        diffs += (data[i] != data[i + 1]) as u64;
                    }

                    SET_BIT.fetch_add(diffs, Ordering::Relaxed);
                    //     l3_cache_ref.initialize(addr + (bucket << LINE_SIZE_EXP_SECOND));
                    //     for el in data {
                    //         l3_cache_ref.add_element(*el as usize);
                    //     }
                    //     l3_cache_ref.finalize();
                },
                L1_BUCKETS_COUNT,
            );

        for (bucket, cache) in l2_cache.iter_mut().enumerate() {
            cache.initialize((bucket << LINE_SIZE_EXP_FIRST));
        }

        let l2_cache_ref = unsafe { lifetime_extend_mut(l2_cache.as_mut()) };
        let mut l1_cache =
            CacheBucket::<_, BULK_SIZE_FIRST, L1_BUCKETS_COUNT, LINE_SIZE_EXP_FIRST>::new(
                move |addr, bucket, data| {
                    // println!("Flush bucket {} {}", bucket, LINE_SIZE_EXP_FIRST);
                    // l2_cache_ref.initialize(addr + (bucket << LINE_SIZE_EXP_FIRST));
                    for el in data {
                        // println!(
                        //     "Add element {} + {:b}",
                        //     addr + (bucket << LINE_SIZE_EXP_FIRST),
                        //     *el
                        // );
                        l2_cache_ref[bucket].add_element(*el as usize);
                    }
                    // l2_cache_ref.finalize();
                },
            );

        l1_cache.initialize(0);

        static INDEX: AtomicU32 = AtomicU32::new(0);

        HierarchicalBloomFilter {
            // filter,
            l1_cache,
            l2_cache,
            l3_cache,
            collisions: 0,
            total: 0,
            l1_flushes: 0,
            l2_flushes: 0,
            l3_flushes: 0,

            progress: Progress::new(),
            total_progress: Progress::new(),
            real_total: 0u64,
            global_total: 0u64,
            gb: 0u64,
            k,
            freeze: ReadsFreezer::optfile_splitted_compressed(format!(
                "test{}",
                INDEX.fetch_add(1, Ordering::Relaxed)
            )),
        }
    }
}

impl<
        F1: FnMut(usize, usize, &mut [u32]),
        F2: FnMut(usize, usize, &mut [u32]),
        F3: FnMut(usize, usize, &mut [u32]),
        const USE_CACHE: bool,
    > HierarchicalBloomFilter<F1, F2, F3, USE_CACHE>
{
    pub fn add_sequence(&mut self, record: &[u8]) {
        self.real_total += record.len() as u64;

        if record.len() < self.k {
            return;
        }

        let mut hashes = nthash::NtHashIterator::new(record, self.k).unwrap();

        for (idx, hash_seed) in hashes.iter_enumerate() {
            if hash_seed % (self.k as u64) == 0 {
                self.freeze.add_read(FastaSequence {
                    ident: &[],
                    seq: &record[idx..idx + self.k],
                    qual: None,
                });
            }

            for hash in &[hash_seed] {
                // if self.decimation && *hash > MINIMIZER_THRESHOLD_VALUE {
                //     continue;
                // }

                // let address = (*hash as usize) % (1 << TOTAL_MEM_EXP_FIRST);
                // let address2 = (hash.rotate_left(32) as usize) % (1 << TOTAL_MEM_EXP_FIRST);

                if USE_CACHE {
                    // let (major_first, minor_first, base_first) =
                    //     CacheBucketsFirst::<BucketValueFirst>::parameters(address);
                    // l1_cache.push(major_first, minor_first as BucketValueFirst, |a, b| {
                    //     self_.process_first_cache_flush(a, b, false)
                    // });
                    // self.l1_cache.add_element(address);
                    // self.l1_cache.add_element(address2);
                } else {
                    // self.filter.increment_cell(address);
                    // self.filter.increment_cell(address2);
                    // if  {
                    //     unsafe {
                    //         self.collisions += 1;
                    //     }
                    // }
                    // unsafe {
                    //     self.total += 1;
                    // }
                    continue;
                }
            }
        }
        // self.progress.event(|t, p| p >= 1000000, |t, p, r, e| {
        //                 if self.use_cache && t % 500000000 == 0 {
        //                     println!("Flushing!!");
        //                     for i in 0..(1 << MAP_SIZE_EXP_FIRST) {
        //                         self.l1_cache.flush(i, |a, b| self_.process_first_cache_flush(a, b, true));
        //                     }
        //                 }
        //                 println!("Rate {:.1}M bases/sec {{{}, {}, {}, {}}} F{{{:.2}, {:.2}, {:.2}}} records [{}] {}/{} => {:.2}% | {:.2}%",
        //                          self.real_total as f64 / e / 1000000.0,
        //                          self.l1_flushes,
        //                          self.l2_flushes,
        //                          self.l3_flushes,
        //                          self.l3_flushes * (BULK_SIZE_THIRD as u64),
        //                          unsafe { TOTAL_FILL_FIRST as f64 / ((1 << MAP_SIZE_EXP_FIRST) * BULK_SIZE_FIRST) as f64 * 100.0 },
        //                          unsafe { TOTAL_FILL_SECOND as f64 / ((1 << MAP_SIZE_EXP_FIRST) * (1 << MAP_SIZE_EXP_SECOND) * BULK_SIZE_SECOND) as f64 * 100.0 },
        //                          unsafe {
        //                              TOTAL_FILL_THIRD as f64 /
        //                                  ((1 << MAP_SIZE_EXP_FIRST) * (1 << MAP_SIZE_EXP_SECOND) * (1 << MAP_SIZE_EXP_THIRD) * BULK_SIZE_THIRD) as f64 * 100.0
        //                          },
        //                          t,
        //
        //                          *collisions,
        //
        //                          *total,
        //
        //                          *collisions as f64 / *total as f64 * 100.0,
        //
        //                          (*total - *collisions) as f64 / ((1usize << TOTAL_MEM_EXP_FIRST) as f64) * 100.0);
        //
        //             self.l1_flushes = 0;
        //             self.l2_flushes = 0;
        //             self.l3_flushes = 0;
        //             self.global_total += self.real_total;
        //             self.real_total = 0;
        // //            progress.restart();
        //             });
    }

    pub fn finalize(&mut self) -> Option<&mut BloomFilter> {
        if USE_CACHE {
            println!("Final flushing!!");
            self.l1_cache.finalize();

            for cache in self.l2_cache.iter_mut() {
                cache.finalize();
            }
        }

        //        records += BinarySerializer::count_entries(file.clone());//, format!("{}.xbin", file));
        //        BinarySerializer::serialize_file(file.clone(), format!("{}.xbin", file));

        //        let dna = BinarySerializer::deserialize_file(format!("{}.bin", file));

        //        println!("Compressed to {}", file);
        //        gb += File::open(file.as_str()).unwrap().metadata().unwrap().len();

        //    result.sort();
        //    for r in result {
        //        println!("{}", std::str::from_utf8(r.as_slice()).unwrap());
        //    }

        // progress.event(
        //     |_, _| true,
        //     |t, p, r, e| {
        //         global_total += real_total;
        //
        //         println!(
        //             "Rate {:.2}M bases/sec records [{}] {}/{} => {:.2}% | {:.2}%",
        //             global_total as f64 / total_progress.elapsed() / 1000000.0,
        //             t,
        //             collisions,
        //             total,
        //             collisions as f64 / total as f64 * 100.0,
        //             unsafe {
        //                 (total - collisions) as f64 / ((1u64 << TOTAL_MEM_EXP_FIRST) as f64) * 100.0
        //             }
        //         );
        //         println!(
        //             "Elapsed {} seconds, read {} records and {} gb!",
        //             total_progress.elapsed(),
        //             t,
        //             (gb as f64) / 1024.0 / 1024.0 / 1024.0
        //         );
        //     },
        // );
        // self.filter.as_mut()
        None
    }
}
