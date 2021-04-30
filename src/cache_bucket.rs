use crate::bloom_processing::*;
use std::ops::{Add, AddAssign, BitAnd, Div};

type SizeType = u32;
pub type AddressType = u32;

#[derive(Copy, Clone)]
pub struct CacheBucket<
    F: FnMut(usize, usize, &mut [AddressType]),
    const BUCKET_SIZE: usize,
    const BUCKETS_COUNT: usize,
    const ADDRESS_START: usize,
> {
    sizes: [SizeType; BUCKETS_COUNT],
    data: [[AddressType; BUCKET_SIZE]; BUCKETS_COUNT],
    parent_value: usize,
    flush_function: F,
}

impl<
        F: FnMut(usize, usize, &mut [AddressType]),
        const BUCKET_SIZE: usize,
        const BUCKETS_COUNT: usize,
        const ADDRESS_START: usize,
    > CacheBucket<F, BUCKET_SIZE, BUCKETS_COUNT, ADDRESS_START>
{
    #[inline(always)]
    pub fn new(
        flush_function: F,
    ) -> Box<CacheBucket<F, BUCKET_SIZE, BUCKETS_COUNT, ADDRESS_START>> {
        let mut boxed: Box<CacheBucket<F, BUCKET_SIZE, BUCKETS_COUNT, ADDRESS_START>> =
            unsafe { Box::new_zeroed().assume_init() };
        boxed.flush_function = flush_function;
        boxed
    }

    #[inline(always)]
    pub fn new_slice(
        flush_function: F,
        size: usize,
    ) -> Box<[CacheBucket<F, BUCKET_SIZE, BUCKETS_COUNT, ADDRESS_START>]> {
        let mut boxed: Box<[CacheBucket<F, BUCKET_SIZE, BUCKETS_COUNT, ADDRESS_START>]> =
            unsafe { Box::new_zeroed_slice(size).assume_init() };
        for el in boxed.iter_mut() {
            el.flush_function = unsafe { std::ptr::read(&flush_function as *const F) };
        }
        boxed
    }
}

impl<
        F: FnMut(usize, usize, &mut [AddressType]),
        const BUCKET_SIZE: usize,
        const BUCKETS_COUNT: usize,
        const ADDRESS_START: usize,
    > FlushableBucket<usize> for CacheBucket<F, BUCKET_SIZE, BUCKETS_COUNT, ADDRESS_START>
{
    fn initialize(&mut self, parent_value: usize) {
        self.parent_value = parent_value;
    }

    fn add_element(&mut self, val: usize) {
        let bucket = val >> ADDRESS_START;
        self.data[bucket][self.sizes[bucket] as usize] =
            (val as AddressType) & (((1usize << ADDRESS_START) - 1) as AddressType);
        self.sizes[bucket] += (1 as SizeType);
        if self.sizes[bucket] >= (BUCKET_SIZE as SizeType) {
            (self.flush_function)(
                self.parent_value,
                bucket,
                &mut self.data[bucket][0..(self.sizes[bucket] as usize)],
            );
            self.sizes[bucket] = 0;
        }
    }

    fn finalize(&mut self) {
        for bucket in 0..BUCKETS_COUNT {
            if self.sizes[bucket] > 0 {
                (self.flush_function)(
                    self.parent_value,
                    bucket,
                    &mut self.data[bucket][0..(self.sizes[bucket] as usize)],
                );
                self.sizes[bucket] = 0;
            }
        }
    }
}

// declare_bucket!(CacheBucketsFirst, MAP_SIZE_EXP_FIRST, BULK_SIZE_FIRST, LINE_SIZE_EXP_FIRST, u32);
// declare_bucket!(CacheBucketsSecond, MAP_SIZE_EXP_SECOND, BULK_SIZE_SECOND, LINE_SIZE_EXP_SECOND, u16);
// declare_bucket!(CacheBucketsThird, MAP_SIZE_EXP_THIRD, BULK_SIZE_THIRD, LINE_SIZE_EXP_THIRD, u8);
