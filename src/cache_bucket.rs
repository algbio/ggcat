use crate::*;

macro_rules! declare_bucket {
    ($Name:ident, $MAP_SIZE_EXP:expr, $BULK_SIZE:expr, $LINE_SIZE_EXP:expr, $SIZE_TYPE:path) => {
        #[derive(Copy, Clone)]
        pub struct $Name<T: Default + Copy + PartialEq + Ord> {
            sizes: [$SIZE_TYPE; 1 << $MAP_SIZE_EXP],
            data: [[T; $BULK_SIZE]; 1 << $MAP_SIZE_EXP]
        }

        impl<T: Default + Copy + PartialEq + Ord> $Name<T> {

            #[inline(always)]
            pub fn new() -> $Name<T> {
                $Name {
                    sizes: [0; 1 << $MAP_SIZE_EXP],
                    data: [[Default::default(); $BULK_SIZE]; 1 << $MAP_SIZE_EXP]
                }
            }

            pub fn push<F: FnMut(usize, &[T])>(&mut self, bucket: usize, val: T, mut lambda: F) {

//                if $LINE_SIZE_EXP == LINE_SIZE_EXP_THIRD {
//                    if self.sizes[bucket] != 0 {
//                        if self.data[bucket][(self.sizes[bucket]-1) as usize] == val {
//                            return;
//                        }
//                    }
//                }

                self.data[bucket][self.sizes[bucket] as usize] = val;
                self.sizes[bucket] += 1;

                if self.sizes[bucket] >= ($BULK_SIZE as $SIZE_TYPE) {
                    self.flush(bucket, lambda)
                }
            }

            #[inline(always)]
            pub fn flush<F: FnMut(usize, &[T])>(&mut self, bucket: usize, mut lambda: F) {

//                self.data[bucket][0..(self.sizes[bucket] as usize)].sort();
                lambda(bucket, &self.data[bucket][0..(self.sizes[bucket] as usize)]);
                self.sizes[bucket] = 0;
            }

            #[inline(always)]
            pub fn parameters(address: usize) -> (usize, usize, usize) {
                let major = address / (1 << $LINE_SIZE_EXP);
                let minor = address % (1 << $LINE_SIZE_EXP);
                let base = major << $LINE_SIZE_EXP;
                (major, minor, base)
            }
        }
    }
}

//declare_bucket!(CacheBucketsFirst, MAP_SIZE_EXP_FIRST, BULK_SIZE_FIRST, LINE_SIZE_EXP_FIRST, u32);
//declare_bucket!(CacheBucketsSecond, MAP_SIZE_EXP_SECOND, BULK_SIZE_SECOND, LINE_SIZE_EXP_SECOND, u16);
//declare_bucket!(CacheBucketsThird, MAP_SIZE_EXP_THIRD, BULK_SIZE_THIRD, LINE_SIZE_EXP_THIRD, u8);