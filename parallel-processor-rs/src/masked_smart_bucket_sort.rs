use crate::multi_thread_buckets::BucketWriter;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::io::Write;

type IndexType = usize;

#[repr(packed)]
#[derive(Eq, PartialOrd, PartialEq, Ord, Copy, Clone, Debug)]
pub struct SortedData<const LEN: usize> {
    pub data: [u8; LEN],
}

impl<const LEN: usize> SortedData<LEN> {
    #[inline(always)]
    pub fn new(data: [u8; LEN]) -> Self {
        Self { data }
    }
}

impl<const LEN: usize> BucketWriter for SortedData<LEN> {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, mut bucket: impl Write, _: &Self::ExtraData) {
        bucket.write(&self.data[..]);
    }
    #[inline(always)]
    fn get_size(&self) -> usize {
        LEN
    }
}

pub trait SortKey<T> {
    type KeyType: Ord;
    const KEY_BITS: usize;

    fn get(value: &T) -> &Self::KeyType;
    fn compare(left: &T, right: &T) -> Ordering;
    fn get_shifted(value: &T, rhs: u8) -> u8;
}

pub struct SortingData<'a, T> {
    data: &'a mut [T],
    mask: u8,
}

pub fn masked_smart_radix_sort<T: Ord, F: SortKey<T>, const PARALLEL: bool>(
    data: &mut [T],
    _start_bits: usize,
) -> [IndexType; 257] {
    smart_radix_sort_::<T, F, PARALLEL>(data, F::KEY_BITS as u8 - 8)
}

fn smart_radix_sort_<T: Ord, F: SortKey<T>, const PARALLEL: bool>(
    data: &mut [T],
    shift: u8,
) -> [IndexType; 257] {
    let mut counts: [IndexType; 256 + 1] = [0; 256 + 1];
    let mut sums: [IndexType; 256 + 1] = [0; 256 + 1];

    for el in data.iter() {
        counts[((F::get_shifted(el, shift)) as usize + 1)] += 1;
    }

    sums[0] = 0;
    for i in 1..(256 + 1) {
        counts[i] += counts[i - 1];
        sums[i] = counts[i];
    }

    let mut i = 0;
    while i < data.len() {
        let mut val = (F::get_shifted(&data[i], shift)) as usize;
        if i == (sums[val] as usize) {
            i += 1;

            sums[val] += 1;
            val += 1;

            while i < data.len() && i == (counts[val] as usize) {
                i = sums[val] as usize;
                val += 1;
            }
        } else {
            data.swap(i, sums[val]);
            sums[val] += 1;
        }
    }

    struct UCWrapper<T> {
        uc: UnsafeCell<T>,
    }
    unsafe impl<T> Sync for UCWrapper<T> {}
    let data_ptr = UCWrapper {
        uc: UnsafeCell::new(data),
    };

    let elab_subarray = |i: usize| {
        let data_ptr = unsafe { std::ptr::read(data_ptr.uc.get()) };
        let slice = &mut data_ptr[counts[i] as usize..counts[i + 1] as usize];

        if slice.len() < 128 {
            slice.sort_unstable_by(|a, b| F::compare(a, b))
        } else if shift >= 8 {
            smart_radix_sort_::<T, F, PARALLEL>(slice, shift - 8);
        }
    };

    if PARALLEL && shift as usize == (F::KEY_BITS - 8) {
        (0..256usize).into_par_iter().for_each(elab_subarray);
    } else {
        (0..256).into_iter().for_each(elab_subarray);
    }

    counts
}
