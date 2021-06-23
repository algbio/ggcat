use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::cell::UnsafeCell;
use std::mem::size_of;

type DataType = u64;
type IndexType = usize;

pub trait SortKey<T> {
    type KeyType: Ord;
    const KEY_BITS: usize;

    fn get(value: &T) -> Self::KeyType;
    fn get_shifted(value: &T, rhs: u8) -> u8;
}

pub fn smart_radix_sort<T, F: SortKey<T>, const PARALLEL: bool>(data: &mut [T]) {
    smart_radix_sort_::<T, F, PARALLEL>(data, F::KEY_BITS as u8 - 8)
}

fn smart_radix_sort_<T, F: SortKey<T>, const PARALLEL: bool>(data: &mut [T], shift: u8) {
    let mut counts: [IndexType; 256 + 1] = [0; 256 + 1];
    let mut sums: [IndexType; 256 + 1] = [0; 256 + 1];

    for el in data.iter() {
        counts[F::get_shifted(el, shift) as usize + 1] += 1;
    }

    sums[0] = 0;
    for i in 1..(256 + 1) {
        counts[i] += counts[i - 1];
        sums[i] = counts[i];
    }

    let mut i = 0;
    while i < data.len() {
        let mut val = F::get_shifted(&data[i], shift) as usize;
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

        //&mut data[counts[i] as usize..counts[i + 1] as usize];
        if slice.len() < 1024 * 1024 {
            slice.sort_unstable_by_key(F::get)
        } else {
            smart_radix_sort_::<T, F, PARALLEL>(slice, shift - 8);
        }
    };

    if PARALLEL && shift as usize == (size_of::<DataType>() - 1) * 8 {
        // let nums: [usize; 256] = [0; 256];
        (0..256usize).into_par_iter().for_each(elab_subarray);
    } else {
        (0..256).into_iter().for_each(elab_subarray);
    }
}
//
// mod tests {
//     use crate::smart_bucket_sort::DataType;
//     use rand::{thread_rng, RngCore};
//     use std::time::Instant;
//
//     #[test]
//     fn test_radix_sort() {
//         let mut x = vec![];
//
//         let mut rand = thread_rng();
//
//         const EL_COUNT: usize = 1024 * 1024 * 1024 * 4;
//         x.reserve(EL_COUNT);
//
//         for i in 0..EL_COUNT {
//             x.push(((rand.next_u64()) as DataType));
//         }
//
//         // let mut cpy = x.clone();
//
//         let now = Instant::now();
//         // smart_radix_sort::<true>(x.as_mut_slice(), ((size_of::<DataType>() - 1) * 8) as u8);
//         println!("RS: {}", now.elapsed().as_secs_f32());
//         assert!(x.is_sorted());
//
//         // let now = Instant::now();
//         // cpy.sort();
//         // println!("SS: {}", now.elapsed().as_secs_f32());
//
//         // assert_eq!(x, cpy);
//     }
// }
