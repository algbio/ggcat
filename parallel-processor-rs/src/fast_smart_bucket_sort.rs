use crate::memory_fs::MemoryFile;
use crate::multi_thread_buckets::BucketWriter;
use rayon::current_num_threads;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::io::Write;
use std::mem::size_of;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

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
    fn compare(left: &T, right: &T) -> Ordering;
    fn get_shifted(value: &T, rhs: u8) -> u8;
}

pub struct SortingData<'a, T> {
    data: &'a mut [T],
    mask: u8,
}

pub fn striped_parallel_smart_radix_sort_memfile<
    T: Ord + Send + Sync + Debug + 'static,
    F: SortKey<T>,
>(
    mem_file: Arc<MemoryFile>,
    dest_buffer: &mut Vec<T>,
) -> usize {
    let chunks: Vec<_> = unsafe { mem_file.get_typed_chunks_mut::<T>().collect() };
    let tot_entries = chunks.iter().map(|x| x.len()).sum();

    dest_buffer.clear();
    dest_buffer.reserve(tot_entries);
    unsafe { dest_buffer.set_len(tot_entries) };

    striped_parallel_smart_radix_sort::<T, F>(chunks.as_slice(), dest_buffer.as_mut_slice());

    assert_eq!(dest_buffer.len(), chunks.iter().map(|x| x.len()).sum());
    assert_eq!(dest_buffer.len(), mem_file.len() / size_of::<T>());

    drop(mem_file);
    tot_entries
}

pub fn striped_parallel_smart_radix_sort<T: Ord + Send + Sync + Debug, F: SortKey<T>>(
    striped_file: &[&mut [T]],
    dest_buffer: &mut [T],
) {
    let num_threads = current_num_threads();
    let queue = crossbeam::queue::ArrayQueue::new(num_threads);

    let first_shift = F::KEY_BITS as u8 - 8;

    for i in 0..num_threads {
        queue.push([0; 256 + 1]);
    }

    striped_file.par_iter().for_each(|chunk| {
        let mut counts = queue.pop().unwrap();
        for el in chunk.iter() {
            counts[((F::get_shifted(el, first_shift)) as usize + 1)] += 1usize;
        }
        queue.push(counts);
    });

    let mut counters = [0; 256 + 1];
    while let Some(counts) = queue.pop() {
        for i in 1..(256 + 1) {
            counters[i] += counts[i];
        }
    }
    const ATOMIC_USIZE_ZERO: AtomicUsize = AtomicUsize::new(0);
    let offsets = [ATOMIC_USIZE_ZERO; 256 + 1];
    let mut offsets_reference = [0; 256 + 1];

    use std::sync::atomic::Ordering;
    for i in 1..(256 + 1) {
        offsets_reference[i] = offsets[i - 1].load(Ordering::Relaxed) + counters[i];
        offsets[i].store(offsets_reference[i], Ordering::Relaxed);
    }

    let dest_buffer_addr = dest_buffer.as_mut_ptr() as usize;
    striped_file.par_iter().for_each(|chunk| {
        let dest_buffer_ptr = dest_buffer_addr as *mut T;

        let chunk_addr = chunk.as_ptr() as usize;
        let chunk_data_mut = unsafe { from_raw_parts_mut(chunk_addr as *mut T, chunk.len()) };

        let choffs = smart_radix_sort_::<T, F, false, true>(chunk_data_mut, F::KEY_BITS as u8 - 8);
        let mut offset = 0;
        for idx in 1..(256 + 1) {
            let count = choffs[idx] - choffs[idx - 1];
            let dest_position = offsets[idx - 1].fetch_add(count, Ordering::Relaxed);

            unsafe {
                std::ptr::copy_nonoverlapping(
                    chunk.as_ptr().add(offset),
                    dest_buffer_ptr.add(dest_position),
                    count,
                );
            }

            offset += count;
        }
    });

    if F::KEY_BITS >= 16 {
        let offsets_reference = offsets_reference;
        (0..256usize).into_par_iter().for_each(|idx| {
            let dest_buffer_ptr = dest_buffer_addr as *mut T;

            let bucket_start = offsets_reference[idx];
            let bucket_len = offsets_reference[idx + 1] - bucket_start;

            let crt_slice =
                unsafe { from_raw_parts_mut(dest_buffer_ptr.add(bucket_start), bucket_len) };
            smart_radix_sort_::<T, F, false, false>(crt_slice, F::KEY_BITS as u8 - 16);
        });
    }
}

pub fn fast_smart_radix_sort<T, F: SortKey<T>, const PARALLEL: bool>(data: &mut [T]) {
    smart_radix_sort_::<T, F, PARALLEL, false>(data, F::KEY_BITS as u8 - 8);
}

fn smart_radix_sort_<T, F: SortKey<T>, const PARALLEL: bool, const SINGLE_STEP: bool>(
    data: &mut [T],
    shift: u8,
) -> [IndexType; 256 + 1] {
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
            smart_radix_sort_::<T, F, false, false>(slice, shift - 8);
        }
    };

    if !SINGLE_STEP {
        if PARALLEL && shift as usize == (F::KEY_BITS - 8) {
            (0..256usize).into_par_iter().for_each(elab_subarray);
        } else {
            (0..256).into_iter().for_each(elab_subarray);
        }
    }
    counts
}
