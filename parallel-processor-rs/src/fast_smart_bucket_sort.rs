use crate::memory_fs::MemoryFile;
use crate::multi_thread_buckets::BucketWriter;
use rand::{thread_rng, RngCore};
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::cmp::min;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::io::Write;
use std::mem::size_of;
use std::ops::Range;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use unchecked_index::{unchecked_index, UncheckedIndex};

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
    unsafe {
        dest_buffer.set_len(tot_entries)
    };

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
    let num_threads = rayon::current_num_threads();
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

pub fn fast_smart_radix_sort<T: Sync + Send, F: SortKey<T>, const PARALLEL: bool>(data: &mut [T]) {
    smart_radix_sort_::<T, F, PARALLEL, false>(data, F::KEY_BITS as u8 - 8);
}

fn smart_radix_sort_<
    T: Sync + Send,
    F: SortKey<T>,
    const PARALLEL: bool,
    const SINGLE_STEP: bool,
>(
    data: &mut [T],
    shift: u8,
) -> [IndexType; 256 + 1] {
    let mut stack = unsafe { unchecked_index(vec![(0..0, 0); shift as usize * 256]) };


    let mut stack_index = 1;
    stack[0] = (0..data.len(), shift);

    let mut ret_counts = [0; 256 + 1];

    let mut first = true;

    while stack_index > 0 {
        stack_index -= 1;
        let (range, shift) = stack[stack_index].clone();

        let mut data = unsafe { unchecked_index(&mut data[range.clone()]) };

        let mut counts: UncheckedIndex<[IndexType; 256 + 1]> =
            unsafe { unchecked_index([0; 256 + 1]) };
        let mut sums: UncheckedIndex<[IndexType; 256 + 1]> =
            unsafe { unchecked_index([0; 256 + 1]) };

        unsafe {
            if PARALLEL {
                const ATOMIC_ZERO: AtomicUsize = AtomicUsize::new(0);
                let mut par_counts: UncheckedIndex<[AtomicUsize; 256 + 1]> =
                    unsafe { unchecked_index([ATOMIC_ZERO; 256 + 1]) };
                let num_threads = rayon::current_num_threads();
                let chunk_size = (data.len() + num_threads - 1) / num_threads;
                data.chunks(chunk_size).par_bridge().for_each(|chunk| {
                    let mut thread_counts = unsafe { unchecked_index([0; 256 + 1]) };

                    for el in chunk {
                        thread_counts[(F::get_shifted(el, shift)) as usize + 1] += 1;
                    }

                    for (p, t) in par_counts.iter().zip(thread_counts.iter()) {
                        p.fetch_add(*t, std::sync::atomic::Ordering::Relaxed);
                    }
                });

                for i in 1..(256 + 1) {
                    counts[i] =
                        counts[i - 1] + par_counts[i].load(std::sync::atomic::Ordering::Relaxed);
                }
                sums = counts;

                let mut bucket_queues = Vec::with_capacity(256);
                for i in 0..256 {
                    bucket_queues.push(crossbeam::channel::unbounded());

                    let range = sums[i]..counts[i + 1];
                    let range_steps = num_threads * 2;
                    let tot_range_len = range.len();
                    let subrange_len = (tot_range_len + range_steps - 1) / range_steps;

                    let mut start = range.start;
                    while start < range.end {
                        let end = min(start + subrange_len, range.end);
                        if start < end {
                            bucket_queues[i].0.send(start..end);
                        }
                        start += subrange_len;
                    }
                }

                let data_ptr = data.as_mut_ptr() as usize;
                (0..num_threads).into_par_iter().for_each(|thread_index| {
                    let mut start_buckets = unsafe { unchecked_index([0; 256]) };
                    let mut end_buckets = unsafe { unchecked_index([0; 256]) };

                    let data = from_raw_parts_mut(data_ptr as *mut T, data.len());


                    let get_bpart = || {
                        let start = thread_rng().next_u32() as usize % 256;
                        let mut res = None;
                        for i in 0..256 {
                            let bucket_num = (i + start) % 256;
                            if let Ok(val) = bucket_queues[bucket_num].1.try_recv() {
                                res = Some((bucket_num, val));
                                break;
                            }
                        }
                        res
                    };

                    let mut buckets_stack: Vec<_> = vec![];

                    while let Some((bidx, bpart)) = get_bpart() {
                        start_buckets[bidx] = bpart.start;
                        end_buckets[bidx] = bpart.end;
                        buckets_stack.push(bidx);


                        while let Some(bucket) = buckets_stack.pop() {
                            while start_buckets[bucket] < end_buckets[bucket] {
                                let mut val =
                                    (F::get_shifted(&data[start_buckets[bucket]], shift)) as usize;

                                while start_buckets[val] == end_buckets[val] {
                                    let next_bucket = match bucket_queues[val].1.try_recv() {
                                        Ok(val) => val,
                                        Err(_) => {
                                            // Final thread
                                            if thread_index == num_threads - 1 {
                                                bucket_queues[val].1.recv().unwrap()
                                            } else {
                                                // Non final thread, exit and let the final thread finish the computation
                                                for i in 0..256 {
                                                    if start_buckets[i] < end_buckets[i] {
                                                        bucket_queues[i]
                                                            .0
                                                            .send(start_buckets[i]..end_buckets[i]);
                                                    }
                                                }
                                                return;
                                            }
                                        }
                                    };
                                    start_buckets[val] = next_bucket.start;
                                    end_buckets[val] = next_bucket.end;
                                    buckets_stack.push(val);
                                }

                                data.swap(start_buckets[bucket], start_buckets[val]);
                                start_buckets[val] += 1;
                            }
                        }
                    }
                });
            } else {
                for el in data.iter() {
                    counts[(F::get_shifted(el, shift)) as usize + 1] += 1;
                }

                for i in 1..(256 + 1) {
                    counts[i] += counts[i - 1];
                }
                sums = counts;

                for bucket in 0..256 {
                    let end = counts[bucket + 1];
                    while sums[bucket] < end {
                        let mut val = (F::get_shifted(&data[sums[bucket]], shift)) as usize;
                        data.swap(sums[bucket], sums[val]);
                        sums[val] += 1;
                    }
                }
            }
        }

        if first {
            ret_counts = *counts;
            first = false;
        }

        struct UCWrapper<T> {
            uc: UnsafeCell<T>,
        }
        unsafe impl<T> Sync for UCWrapper<T> {}
        let data_ptr = UCWrapper {
            uc: UnsafeCell::new(data),
        };

        if !SINGLE_STEP && shift >= 8 {
            if PARALLEL && shift as usize == (F::KEY_BITS - 8) {
                (0..256usize)
                    .into_par_iter()
                    .filter(|x| (counts[(*x as usize) + 1] - counts[*x as usize]) > 1)
                    .for_each(|i| {
                        let mut data_ptr = unsafe { std::ptr::read(data_ptr.uc.get()) };
                        let slice = &mut data_ptr[counts[i] as usize..counts[i + 1] as usize];
                        smart_radix_sort_::<T, F, false, false>(slice, shift - 8);
                    });
            } else {
                (0..256).into_iter().for_each(|i| {
                    let slice_len = counts[i + 1] - counts[i];
                    let mut data_ptr = unsafe { std::ptr::read(data_ptr.uc.get()) };

                    match slice_len {
                        2 => {
                            if F::compare(&data_ptr[counts[i]], &data_ptr[counts[i] + 1])
                                == Ordering::Greater
                            {
                                data_ptr.swap(counts[i], counts[i] + 1);
                            }
                        }
                        0 | 1 => return,

                        _ => {}
                    }

                    if slice_len < 192 {
                        let slice = &mut data_ptr[counts[i] as usize..counts[i + 1] as usize];
                        slice.sort_unstable_by(F::compare);
                        return;
                    }

                    stack[stack_index] = (
                        range.start + counts[i] as usize..range.start + counts[i + 1] as usize,
                        shift - 8,
                    );
                    stack_index += 1;
                });
            }
        }
    }
    ret_counts
}

mod tests {
    use crate::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
    use rand::{thread_rng, RngCore};
    use rayon::prelude::*;
    use std::cmp::Ordering;
    use std::time::Instant;

    const VEC_SIZE: usize = 1000000000;

    struct U64SortKey;
    impl SortKey<u64> for U64SortKey {
        type KeyType = u64;
        const KEY_BITS: usize = std::mem::size_of::<u64>() * 8;

        #[inline(always)]
        fn compare(left: &u64, right: &u64) -> std::cmp::Ordering {
            left.cmp(right)
        }

        #[inline(always)]
        fn get_shifted(value: &u64, rhs: u8) -> u8 {
            (*value >> rhs) as u8
        }
    }

    #[test]
    fn sorting_test() {
        let mut data = vec![0; VEC_SIZE];

        data.par_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = thread_rng().next_u64());

        println!("Started sorting...");
        let start = Instant::now();
        fast_smart_radix_sort::<_, U64SortKey, true>(data.as_mut_slice());
        println!("Done sorting => {:.2?}!", start.elapsed());
        assert!(data.is_sorted_by(|a, b| {
            Some(match a.cmp(b) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => {
                    panic!("{} > {}!", a, b);
                }
            })
        }));
    }
}
