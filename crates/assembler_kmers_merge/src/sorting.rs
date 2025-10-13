use std::cmp::Ordering;

use bitbound::{array::BitBoundArray, bounded::BoundedUsize};
use io::concurrent::temp_reads::creads_utils::DeserializedReadIndependent;

pub trait RadixSortableRead {
    type Context<'a>: Copy;
    unsafe fn get_u64_unchecked_overread(
        &self,
        context: Self::Context<'_>,
        offset_bytes: usize,
    ) -> u64;

    fn compare(&self, other: &Self, context: Self::Context<'_>) -> Ordering;
}

impl<E> RadixSortableRead for DeserializedReadIndependent<E> {
    type Context<'a> = &'a Vec<u8>;
    unsafe fn get_u64_unchecked_overread(
        &self,
        context: Self::Context<'_>,
        offset_bytes: usize,
    ) -> u64 {
        unsafe {
            self.read.get_u64_unchecked_aligned(
                context,
                offset_bytes * 4 + (self.minimizer_pos as usize),
            )
        }
    }

    fn compare(&self, other: &Self, context: Self::Context<'_>) -> Ordering {
        unsafe {
            self.read
                .get_centered_suffix_difference(
                    &other.read,
                    context,
                    self.minimizer_pos as usize,
                    other.minimizer_pos as usize,
                )
                .1
                .reverse()
        }
    }
}

// pub struct ReadIndex<R: RadixSortableRead>(usize, PhantomData<R>);

// impl<R: RadixSortableRead> ReadIndex<R> {
//     pub fn new(index: usize) -> Self {
//         Self(index, PhantomData)
//     }
// }

// impl<R: RadixSortableRead + 'static> RadixSortableRead for ReadIndex<R> {
//     type Context<'a> = (&'a [R], R::Context<'a>);
//     unsafe fn get_u64_unchecked_overread(
//         &self,
//         context: Self::Context<'_>,
//         offset_bytes: usize,
//     ) -> u64 {
//         unsafe {
//             context
//                 .0
//                 .get_unchecked(self.0)
//                 .get_u64_unchecked_overread(context.1, offset_bytes)
//         }
//     }
// }

pub fn radix_sort_reads<R: RadixSortableRead + Copy>(
    input_temp_reads: &mut [R],
    output_reads: &mut [R],
    context: R::Context<'_>,
    skip_offset_bytes: usize,
) {
    const RADIX_SIZE: usize = 256;

    let mut counts = BitBoundArray([0usize; RADIX_SIZE + 1]);
    let mut last_values = BitBoundArray([u64::MAX; RADIX_SIZE]);
    let mut differences = BitBoundArray([0u64; RADIX_SIZE]);

    for read in input_temp_reads.iter() {
        let value = unsafe { read.get_u64_unchecked_overread(context, skip_offset_bytes) };
        let bucket = BoundedUsize::<RADIX_SIZE>::wrapping_masked(value as usize);
        let bucket_plus1 =
            unsafe { BoundedUsize::<{ RADIX_SIZE + 1 }>::new_unchecked(bucket.into_inner() + 1) };
        counts[bucket_plus1] += 1;
        let shifted = value >> 8;

        if last_values[bucket] != u64::MAX {
            differences[bucket] |= last_values[bucket] ^ shifted;
        } else {
            last_values[bucket] = shifted;
        }
    }

    for i in 1..(RADIX_SIZE + 1) {
        let i_minus1 = unsafe { BoundedUsize::<RADIX_SIZE>::new_unchecked(i - 1) };
        let i = unsafe { BoundedUsize::<{ RADIX_SIZE + 1 }>::new_unchecked(i) };
        counts[i] += counts[i_minus1];
    }
    let mut sums = counts;

    for read in input_temp_reads.iter() {
        let bucket = unsafe {
            BoundedUsize::<RADIX_SIZE>::wrapping_masked(
                read.get_u64_unchecked_overread(context, skip_offset_bytes) as usize,
            )
        };
        output_reads[sums[bucket]] = *read;
        sums[bucket] += 1;
    }

    let mut bucket_start = 0;
    for bucket in 0..RADIX_SIZE {
        let bucket = unsafe { BoundedUsize::<RADIX_SIZE>::new_unchecked(bucket) };
        let bucket_end = sums[bucket];
        if bucket_end - bucket_start > 1 {
            let next_skip_offset_bytes = skip_offset_bytes
                + 1
                + (differences[bucket].trailing_zeros() as usize / 8)
                    .min(7 /* the first byte is in the + 1 */);

            if next_skip_offset_bytes < 8 {
                // println!(
                //     "Recurse: {} => {:?}!",
                //     next_skip_offset_bytes,
                //     bucket_start..bucket_end
                // );
                if (bucket_start..bucket_end).len() > 128 {
                    input_temp_reads[bucket_start..bucket_end]
                        .copy_from_slice(&output_reads[bucket_start..bucket_end]);

                    println!(
                        "Sorting: {:?} with diff: {:x}",
                        bucket_start..bucket_end,
                        differences[bucket]
                    );
                    radix_sort_reads(
                        &mut input_temp_reads[bucket_start..bucket_end],
                        &mut output_reads[bucket_start..bucket_end],
                        context,
                        next_skip_offset_bytes,
                    );
                } else {
                    output_reads[bucket_start..bucket_end]
                        .sort_unstable_by(|a, b| a.compare(b, context));
                }
            }
            bucket_start = bucket_end;
        }
    }
}
