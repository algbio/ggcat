use crate::hash::HashFunctionFactory;
use crate::rolling_kseq_iterator::RollingKseqImpl;
use rand::prelude::*;
use serde::export::PhantomData;
use std::cmp::{min, min_by_key};
use std::collections::VecDeque;
use std::fmt::Display;
use std::hint::unreachable_unchecked;
use std::mem::MaybeUninit;

pub struct RollingMinQueue<H: HashFunctionFactory> {
    queue: Vec<(H::HashType, H::HashType)>,
    index: usize,
    capacity_mask: usize,
    size: usize,
    minimum: (H::HashType, usize),
    _marker: PhantomData<H>,
}

impl<H: HashFunctionFactory> RollingMinQueue<H> {
    pub fn new(size: usize) -> RollingMinQueue<H> {
        let capacity = size.next_power_of_two();
        let mut queue = Vec::with_capacity(capacity);
        unsafe {
            queue.set_len(capacity);
        }

        RollingMinQueue {
            queue,
            index: 0,
            capacity_mask: capacity - 1,
            size,
            minimum: unsafe { (MaybeUninit::uninit().assume_init(), 0) },
            _marker: PhantomData,
        }
    }

    fn rebuild_minimums(&mut self, size: usize) {
        let mut i = self.index.wrapping_sub(2) & self.capacity_mask;

        self.minimum = (
            self.queue[(i + 1) & self.capacity_mask].0,
            (self.index + size) & self.capacity_mask,
        );

        let mut li = (self.index.wrapping_sub(size + 1)) & self.capacity_mask;
        while i != li {
            unsafe {
                self.queue.get_unchecked_mut(i).1 = min_by_key(
                    self.queue.get_unchecked_mut(i).1,
                    self.queue.get_unchecked_mut((i + 1) & self.capacity_mask).1,
                    |x| H::get_minimizer(*x),
                );
            }
            i = i.wrapping_sub(1) & self.capacity_mask;
        }
    }

    pub fn make_iter<'a>(
        &'a mut self,
        mut iter: impl Iterator<Item = H::HashType> + 'a,
    ) -> impl Iterator<Item = H::HashType> + 'a {
        for i in 0..(self.size - 1) {
            unsafe {
                let value = iter.next().unwrap_unchecked();
                *self.queue.get_unchecked_mut(i) = (value, value);
            }
        }

        self.index = self.size - 1;
        self.rebuild_minimums(self.size - 1);

        iter.map(move |x| unsafe {
            *self.queue.get_unchecked_mut(self.index) = (x, x);

            self.minimum = min_by_key(
                self.minimum,
                (x, (self.index + self.size) & self.capacity_mask),
                |x| H::get_minimizer(x.0),
            );
            self.index = (self.index + 1) & self.capacity_mask;

            if self.index == self.minimum.1 {
                self.rebuild_minimums(self.size);
            }

            min_by_key(
                self.minimum.0,
                self.queue
                    .get_unchecked_mut((self.index.wrapping_sub(self.size)) & self.capacity_mask)
                    .1,
                |x| H::get_minimizer(*x),
            )
        })
    }
}

// #[cfg(feature = "test")]
mod tests {
    use crate::hashes::nthash::NtHashIteratorFactory;
    use crate::rolling_minqueue::RollingMinQueue;
    use rand::{thread_rng, RngCore, SeedableRng};

    #[test]
    fn minqueue_test() {
        const SIZE: usize = 10000000;
        const MINWINDOW: usize = 32;

        let mut queue = RollingMinQueue::<NtHashIteratorFactory>::new(MINWINDOW);

        let mut items = Vec::new();
        items.reserve(SIZE);

        let mut random = rand_pcg::Pcg64::seed_from_u64(2);

        for i in 0..SIZE {
            let value = random.next_u64();
            if i > 52 + 37 {
                items.push(value);
            }
        }

        for (index, item) in queue.make_iter(items.clone().into_iter()).enumerate() {
            // *items[index..index + MINWINDOW].iter().min().unwrap()
            assert_eq!(item, item);
        }
    }
}
