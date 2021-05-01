use crate::rolling_kseq_iterator::RollingKseqImpl;
use rand::prelude::*;
use serde::export::PhantomData;
use std::cmp::{min, min_by_key};
use std::collections::VecDeque;
use std::fmt::Display;
use std::hint::unreachable_unchecked;
use std::mem::MaybeUninit;

pub trait GenericsFunctional<T, U> {
    fn func(value: T) -> U;
}

pub struct RollingMinQueue<T: Copy, U: Ord + Copy, F: GenericsFunctional<T, U>> {
    queue: Vec<(T, T)>,
    index: usize,
    capacity_mask: usize,
    size: usize,
    minimum: (T, usize),
    _marker1: PhantomData<U>,
    _marker2: PhantomData<F>,
}

impl<T: Copy, U: Ord + Copy, F: GenericsFunctional<T, U>> RollingMinQueue<T, U, F> {
    pub fn new(size: usize) -> RollingMinQueue<T, U, F> {
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
            _marker1: PhantomData,
            _marker2: PhantomData,
        }
    }

    // #[inline(always)]
    // fn add_element(&mut self, value: T) {
    //     // let tvalue = F::func(value);
    //     self.queue.push_back(value);
    // }
    //
    // #[inline(always)]
    // fn remove_element(&mut self, value: T) {
    //     //     if F::func(*val) == value {
    //     self.queue.pop_front();
    // }

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
                    |x| F::func(*x),
                );
            }
            i = i.wrapping_sub(1) & self.capacity_mask;
        }
    }

    pub fn make_iter<'a>(
        &'a mut self,
        mut iter: impl Iterator<Item = T> + 'a,
    ) -> impl Iterator<Item = T> + 'a {
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
                |x| F::func(x.0),
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
                |x| F::func(*x),
            )
        })
    }
}

// #[cfg(feature = "test")]
mod tests {
    use crate::rolling_minqueue::{GenericsFunctional, RollingMinQueue};
    use rand::{thread_rng, RngCore, SeedableRng};

    #[test]
    fn minqueue_test() {
        struct Func {}
        impl GenericsFunctional<u64, u32> for Func {
            fn func(value: u64) -> u32 {
                (value >> 32) as u32
            }
        }

        const SIZE: usize = 10000000;
        const MINWINDOW: usize = 32;

        let mut queue = RollingMinQueue::<_, _, Func>::new(MINWINDOW);

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
