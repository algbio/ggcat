use crate::rolling_kseq_iterator::RollingKseqImpl;
use serde::export::PhantomData;
use std::collections::VecDeque;
use std::fmt::Display;
use std::hint::unreachable_unchecked;

pub trait GenericsFunctional<T, U> {
    fn func(value: T) -> U;
}

pub struct RollingMinQueue<T: Copy, U: Ord + Copy, F: GenericsFunctional<T, U>> {
    queue: VecDeque<T>,
    _marker1: PhantomData<U>,
    _marker2: PhantomData<F>,
}

impl<T: Copy, U: Ord + Copy, F: GenericsFunctional<T, U>> RollingMinQueue<T, U, F> {
    pub fn new() -> RollingMinQueue<T, U, F> {
        RollingMinQueue {
            queue: VecDeque::new(),
            _marker1: PhantomData,
            _marker2: PhantomData,
        }
    }

    #[inline(always)]
    fn add_element(&mut self, value: T) {
        let tvalue = F::func(value);
        while let Some(val) = self.queue.back() {
            if F::func(*val) > tvalue {
                self.queue.pop_back();
            } else {
                break;
            }
        }
        self.queue.push_back(value);
    }

    #[inline(always)]
    fn remove_element(&mut self, value: T) {
        let value = F::func(value);
        if let Some(val) = self.queue.front() {
            if F::func(*val) == value {
                self.queue.pop_front();
            }
        }
    }
}

impl<T: Copy, U: Ord + Copy + Display, F: GenericsFunctional<T, U>> RollingKseqImpl<T, T>
    for RollingMinQueue<T, U, F>
{
    #[inline(always)]
    fn clear(&mut self, ksize: usize) {
        self.queue.clear()
    }

    fn init(&mut self, index: usize, value: T) {
        self.add_element(value)
    }

    #[inline(always)]
    fn iter(&mut self, index: usize, out_value: T, in_value: T) -> T {
        self.add_element(in_value);
        let result = *self
            .queue
            .front()
            .unwrap_or_else(|| unsafe { unreachable_unchecked() });
        self.remove_element(out_value);
        result
    }
}
