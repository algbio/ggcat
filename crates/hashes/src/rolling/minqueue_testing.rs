#![allow(dead_code)]
use crate::default::MNHUnextendable;
use std::cmp::min_by_key;

struct RollingMinQueueOld {
    queue: Vec<(MNHUnextendable, MNHUnextendable)>,
    index: usize,
    capacity_mask: usize,
    size: usize,
    minimum: (MNHUnextendable, usize),
}

impl RollingMinQueueOld {
    pub fn new(size: usize) -> RollingMinQueueOld {
        let capacity = size.next_power_of_two();
        let mut queue = Vec::with_capacity(capacity);
        unsafe {
            queue.set_len(capacity);
        }

        RollingMinQueueOld {
            queue,
            index: 0,
            capacity_mask: capacity - 1,
            size,
            minimum: (MNHUnextendable::default(), 0),
        }
    }

    fn rebuild_minimums(&mut self, size: usize) {
        let mut i = self.index.wrapping_sub(2) & self.capacity_mask;

        self.minimum = (
            self.queue[(i + 1) & self.capacity_mask].0,
            (self.index + size) & self.capacity_mask,
        );

        let li = (self.index.wrapping_sub(size + 1)) & self.capacity_mask;
        while i != li {
            unsafe {
                self.queue.get_unchecked_mut(i).1 = min_by_key(
                    self.queue.get_unchecked_mut(i).1,
                    self.queue.get_unchecked_mut((i + 1) & self.capacity_mask).1,
                    |x| (*x),
                );
            }
            i = i.wrapping_sub(1) & self.capacity_mask;
        }
    }

    pub fn make_iter<'a, I: Iterator<Item = MNHUnextendable> + 'a>(
        &'a mut self,
        mut iter: I,
    ) -> impl Iterator<Item = MNHUnextendable> + 'a {
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
                |x| (x.0),
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
                |x| (*x),
            )
        })
    }
}

struct RollingMinQueue<X, const FIRST_BIT_UNIQUE_FLAG: bool> {
    queue: Vec<(MNHUnextendable, X)>,
    index: usize,
    capacity_mask: usize,
    size: usize,
    minimum: (MNHUnextendable, X),
    rebuild_trigger: usize,
}

/*
    if FIRST_BIT_UNIQUE_FLAG is true, the least significant bit of the minimizer is a flag indicating that the minimizer is unique in its window.
    ex: min & 0x1 == 1 => the minimizer is unique
    ex: min & 0x1 == 0 => the minimizer is duplicated

    in this way, taking the minimum between two minimizers preserves the duplicated status of the smaller minimizer,
    and in case of equality prefers the duplicated status
*/

impl<X: Default + Copy, const FIRST_BIT_UNIQUE_FLAG: bool>
    RollingMinQueue<X, FIRST_BIT_UNIQUE_FLAG>
{
    const MINIMIZER_MASK: u64 = if FIRST_BIT_UNIQUE_FLAG {
        u64::MAX - 1
    } else {
        u64::MAX
    };

    pub const UNIQUE_FLAG: u64 = if FIRST_BIT_UNIQUE_FLAG { 1 } else { 0 };

    pub fn new(size: usize) -> RollingMinQueue<X, FIRST_BIT_UNIQUE_FLAG> {
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
            minimum: (MNHUnextendable::default(), X::default()),
            rebuild_trigger: 0,
        }
    }

    fn rebuild_minimums(&mut self, size: usize) {
        let mut i = self.index.wrapping_sub(2) & self.capacity_mask;

        self.minimum = (u64::MAX, X::default());
        self.rebuild_trigger = (self.index + size) & self.capacity_mask;

        let li = (self.index.wrapping_sub(size + 1)) & self.capacity_mask;

        while i != li {
            unsafe {
                let prev_index = (i + 1) & self.capacity_mask;
                let prev_value = *self.queue.get_unchecked(prev_index);
                let crt_value = *self.queue.get_unchecked(i);

                *self.queue.get_unchecked_mut(i) = min_by_key(crt_value, prev_value, |x| (x.0));

                // Equality works here because the only time this condition is useful
                // is if both the prev_value and crt_value unique bit flags are unique, and thus
                // if the minimizers are the same then prev_value == crt_value
                let is_duplicated = FIRST_BIT_UNIQUE_FLAG && (prev_value.0) == (crt_value.0);

                // Clear the unique status if the minimizers are equal
                self.queue.get_unchecked_mut(i).0 &= !(is_duplicated as u64);
            }
            i = i.wrapping_sub(1) & self.capacity_mask;
        }
    }

    pub fn make_iter<'a, I: Iterator<Item = (MNHUnextendable, X)> + 'a>(
        &'a mut self,
        mut iter: I,
    ) -> impl Iterator<Item = (MNHUnextendable, X)> + 'a {
        for i in 0..(self.size - 1) {
            unsafe {
                let value = iter.next().unwrap_unchecked();
                *self.queue.get_unchecked_mut(i) = value;
            }
        }

        self.index = self.size - 1;
        self.rebuild_minimums(self.size - 1);

        iter.map(move |x| unsafe {
            *self.queue.get_unchecked_mut(self.index) = x;

            let is_duplicated = FIRST_BIT_UNIQUE_FLAG && (self.minimum.0) == (x.0);

            self.minimum = min_by_key(self.minimum, x, |x| (x.0));
            self.minimum.0 &= !(is_duplicated as u64);

            self.index = (self.index + 1) & self.capacity_mask;

            if self.index == self.rebuild_trigger {
                self.rebuild_minimums(self.size);
            }

            let queue_minimum_index = (self.index.wrapping_sub(self.size)) & self.capacity_mask;
            let queue_minimum = *self.queue.get_unchecked(queue_minimum_index);

            let is_duplicated = FIRST_BIT_UNIQUE_FLAG && (self.minimum.0) == (queue_minimum.0);
            let (minimizer, extra) = min_by_key(self.minimum, queue_minimum, |x| (x.0));

            (minimizer & !(is_duplicated as u64), extra)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::Wrapping;

    use crate::{
        ExtendableHashTraitType, HashFunction, HashFunctionFactory,
        default::MNHFactory,
        rolling::{
            batch_minqueue::BatchMinQueue,
            minqueue_testing::{RollingMinQueue, RollingMinQueueOld},
        },
    };
    use rand::{RngCore, SeedableRng};

    type NewQueue = RollingMinQueue<(), true>;

    #[test]
    fn minqueue_window_duplicates_test() {
        let mut target: Vec<_>;

        // {
        //     println!("I: {}", i);
        //     let path = "/tmp/hashes";
        //     let contents = std::fs::read_to_string(path).expect("Unable to read file");

        //     target = contents
        //         .split(',')
        //         .map(|s| s.parse().expect("Unable to parse hash"))
        //         .collect();
        //     target = target[79260 + i..79260 + 32].to_vec()
        // }

        // target = vec![1, 3, 1, 0, 5, 7, 7, 1, 2, 2, 2, 4, 1];

        let m = 11;
        target = MNHFactory::new(&b"ACTTTGATGCCGAAACGCTCGGCATCAAACAGCGGCGAAAGCGTC"[..], m)
            .iter()
            .map(|h| h.to_unextendable())
            .collect();

        target.iter_mut().for_each(|x| *x |= 1);
        println!("TARGET: {:?}", target);
        let mut queue = BatchMinQueue::new(16);
        queue.get_minimizer_splits::<_, true>(
            target.iter().copied().map(|t| (t, ())),
            0,
            0,
            |index, s, last, dupl| {
                // if s.0 == 1027188445256888837 || s.0 == 1027188445256888836 {
                println!(
                    "Index = {}, s = {:?}, last = {:?}, dupl = {:?}",
                    index, s, last, dupl
                );
                // }
            },
        );
    }

    #[test]
    fn minqueue_test() {
        const SIZE: usize = 10000000;
        const MINWINDOW: usize = 13;

        let mut queue = NewQueue::new(MINWINDOW);
        let mut queue_old = RollingMinQueueOld::new(MINWINDOW);

        let mut items = Vec::new();
        items.reserve(SIZE);

        let mut random = pcg_rand::Pcg64::seed_from_u64(2);

        for _ in 0..SIZE {
            let value = random.next_u64();
            // if i > 52 + 37 {
            items.push(value | 1);
            // }

            // if random.gen_bool(0.5) {
            //     items.push(117 << 32 | 1);
            // }
        }

        // items.sort_by_key(|a| (*a));

        items.reverse();
        let start = std::time::Instant::now();

        let res = queue
            .make_iter(items.iter().copied().map(|v| (v, ())))
            .map(|v| Wrapping(v.0))
            .sum::<Wrapping<u64>>();

        println!("Elapsed: {:?} value: {}", start.elapsed(), res.0);

        let start = std::time::Instant::now();

        // let res = queue_old
        //     .make_iter(items.iter().copied())
        //     .map(Wrapping)
        //     .sum::<Wrapping<u64>>();

        let mut last = 0;
        let mut res = 0;
        queue_old.make_iter(items.iter().copied()).for_each(|v| {
            if v != last {
                res += v;
                last = v;
            }
        });

        println!("Elapsed old: {:?} value: {}", start.elapsed(), res);

        let mut fast_queue = BatchMinQueue::new(MINWINDOW);

        let start = std::time::Instant::now();

        let mut res = 0u64;

        fast_queue.get_minimizer_splits::<_, true>(
            items.iter().copied().map(|v| (v, ())),
            0,
            0,
            |_idx, s, _, _| {
                res = res.wrapping_add(s.0);
            },
        );

        println!("Elapsed fast: {:?} value: {:?}", start.elapsed(), res);
        std::hint::black_box(&fast_queue);

        let mut index = 0;
        fast_queue.get_minimizers::<_, true>(
            items.iter().copied().map(|v| (v, ())),
            0,
            |item, _, _| {
                let minimizer_mask = !BatchMinQueue::<()>::unique_flag::<true>();

                let min_count = items[index..index + MINWINDOW]
                    .iter()
                    .filter(|v| (**v) & minimizer_mask == (item.0) & minimizer_mask)
                    .count();

                assert_eq!(
                    min_count > 1,
                    item.0 & 0x1 == 0,
                    "Error slice: {:?} with min: {}",
                    items[index..index + MINWINDOW]
                        .iter()
                        .map(|v| (*v))
                        .collect::<Vec<_>>(),
                    (item.0)
                );

                assert_eq!(
                    (item.0) & minimizer_mask,
                    (*items[index..index + MINWINDOW]
                        .iter()
                        .min_by_key(|x| (**x))
                        .unwrap())
                        & minimizer_mask,
                    "Error slice: {:?}",
                    &items[index..index + MINWINDOW]
                );
                index += 1;
            },
            |_| {},
        );

        // for (index, item) in queue
        //     .make_iter(items.iter().copied().map(|v| (v, ())))
        //     .enumerate()
        // {
        //     let min_count = items[index..index + MINWINDOW]
        //         .iter()
        //         .filter(|v| {
        //             (**v) & (NewQueue::MINIMIZER_MASK as u32)
        //                 == (item.0)
        //                     & (NewQueue::MINIMIZER_MASK as u32)
        //         })
        //         .count();

        //     // assert!(
        //     //     item & 0x1 == 1,
        //     //     "Item: {} is not unique: {:?}",
        //     //     (item),
        //     //     items[index..index + MINWINDOW]
        //     //         .iter()
        //     //         .map(|v| (*v))
        //     //         .collect::<Vec<_>>()
        //     // );

        //     // assert_eq!(
        //     //     min_count > 1,
        //     //     item.0 & 0x1 == 0,
        //     //     "Error slice: {:?} with min: {}",
        //     //     items[index..index + MINWINDOW]
        //     //         .iter()
        //     //         .map(|v| (*v))
        //     //         .collect::<Vec<_>>(),
        //     //     (item.0)
        //     // );

        //     assert_eq!(
        //         (item.0) & NewQueue::MINIMIZER_MASK as u32,
        //         (
        //             *items[index..index + MINWINDOW]
        //                 .iter()
        //                 .min_by_key(|x| (**x))
        //                 .unwrap()
        //         ) & NewQueue::MINIMIZER_MASK as u32,
        //         "Error slice: {:?}",
        //         &items[index..index + MINWINDOW]
        //     );
        // }
    }
}
