use std::{cell::Cell, fmt::Debug};

#[derive(Copy, Clone, Debug, Default)]
struct ExtraInfo {
    position: usize,
}

pub struct BatchMinQueue<X> {
    size: usize,
    backward: Vec<(u64, X)>,
    splits: Vec<((u64, X), ExtraInfo)>,
}

#[inline]
#[cold]
fn cold() {}

impl<X: Clone + Copy + Default + Debug + PartialEq> BatchMinQueue<X> {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            backward: vec![Default::default(); size - 1],
            splits: vec![Default::default(); size + 2],
        }
    }

    #[inline(always)]
    pub const fn unique_flag<const ENABLE_DUPLICATE_CHECKING: bool>() -> u64 {
        if ENABLE_DUPLICATE_CHECKING { 1 } else { 0 }
    }

    #[inline(always)]
    pub const fn hash_mask<const ENABLE_DUPLICATE_CHECKING: bool>() -> u64 {
        !Self::unique_flag::<ENABLE_DUPLICATE_CHECKING>()
    }

    pub fn get_minimizers<
        I: ExactSizeIterator<Item = (u64, X)>,
        const ENABLE_DUPLICATE_CHECKING: bool,
    >(
        &mut self,
        mut iter: I,
        skip_ending_count: usize,
        mut minimizers_callback: impl FnMut((u64, X), usize),
        mut minimizers_flush: impl FnMut(bool),
    ) {
        let mut size = iter.len() - skip_ending_count;
        if size < self.backward.len() {
            return;
        }

        unsafe {
            self.backward.fill_with(|| iter.next().unwrap_unchecked());
            let mut offset = 0;
            size -= self.backward.len();

            while offset < size {
                let remaining = (size - offset).min(self.backward.len());

                for i in (0..(self.backward.len() - 1)).rev() {
                    let mut current = *self.backward.get_unchecked(i);
                    let next = *self.backward.get_unchecked(i + 1);

                    let is_duplicated = current.0 == next.0;
                    if ENABLE_DUPLICATE_CHECKING && is_duplicated {
                        cold();
                        current.0 &= Self::hash_mask::<ENABLE_DUPLICATE_CHECKING>();
                    }

                    *self.backward.get_unchecked_mut(i) =
                        std::cmp::min_by_key(current, next, |a| a.0);
                }

                let new_item = iter.next().unwrap_unchecked();

                let mut first_minimum =
                    std::cmp::min_by_key(new_item, *self.backward.get_unchecked(0), |a| a.0);

                let is_duplicated = new_item.0 == self.backward.get_unchecked(0).0;

                if ENABLE_DUPLICATE_CHECKING && is_duplicated {
                    cold();
                    first_minimum.0 &= Self::hash_mask::<ENABLE_DUPLICATE_CHECKING>();
                }

                minimizers_callback((first_minimum.0, first_minimum.1), offset);

                let mut last_forward = new_item;
                *self.backward.get_unchecked_mut(0) = new_item;

                for i in 1..remaining {
                    let new_item = iter.next().unwrap_unchecked();
                    let current_backward = self.backward.get_unchecked_mut(i);

                    // If the new item is equal to the forward part, then it is for sure duplicated till the end of the current batch
                    let new_item_duplicated = new_item.0 == last_forward.0;
                    if ENABLE_DUPLICATE_CHECKING && new_item_duplicated {
                        cold();
                        last_forward.0 &= Self::hash_mask::<ENABLE_DUPLICATE_CHECKING>();
                    }

                    last_forward = std::cmp::min_by_key(last_forward, new_item, |a| a.0);

                    let mut current_minimum =
                        std::cmp::min_by_key(last_forward, *current_backward, |a| a.0);

                    // If the forward and backward minimums match, there is a duplicate ONLY for the current minimum
                    let is_duplicated = last_forward.0 == current_backward.0;

                    if ENABLE_DUPLICATE_CHECKING && is_duplicated {
                        cold();
                        current_minimum.0 &= Self::hash_mask::<ENABLE_DUPLICATE_CHECKING>();
                        // If current minimum is duplicated, then clear the uniqueness flag also for the last out
                    }

                    minimizers_callback((current_minimum.0, current_minimum.1), offset + i);

                    *current_backward = new_item;
                }
                offset += remaining;
                minimizers_flush(offset == size);
            }
        }
    }

    /// Splits `iter`, but for its first `skip_beginning_count` and last `skip_ending_count` items,
    /// where the minimizer of its windows changes: to another value, the unique flag included,
    /// or to another item, as its `X` tells, which holds the item's position. So every part has
    /// a single minimizer item, and spans at most `size` windows: with windows of the
    /// k - m + 1 m-mers of a k-mer, at most 2k - m bases. `splits_callback` gets each part's
    /// end, the index of the window after it (of its last window for the input's last part),
    /// with its minimizer, and whether it is the last.
    #[inline(always)]
    pub fn get_minimizer_splits<
        I: ExactSizeIterator<Item = (u64, X)>,
        const ENABLE_DUPLICATE_CHECKING: bool,
    >(
        &mut self,
        mut iter: I,
        skip_beginning_count: usize,
        skip_ending_count: usize,
        mut splits_callback: impl FnMut(usize, (u64, X), bool),
    ) {
        let last_value: Cell<(u64, X)> = Cell::new(Default::default());
        let mut is_first = true;

        let last_index = iter.len() - self.size;

        // Skip if needed
        for _ in 0..skip_beginning_count {
            iter.next();
        }

        let splits_ptr = Cell::new(self.splits.as_mut_ptr());
        let splits_start = self.splits.as_mut_ptr();

        self.get_minimizers::<_, { ENABLE_DUPLICATE_CHECKING }>(
            iter,
            skip_ending_count,
            #[inline(always)]
            |m, index| {
                let has_different_value =
                    !is_first && (last_value.get().0 != m.0 || last_value.get().1 != m.1);

                unsafe {
                    let splits_pos = &mut *splits_ptr.get();
                    splits_pos.0 = last_value.get();
                    splits_pos.1.position = index + skip_beginning_count;
                    splits_ptr.set(splits_ptr.get().add(has_different_value as usize));
                }
                is_first = false;
                last_value.set(m);
            },
            |is_last| unsafe {
                if is_last {
                    let splits_pos = &mut *splits_ptr.get();
                    splits_pos.0 = last_value.get();
                    splits_pos.1.position = last_index;
                    splits_ptr.set(splits_ptr.get().add(1));
                }

                let mut cursor = splits_start;
                while cursor != splits_ptr.get() {
                    let last = cursor.add(1) == splits_ptr.get();
                    splits_callback((*cursor).1.position, (*cursor).0, last && is_last);
                    cursor = cursor.add(1);
                }

                splits_ptr.set(splits_start);
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::BatchMinQueue;

    /// `count` items of only a few values, so that a window's minimum often appears more than
    /// once, most with the unique flag set, as with duplicate checking they come.
    fn items(count: usize, seed: u64) -> Vec<u64> {
        let mut state = seed | 1;
        (0..count)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                ((state % 6) << 1) | (state % 8 != 0) as u64
            })
            .collect()
    }

    /// Each window's minimum, with its item's position as its `X`, from `get_minimizers`.
    fn windows<const DUP: bool>(items: &[u64], size: usize, skip_end: usize) -> Vec<(u64, usize)> {
        let mut found = Vec::new();
        BatchMinQueue::<usize>::new(size).get_minimizers::<_, DUP>(
            items
                .iter()
                .enumerate()
                .map(|(position, &item)| (item, position)),
            skip_end,
            |minimum, index| {
                assert_eq!(index, found.len());
                found.push(minimum);
            },
            |_| {},
        );
        found
    }

    #[test]
    fn every_window_reports_its_minimum_with_its_items_extra() {
        let mask = BatchMinQueue::<usize>::hash_mask::<true>();
        for size in [2, 3, 5, 13, 31] {
            for seed in 1..20 {
                let items = items(400, seed);
                for (index, (value, position)) in
                    windows::<true>(&items, size, 0).into_iter().enumerate()
                {
                    let window = &items[index..index + size];
                    assert!(
                        (index..index + size).contains(&position),
                        "size {size}, window {index}"
                    );
                    assert_eq!(
                        items[position] & mask,
                        value & mask,
                        "size {size}, window {index}"
                    );
                    assert_eq!(value & mask, window.iter().min().unwrap() & mask);
                }
                for (index, (value, position)) in
                    windows::<false>(&items, size, 0).into_iter().enumerate()
                {
                    assert!(
                        (index..index + size).contains(&position),
                        "size {size}, window {index}"
                    );
                    assert_eq!(items[position], value);
                    assert_eq!(value, *items[index..index + size].iter().min().unwrap());
                }
            }
        }
    }

    /// The splits `get_minimizer_splits` makes of `items`, their positions as their `X`,
    /// against those their windows' minima call for: a part ends where its minimizer's value
    /// or position changes, and spans `size` windows at most.
    fn splits_follow_the_minimizer_position<const DUP: bool>(
        items: &[u64],
        size: usize,
        skip_begin: usize,
        skip_end: usize,
    ) {
        let mut found = Vec::new();
        BatchMinQueue::<usize>::new(size).get_minimizer_splits::<_, DUP>(
            items
                .iter()
                .enumerate()
                .map(|(position, &item)| (item, position)),
            skip_begin,
            skip_end,
            |index, minimum, last| found.push((index, minimum, last)),
        );
        // The positions `get_minimizers` sees start after the items skipped.
        let windows: Vec<_> = windows::<DUP>(&items[skip_begin..], size, skip_end)
            .into_iter()
            .map(|(value, position)| (value, position + skip_begin))
            .collect();
        let mut expected = Vec::new();
        for pair in 1..windows.len() {
            if windows[pair] != windows[pair - 1] {
                expected.push((pair + skip_begin, windows[pair - 1], false));
            }
        }
        if let Some(&minimum) = windows.last() {
            expected.push((items.len() - size, minimum, true));
        }
        assert_eq!(
            found, expected,
            "size {size}, skips {skip_begin} {skip_end}"
        );
        let mut start = skip_begin;
        for &(end, _, last) in &found[..found.len().saturating_sub(1)] {
            assert!(
                end - start <= size,
                "a part of {} windows, size {size}",
                end - start
            );
            assert!(!last);
            start = end;
        }
    }

    #[test]
    fn super_kmers_split_at_every_change_of_the_minimizer_position() {
        for size in [2, 3, 5, 13, 31] {
            for seed in 1..20 {
                let items = items(400, seed);
                for (skip_begin, skip_end) in [(0, 0), (1, 0), (0, 1), (1, 1)] {
                    splits_follow_the_minimizer_position::<true>(
                        &items, size, skip_begin, skip_end,
                    );
                    splits_follow_the_minimizer_position::<false>(
                        &items, size, skip_begin, skip_end,
                    );
                }
            }
        }
    }

    #[test]
    fn a_repeated_minimizer_still_splits_where_its_window_leaves_it() {
        // A homopolymer: every window's minimum is the same value, at a position that moves.
        let items = vec![7u64; 200];
        for size in [2, 5, 31] {
            let mut ends = Vec::new();
            BatchMinQueue::<usize>::new(size).get_minimizer_splits::<_, false>(
                items
                    .iter()
                    .enumerate()
                    .map(|(position, &item)| (item, position)),
                0,
                0,
                |index, _, _| ends.push(index),
            );
            let mut start = 0;
            for &end in &ends {
                assert!(end - start <= size, "size {size}: {ends:?}");
                start = end;
            }
            assert!(
                ends.len() >= (items.len() - size + 1) / size,
                "size {size}: {ends:?}"
            );
        }
    }
}
