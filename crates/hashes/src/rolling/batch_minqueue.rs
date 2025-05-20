use std::{cell::Cell, fmt::Debug};

#[derive(Copy, Clone, Debug, Default)]
struct ExtraInfo {
    position: usize,
}

pub struct BatchMinQueue<X> {
    size: usize,
    backward: Vec<(u64, X)>,
    splits: Vec<((u64, X), ExtraInfo)>,
    duplicates: Vec<bool>,
}

#[inline]
#[cold]
fn cold() {}

impl<X: Clone + Copy + Default + Debug> BatchMinQueue<X> {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            backward: vec![Default::default(); size - 1],
            splits: vec![Default::default(); size + 1],
            duplicates: vec![false; size + 1],
        }
    }

    #[inline(always)]
    pub const fn unique_flag<const ENABLE_DUPLICATE_CHECKING: bool>() -> u64 {
        if ENABLE_DUPLICATE_CHECKING {
            1
        } else {
            0
        }
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
        mut minimizers_callback: impl FnMut((u64, X), usize, bool),
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

            let mut last_out = u64::MAX;

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

                minimizers_callback(
                    (first_minimum.0, first_minimum.1),
                    offset,
                    ENABLE_DUPLICATE_CHECKING && offset > 0 && last_out == new_item.0,
                );
                if ENABLE_DUPLICATE_CHECKING {
                    last_out = (*self.backward.get_unchecked_mut(0)).0;
                }
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
                    let window_duplicate = ENABLE_DUPLICATE_CHECKING && last_out == last_forward.0;
                    if ENABLE_DUPLICATE_CHECKING {
                        last_out = current_backward.0;
                    }

                    if ENABLE_DUPLICATE_CHECKING && is_duplicated {
                        cold();
                        current_minimum.0 &= Self::hash_mask::<ENABLE_DUPLICATE_CHECKING>();
                        // If current minimum is duplicated, then clear the uniqueness flag also for the last out
                        last_out &= Self::hash_mask::<ENABLE_DUPLICATE_CHECKING>();
                    }

                    minimizers_callback(
                        (current_minimum.0, current_minimum.1),
                        offset + i,
                        window_duplicate,
                    );

                    *current_backward = new_item;
                }
                offset += remaining;
                minimizers_flush(offset == size);
            }
        }
    }

    #[inline(always)]
    pub fn get_minimizer_splits<
        I: ExactSizeIterator<Item = (u64, X)>,
        const ENABLE_DUPLICATE_CHECKING: bool,
    >(
        &mut self,
        mut iter: I,
        skip_beginning_count: usize,
        skip_ending_count: usize,
        mut splits_callback: impl FnMut(usize, (u64, X), bool, bool),
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

        // Struct to pass data to the set_duplicates function, used to avoid passing many parameters and bloat calling code
        struct DuplicatesData<X> {
            window_duplicates: *mut bool,
            splits_start: *mut ((u64, X), ExtraInfo),
        }

        let dupl_data = DuplicatesData {
            window_duplicates: self.duplicates.as_mut_ptr(),
            splits_start,
        };

        // This function is separated because it is called very rarely and inlining it slows down the hot path
        #[inline(never)]
        fn set_window_duplicates<X>(
            data: &DuplicatesData<X>,
            splits_ptr: *mut ((u64, X), ExtraInfo),
            is_different: bool,
        ) {
            unsafe {
                let index = splits_ptr.offset_from(data.splits_start) as usize;
                *data.window_duplicates.add(index + (is_different as usize)) = true;
            }
        }

        let duplicates_ptr = self.duplicates.as_mut_ptr();

        self.get_minimizers::<_, { ENABLE_DUPLICATE_CHECKING }>(
            iter,
            skip_ending_count,
            #[inline(always)]
            |m, index, is_dupl| {
                let is_different = !is_first && last_value.get().0 != m.0;

                if is_dupl {
                    set_window_duplicates(&dupl_data, splits_ptr.get(), is_different);
                }

                unsafe {
                    let splits_pos = &mut *splits_ptr.get();
                    splits_pos.0 = last_value.get();
                    splits_pos.1.position = index + skip_beginning_count;
                    splits_ptr.set(splits_ptr.get().add(is_different as usize));
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
                let mut diff_cursor = duplicates_ptr;
                while cursor != splits_ptr.get() {
                    let last = cursor.add(1) == splits_ptr.get();
                    splits_callback(
                        (*cursor).1.position,
                        (*cursor).0,
                        last && is_last,
                        *diff_cursor,
                    );
                    *diff_cursor = false;
                    cursor = cursor.add(1);
                    diff_cursor = diff_cursor.add(1);
                }

                splits_ptr.set(splits_start);
            },
        );
    }
}
