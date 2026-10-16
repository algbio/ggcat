//! A SIMD [`BatchMinQueue`]: the minimizer of every window of every lane's m-mers, all lanes at
//! once, in the vectors of a [`HashKernel`], from the hashes of [`CanonicalNtHashLanes`] in the
//! loop that rolls them. Each lane is one sequence: its minimizers and its splits are those of a
//! [`BatchMinQueue`] over that lane's m-mers, with their canonical hashes as the values, as
//! [`NtHashLanes::canonical`](super::nthash::NtHashLanes::canonical) gives them, and
//! [`MinimizerLanes::extra`] as their `X`.
//!
//! [`BatchMinQueue`]: crate::rolling::batch_minqueue::BatchMinQueue

use super::nthash::{CanonicalNtHashLanes, roll};
use super::tasks::{LanesConsumer, LanesTask};
use super::{BaseLanes, HashKernel, HashVector, SIMD_LANES};

/// The minimum of one window of every lane.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MinimizerLanes {
    /// The canonical hash of the m-mer, its unique flag in bit 0. With duplicate checking, the
    /// flag is also cleared where the window holds that hash more than once.
    pub value: [u32; SIMD_LANES],
    /// The m-mer's index in its lane, shifted left by one, with bit 0 set where its forward
    /// strand is the canonical one: the index is kept modulo 2^31.
    pub extra: [u32; SIMD_LANES],
}

/// The lanes' bits in a mask.
const LANES_MASK: u32 = (1 << SIMD_LANES) - 1;

/// The minimizers of windows of `size` m-mers, in every lane: what
/// [`BatchMinQueue`](crate::rolling::batch_minqueue::BatchMinQueue) gives for each lane, by its
/// algorithm, in vectors, a batch of `size - 1` m-mers at a time. It keeps its buffers from one
/// set of lanes to the next.
pub struct BatchMinQueueLanes {
    size: usize,
    /// The canonical hashes and the extras of two batches: the one before, and the one coming in.
    values: [Batches; 2],
    extras: [Batches; 2],
}

/// A batch's values or extras, a vector per position.
type Batches = Vec<[u32; SIMD_LANES]>;

impl BatchMinQueueLanes {
    /// For windows of `size` m-mers, two at least.
    pub fn new(size: usize) -> Self {
        assert!(size >= 2, "windows of {size} m-mers");
        // A batch, and a slot for the positions after it that the hashing loop hands out.
        let batch = || vec![[0; SIMD_LANES]; size];
        Self {
            size,
            values: [batch(), batch()],
            extras: [batch(), batch()],
        }
    }

    /// Calls `minimizers_callback` with the minimum of every window of every lane, but for the
    /// last `skip_ending_count` m-mers, with the window's index, and `minimizers_flush` after
    /// each batch of windows, with whether it is the last: as [`BatchMinQueue`]'s
    /// `get_minimizers` does for each lane. On the vectors of `kernel`, which this CPU has to
    /// support.
    ///
    /// [`BatchMinQueue`]: crate::rolling::batch_minqueue::BatchMinQueue
    pub fn get_minimizers<
        S: BaseLanes<SIMD_LANES> + Copy,
        const ENABLE_DUPLICATE_CHECKING: bool,
    >(
        &mut self,
        hashes: CanonicalNtHashLanes<S, SIMD_LANES>,
        kernel: HashKernel,
        skip_ending_count: usize,
        minimizers_callback: impl FnMut(usize, &MinimizerLanes),
        minimizers_flush: impl FnMut(bool),
    ) {
        let Self {
            size,
            values,
            extras,
        } = self;
        let mut output = Minimizers {
            callback: minimizers_callback,
            flush: minimizers_flush,
        };
        let task = Queue::<_, ENABLE_DUPLICATE_CHECKING>::new(
            (*size, values, extras),
            hashes.len(),
            0,
            skip_ending_count,
            &mut output,
        );
        hashes.run(kernel, task);
    }

    /// Splits every lane, but for its first `skip_beginning_count` and last `skip_ending_count`
    /// m-mers, where the minimizer of its windows changes: `splits_callback` gets each part's
    /// lane, its end, its minimizer's value and extra, and whether it is the lane's last, as
    /// [`BatchMinQueue`]'s `get_minimizer_splits` gives them for each lane. On the vectors of
    /// `kernel`, which this CPU has to support.
    ///
    /// [`BatchMinQueue`]: crate::rolling::batch_minqueue::BatchMinQueue
    pub fn get_minimizer_splits<
        S: BaseLanes<SIMD_LANES> + Copy,
        const ENABLE_DUPLICATE_CHECKING: bool,
    >(
        &mut self,
        hashes: CanonicalNtHashLanes<S, SIMD_LANES>,
        kernel: HashKernel,
        skip_beginning_count: usize,
        skip_ending_count: usize,
        splits_callback: impl FnMut(usize, usize, (u32, u32), bool),
    ) {
        let Self {
            size,
            values,
            extras,
        } = self;
        let mut output = Splits {
            callback: splits_callback,
            skip_beginning_count,
            // As the scalar queue's: a lane's m-mers, the skipped ones too, less a window.
            last_index: hashes.len().wrapping_sub(*size),
            splitting: 0,
        };
        let task = Queue::<_, ENABLE_DUPLICATE_CHECKING>::new(
            (*size, values, extras),
            hashes.len(),
            skip_beginning_count,
            skip_ending_count,
            &mut output,
        );
        hashes.run(kernel, task);
    }
}

/// What the queue hands its windows to, in the vectors `V` of its kernel.
trait WindowsOutput {
    /// The minimum of window `index` of every lane, value and extra, and that of the window
    /// before it, any before the first window.
    ///
    /// # Safety
    ///
    /// As [`HashVector`]'s.
    unsafe fn window<V: HashVector<SIMD_LANES>>(
        &mut self,
        index: usize,
        minimum: (V, V),
        before: (V, V),
    );

    /// After each batch of windows, with whether it was the last, and its last window's minimum.
    ///
    /// # Safety
    ///
    /// As [`HashVector`]'s.
    unsafe fn flush<V: HashVector<SIMD_LANES>>(&mut self, is_last: bool, last: (V, V));
}

/// Every window's minimum, to callbacks: what `get_minimizers` hands out.
struct Minimizers<F, G> {
    callback: F,
    flush: G,
}

impl<F: FnMut(usize, &MinimizerLanes), G: FnMut(bool)> WindowsOutput for Minimizers<F, G> {
    #[inline(always)]
    unsafe fn window<V: HashVector<SIMD_LANES>>(
        &mut self,
        index: usize,
        (value, extra): (V, V),
        _before: (V, V),
    ) {
        let minimum = unsafe {
            MinimizerLanes {
                value: value.to_array(),
                extra: extra.to_array(),
            }
        };
        (self.callback)(index, &minimum);
    }

    #[inline(always)]
    unsafe fn flush<V: HashVector<SIMD_LANES>>(&mut self, is_last: bool, _last: (V, V)) {
        (self.flush)(is_last);
    }
}

/// The parts of each lane, a part ending where its windows' minimum changes: what
/// `get_minimizer_splits` hands out. A window's minima stay in vectors; only those of the lanes
/// whose part ends are taken out, for the callback.
struct Splits<F> {
    callback: F,
    skip_beginning_count: usize,
    /// Where the last part of a lane ends.
    last_index: usize,
    /// The lanes whose part a change of minimum ends: none at the first window, before which
    /// no part ends.
    splitting: u32,
}

impl<F: FnMut(usize, usize, (u32, u32), bool)> WindowsOutput for Splits<F> {
    #[inline(always)]
    unsafe fn window<V: HashVector<SIMD_LANES>>(
        &mut self,
        index: usize,
        (value, extra): (V, V),
        (before_value, before_extra): (V, V),
    ) {
        let same = unsafe {
            V::mask_bits(value.equal(before_value)) & V::mask_bits(extra.equal(before_extra))
        };
        let mut changed = !same & self.splitting;
        self.splitting = LANES_MASK;
        if changed != 0 {
            // The parts of the lanes whose minimum changes end before this window, with the
            // window before's minimizer.
            let (values, extras) = unsafe { (before_value.to_array(), before_extra.to_array()) };
            while changed != 0 {
                let lane = changed.trailing_zeros() as usize;
                let minimizer = (values[lane], extras[lane]);
                (self.callback)(lane, index + self.skip_beginning_count, minimizer, false);
                changed &= changed - 1;
            }
        }
    }

    #[inline(always)]
    unsafe fn flush<V: HashVector<SIMD_LANES>>(&mut self, is_last: bool, (value, extra): (V, V)) {
        if is_last {
            let (values, extras) = unsafe { (value.to_array(), extra.to_array()) };
            for lane in 0..SIMD_LANES {
                let minimizer = (values[lane], extras[lane]);
                (self.callback)(lane, self.last_index, minimizer, true);
            }
        }
    }
}

/// The queue to run on the hashes, the positions from `begin` to `end` in, for `output`.
struct Queue<'a, O, const DUPLICATES: bool> {
    size: usize,
    values: &'a mut [Batches; 2],
    extras: &'a mut [Batches; 2],
    begin: usize,
    end: usize,
    output: &'a mut O,
}

impl<'a, O: WindowsOutput, const DUPLICATES: bool> Queue<'a, O, DUPLICATES> {
    /// With a queue's size and batches, for `count` positions, but the first
    /// `skip_beginning_count` and the last `skip_ending_count`.
    fn new(
        (size, values, extras): (usize, &'a mut [Batches; 2], &'a mut [Batches; 2]),
        count: usize,
        skip_beginning_count: usize,
        skip_ending_count: usize,
        output: &'a mut O,
    ) -> Self {
        let end = count.saturating_sub(skip_ending_count);
        Self {
            size,
            values,
            extras,
            begin: skip_beginning_count.min(end),
            end,
            output,
        }
    }
}

impl<O: WindowsOutput, const DUPLICATES: bool> LanesTask<SIMD_LANES> for Queue<'_, O, DUPLICATES> {
    #[inline(always)]
    unsafe fn run<S: BaseLanes<SIMD_LANES>, V: HashVector<SIMD_LANES>>(self, seq: S, m: usize) {
        let [previous_values, current_values] = self.values;
        let [previous_extras, current_extras] = self.extras;
        let mut consumer = QueueConsumer::<V, O, DUPLICATES> {
            previous: (
                previous_values.as_mut_slice(),
                previous_extras.as_mut_slice(),
            ),
            current: (current_values.as_mut_slice(), current_extras.as_mut_slice()),
            batch: self.size - 1,
            filled: 0,
            step: 0,
            begin: self.begin,
            end: self.end,
            started: false,
            windows: 0,
            // SAFETY: the caller's.
            last: unsafe { (V::splat(0), V::splat(0)) },
            output: self.output,
        };
        unsafe { roll::<S, V, _, SIMD_LANES>(seq, m, &mut consumer) }
    }
}

/// The values and extras of a batch.
type Batch<'a> = (&'a mut [[u32; SIMD_LANES]], &'a mut [[u32; SIMD_LANES]]);

/// The queue as the hashing loop feeds it, on the vectors `V`. Each position's canonical hash
/// and extra go to the batch coming in; once full, with the batch before, it gives the minima of
/// the windows its m-mers end, as the scalar queue computes them, and becomes the batch before.
struct QueueConsumer<'a, V, O, const DUPLICATES: bool> {
    /// The batch before: its m-mers as they came, then the minima from each to its end.
    previous: Batch<'a>,
    /// The batch coming in, `filled` m-mers so far.
    current: Batch<'a>,
    /// The m-mers of a batch: a window's, less one.
    batch: usize,
    filled: usize,
    /// One while the positions handed out are queued, zero while they are skipped: their
    /// hashes then go to the same slot, the next to fill.
    step: usize,
    /// The positions queued, from `begin` to `end`.
    begin: usize,
    end: usize,
    /// Whether the first batch is in. It only fills the queue: no window ends in it.
    started: bool,
    /// The windows handed out so far.
    windows: usize,
    /// The last window's minimum, value and extra.
    last: (V, V),
    output: &'a mut O,
}

impl<V: HashVector<SIMD_LANES>, O: WindowsOutput, const DUPLICATES: bool>
    QueueConsumer<'_, V, O, DUPLICATES>
{
    /// # Safety
    ///
    /// As [`HashVector`]'s; `index` is below the batches' length.
    #[inline(always)]
    unsafe fn previous_at(&self, index: usize) -> (V, V) {
        unsafe {
            (
                V::from_array(*self.previous.0.get_unchecked(index)),
                V::from_array(*self.previous.1.get_unchecked(index)),
            )
        }
    }

    /// # Safety
    ///
    /// As [`Self::previous_at`].
    #[inline(always)]
    unsafe fn current_at(&self, index: usize) -> (V, V) {
        unsafe {
            (
                V::from_array(*self.current.0.get_unchecked(index)),
                V::from_array(*self.current.1.get_unchecked(index)),
            )
        }
    }

    /// The windows the m-mers of the current batch end, `filled` of them, with their minima:
    /// the scalar queue's loop over a batch, its branches on duplicates turned to masks.
    ///
    /// # Safety
    ///
    /// As [`HashVector`]'s; the batch before is in.
    #[inline(always)]
    unsafe fn windows_of(&mut self, is_last: bool) {
        unsafe {
            // The minima of the batch before, from each of its m-mers to its end. Each comes from
            // the one after, kept in registers: stored, for the windows, and not read back.
            let (mut next_value, mut next_extra) = self.previous_at(self.batch - 1);
            for index in (0..self.batch - 1).rev() {
                let (mut value, extra) = self.previous_at(index);
                if DUPLICATES {
                    value = value.clear_flag(value.equal(next_value));
                }
                let next = next_value.less(value);
                next_value = V::select(next, next_value, value);
                next_extra = V::select(next, next_extra, extra);
                *self.previous.0.get_unchecked_mut(index) = next_value.to_array();
                *self.previous.1.get_unchecked_mut(index) = next_extra.to_array();
            }
            // A window's minimum: of the batch before, from the window's first m-mer, and of the
            // batch coming in, up to its last.
            let (value, extra) = self.current_at(0);
            let (before_value, before_extra) = self.previous_at(0);
            let before = before_value.less(value);
            let mut minimum = V::select(before, before_value, value);
            if DUPLICATES {
                minimum = minimum.clear_flag(value.equal(before_value));
            }
            self.window(minimum, V::select(before, before_extra, extra));
            let (mut coming_value, mut coming_extra) = (value, extra);
            for index in 1..self.filled {
                let (value, extra) = self.current_at(index);
                let (before_value, before_extra) = self.previous_at(index);
                if DUPLICATES {
                    coming_value = coming_value.clear_flag(value.equal(coming_value));
                }
                let new = value.less(coming_value);
                coming_value = V::select(new, value, coming_value);
                coming_extra = V::select(new, extra, coming_extra);
                let before = before_value.less(coming_value);
                let mut minimum = V::select(before, before_value, coming_value);
                if DUPLICATES {
                    minimum = minimum.clear_flag(coming_value.equal(before_value));
                }
                self.window(minimum, V::select(before, before_extra, coming_extra));
            }
            self.output.flush(is_last, self.last);
        }
    }

    /// Hands out the next window's minimum.
    ///
    /// # Safety
    ///
    /// As [`HashVector`]'s.
    #[inline(always)]
    unsafe fn window(&mut self, value: V, extra: V) {
        let before = std::mem::replace(&mut self.last, (value, extra));
        unsafe { self.output.window(self.windows, (value, extra), before) };
        self.windows += 1;
    }
}

impl<V: HashVector<SIMD_LANES>, O: WindowsOutput, const DUPLICATES: bool>
    LanesConsumer<V, SIMD_LANES> for QueueConsumer<'_, V, O, DUPLICATES>
{
    #[inline(always)]
    unsafe fn prepare(&mut self, index: usize, available: usize) -> usize {
        if index < self.begin {
            self.step = 0;
            return available.min(self.begin - index);
        }
        if index >= self.end {
            self.step = 0;
            return available;
        }
        self.step = 1;
        if self.filled == self.batch {
            // More m-mers are to come, so not the last batch.
            if self.started {
                // SAFETY: the caller's; the batch before is in.
                unsafe { self.windows_of(false) };
            }
            self.started = true;
            std::mem::swap(&mut self.previous, &mut self.current);
            self.filled = 0;
        }
        available
            .min(self.batch - self.filled)
            .min(self.end - index)
    }

    #[inline(always)]
    unsafe fn consume(&mut self, index: usize, forward: V, reverse: V) {
        unsafe {
            let value = V::canonical(forward, reverse);
            let base = (index as u32) << 1;
            let extra = V::select(forward.less(reverse), V::splat(base | 1), V::splat(base));
            // SAFETY: `filled` is at most a batch, and the buffers have a slot more.
            *self.current.0.get_unchecked_mut(self.filled) = value.to_array();
            *self.current.1.get_unchecked_mut(self.filled) = extra.to_array();
        }
        self.filled += self.step;
    }

    #[inline(always)]
    unsafe fn finish(&mut self) {
        // With no batch before, the queue never filled: no windows, as for the scalar queue.
        if self.started && self.filled > 0 {
            // SAFETY: the caller's.
            unsafe { self.windows_of(true) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rolling::batch_minqueue::BatchMinQueue;
    use crate::simd::PackedLanes;

    /// `count` base codes, from `seed`: random stretches and periodic ones, of periods 1 to 4,
    /// so that windows hold the same m-mer more than once.
    fn codes(count: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut codes = Vec::with_capacity(count);
        while codes.len() < count {
            let stretch = (next() % 40 + 1) as usize;
            if next() % 2 == 0 {
                codes.extend((0..stretch).map(|_| (next() % 4) as u8));
            } else {
                let period: Vec<u8> = (0..next() % 4 + 1).map(|_| (next() % 4) as u8).collect();
                codes.extend((0..stretch).map(|index| period[index % period.len()]));
            }
        }
        codes.truncate(count);
        codes
    }

    /// `codes` packed four to a byte, from base `first` of the first byte.
    fn packed(codes: &[u8], first: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; (first + codes.len()).div_ceil(4).max(1)];
        for (index, &code) in codes.iter().enumerate() {
            let base = first + index;
            bytes[base / 4] |= code << (2 * (base % 4));
        }
        bytes
    }

    /// Each lane's m-mers as the scalar queue is to take them: their canonical hashes and
    /// extras, as the plain-arrays hash hands them out.
    fn lane_items(lanes: PackedLanes<SIMD_LANES>, m: usize) -> Vec<Vec<(u64, u32)>> {
        let mut items = vec![Vec::new(); SIMD_LANES];
        CanonicalNtHashLanes::new(lanes, m).unwrap().for_each(
            HashKernel::Portable,
            |index, hash| {
                for lane in 0..SIMD_LANES {
                    let extra = (index as u32) << 1 | (hash.forward >> lane & 1);
                    items[lane].push((hash.canonical[lane] as u64, extra));
                }
            },
        );
        items
    }

    /// Every case: the lanes of inputs of `count` bases from base `first`, overlapping by
    /// `overlap`, with m-mers of `m` bases, in windows of `size`.
    fn for_every_case(mut check: impl FnMut(PackedLanes<SIMD_LANES>, usize, usize, String)) {
        for m in [1, 2, 5, 12] {
            for size in [2, 3, 4, 19, 33] {
                for count in [m, m + 5, 100, 700, 3001] {
                    for first in [0, 3] {
                        for overlap in [0, m - 1 + size] {
                            let codes = codes(count, (m * 7919 + size * 31 + count + first) as u64);
                            let bytes = packed(&codes, first);
                            let lanes =
                                PackedLanes::<SIMD_LANES>::new(&bytes, first, count, overlap);
                            if lanes.lane_bases() < m {
                                continue;
                            }
                            let case = format!(
                                "m {m}, size {size}, count {count}, first {first}, {overlap} over"
                            );
                            check(lanes, m, size, case);
                        }
                    }
                }
            }
        }
    }

    fn minimizers_match<const DUP: bool>(
        lanes: PackedLanes<SIMD_LANES>,
        m: usize,
        size: usize,
        case: &str,
    ) {
        let items = lane_items(lanes, m);
        for skip_end in [0, 1, 7] {
            if items[0].len() < skip_end {
                continue;
            }
            let mut expected = vec![Vec::new(); SIMD_LANES];
            let mut expected_flushes = Vec::new();
            for lane in 0..SIMD_LANES {
                let mut flushes = Vec::new();
                BatchMinQueue::<u32>::new(size).get_minimizers::<_, DUP>(
                    items[lane].iter().copied(),
                    skip_end,
                    |(value, extra), index| expected[lane].push((index, value as u32, extra)),
                    |is_last| flushes.push(is_last),
                );
                expected_flushes = flushes;
            }
            for kernel in HashKernel::supported() {
                let (mut found, mut flushes) = (vec![Vec::new(); SIMD_LANES], Vec::new());
                BatchMinQueueLanes::new(size).get_minimizers::<_, DUP>(
                    CanonicalNtHashLanes::new(lanes, m).unwrap(),
                    kernel,
                    skip_end,
                    |index, minimum| {
                        for lane in 0..SIMD_LANES {
                            found[lane].push((index, minimum.value[lane], minimum.extra[lane]));
                        }
                    },
                    |is_last| flushes.push(is_last),
                );
                for lane in 0..SIMD_LANES {
                    assert_eq!(
                        found[lane], expected[lane],
                        "{kernel:?}, duplicates {DUP}, {case}, skip {skip_end}, lane {lane}"
                    );
                }
                assert_eq!(
                    flushes, expected_flushes,
                    "{kernel:?}, {case}, skip {skip_end}"
                );
            }
        }
    }

    #[test]
    fn every_lane_gets_the_minimizers_of_the_scalar_queue() {
        for_every_case(|lanes, m, size, case| {
            minimizers_match::<false>(lanes, m, size, &case);
            minimizers_match::<true>(lanes, m, size, &case);
        });
    }

    fn splits_match<const DUP: bool>(
        lanes: PackedLanes<SIMD_LANES>,
        m: usize,
        size: usize,
        case: &str,
    ) {
        let items = lane_items(lanes, m);
        for (skip_begin, skip_end) in [(0, 0), (1, 0), (0, 1), (1, 1), (3, 2)] {
            let mut expected = vec![Vec::new(); SIMD_LANES];
            // The scalar queue's end of the last part needs a window of m-mers, at least.
            if items[0].len() >= size.max(skip_begin + skip_end) {
                for lane in 0..SIMD_LANES {
                    BatchMinQueue::<u32>::new(size).get_minimizer_splits::<_, DUP>(
                        items[lane].iter().copied(),
                        skip_begin,
                        skip_end,
                        |index, (value, extra), is_last| {
                            expected[lane].push((index, (value as u32, extra), is_last))
                        },
                    );
                }
            }
            for kernel in HashKernel::supported() {
                let mut found = vec![Vec::new(); SIMD_LANES];
                BatchMinQueueLanes::new(size).get_minimizer_splits::<_, DUP>(
                    CanonicalNtHashLanes::new(lanes, m).unwrap(),
                    kernel,
                    skip_begin,
                    skip_end,
                    |lane, index, minimizer, is_last| found[lane].push((index, minimizer, is_last)),
                );
                for lane in 0..SIMD_LANES {
                    assert_eq!(
                        found[lane], expected[lane],
                        "{kernel:?}, dup {DUP}, {case}, skips {skip_begin} {skip_end}, lane {lane}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_lane_is_split_as_the_scalar_queue_splits_it() {
        for_every_case(|lanes, m, size, case| {
            splits_match::<false>(lanes, m, size, &case);
            splits_match::<true>(lanes, m, size, &case);
        });
    }

    #[test]
    fn a_queue_is_reused_from_one_set_of_lanes_to_the_next() {
        let mut queue = BatchMinQueueLanes::new(19);
        for seed in 1..6 {
            let codes = codes(2000 + 37 * seed as usize, seed);
            let bytes = packed(&codes, 0);
            let lanes = PackedLanes::<SIMD_LANES>::new(&bytes, 0, codes.len(), 30);
            let mut reused = Vec::new();
            queue.get_minimizer_splits::<_, true>(
                CanonicalNtHashLanes::new(lanes, 12).unwrap(),
                HashKernel::detect(),
                1,
                1,
                |lane, index, minimizer, is_last| reused.push((lane, index, minimizer, is_last)),
            );
            let mut fresh = Vec::new();
            BatchMinQueueLanes::new(19).get_minimizer_splits::<_, true>(
                CanonicalNtHashLanes::new(lanes, 12).unwrap(),
                HashKernel::detect(),
                1,
                1,
                |lane, index, minimizer, is_last| fresh.push((lane, index, minimizer, is_last)),
            );
            assert_eq!(reused, fresh, "seed {seed}");
        }
    }
}
