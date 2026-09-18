//! SIMD-accelerated batched minimizer computation.

use std::fmt::Debug;
use wide::{i32x8, u32x8};

use crate::hashing::SIMD_LANES;

pub trait SimdMinQueueExtra: Clone + Copy + Default + Debug + PartialEq {
    type Vector: Clone + Copy + Default;

    fn load(values: [Self; SIMD_LANES]) -> Self::Vector;
    fn store(values: Self::Vector) -> [Self; SIMD_LANES];
    fn lane(values: Self::Vector, lane: usize) -> Self;
    fn blend(mask: u32x8, first: Self::Vector, second: Self::Vector) -> Self::Vector;
    fn eq_mask(first: Self::Vector, second: Self::Vector) -> u32x8;
}

impl SimdMinQueueExtra for () {
    type Vector = ();
    #[inline(always)]
    fn load(_: [(); SIMD_LANES]) {}
    #[inline(always)]
    fn store(_: ()) -> [(); SIMD_LANES] {
        [(); SIMD_LANES]
    }
    #[inline(always)]
    fn lane(_: (), _: usize) {}
    #[inline(always)]
    fn blend(_: u32x8, _: (), _: ()) {}
    #[inline(always)]
    fn eq_mask(_: (), _: ()) -> u32x8 {
        u32x8::splat(u32::MAX)
    }
}

impl SimdMinQueueExtra for u32 {
    type Vector = u32x8;
    #[inline(always)]
    fn load(values: [u32; SIMD_LANES]) -> u32x8 {
        u32x8::new(values)
    }
    #[inline(always)]
    fn store(values: u32x8) -> [u32; SIMD_LANES] {
        values.to_array()
    }
    #[inline(always)]
    fn lane(values: u32x8, lane: usize) -> u32 {
        values.to_array()[lane]
    }
    #[inline(always)]
    fn blend(mask: u32x8, first: u32x8, second: u32x8) -> u32x8 {
        mask.blend(first, second)
    }
    #[inline(always)]
    fn eq_mask(first: u32x8, second: u32x8) -> u32x8 {
        first.cmp_eq(second)
    }
}

#[derive(Copy, Clone)]
struct SplitEvent<V> {
    hashes: u32x8,
    extra: V,
    position: usize,
    changed_lanes: u8,
    finished_lanes: u8,
    /// Lanes whose next run begins at `position`, after invalid windows.
    started_lanes: u8,
}

/// One maximal stretch of windows of a single lane sharing the same minimizer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LaneRun<X> {
    pub lane: usize,
    /// First window of the run.
    pub start: usize,
    /// One past the last window of the run.
    pub end: usize,
    pub hash: u32,
    pub extra: X,
    /// The run ended because the lane left valid data, not because the
    /// minimizer changed.
    pub finished: bool,
}

trait MinimizerSink<V: Copy> {
    fn minimizer(&mut self, hashes: u32x8, extra: V, index: usize);
    fn flush(&mut self, last: bool);
}

struct CallbackSink<F, G> {
    callback: F,
    flush: G,
}
impl<V: Copy, F: FnMut((u32x8, V), usize), G: FnMut(bool)> MinimizerSink<V> for CallbackSink<F, G> {
    #[inline(always)]
    fn minimizer(&mut self, h: u32x8, x: V, i: usize) {
        (self.callback)((h, x), i)
    }
    #[inline(always)]
    fn flush(&mut self, last: bool) {
        (self.flush)(last)
    }
}

#[inline(always)]
fn lane_mask(mask: u32x8) -> u8 {
    // Comparison results are either zero or all ones, so the sign-bit mask is
    // exactly the set of changed lanes.
    let signed: i32x8 = unsafe { std::mem::transmute(mask) };
    signed.move_mask() as u8
}

pub struct SimdBatchMinQueue<X: SimdMinQueueExtra> {
    size: usize,
    backward: Vec<(u32x8, X::Vector)>,
    events: Vec<SplitEvent<X::Vector>>,
}

impl<X: SimdMinQueueExtra> SimdBatchMinQueue<X> {
    pub fn new(size: usize) -> Self {
        assert!(size >= 2, "SIMD minimizer window must be at least two");
        Self {
            size,
            backward: vec![(u32x8::splat(0), X::Vector::default()); size - 1],
            events: Vec::with_capacity(size + 2),
        }
    }

    #[inline(always)]
    fn min_item(first: (u32x8, X::Vector), second: (u32x8, X::Vector)) -> (u32x8, X::Vector) {
        let take_first = !second.0.cmp_lt(first.0);
        (
            first.0.min(second.0),
            X::blend(take_first, first.1, second.1),
        )
    }

    #[inline(always)]
    fn process<I, S, const DUP: bool>(
        backward: &mut [(u32x8, X::Vector)],
        mut iter: I,
        skip_end: usize,
        sink: &mut S,
    ) where
        I: ExactSizeIterator<Item = (u32x8, X::Vector)>,
        S: MinimizerSink<X::Vector>,
    {
        let mut size = iter.len().saturating_sub(skip_end);
        if size < backward.len() {
            return;
        }
        for slot in backward.iter_mut() {
            *slot = iter.next().unwrap();
        }
        size -= backward.len();
        let one = u32x8::splat(1);
        let mut offset = 0;
        while offset < size {
            let remaining = (size - offset).min(backward.len());
            for i in (0..backward.len() - 1).rev() {
                let mut current = backward[i];
                let next = backward[i + 1];
                if DUP {
                    current.0 &= !(current.0.cmp_eq(next.0) & one);
                }
                backward[i] = Self::min_item(current, next);
            }
            let new_item = iter.next().unwrap();
            let back = backward[0];
            let mut minimum = Self::min_item(new_item, back);
            if DUP {
                minimum.0 &= !(new_item.0.cmp_eq(back.0) & one);
            }
            sink.minimizer(minimum.0, minimum.1, offset);
            let mut forward = new_item;
            backward[0] = new_item;
            for i in 1..remaining {
                let new_item = iter.next().unwrap();
                if DUP {
                    forward.0 &= !(new_item.0.cmp_eq(forward.0) & one);
                }
                forward = Self::min_item(forward, new_item);
                let back = backward[i];
                let mut minimum = Self::min_item(forward, back);
                if DUP {
                    minimum.0 &= !(forward.0.cmp_eq(back.0) & one);
                }
                sink.minimizer(minimum.0, minimum.1, offset + i);
                backward[i] = new_item;
            }
            offset += remaining;
            sink.flush(offset == size);
        }
    }

    pub fn get_minimizers<
        I: ExactSizeIterator<Item = (u32x8, [X; SIMD_LANES])>,
        const DUP: bool,
        const ACTIVE: usize,
    >(
        &mut self,
        iter: I,
        skip_end: usize,
        mut cb: impl FnMut(usize, (u32, X), usize),
        mut flush: impl FnMut(usize, bool),
    ) {
        assert!(ACTIVE > 0 && ACTIVE <= SIMD_LANES);
        self.get_minimizers_simd::<_, DUP>(
            iter,
            skip_end,
            |(h, x), i| {
                let hashes = h.to_array();
                for lane in 0..ACTIVE {
                    cb(lane, (hashes[lane], x[lane]), i);
                }
            },
            |last| {
                for lane in 0..ACTIVE {
                    flush(lane, last)
                }
            },
        );
    }

    pub fn get_minimizers_simd<
        I: ExactSizeIterator<Item = (u32x8, [X; SIMD_LANES])>,
        const DUP: bool,
    >(
        &mut self,
        iter: I,
        skip_end: usize,
        mut cb: impl FnMut((u32x8, [X; SIMD_LANES]), usize),
        flush: impl FnMut(bool),
    ) {
        let mapped = iter.map(|(h, x)| (h, X::load(x)));
        let mut sink = CallbackSink {
            callback: |(h, x), i| cb((h, X::store(x)), i),
            flush,
        };
        Self::process::<_, _, DUP>(&mut self.backward, mapped, skip_end, &mut sink);
    }

    fn splits_core<
        I: ExactSizeIterator<Item = (u32x8, X::Vector)>,
        const DUP: bool,
        const ACTIVE: usize,
    >(
        &mut self,
        iter: I,
        mut cb: impl FnMut(usize, usize, (u32, X), bool),
    ) {
        assert!(ACTIVE > 0 && ACTIVE <= SIMD_LANES);
        let last_index = iter.len().saturating_sub(self.size);
        self.events.clear();
        struct SplitSink<'a, X: SimdMinQueueExtra, F, const ACTIVE: usize, const DUP: bool> {
            events: &'a mut Vec<SplitEvent<X::Vector>>,
            callback: &'a mut F,
            last_index: usize,
            first: bool,
            last_h: u32x8,
            last_x: X::Vector,
            marker: std::marker::PhantomData<X>,
        }
        impl<
            X: SimdMinQueueExtra,
            F: FnMut(usize, usize, (u32, X), bool),
            const ACTIVE: usize,
            const DUP: bool,
        > MinimizerSink<X::Vector> for SplitSink<'_, X, F, ACTIVE, DUP>
        {
            #[inline(always)]
            fn minimizer(&mut self, h: u32x8, x: X::Vector, index: usize) {
                if !self.first {
                    // Without duplicate separation no bit is reserved, so a
                    // change of metadata always splits, as in the scalar queue.
                    let unique = if DUP {
                        (h & u32x8::splat(1)).cmp_eq(u32x8::splat(1))
                    } else {
                        u32x8::splat(u32::MAX)
                    };
                    let changed = !self.last_h.cmp_eq(h) | (!X::eq_mask(self.last_x, x) & unique);
                    let bits = lane_mask(changed);
                    if bits != 0 {
                        self.events.push(SplitEvent {
                            hashes: self.last_h,
                            extra: self.last_x,
                            position: index,
                            changed_lanes: bits,
                            finished_lanes: 0,
                            started_lanes: 0,
                        });
                    }
                }
                self.last_h = h;
                self.last_x = x;
                self.first = false;
            }
            #[inline(always)]
            fn flush(&mut self, last: bool) {
                for lane in 0..ACTIVE {
                    for event in self.events.iter() {
                        if event.changed_lanes & (1 << lane) != 0 {
                            (self.callback)(
                                lane,
                                event.position,
                                (event.hashes.to_array()[lane], X::lane(event.extra, lane)),
                                false,
                            );
                        }
                    }
                    if last {
                        (self.callback)(
                            lane,
                            self.last_index,
                            (self.last_h.to_array()[lane], X::lane(self.last_x, lane)),
                            true,
                        );
                    }
                }
                self.events.clear();
            }
        }
        let mut sink: SplitSink<'_, X, _, ACTIVE, DUP> = SplitSink {
            events: &mut self.events,
            callback: &mut cb,
            last_index,
            first: true,
            last_h: u32x8::splat(0),
            last_x: X::Vector::default(),
            marker: std::marker::PhantomData,
        };
        Self::process::<_, _, DUP>(&mut self.backward, iter, 0, &mut sink);
    }

    pub fn get_minimizer_splits<
        I: ExactSizeIterator<Item = (u32x8, [X; SIMD_LANES])>,
        const DUP: bool,
        const ACTIVE: usize,
    >(
        &mut self,
        iter: I,
        cb: impl FnMut(usize, usize, (u32, X), bool),
    ) {
        self.splits_core::<_, DUP, ACTIVE>(iter.map(|(h, x)| (h, X::load(x))), cb)
    }

    /// Direct slice input avoids generic iterator/source overhead in the hot path.
    pub fn get_minimizer_splits_slice<const DUP: bool, const ACTIVE: usize>(
        &mut self,
        input: &[(u32x8, [X; SIMD_LANES])],
        cb: impl FnMut(usize, usize, (u32, X), bool),
    ) {
        self.splits_core::<_, DUP, ACTIVE>(input.iter().map(|&(h, x)| (h, X::load(x))), cb)
    }

    /// Splits every lane into runs of windows sharing one minimizer, skipping
    /// the windows the caller marked invalid.
    ///
    /// `valid_lanes` holds one byte per minimizer window, that is
    /// `iter.len() + 1 - window` bytes; bit `lane` is set when that lane's
    /// window lies entirely inside one sequence. Invalid windows end the run
    /// they follow and start a fresh one afterwards, so runs on opposite sides
    /// of a sequence boundary are never joined. Runs of one lane are reported
    /// in order; different lanes interleave.
    pub fn get_valid_minimizer_splits<
        I: ExactSizeIterator<Item = (u32x8, X::Vector)>,
        const DUP: bool,
    >(
        &mut self,
        iter: I,
        valid_lanes: &[u8],
        mut cb: impl FnMut(LaneRun<X>),
    ) {
        let windows_count = iter.len().saturating_sub(self.size - 1);
        assert_eq!(
            valid_lanes.len(),
            windows_count,
            "one validity byte per minimizer window"
        );
        self.events.clear();

        struct ValidRunSink<'a, X: SimdMinQueueExtra, F, const DUP: bool> {
            events: &'a mut Vec<SplitEvent<X::Vector>>,
            valid_lanes: &'a [u8],
            callback: &'a mut F,
            windows_count: usize,
            first: bool,
            last_h: u32x8,
            last_x: X::Vector,
            last_valid: u8,
            run_start: [usize; SIMD_LANES],
        }

        impl<X: SimdMinQueueExtra, F: FnMut(LaneRun<X>), const DUP: bool> MinimizerSink<X::Vector>
            for ValidRunSink<'_, X, F, DUP>
        {
            #[inline(always)]
            fn minimizer(&mut self, h: u32x8, x: X::Vector, index: usize) {
                let valid = self.valid_lanes[index];
                if self.first {
                    if valid != 0 {
                        self.events.push(SplitEvent {
                            hashes: h,
                            extra: x,
                            position: index,
                            changed_lanes: 0,
                            finished_lanes: 0,
                            started_lanes: valid,
                        });
                    }
                } else {
                    let unique = if DUP {
                        (h & u32x8::splat(1)).cmp_eq(u32x8::splat(1))
                    } else {
                        u32x8::splat(u32::MAX)
                    };
                    let changed =
                        lane_mask(!self.last_h.cmp_eq(h) | (!X::eq_mask(self.last_x, x) & unique));
                    let finished = self.last_valid & !valid;
                    let emit = self.last_valid & (changed | !valid);
                    let started = valid & !self.last_valid;
                    if (emit | started) != 0 {
                        self.events.push(SplitEvent {
                            hashes: self.last_h,
                            extra: self.last_x,
                            position: index,
                            changed_lanes: emit,
                            finished_lanes: finished,
                            started_lanes: started,
                        });
                    }
                }
                self.last_h = h;
                self.last_x = x;
                self.last_valid = valid;
                self.first = false;
            }

            #[inline(always)]
            fn flush(&mut self, last: bool) {
                for event in self.events.iter() {
                    if event.changed_lanes != 0 {
                        let hashes = event.hashes.to_array();
                        let extras = X::store(event.extra);
                        let mut lanes = event.changed_lanes;
                        while lanes != 0 {
                            let lane = lanes.trailing_zeros() as usize;
                            lanes &= lanes - 1;
                            (self.callback)(LaneRun {
                                lane,
                                start: self.run_start[lane],
                                end: event.position,
                                hash: hashes[lane],
                                extra: extras[lane],
                                finished: event.finished_lanes & (1 << lane) != 0,
                            });
                            self.run_start[lane] = event.position;
                        }
                    }
                    let mut lanes = event.started_lanes;
                    while lanes != 0 {
                        let lane = lanes.trailing_zeros() as usize;
                        lanes &= lanes - 1;
                        self.run_start[lane] = event.position;
                    }
                }
                if last && self.last_valid != 0 {
                    let hashes = self.last_h.to_array();
                    let extras = X::store(self.last_x);
                    let mut lanes = self.last_valid;
                    while lanes != 0 {
                        let lane = lanes.trailing_zeros() as usize;
                        lanes &= lanes - 1;
                        (self.callback)(LaneRun {
                            lane,
                            start: self.run_start[lane],
                            end: self.windows_count,
                            hash: hashes[lane],
                            extra: extras[lane],
                            finished: true,
                        });
                    }
                }
                self.events.clear();
            }
        }

        let mut sink: ValidRunSink<'_, X, _, DUP> = ValidRunSink {
            events: &mut self.events,
            valid_lanes,
            callback: &mut cb,
            windows_count,
            first: true,
            last_h: u32x8::splat(0),
            last_x: X::Vector::default(),
            last_valid: 0,
            run_start: [0; SIMD_LANES],
        };
        Self::process::<_, _, DUP>(&mut self.backward, iter, 0, &mut sink);
    }
}

impl SimdBatchMinQueue<()> {
    pub fn get_minimizer_hashes_simd<I: ExactSizeIterator<Item = u32x8>, const DUP: bool>(
        &mut self,
        iter: I,
        skip_end: usize,
        mut cb: impl FnMut(u32x8, usize),
        flush: impl FnMut(bool),
    ) {
        let mut sink = CallbackSink {
            callback: |(h, _), i| cb(h, i),
            flush,
        };
        Self::process::<_, _, DUP>(
            &mut self.backward,
            iter.map(|h| (h, ())),
            skip_end,
            &mut sink,
        );
    }

    /// Metadata-free split fast path; input and queue state remain SIMD-native.
    pub fn get_minimizer_hash_splits_slice<const DUP: bool, const ACTIVE: usize>(
        &mut self,
        input: &[u32x8],
        mut cb: impl FnMut(usize, usize, u32, bool),
    ) {
        self.splits_core::<_, DUP, ACTIVE>(
            input.iter().copied().map(|h| (h, ())),
            |lane, index, (hash, ()), last| cb(lane, index, hash, last),
        );
    }
}

#[cfg(test)]
mod simd_tests {
    use super::{SimdBatchMinQueue, SimdMinQueueExtra};
    use crate::hashing::SIMD_LANES;
    use hashes::rolling::batch_minqueue::BatchMinQueue;
    use wide::u32x8;

    fn inputs(len: usize) -> Vec<(u32x8, [u32; SIMD_LANES])> {
        (0..len)
            .map(|i| {
                let hashes = std::array::from_fn(|lane| {
                    let raw = if i % 7 == 0 {
                        10
                    } else {
                        ((i * 13 + lane * 17) % 29) as u32 * 2
                    };
                    raw | 1
                });
                (
                    u32x8::from(hashes),
                    std::array::from_fn(|lane| (i * SIMD_LANES + lane) as u32),
                )
            })
            .collect()
    }

    fn compare_minimizers<const DUP: bool, const ACTIVE: usize>(window: usize, skip_end: usize) {
        let data = inputs(79);
        let mut got = vec![Vec::new(); ACTIVE];
        SimdBatchMinQueue::new(window).get_minimizers::<_, DUP, ACTIVE>(
            data.iter().copied(),
            skip_end,
            |lane, item, index| got[lane].push((item, index)),
            |_, _| {},
        );
        for lane in 0..ACTIVE {
            let mut expected = Vec::new();
            BatchMinQueue::new(window).get_minimizers::<_, DUP>(
                data.iter()
                    .map(|(h, x)| (h.to_array()[lane] as u64, x[lane])),
                skip_end,
                |item, index| expected.push((item, index)),
                |_| {},
            );
            let expected: Vec<_> = expected
                .into_iter()
                .map(|((h, x), i)| ((h as u32, x), i))
                .collect();
            assert_eq!(got[lane], expected);
        }
    }

    #[test]
    fn simd_minimizers_match_scalar() {
        for w in [2, 3, 8, 17] {
            for s in [0, 1, 5] {
                compare_minimizers::<false, 4>(w, s);
                compare_minimizers::<true, 3>(w, s);
            }
        }
    }

    fn compare_splits<const DUP: bool, const ACTIVE: usize>(len: usize, window: usize) {
        let data = inputs(len);
        let mut got = vec![Vec::new(); ACTIVE];
        SimdBatchMinQueue::new(window)
            .get_minimizer_splits_slice::<DUP, ACTIVE>(&data, |lane, i, item, last| {
                got[lane].push((i, item, last))
            });
        for lane in 0..ACTIVE {
            let mut expected = Vec::new();
            BatchMinQueue::new(window).get_minimizer_splits::<_, DUP>(
                data.iter()
                    .map(|(h, x)| (h.to_array()[lane] as u64, x[lane])),
                0,
                0,
                |i, item, last| expected.push((i, item, last)),
            );
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(i, (h, x), last)| (i, (h as u32, x), last))
                .collect();
            assert_eq!(
                got[lane], expected,
                "lane {lane}, len {len}, window {window}, dup {DUP}"
            );
        }
    }

    #[test]
    fn simd_splits_exactly_match_scalar() {
        for len in [1, 2, 7, 31, 83, 129] {
            for window in [2, 3, 7, 17] {
                if len >= window {
                    compare_splits::<false, 3>(len, window);
                    compare_splits::<true, 8>(len, window);
                }
            }
        }
    }

    #[test]
    fn simd_splits_without_metadata_match_scalar() {
        let data = inputs(71);
        let hashes: Vec<_> = data.iter().map(|x| x.0).collect();
        let mut got = vec![Vec::new(); 5];
        SimdBatchMinQueue::new(7)
            .get_minimizer_hash_splits_slice::<false, 5>(&hashes, |lane, i, h, last| {
                got[lane].push((i, h, last))
            });
        for lane in 0..5 {
            let mut expected = Vec::new();
            BatchMinQueue::new(7).get_minimizer_splits::<_, false>(
                hashes.iter().map(|h| (h.to_array()[lane] as u64, ())),
                0,
                0,
                |i, item, last| expected.push((i, item.0 as u32, last)),
            );
            assert_eq!(got[lane], expected);
        }
    }

    /// Mirrors the run logic of `get_valid_minimizer_splits` one lane at a
    /// time, on top of the scalar window minima.
    fn expected_runs<const DUP: bool>(
        minimizers: &[(u64, u32)],
        valid_lanes: &[u8],
        lane: usize,
    ) -> Vec<(usize, usize, u32, u32, bool)> {
        let mut runs = Vec::new();
        let mut run_start = 0;
        let mut last: Option<(u32, u32)> = None;
        for (window, &(hash, extra)) in minimizers.iter().enumerate() {
            let (hash, valid) = (hash as u32, valid_lanes[window] >> lane & 1 == 1);
            match (last, valid) {
                (Some((last_hash, last_extra)), true) => {
                    let unique = !DUP || hash & 1 == 1;
                    if last_hash != hash || (last_extra != extra && unique) {
                        runs.push((run_start, window, last_hash, last_extra, false));
                        run_start = window;
                    }
                    last = Some((hash, extra));
                }
                (Some((last_hash, last_extra)), false) => {
                    runs.push((run_start, window, last_hash, last_extra, true));
                    last = None;
                }
                (None, true) => {
                    run_start = window;
                    last = Some((hash, extra));
                }
                (None, false) => {}
            }
        }
        if let Some((last_hash, last_extra)) = last {
            runs.push((run_start, minimizers.len(), last_hash, last_extra, true));
        }
        runs
    }

    fn compare_valid_runs<const DUP: bool>(length: usize, window: usize, seed: usize) {
        let data = inputs(length);
        let windows_count = data.len() + 1 - window;
        let valid_lanes: Vec<u8> = (0..windows_count)
            .map(|index| {
                let mut mask = 0u8;
                for lane in 0..SIMD_LANES {
                    let noise = (index * 7 + lane * 13 + seed) % 23;
                    let valid = match lane {
                        0 => !(4..9).contains(&index),
                        1 => noise % 5 != 0,
                        2 => index + 5 < windows_count,
                        3 => false,
                        _ => noise % 11 != 3,
                    };
                    mask |= (valid as u8) << lane;
                }
                mask
            })
            .collect();

        let mut got = vec![Vec::new(); SIMD_LANES];
        SimdBatchMinQueue::<u32>::new(window).get_valid_minimizer_splits::<_, DUP>(
            data.iter().map(|&(h, x)| (h, u32::load(x))),
            &valid_lanes,
            |run| got[run.lane].push((run.start, run.end, run.hash, run.extra, run.finished)),
        );

        for lane in 0..SIMD_LANES {
            let mut minimizers = Vec::new();
            BatchMinQueue::new(window).get_minimizers::<_, DUP>(
                data.iter()
                    .map(|(h, x)| (h.to_array()[lane] as u64, x[lane])),
                0,
                |item, _| minimizers.push(item),
                |_| {},
            );
            assert_eq!(minimizers.len(), windows_count);
            assert_eq!(
                got[lane],
                expected_runs::<DUP>(&minimizers, &valid_lanes, lane),
                "lane {lane}, length {length}, window {window}, dup {DUP}"
            );
        }
    }

    #[test]
    fn valid_runs_match_the_scalar_queue() {
        for length in [7usize, 31, 83, 129] {
            for window in [2usize, 3, 7, 17] {
                if length >= window {
                    compare_valid_runs::<true>(length, window, 0);
                    compare_valid_runs::<false>(length, window, 5);
                }
            }
        }
    }

    #[test]
    fn valid_runs_are_empty_without_valid_windows() {
        let data = inputs(64);
        let mut count = 0;
        SimdBatchMinQueue::<u32>::new(7).get_valid_minimizer_splits::<_, true>(
            data.iter().map(|&(h, x)| (h, u32::load(x))),
            &vec![0u8; data.len() - 6],
            |_| count += 1,
        );
        assert_eq!(count, 0);
    }

    #[test]
    fn repeated_hashes_split_on_the_minimizer_position() {
        const WINDOW: usize = 4;
        // The same odd hash at two positions more than one window apart: the
        // value never changes but the position does, which must still split.
        let data: Vec<(u32x8, [u32; SIMD_LANES])> = (0..16)
            .map(|index| {
                let hash = if index == 2 || index == 9 { 3u32 } else { 1001 };
                (u32x8::splat(hash), std::array::from_fn(|_| index as u32))
            })
            .collect();
        let valid = vec![u8::MAX; data.len() + 1 - WINDOW];
        let mut runs = Vec::new();
        SimdBatchMinQueue::<u32>::new(WINDOW).get_valid_minimizer_splits::<_, true>(
            data.iter().map(|&(h, x)| (h, u32::load(x))),
            &valid,
            |run| {
                if run.lane == 0 {
                    runs.push((run.start, run.end, run.extra))
                }
            },
        );
        assert!(
            runs.len() >= 3,
            "position changes must split the runs: {runs:?}"
        );
        assert_eq!(runs[0].0, 0);
        assert_eq!(runs.last().unwrap().1, valid.len());
        for pair in runs.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
    }

    #[test]
    fn simd_hash_only_fast_path_matches_generic_path() {
        let data = inputs(97);
        let mut generic = Vec::new();
        SimdBatchMinQueue::new(13).get_minimizers_simd::<_, true>(
            data.iter().copied().map(|(h, x)| (h, x.map(|_| ()))),
            4,
            |item, i| generic.push((item.0, i)),
            |_| {},
        );
        let mut fast = Vec::new();
        SimdBatchMinQueue::new(13).get_minimizer_hashes_simd::<_, true>(
            data.iter().map(|x| x.0),
            4,
            |h, i| fast.push((h, i)),
            |_| {},
        );
        assert_eq!(fast, generic);
    }
}
