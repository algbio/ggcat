//! The queues of the copies of [`super::CopybackTracker`], by key.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// A min-queue of copies by key: `(key, copy)` entries, the copy the tracker's index of it,
/// popped from the smallest key. The keys come in any order, as the copies wait for points of
/// the stream anywhere in the window. A binary heap for now: what is inside is free to change,
/// as long as the entries come out by key.
pub(super) struct CopyMinQueue {
    heap: BinaryHeap<Reverse<(u64, u64)>>,
}

impl CopyMinQueue {
    pub(super) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.heap.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub(super) fn push(&mut self, key: u64, copy: u64) {
        self.heap.push(Reverse((key, copy)));
    }

    /// The entry with the smallest key.
    pub(super) fn peek(&self) -> Option<(u64, u64)> {
        self.heap.peek().map(|&Reverse(entry)| entry)
    }

    /// Takes the entry [`Self::peek`] shows out.
    pub(super) fn pop(&mut self) {
        self.heap.pop();
    }

    /// The smallest key, `u64::MAX` when empty.
    pub(super) fn next_key(&self) -> u64 {
        self.peek().map_or(u64::MAX, |(key, _)| key)
    }

    /// Takes every entry out, in any order.
    pub(super) fn drain(&mut self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.heap.drain().map(|Reverse(entry)| entry)
    }

    pub(super) fn clear(&mut self) {
        self.heap.clear();
    }

    /// Keeps the entries whose copy `keep` accepts.
    pub(super) fn retain(&mut self, keep: impl Fn(u64) -> bool) {
        self.heap.retain(|&Reverse((_, copy))| keep(copy));
    }
}
