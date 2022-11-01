use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use std::cell::Cell;
use std::mem::size_of;

const FLAGS_COUNT: usize = 2;
const FLAGS_SHIFT: usize = size_of::<usize>() * 8 - FLAGS_COUNT;
const USED_MARKER: usize = 1 << (FLAGS_SHIFT - 1);
const COUNTER_MASK: usize = (1 << (FLAGS_SHIFT - 1)) - 1;

pub const COUNTER_BITS: usize = FLAGS_SHIFT - 1;

pub struct MapEntry<CHI> {
    count_flags: Cell<usize>,
    pub color_index: CHI,
}

unsafe impl<CHI> Sync for MapEntry<CHI> {}

impl<CHI> MapEntry<CHI> {
    pub fn new(color_index: CHI) -> Self {
        Self {
            count_flags: Cell::new(0),
            color_index,
        }
    }

    #[inline(always)]
    pub fn incr(&mut self) {
        self.count_flags.set(self.count_flags.get() + 1);
    }

    #[inline(always)]
    pub fn set_used(&self) {
        self.count_flags.set(self.count_flags.get() | USED_MARKER);
    }

    #[inline(always)]
    pub fn is_used(&self) -> bool {
        (self.count_flags.get() & USED_MARKER) == USED_MARKER
    }

    #[inline(always)]
    pub fn get_counter(&self) -> usize {
        self.count_flags.get() & COUNTER_MASK
    }

    pub fn set_counter_after_check(&mut self, value: usize) {
        self.count_flags
            .set((self.count_flags.get() & !COUNTER_MASK) | (value & COUNTER_MASK));
    }

    #[inline(always)]
    pub fn update_flags(&mut self, flags: u8) {
        self.count_flags
            .set(self.count_flags.get() | ((flags as usize) << FLAGS_SHIFT));
    }

    #[inline(always)]
    pub fn get_flags(&self) -> u8 {
        (self.count_flags.get() >> FLAGS_SHIFT) as u8
    }

    pub fn get_kmer_multiplicity(&self) -> usize {
        // If the current set has both the partial sequences endings, we should divide the counter by 2,
        // as all the kmers are counted exactly two times
        self.get_counter()
            >> ((self.get_flags() == (READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END)) as u8)
    }
}
