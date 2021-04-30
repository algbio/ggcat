use std::cmp::min;
use std::mem::size_of;

struct Bucket {
    key: u64,
    // Bit meanings:
    // 0 => occupied
    count_and_flags: u32,
    index: u32,
}

impl Bucket {
    #[inline(always)]
    fn new(key: u64, index: u32) -> Self {
        Self {
            key,
            count_and_flags: (1 << 8) | 1,
            index,
        }
    }

    #[inline(always)]
    fn is_occupied(&self) -> bool {
        self.count_and_flags & 0x1 != 0
    }

    #[inline(always)]
    fn free(&mut self) {
        self.count_and_flags &= !0x1;
    }

    #[inline(always)]
    fn increment_value(&mut self) {
        let mut counter = self.count_and_flags >> 8;
        let flags = self.count_and_flags & 0xFF;
        counter += 1;
        self.count_and_flags = (counter << 8) | flags;
    }

    #[inline(always)]
    fn get_value(&self) -> u32 {
        self.count_and_flags >> 8
    }
}

pub struct OptimizedHashMap {
    table: Box<[Bucket]>,
    occupied: Vec<usize>,
    max_capacity_order: usize,
    current_capacity_mask: usize,
}

impl OptimizedHashMap {
    pub fn new(max_capacity_order: usize, molt_filter: usize) -> Self {
        OptimizedHashMap {
            table: unsafe { Box::new_zeroed_slice(2 << max_capacity_order).assume_init() },
            occupied: Vec::with_capacity(1 << max_capacity_order),
            max_capacity_order: 0,
            current_capacity_mask: 0,
        }
    }

    pub fn clear_and_reserve(&mut self, capacity: usize) {
        self.clear();
        self.current_capacity_mask =
            (1 << size_of::<usize>() * 8 - capacity.leading_zeros() as usize) - 1;
    }

    pub fn increment_entry(&mut self, key: u64, index: u32) {
        let mut hindex = key as usize & self.current_capacity_mask;

        while self.table[hindex].key != key && self.table[hindex].is_occupied() {
            hindex = (hindex + 1) & self.current_capacity_mask;
        }

        if self.table[hindex].is_occupied() {
            self.table[hindex].increment_value();
        } else {
            self.table[hindex] = Bucket::new(key, index);
            self.occupied.push(hindex);
        }
    }

    pub fn clear(&mut self) {
        for el in self.occupied.iter() {
            self.table[*el].free();
        }
        self.occupied.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.occupied
            .iter()
            .map(move |x| (self.table[*x].index, self.table[*x].get_value()))
    }
}
