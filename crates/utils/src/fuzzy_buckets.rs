use rustc_hash::FxBuildHasher;
use std::hash::BuildHasher;

use crate::fuzzy_hashmap::resize_vec_default;

pub struct FuzzyBuckets<T: Copy + Default> {
    hashmap: Vec<T>,
    capacity_mask: usize,
}

impl<T: Copy + Default> FuzzyBuckets<T> {
    pub fn new(capacity: usize) -> Self {
        let hashmap_capacity = capacity.next_power_of_two();

        let mut hashmap: Vec<T> = vec![];
        resize_vec_default(hashmap_capacity, &mut hashmap);

        Self {
            hashmap,
            capacity_mask: hashmap_capacity - 1, // hashmap_capacity is a power of 2
        }
    }

    pub fn initialize(&mut self, capacity: usize) {
        let capacity = capacity.next_power_of_two();
        resize_vec_default(capacity, &mut self.hashmap);
        self.capacity_mask = capacity - 1;
        self.hashmap[0..capacity].fill(T::default());
    }

    pub fn get_element_mut(&mut self, hash: u64) -> &mut T {
        let hash = FxBuildHasher.hash_one(hash);
        let position = (hash as usize) & self.capacity_mask;
        // println!("Accessing position: {}", position);
        // Safety: position is in and with capacity_mask
        unsafe { self.hashmap.get_unchecked_mut(position) }
    }
}
