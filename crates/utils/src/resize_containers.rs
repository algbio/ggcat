use std::ops::{Deref, DerefMut};

use hashbrown::HashTable;
use rustc_hash::FxHashMap;

fn reassign<T>(value: &mut T, new_value: impl FnOnce() -> T) {
    unsafe {
        let old_value = std::ptr::read(value as *mut T);
        drop(old_value);
        std::ptr::write(value as *mut T, new_value());
    }
}

pub trait ResizableContainer: Sized {
    fn new_internal(capacity: usize) -> Self;
    fn clear_internal(&mut self);
    fn get_capacity_internal(&self) -> usize;

    fn clear_or_reinit(&mut self, max_capacity: usize) {
        if self.get_capacity_internal() > max_capacity {
            reassign(self, || Self::new_internal(max_capacity));
        } else {
            self.clear_internal();
        }
    }
}

impl<T> ResizableContainer for Vec<T> {
    fn new_internal(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear_internal(&mut self) {
        self.clear();
    }

    fn get_capacity_internal(&self) -> usize {
        self.capacity()
    }
}

impl<K, V> ResizableContainer for FxHashMap<K, V> {
    fn new_internal(capacity: usize) -> Self {
        FxHashMap::with_capacity_and_hasher(capacity, Default::default())
    }

    fn clear_internal(&mut self) {
        self.clear();
    }

    fn get_capacity_internal(&self) -> usize {
        self.capacity()
    }
}

impl<T> ResizableContainer for HashTable<T> {
    fn new_internal(capacity: usize) -> Self {
        HashTable::with_capacity(capacity)
    }

    fn clear_internal(&mut self) {
        self.clear();
    }

    fn get_capacity_internal(&self) -> usize {
        self.capacity()
    }
}

#[repr(transparent)]
pub struct FixedSizeResizableContainer<C: ResizableContainer, const SIZE: usize>(C);

impl<C: ResizableContainer, const SIZE: usize> Deref for FixedSizeResizableContainer<C, SIZE> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C: ResizableContainer, const SIZE: usize> DerefMut for FixedSizeResizableContainer<C, SIZE> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<C: ResizableContainer, const SIZE: usize> FixedSizeResizableContainer<C, SIZE> {
    pub fn new() -> Self {
        Self(C::new_internal(SIZE))
    }

    pub fn clear(&mut self) {
        self.0.clear_or_reinit(SIZE);
    }

    pub fn clear_with_required_capacity(&mut self, required_capacity: usize) {
        if self.0.get_capacity_internal() < required_capacity {
            reassign(&mut self.0, || C::new_internal(required_capacity));
        } else {
            self.0.clear_or_reinit(required_capacity);
        }
        // total_sequences_count
    }
}

pub type ResizableVec<T, const SIZE: usize> = FixedSizeResizableContainer<Vec<T>, SIZE>;
pub type ResizableHashMap<K, V, const SIZE: usize> =
    FixedSizeResizableContainer<FxHashMap<K, V>, SIZE>;

pub type ResizableHashTable<T, const SIZE: usize> = FixedSizeResizableContainer<HashTable<T>, SIZE>;
