use crate::mem_tracker::{create_hashmap_entry, MemoryInfo};
use std::cmp::max;
use std::mem::{size_of, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// #[repr(transparent)]
pub struct TrackedVec<T> {
    value: Vec<T>,
    mem_info: Arc<MemoryInfo>,
}

impl<T> TrackedVec<T> {
    #[inline(always)]
    #[track_caller]
    pub fn new() -> Self {
        let location = Location::caller();
        let mem_info = create_hashmap_entry(location, std::any::type_name::<T>());

        Self {
            value: Vec::new(),
            mem_info,
        }
    }

    #[inline(always)]
    #[track_caller]
    pub fn with_capacity(capacity: usize) -> Self {
        let location = Location::caller();
        let last_size = capacity * size_of::<T>();

        let mem_info = create_hashmap_entry(location, std::any::type_name::<T>());
        mem_info.bytes.store(last_size, Ordering::Relaxed);

        Self {
            value: Vec::with_capacity(capacity),
            mem_info,
        }
    }

    #[inline(always)]
    pub fn update_maximum_usage(&mut self, resident: usize) {
        let new_last_size = self.capacity() * size_of::<T>();
        let new_resident_bytes = max(
            self.mem_info.resident_bytes.load(Ordering::Relaxed),
            resident * size_of::<T>(),
        );

        self.mem_info.bytes.store(new_last_size, Ordering::Relaxed);
        self.mem_info
            .resident_bytes
            .store(new_resident_bytes, Ordering::Relaxed);
    }
}

impl<T> Deref for TrackedVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for TrackedVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
