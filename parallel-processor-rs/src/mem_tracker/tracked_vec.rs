#[cfg(feature = "track-usage")]
use crate::mem_tracker::{create_hashmap_entry, MemoryInfo};
use std::ops::{Deref, DerefMut};

// #[repr(transparent)]
pub struct TrackedVec<T> {
    value: Vec<T>,
    #[cfg(feature = "track-usage")]
    mem_info: Arc<MemoryInfo>,
}

impl<T> TrackedVec<T> {
    #[inline(always)]
    #[track_caller]
    pub fn new() -> Self {
        Self {
            value: Vec::new(),
            #[cfg(feature = "track-usage")]
            mem_info: {
                let location = Location::caller();
                create_hashmap_entry(location, std::any::type_name::<T>())
            },
        }
    }

    #[inline(always)]
    #[track_caller]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            value: Vec::with_capacity(capacity),
            #[cfg(feature = "track-usage")]
            mem_info: {
                let location = Location::caller();
                let last_size = capacity * size_of::<T>();

                let mem_info = create_hashmap_entry(location, std::any::type_name::<T>());
                mem_info.bytes.store(last_size, Ordering::Relaxed);
                mem_info
            },
        }
    }

    #[inline(always)]
    #[cfg(feature = "track-usage")]
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
