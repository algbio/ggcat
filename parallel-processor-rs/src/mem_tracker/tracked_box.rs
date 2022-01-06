#[cfg(feature = "track-usage")]
use crate::mem_tracker::{create_hashmap_entry, MemoryInfo};
use std::cmp::max;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// #[repr(transparent)]
pub struct TrackedBox<T: ?Sized> {
    value: Box<T>,
    #[cfg(feature = "track-usage")]
    mem_info: Arc<MemoryInfo>,
}

impl<T> TrackedBox<T> {
    #[inline(always)]
    #[track_caller]
    pub fn new(value: T) -> Self {
        Self {
            value: Box::new(value),
            #[cfg(feature = "track-usage")]
            mem_info: {
                let location = Location::caller();
                let last_size = std::mem::size_of::<T>();

                let mem_info = create_hashmap_entry(location, std::any::type_name::<T>());
                mem_info.bytes.store(last_size, Ordering::Relaxed);
                mem_info
            },
        }
    }
}

impl<T> TrackedBox<[T]> {
    #[inline(always)]
    #[cfg(feature = "track-usage")]
    pub fn notify_maximum_usage(&mut self, ptr: *const T) {
        let new_resident_bytes = max(
            self.mem_info.resident_bytes.load(Ordering::Relaxed),
            ptr as usize - self.value.as_ptr() as usize,
        );
        self.mem_info
            .resident_bytes
            .store(new_resident_bytes, Ordering::Relaxed);
    }
}

impl<T> TrackedBox<[MaybeUninit<T>]> {
    #[inline(always)]
    #[track_caller]
    pub fn new_uninit_slice(len: usize) -> Self {
        Self {
            value: Box::new_uninit_slice(len),
            #[cfg(feature = "track-usage")]
            mem_info: {
                let location = Location::caller();
                let last_size = std::mem::size_of::<T>() * len;

                let mem_info = create_hashmap_entry(location, std::any::type_name::<T>());
                mem_info.bytes.store(last_size, Ordering::Relaxed);
                mem_info
            },
        }
    }

    #[track_caller]
    pub fn new_zeroed_slice(len: usize) -> Self {
        Self {
            value: Box::new_zeroed_slice(len),
            #[cfg(feature = "track-usage")]
            mem_info: {
                let location = Location::caller();
                let last_size = std::mem::size_of::<T>() * len;

                let mem_info = create_hashmap_entry(location, std::any::type_name::<T>());
                mem_info.bytes.store(last_size, Ordering::Relaxed);
                mem_info
            },
        }
    }

    pub unsafe fn assume_init(mut self) -> TrackedBox<[T]> {
        let new_box = TrackedBox {
            value: std::mem::take(&mut self.value).assume_init(),
            #[cfg(feature = "track-usage")]
            mem_info: self.mem_info,
        };
        new_box
    }
}

impl<T: ?Sized> Deref for TrackedBox<T> {
    type Target = Box<T>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ?Sized> DerefMut for TrackedBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
