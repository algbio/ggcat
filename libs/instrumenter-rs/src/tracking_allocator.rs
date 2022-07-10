use crate::enabled::get_subscriber;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct TrackingAllocator;

thread_local! {
    static IS_NESTED: AtomicBool = AtomicBool::new(false);
}

struct NestedGuard;
impl Drop for NestedGuard {
    fn drop(&mut self) {
        IS_NESTED.with(|n| n.store(false, Ordering::Relaxed));
    }
}

macro_rules! nested_check {
    ($retval:expr) => {
        if IS_NESTED.with(|n| n.swap(true, Ordering::Relaxed)) {
            return $retval;
        }
        let _guard = NestedGuard;
    };
}

const MIN_TRACKED_ALLOC: usize = 64;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);

        if layout.size() < MIN_TRACKED_ALLOC {
            return ptr;
        }

        nested_check!(ptr);
        assert!(IS_NESTED.with(|x| x.load(Ordering::Relaxed)));
        if let Some(subscriber) = get_subscriber() {
            subscriber.notify_memory_alloc(ptr as usize, layout.size());
        }

        return ptr;
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);

        if layout.size() < MIN_TRACKED_ALLOC {
            return;
        }

        nested_check!(());
        if let Some(subscriber) = get_subscriber() {
            subscriber.notify_memory_dealloc(ptr as usize);
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);

        if layout.size() < MIN_TRACKED_ALLOC {
            return ptr;
        }

        nested_check!(ptr);
        if let Some(subscriber) = get_subscriber() {
            subscriber.notify_memory_alloc(ptr as usize, layout.size());
        }

        return ptr;
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = System.realloc(ptr, layout, new_size);

        if layout.size() < MIN_TRACKED_ALLOC {
            return ptr;
        }

        nested_check!(ptr);
        if let Some(subscriber) = get_subscriber() {
            subscriber.notify_memory_dealloc(ptr as usize);
        }
        if let Some(subscriber) = get_subscriber() {
            subscriber.notify_memory_alloc(ptr as usize, layout.size());
        }

        return ptr;
    }
}
