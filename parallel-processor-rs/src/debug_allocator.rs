use dashmap::DashMap;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

pub struct DebugAllocator {
    default_allocator: std::alloc::System,
}

struct AllocationInfo {
    bt: String,
    current_count: AtomicUsize,
    current_size: AtomicUsize,
    max_size: AtomicUsize,
    total_count: AtomicUsize,
}

impl AllocationInfo {
    pub fn as_writable(&self) -> AllocationInfoWritable {
        AllocationInfoWritable {
            bt: self.bt.clone(),
            current_count: self.current_count.load(Ordering::Relaxed),
            current_size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_size.load(Ordering::Relaxed),
            total_count: self.total_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct AllocationInfoWritable {
    bt: String,
    current_count: usize,
    current_size: usize,
    max_size: usize,
    total_count: usize,
}

lazy_static! {
    static ref ALLOCATION_INFOS: DashMap<String, AllocationInfo> = DashMap::new();
    static ref ADDRESSES_BACKTRACE: DashMap<usize, String> = DashMap::new();
}

pub fn debug_print_allocations(dir: impl AsRef<Path>, period: Duration) {
    let dir = dir.as_ref().to_path_buf();
    std::thread::spawn(move || {
        IS_NESTED.with(|n| n.store(true, Ordering::Relaxed));
        let mut count = 1;
        loop {
            std::thread::sleep(period);

            let path = dir.join(format!("memory-log{}.json", count));

            let mut allocations: Vec<_> =
                ALLOCATION_INFOS.iter().map(|x| x.as_writable()).collect();

            allocations.sort_by(|x, y| y.max_size.cmp(&x.max_size));

            let _ = File::create(path)
                .unwrap()
                .write_all(to_string_pretty(&allocations).unwrap().as_bytes());

            count += 1;
        }
    });
}

fn store_backtrace(addr: *mut u8, size: usize) {
    let bt: backtrace::Backtrace = backtrace::Backtrace::new();

    let bt_string = format!("{:?}", bt);

    let parts = bt_string.split("  5:").collect::<Vec<_>>();

    let bt_string = parts.last().unwrap().to_string();

    ADDRESSES_BACKTRACE.insert(addr as usize, bt_string.clone());

    let info = ALLOCATION_INFOS
        .entry(bt_string.clone())
        .or_insert(AllocationInfo {
            bt: bt_string,
            current_count: AtomicUsize::new(0),
            current_size: AtomicUsize::new(0),
            max_size: AtomicUsize::new(0),
            total_count: AtomicUsize::new(0),
        });

    info.current_count.fetch_add(1, Ordering::Relaxed);
    info.current_size.fetch_add(size, Ordering::Relaxed);
    info.total_count.fetch_add(1, Ordering::Relaxed);
    info.max_size
        .fetch_max(info.current_size.load(Ordering::Relaxed), Ordering::Relaxed);
}

fn update_backtrace(ptr: *mut u8, new_ptr: *mut u8, diff: isize) {
    let (_, bt) = ADDRESSES_BACKTRACE.remove(&(ptr as usize)).unwrap();

    let aref = ALLOCATION_INFOS.get(&bt).unwrap();
    if diff > 0 {
        aref.current_size
            .fetch_add(diff as usize, Ordering::Relaxed);
        aref.max_size
            .fetch_max(aref.current_size.load(Ordering::Relaxed), Ordering::Relaxed);
    } else {
        aref.current_size
            .fetch_sub((-diff) as usize, Ordering::Relaxed);
    }

    ADDRESSES_BACKTRACE.insert(new_ptr as usize, bt);
}

fn dealloc_backtrace(ptr: *mut u8, size: usize) {
    let (_, bt) = ADDRESSES_BACKTRACE.remove(&(ptr as usize)).unwrap();

    let aref = ALLOCATION_INFOS.get(&bt).unwrap();
    aref.current_count.fetch_sub(1, Ordering::Relaxed);
    aref.current_size.fetch_sub(size, Ordering::Relaxed);
}

impl DebugAllocator {
    pub const fn new() -> Self {
        Self {
            default_allocator: System,
        }
    }
}

thread_local! {
    static IS_NESTED: AtomicBool = AtomicBool::new(false);
}

unsafe impl GlobalAlloc for DebugAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.default_allocator.alloc(layout);
        if !IS_NESTED.with(|n| n.swap(true, Ordering::Relaxed)) {
            store_backtrace(ptr, layout.size());
            IS_NESTED.with(|n| n.store(false, Ordering::Relaxed));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if !IS_NESTED.with(|n| n.swap(true, Ordering::Relaxed)) {
            dealloc_backtrace(ptr, layout.size());
            IS_NESTED.with(|n| n.store(false, Ordering::Relaxed));
        }
        self.default_allocator.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.default_allocator.alloc_zeroed(layout);
        if !IS_NESTED.with(|n| n.swap(true, Ordering::Relaxed)) {
            store_backtrace(ptr, layout.size());
            IS_NESTED.with(|n| n.store(false, Ordering::Relaxed));
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.default_allocator.realloc(ptr, layout, new_size);
        if !IS_NESTED.with(|n| n.swap(true, Ordering::Relaxed)) {
            update_backtrace(ptr, new_ptr, (new_size as isize) - (layout.size() as isize));
            IS_NESTED.with(|n| n.store(false, Ordering::Relaxed));
        }
        new_ptr
    }
}
