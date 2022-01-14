use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::allocator::CHUNKS_ALLOCATOR;
use crate::memory_fs::file::flush::GlobalFlush;
use crate::memory_fs::file::internal::MemoryFileInternal;
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::panic::Location;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

pub mod tracked_box;
pub mod tracked_vec;

pub struct MemoryInfo {
    pub bytes: AtomicUsize,
    pub resident_bytes: AtomicUsize,
}

pub static MEMORY_INFO: Mutex<
    Option<HashMap<(&'static Location<'static>, &'static str), Vec<Weak<MemoryInfo>>>>,
> = Mutex::new(None);

pub fn init_memory_info() {
    *MEMORY_INFO.lock() = Some(HashMap::new());
}

#[cfg(feature = "track-usage")]
pub fn create_hashmap_entry(
    location: &'static Location,
    type_name: &'static str,
) -> Arc<MemoryInfo> {
    let mut mem_info_map = MEMORY_INFO.lock();

    let new_mem_info = Arc::new(MemoryInfo {
        bytes: AtomicUsize::new(0),
        resident_bytes: AtomicUsize::new(0),
    });

    mem_info_map
        .as_mut()
        .unwrap()
        .entry((location, type_name))
        .or_insert(Vec::new())
        .push(Arc::downgrade(&new_mem_info));

    new_mem_info
}

pub fn start_info_logging() {
    thread::spawn(|| loop {
        print_memory_info();
        thread::sleep(Duration::from_millis(3000));
    });
}

pub fn print_memory_info() {
    let mut info_map = MEMORY_INFO.lock();

    let mut tot_reserved = 0;
    let mut tot_resident = 0;
    let mut tot_objects_count = 0;

    for (pos, info) in info_map.as_mut().unwrap().iter_mut() {
        let mut reserved = 0;
        let mut resident = 0;
        let mut objects_count = 0;

        info.drain_filter(|x| match x.upgrade() {
            None => true,
            Some(val) => {
                reserved += val.bytes.load(Ordering::Relaxed);
                resident += val.resident_bytes.load(Ordering::Relaxed);
                objects_count += 1;
                false
            }
        })
        .for_each(drop);

        println!(
            "Allocation: {:?} => alloc: {:.2} resident: {:.2} [tot {} objects]",
            pos.0,
            MemoryDataSize::from_octets(reserved as f64),
            MemoryDataSize::from_octets(resident as f64),
            objects_count,
        );

        tot_reserved += reserved;
        tot_resident += resident;
        tot_objects_count += objects_count;
    }

    println!(
        "********* GLOBAL STATS: alloc: {:.2} resident: {:.2} [tot {} objects]  FLUSH QUEUE: {:?} *********",
        MemoryDataSize::from_octets(tot_reserved as f64),
        MemoryDataSize::from_octets(tot_resident as f64),
        tot_objects_count,
        if GlobalFlush::is_initialized() { GlobalFlush::global_queue_occupation() } else { (0, 0) }
    );

    // // many statistics are cached and only updated when the epoch is advanced.
    // epoch::advance().unwrap();
    //
    // let allocated = stats::allocated::read().unwrap();
    // let resident = stats::resident::read().unwrap();
    //
    // println!(
    //     "********* ALLOCATOR STATS: alloc: {:.2} resident: {:.2} [tot {} objects] *********",
    //     MemoryDataSize::from_octets(allocated as f64),
    //     MemoryDataSize::from_octets(resident as f64),
    //     tot_objects_count
    // );

    println!(
        "********* MEMORY FS STATS: alloc: {:.2} reserved: {:.2} resident: {:.2} files count: {} *********",
        CHUNKS_ALLOCATOR.get_total_memory(),
        CHUNKS_ALLOCATOR.get_reserved_memory(),
        CHUNKS_ALLOCATOR.get_total_memory() - CHUNKS_ALLOCATOR.get_free_memory(),
        MemoryFileInternal::active_files_count()
    );
}
