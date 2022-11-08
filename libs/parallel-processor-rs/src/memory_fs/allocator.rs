use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::{MemoryFs, FILES_FLUSH_HASH_MAP};
use parking_lot::lock_api::RawMutex as _;
use parking_lot::{Condvar, Mutex};
use std::alloc::{alloc, dealloc, Layout};
use std::cmp::{max, min};
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const ALLOCATOR_ALIGN: usize = 4096;
const OUT_OF_MEMORY_ALLOCATION_SIZE: MemoryDataSize = MemoryDataSize::from_mebioctets(256);
const MAXIMUM_CHUNK_SIZE_LOG: usize = 18;
const MINIMUM_CHUNK_SIZE_LOG: usize = 8;

#[macro_export]
#[cfg(feature = "track-usage")]
macro_rules! chunk_usage {
    ($mode:ident $({ $($param:ident : $value:expr),* })?) => {
        $crate::memory_fs::allocator::ChunkUsage_::$mode $({
            $($param: $value),*
        })?
    }
}

#[macro_export]
#[cfg(not(feature = "track-usage"))]
macro_rules! chunk_usage {
    ($mode:ident $({ $($param:ident : $value:expr),* })?) => {
        ()
    };
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ChunkUsage_ {
    FileBuffer { path: String },
    InMemoryFile { path: String },
    ReadStriped { path: String },
    TemporarySpace,
}

pub struct AllocatedChunk {
    memory: usize,
    len: AtomicUsize,
    max_len_log2: usize,
    dealloc_fn: fn(usize, usize),
    #[cfg(feature = "track-usage")]
    usage: ChunkUsage_,
}
unsafe impl Sync for AllocatedChunk {}
unsafe impl Send for AllocatedChunk {}

impl Clone for AllocatedChunk {
    #[track_caller]
    fn clone(&self) -> Self {
        panic!("This method should not be called, check for vector cloning!");
    }
}

impl AllocatedChunk {
    pub const INVALID: Self = Self {
        memory: 0,
        len: AtomicUsize::new(0),
        max_len_log2: 0,
        dealloc_fn: |_, _| {},
        #[cfg(feature = "track-usage")]
        usage: ChunkUsage_::TemporarySpace,
    };

    #[inline(always)]
    #[allow(dead_code)]
    fn zero_memory(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.memory as *mut u8, 0, 1 << self.max_len_log2);
        }
    }

    #[inline(always)]
    #[allow(mutable_transmutes)]
    pub unsafe fn write_bytes_noextend_single_thread(&self, data: *const u8, len: usize) {
        let off_len = std::mem::transmute::<&AtomicUsize, &mut usize>(&self.len);
        std::ptr::copy_nonoverlapping(data, (self.memory + *off_len) as *mut u8, len);
        *off_len += len;
    }

    #[inline(always)]
    #[allow(mutable_transmutes)]
    pub unsafe fn write_zero_bytes_noextend_single_thread(&self, len: usize) {
        let off_len = std::mem::transmute::<&AtomicUsize, &mut usize>(&self.len);
        std::ptr::write_bytes((self.memory + *off_len) as *mut u8, 0, len);
        *off_len += len;
    }

    #[inline(always)]
    #[allow(mutable_transmutes)]
    pub unsafe fn prealloc_bytes_single_thread(&self, len: usize) -> &'static mut [u8] {
        let off_len = std::mem::transmute::<&AtomicUsize, &mut usize>(&self.len);
        let slice = from_raw_parts_mut((self.memory + *off_len) as *mut u8, len);
        *off_len += len;
        slice
    }

    pub fn write_bytes_noextend(&self, data: &[u8]) -> Option<u64> {
        let result = self
            .len
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value + data.len() <= (1 << self.max_len_log2) {
                    Some(value + data.len())
                } else {
                    None
                }
            });

        match result {
            Ok(addr_offset) => {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        (self.memory + addr_offset) as *mut u8,
                        data.len(),
                    );
                }
                Some(addr_offset as u64)
            }
            Err(_) => None,
        }
    }

    #[inline(always)]
    pub fn has_space_for(&self, len: usize) -> bool {
        self.len.load(Ordering::Relaxed) + len <= (1 << self.max_len_log2)
    }

    #[inline(always)]
    pub unsafe fn get_mut_slice(&self) -> &'static mut [u8] {
        from_raw_parts_mut(self.memory as *mut u8, self.len.load(Ordering::Relaxed))
    }

    #[inline(always)]
    pub unsafe fn get_mut_ptr(&self) -> *mut u8 {
        self.memory as *mut u8
    }

    #[inline(always)]
    pub unsafe fn get_object_reference_mut<T>(&self, offset_in_bytes: usize) -> &'static mut T {
        &mut *((self.memory as *mut u8).add(offset_in_bytes) as *mut T)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn max_len(&self) -> usize {
        1 << self.max_len_log2
    }

    #[inline(always)]
    pub fn remaining_bytes(&self) -> usize {
        (1 << self.max_len_log2) - self.len.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn clear(&self) {
        self.len.store(0, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn get(&self) -> &[u8] {
        unsafe { from_raw_parts(self.memory as *mut u8, self.len.load(Ordering::Relaxed)) }
    }

    #[inline(always)]
    pub unsafe fn set_len(&self, len: usize) {
        self.len.store(len, Ordering::Relaxed);
    }
}

impl Drop for AllocatedChunk {
    fn drop(&mut self) {
        (self.dealloc_fn)(self.memory, self.max_len_log2);
    }
}

pub struct ChunksAllocator {
    buffers_list: Mutex<Vec<(usize, usize)>>,
    chunks_wait_condvar: Condvar,
    chunks: Mutex<Vec<usize>>,
    min_free_chunks: AtomicUsize,
    chunks_total_count: AtomicUsize,
    chunk_padded_size: AtomicUsize,
    chunk_usable_size: AtomicUsize,
    chunks_log_size: AtomicUsize,
}
unsafe impl Sync for ChunksAllocator {}
unsafe impl Send for ChunksAllocator {}

#[cfg(feature = "track-usage")]
static USAGE_MAP: Mutex<Option<HashMap<ChunkUsage_, usize>>> =
    Mutex::const_new(parking_lot::RawMutex::INIT, None);

impl ChunksAllocator {
    const fn new() -> ChunksAllocator {
        ChunksAllocator {
            buffers_list: Mutex::new(vec![]),
            chunks_wait_condvar: Condvar::new(),
            chunks: Mutex::const_new(parking_lot::RawMutex::INIT, Vec::new()),
            min_free_chunks: AtomicUsize::new(0),
            chunks_total_count: AtomicUsize::new(0),
            chunk_padded_size: AtomicUsize::new(0),
            chunk_usable_size: AtomicUsize::new(0),
            chunks_log_size: AtomicUsize::new(0),
        }
    }

    fn allocate_contiguous_chunk(&self, chunks_count: usize) -> impl Iterator<Item = usize> {
        let chunk_padded_size = self.chunk_padded_size.load(Ordering::Relaxed);

        self.chunks_total_count
            .fetch_add(chunks_count, Ordering::Relaxed);

        self.min_free_chunks.store(chunks_count, Ordering::Relaxed);

        let data = unsafe {
            alloc(Layout::from_size_align_unchecked(
                chunks_count * chunk_padded_size,
                ALLOCATOR_ALIGN,
            ))
        };

        #[cfg(feature = "memory-guards")]
        unsafe {
            let first_guard = data.add(chunk_usable_size.load(Ordering::Relaxed));
            for i in 0..chunks_count {
                let guard = first_guard.add(i * ALLOCATED_CHUNK_PADDED_SIZE);
                libc::mprotect(guard as *mut libc::c_void, 4096, libc::PROT_NONE);
            }
        }

        self.buffers_list.lock().push((data as usize, chunks_count));

        (0..chunks_count)
            .into_iter()
            .rev()
            .map(move |c| data as usize + (c * chunk_padded_size))
    }

    pub fn initialize(
        &self,
        memory: MemoryDataSize,
        mut chunks_log_size: usize,
        min_chunks_count: usize,
    ) {
        if self.buffers_list.lock().len() > 0 {
            // Already allocated
            return;
        }

        #[cfg(feature = "track-usage")]
        {
            *USAGE_MAP.lock() = Some(HashMap::new());
        }

        chunks_log_size = min(
            MAXIMUM_CHUNK_SIZE_LOG,
            max(MINIMUM_CHUNK_SIZE_LOG, chunks_log_size),
        );

        let chunk_usable_size = 1usize << chunks_log_size;
        let chunk_padded_size = chunk_usable_size
            + if cfg!(feature = "memory-guards") {
                4096
            } else {
                0
            };
        let total_padded_mem_size: MemoryDataSize =
            MemoryDataSize::from_octets(chunk_padded_size as f64);

        let chunks_count = max(min_chunks_count, (memory / total_padded_mem_size) as usize);

        self.chunk_padded_size
            .store(chunk_padded_size, Ordering::Relaxed);
        self.chunk_usable_size
            .store(chunk_usable_size, Ordering::Relaxed);
        self.chunks_log_size
            .store(chunks_log_size, Ordering::Relaxed);

        let chunks_iter = self.allocate_contiguous_chunk(chunks_count);

        self.chunks.lock().extend(chunks_iter);

        println!(
            "Allocator initialized: mem: {} chunks: {} log2: {}",
            memory, chunks_count, chunks_log_size
        );
    }

    pub fn giveback_free_memory(&self) {
        let chunks = self.chunks.lock();
        for chunk_start in chunks.iter() {
            #[cfg(not(target_os = "windows"))]
            unsafe {
                libc::madvise(
                    *chunk_start as *mut libc::c_void,
                    self.chunk_padded_size.load(Ordering::Relaxed),
                    libc::MADV_DONTNEED,
                );
            }
        }
    }

    pub fn giveback_all_memory(&self) {
        MemoryFs::flush_all_to_disk();

        loop {
            {
                // Wait for all the chunks to be freed
                if self.chunks.lock().len() == self.chunks_total_count.load(Ordering::Relaxed) {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        #[cfg(not(target_os = "windows"))]
        unsafe {
            for (buffer, chunks_count) in self.buffers_list.lock().iter() {
                libc::madvise(
                    *buffer as *mut libc::c_void,
                    chunks_count * self.chunk_padded_size.load(Ordering::Relaxed),
                    libc::MADV_DONTNEED,
                );
            }
        }
    }

    pub fn request_chunk(
        &self,
        #[cfg(feature = "track-usage")] usage: ChunkUsage_,
        #[cfg(not(feature = "track-usage"))] _: (),
    ) -> AllocatedChunk {
        let mut tries_count = 0;
        let mut chunks_lock = self.chunks.lock();

        loop {
            let el = chunks_lock.pop();
            let free_count = chunks_lock.len();
            drop(chunks_lock);

            match el.map(|chunk| AllocatedChunk {
                memory: chunk,
                len: AtomicUsize::new(0),
                max_len_log2: self.chunks_log_size.load(Ordering::Relaxed),
                #[cfg(feature = "track-usage")]
                usage: usage.clone(),
                dealloc_fn: |ptr, _size_log2| {
                    CHUNKS_ALLOCATOR.chunks.lock().push(ptr);
                    CHUNKS_ALLOCATOR.chunks_wait_condvar.notify_one();
                },
            }) {
                None => {
                    if !MemoryFs::reduce_pressure() {
                        tries_count += 1;
                    }

                    chunks_lock = self.chunks.lock();
                    if chunks_lock.len() == 0 {
                        if !self
                            .chunks_wait_condvar
                            .wait_for(&mut chunks_lock, Duration::from_millis(500))
                            .timed_out()
                        {
                            tries_count = 0;
                            continue;
                        }
                    }

                    if tries_count > 10 {
                        #[cfg(feature = "track-usage")]
                        {
                            MemoryFileInternal::debug_dump_files();
                            println!(
                                "Usages: {:?}",
                                USAGE_MAP
                                    .lock()
                                    .as_ref()
                                    .unwrap()
                                    .iter()
                                    .filter(|x| *x.1 != 0)
                                    .map(|x| format!("{:?}", x))
                                    .collect::<Vec<_>>()
                                    .join("\n")
                            )
                        }

                        // We're out of memory, let's try to allocate another chunk
                        if let Some(mut free_chunk_pool) = self.chunks.try_lock() {
                            let alloc_multiplier =
                                1 << (self.buffers_list.lock().len().saturating_sub(1));

                            let extra_chunks_count = OUT_OF_MEMORY_ALLOCATION_SIZE.as_bytes()
                                * alloc_multiplier
                                / self.chunk_usable_size.load(Ordering::Relaxed);
                            let chunks_iter = self.allocate_contiguous_chunk(extra_chunks_count);
                            free_chunk_pool.extend(chunks_iter);
                            println!(
                                "Allocated {} extra chunks for temporary files ({})",
                                extra_chunks_count,
                                OUT_OF_MEMORY_ALLOCATION_SIZE * alloc_multiplier as f64
                            );
                        }
                        // Reset the tries counter
                        tries_count = 0;
                    }
                }
                Some(chunk) => {
                    self.min_free_chunks
                        .fetch_min(free_count, Ordering::Relaxed);
                    #[cfg(feature = "track-usage")]
                    {
                        *USAGE_MAP
                            .lock()
                            .as_mut()
                            .unwrap()
                            .entry(usage.clone())
                            .or_insert(0) += 1;
                    }

                    return chunk;
                }
            }
        }
    }

    pub fn get_free_memory(&self) -> MemoryDataSize {
        MemoryDataSize::from_octets(
            (self.chunks.lock().len() * self.chunk_usable_size.load(Ordering::Relaxed)) as f64,
        )
    }

    pub fn get_reserved_memory(&self) -> MemoryDataSize {
        MemoryDataSize::from_octets(
            ((self.chunks_total_count.load(Ordering::Relaxed)
                - self.min_free_chunks.load(Ordering::Relaxed))
                * self.chunk_usable_size.load(Ordering::Relaxed)) as f64,
        )
    }

    pub fn get_total_memory(&self) -> MemoryDataSize {
        MemoryDataSize::from_octets(
            (self.chunks_total_count.load(Ordering::Relaxed)
                * self.chunk_usable_size.load(Ordering::Relaxed)) as f64,
        )
    }

    pub fn deinitialize(&self) {
        let mut chunks = self.chunks.lock();

        let mut counter = 0;
        // Wait for the chunks to be written on disk
        while chunks.len() != self.chunks_total_count.load(Ordering::Relaxed) {
            drop(chunks);
            std::thread::sleep(Duration::from_millis(200));

            counter += 1;
            if counter % 256 == 0 {
                println!("WARNING: Cannot flush all the data!");
            }

            chunks = self.chunks.lock();
        }

        unsafe {
            FILES_FLUSH_HASH_MAP.take();
        }

        {
            chunks.clear();
            self.chunks_total_count.swap(0, Ordering::Relaxed);

            for (addr, chunks_count) in self.buffers_list.lock().drain(..) {
                unsafe {
                    dealloc(
                        addr as *mut u8,
                        Layout::from_size_align_unchecked(
                            chunks_count * self.chunk_padded_size.load(Ordering::Relaxed),
                            ALLOCATOR_ALIGN,
                        ),
                    )
                }
            }
        }
    }
}

pub static CHUNKS_ALLOCATOR: ChunksAllocator = ChunksAllocator::new();

#[cfg(test)]
mod tests {
    use crate::memory_data_size::MemoryDataSize;
    use crate::memory_fs::allocator::CHUNKS_ALLOCATOR;
    use rayon::prelude::*;

    #[test]
    fn allocate_memory() {
        CHUNKS_ALLOCATOR.initialize(MemoryDataSize::from_gibioctets(8), 22, 0);
        for i in 0..5 {
            let mut allocated_chunks: Vec<_> = std::iter::from_fn(move || {
                Some(CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(TemporarySpace)))
            })
            .take(1024 * 2)
            .collect();

            allocated_chunks.par_iter_mut().for_each(|x| {
                x.zero_memory();
            });
        }
        CHUNKS_ALLOCATOR.deinitialize();
    }
}
