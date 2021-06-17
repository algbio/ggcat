use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::{MemoryFile, FILES_FLUSH_HASH_MAP};
use parking_lot::lock_api::Mutex;
use parking_lot::RawMutex;
use rayon::iter::{
    IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelBridge, ParallelIterator,
};
use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::path::PathBuf;
use std::ptr::null_mut;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub const ALLOCATED_CHUNK_USABLE_SIZE: usize = 1024 * 1024 * 4; // 4MB chunk buffer size
const ALLOCATED_CHUNK_PADDED_SIZE: usize = ALLOCATED_CHUNK_USABLE_SIZE
    + if cfg!(feature = "memory-guards") {
        4096
    } else {
        0
    };

const ALLOCATED_CHUNK_SIZE_PADDED_MEM: MemoryDataSize =
    MemoryDataSize::from_octets(ALLOCATED_CHUNK_PADDED_SIZE as f64);
pub const ALLOCATED_CHUNK_SIZE_USABLE_MEM: MemoryDataSize =
    MemoryDataSize::from_octets(ALLOCATED_CHUNK_USABLE_SIZE as f64);

const ALLOCATOR_ALIGN: usize = 4096;

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
    #[cfg(feature = "track-usage")]
    usage: ChunkUsage_,
}
unsafe impl Sync for AllocatedChunk {}
unsafe impl Send for AllocatedChunk {}

const OVERPROVISIONING_FACTOR: f64 = 1.07;

impl Clone for AllocatedChunk {
    #[track_caller]
    fn clone(&self) -> Self {
        panic!("This method should not be called, check for vector cloning!");
    }
}

impl AllocatedChunk {
    #[inline(always)]
    fn zero_memory(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.memory as *mut u8, 0, ALLOCATED_CHUNK_USABLE_SIZE);
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

    pub fn write_bytes_noextend(&self, data: &[u8]) -> bool {
        let result = self
            .len
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value + data.len() <= ALLOCATED_CHUNK_USABLE_SIZE {
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
                true
            }
            Err(_) => false,
        }
    }

    #[inline(always)]
    pub fn has_space_for(&self, len: usize) -> bool {
        self.len.load(Ordering::Relaxed) + len <= ALLOCATED_CHUNK_USABLE_SIZE
    }

    #[inline(always)]
    pub unsafe fn get_mut_slice(&self) -> &'static mut [u8] {
        unsafe { from_raw_parts_mut(self.memory as *mut u8, self.len.load(Ordering::Relaxed)) }
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
    pub fn remaining_bytes(&self) -> usize {
        ALLOCATED_CHUNK_USABLE_SIZE - self.len.load(Ordering::Relaxed)
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
        CHUNKS_ALLOCATOR.chunks.lock().push(self.memory);

        #[cfg(feature = "track-usage")]
        {
            USAGE_MAP
                .lock()
                .as_mut()
                .unwrap()
                .entry(self.usage.clone())
                .and_modify(|x| *x -= 1);
        }
    }
}

pub struct ChunksAllocator {
    big_buffer_start_addr: AtomicUsize,
    chunks: Mutex<RawMutex, Vec<usize>>,
    chunks_total_count: AtomicUsize,
}
unsafe impl Sync for ChunksAllocator {}
unsafe impl Send for ChunksAllocator {}

#[cfg(feature = "track-usage")]
static USAGE_MAP: Mutex<RawMutex, Option<HashMap<ChunkUsage_, usize>>> = Mutex::new(None);

impl ChunksAllocator {
    const fn new() -> ChunksAllocator {
        ChunksAllocator {
            big_buffer_start_addr: AtomicUsize::new(0),
            chunks: Mutex::new(Vec::new()),
            chunks_total_count: AtomicUsize::new(0),
        }
    }

    pub fn initialize(&self, memory: MemoryDataSize) {
        #[cfg(feature = "track-usage")]
        {
            *USAGE_MAP.lock() = Some(HashMap::new());
        }
        if self.big_buffer_start_addr.load(Ordering::Relaxed) != 0 {
            // Already allocated
            return;
        }

        let chunks_count =
            (memory * OVERPROVISIONING_FACTOR / ALLOCATED_CHUNK_SIZE_PADDED_MEM) as usize;

        self.chunks_total_count
            .store(chunks_count, Ordering::Relaxed);

        let data = unsafe {
            alloc(Layout::from_size_align_unchecked(
                chunks_count * ALLOCATED_CHUNK_PADDED_SIZE,
                ALLOCATOR_ALIGN,
            ))
        };

        let free_chunks: Vec<_> = (0..chunks_count)
            .into_iter()
            .rev()
            .map(|c| data as usize + (c * ALLOCATED_CHUNK_PADDED_SIZE))
            .collect();

        #[cfg(feature = "memory-guards")]
        unsafe {
            let first_guard = data.add(ALLOCATED_CHUNK_USABLE_SIZE);
            for i in 0..chunks_count {
                let guard = first_guard.add(i * ALLOCATED_CHUNK_PADDED_SIZE);
                libc::mprotect(guard as *mut libc::c_void, 4096, libc::PROT_NONE);
            }
        }

        self.big_buffer_start_addr
            .store(data as usize, Ordering::Relaxed);
        *self.chunks.lock() = free_chunks;
    }

    pub fn giveback_memory(&self) {
        MemoryFile::flush_to_disk();

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
            libc::madvise(
                self.big_buffer_start_addr.load(Ordering::Relaxed) as *mut libc::c_void,
                self.chunks_total_count.load(Ordering::Relaxed) * ALLOCATED_CHUNK_PADDED_SIZE,
                libc::MADV_DONTNEED,
            );
        }
    }

    pub fn request_chunk(
        &self,
        #[cfg(feature = "track-usage")] usage: ChunkUsage_,
        #[cfg(not(feature = "track-usage"))] _: (),
    ) -> AllocatedChunk {
        match self.chunks.lock().pop().map(|chunk| AllocatedChunk {
            memory: chunk,
            len: AtomicUsize::new(0),
            #[cfg(feature = "track-usage")]
            usage: usage.clone(),
        }) {
            None => {
                #[cfg(feature = "track-usage")]
                {
                    panic!(
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
                panic!("Out of memory!");
            }
            Some(chunk) => {
                #[cfg(feature = "track-usage")]
                {
                    *USAGE_MAP
                        .lock()
                        .as_mut()
                        .unwrap()
                        .entry(usage.clone())
                        .or_insert(0) += 1;
                }

                chunk
            }
        }
    }

    pub fn get_free_memory(&self) -> MemoryDataSize {
        MemoryDataSize::from_octets((self.chunks.lock().len() * ALLOCATED_CHUNK_USABLE_SIZE) as f64)
    }

    pub fn get_total_memory(&self) -> MemoryDataSize {
        MemoryDataSize::from_octets(
            (self.chunks_total_count.load(Ordering::Relaxed) * ALLOCATED_CHUNK_USABLE_SIZE) as f64,
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
            let chunks_count = self.chunks_total_count.swap(0, Ordering::Relaxed);

            let addr = self.big_buffer_start_addr.swap(0, Ordering::Relaxed);
            if addr != 0 {
                unsafe {
                    dealloc(
                        addr as *mut u8,
                        Layout::from_size_align_unchecked(
                            chunks_count * ALLOCATED_CHUNK_PADDED_SIZE,
                            ALLOCATOR_ALIGN,
                        ),
                    )
                }
            }
        }
    }
}

pub static CHUNKS_ALLOCATOR: ChunksAllocator = ChunksAllocator::new();

#[test]
fn allocate_memory() {
    CHUNKS_ALLOCATOR.initialize(MemoryDataSize::from_gibioctets(64.0));
    for i in 0..5 {
        let mut allocated_chunks: Vec<_> =
            std::iter::from_fn(move || CHUNKS_ALLOCATOR.request_chunk(ChunkUsage_::TemporarySpace))
                .collect();

        allocated_chunks.par_iter_mut().for_each(|x| {
            x.zero_memory();
        });
    }
    CHUNKS_ALLOCATOR.deinitialize().unwrap();
}
