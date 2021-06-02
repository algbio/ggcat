use parking_lot::lock_api::RwLockWriteGuard;
use parking_lot::{Mutex, RwLock};
use std::cell::UnsafeCell;

pub struct ConcurrentMemoryChunk {
    data: UnsafeCell<Vec<u8>>,
    position: Mutex<usize>,
    realloc_guard: RwLock<()>,
}

unsafe impl Sync for ConcurrentMemoryChunk {}
unsafe impl Send for ConcurrentMemoryChunk {}

impl ConcurrentMemoryChunk {
    pub fn from_memory(memory: Vec<u8>) -> ConcurrentMemoryChunk {
        let len = memory.len();
        ConcurrentMemoryChunk {
            data: UnsafeCell::new(memory),
            position: Mutex::new(len),
            realloc_guard: RwLock::new(()),
        }
    }

    pub fn new(capacity: usize) -> ConcurrentMemoryChunk {
        let mut vec = Vec::with_capacity(capacity);
        unsafe {
            vec.set_len(capacity);
        }

        ConcurrentMemoryChunk {
            data: UnsafeCell::new(vec),
            position: Mutex::new(0),
            realloc_guard: RwLock::new(()),
        }
    }

    pub fn get(&self) -> &[u8] {
        let position = self.position.lock();
        unsafe { &(*self.data.get())[0..*position] }
    }

    pub fn len(&self) -> usize {
        *self.position.lock()
    }

    pub fn capacity(&self) -> usize {
        let _position = self.position.lock();
        unsafe { (*self.data.get()).len() }
    }

    pub unsafe fn get_mut(&self) -> &mut [u8] {
        let position = self.position.lock();
        unsafe { &mut (*self.data.get())[0..*position] }
    }

    pub fn clear(&self) {
        *self.position.lock() = 0;
    }

    pub fn write_bytes_noextend(&self, bytes: &[u8]) -> Result<(), ()> {
        let wsafe_lock;
        let write_position;

        let mut position_lock = self.position.lock();
        if unsafe { (*self.data.get()).len() } < *position_lock + bytes.len() {
            return Err(());
        } else {
            write_position = *position_lock;
            *position_lock += bytes.len();
            wsafe_lock = self.realloc_guard.read();
            drop(position_lock);
        }

        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                (*self.data.get()).as_mut_ptr().add(write_position),
                bytes.len(),
            );
        }
        drop(wsafe_lock);
        Ok(())
    }

    pub fn has_space_for(&self, len: usize) -> bool {
        let position_lock = self.position.lock();
        return *position_lock + len <= unsafe { (*self.data.get()).len() };
    }

    pub fn write_bytes_extend(&self, bytes: &[u8]) -> bool {
        let mut position_lock = self.position.lock();
        let write_position;
        let wsafe_lock;
        let mut was_resized = false;

        // Reallocation needed
        if unsafe { (*self.data.get()).len() } < *position_lock + bytes.len() {
            let wrealloc_lock = self.realloc_guard.write();
            let cap;
            let len;
            unsafe {
                (*self.data.get()).set_len(*position_lock);
                (*self.data.get()).reserve_exact((*position_lock / 10) + bytes.len());
                (*self.data.get()).set_len((*self.data.get()).capacity());
                write_position = *position_lock;
                *position_lock += bytes.len();

                cap = (*self.data.get()).len();
                len = write_position;

                drop(position_lock);
            }
            wsafe_lock = RwLockWriteGuard::downgrade(wrealloc_lock);
            was_resized = true;
        }
        // Allowed to write to this position
        else {
            write_position = *position_lock;
            *position_lock += bytes.len();
            wsafe_lock = self.realloc_guard.read();
            drop(position_lock);
        }

        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                (*self.data.get()).as_mut_ptr().add(write_position),
                bytes.len(),
            );
        }
        drop(wsafe_lock);
        was_resized
    }
}
