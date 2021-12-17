use crate::utils::flexible_pool::{FlexiblePool, PoolableObject};
use parallel_processor::multi_thread_buckets::BucketType;
use parking_lot::{RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cell::UnsafeCell;
use std::cmp::{max, min};
use std::intrinsics::unlikely;
use std::mem::{size_of, MaybeUninit};
use std::path::PathBuf;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct AsyncVecInner<T> {
    buffer: UnsafeCell<Vec<MaybeUninit<T>>>,
    writing_length: AtomicUsize,
}

pub struct AsyncVec<T> {
    inner: RwLock<AsyncVecInner<T>>,
    index: usize,
    on_finalize: Option<Arc<Box<dyn Fn(Self)>>>,
}

unsafe impl<T> Sync for AsyncVec<T> {}
unsafe impl<T> Send for AsyncVec<T> {}

impl<T> PoolableObject for AsyncVec<T> {
    type AllocData = usize;

    fn allocate(capacity: Self::AllocData) -> Self {
        Self::with_capacity(capacity)
    }

    fn reinitialize(&mut self) {
        self.clear();
    }
}

impl<T: Send> BucketType for AsyncVec<T> {
    type InitType = (FlexiblePool<Self>, Arc<Box<dyn Fn(Self)>>);
    type DataType = T;

    const SUPPORTS_LOCK_FREE: bool = true;

    fn new((pool, on_finalize): &Self::InitType, index: usize) -> Self {
        let mut self_ = pool.take_object_owned();
        self_.index = index;
        self_.on_finalize = Some(on_finalize.clone());
        self_
    }

    fn write_data(&mut self, _data: &[T]) {
        panic!("Not supported!");
    }

    fn write_data_lock_free(&self, data: &[T]) {
        self.push_async_slice(data)
    }

    fn get_path(&self) -> PathBuf {
        panic!("Not supported!");
    }

    fn finalize(mut self) {
        let finalizer = self.on_finalize.take();
        if let Some(finalizer) = finalizer {
            (finalizer)(self);
        }
    }
}

impl<T> AsyncVec<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(AsyncVecInner {
                buffer: UnsafeCell::new({
                    let mut vec = Vec::with_capacity(max(32, capacity));
                    unsafe {
                        vec.set_len(vec.capacity());
                    }
                    vec
                }),
                writing_length: AtomicUsize::new(0),
            }),
            index: 0,
            on_finalize: None,
        }
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    #[inline]
    pub fn clear(&self) {
        let buf = self.inner.write();
        buf.writing_length.store(0, Ordering::Relaxed);
    }

    #[inline]
    pub fn push_async(&self, value: T) {
        self.push_async_slice(unsafe { from_raw_parts(&value as *const T, 1) });
    }

    #[inline]
    pub fn push_async_slice(&self, value: &[T]) {
        let mut buf = self.inner.read();
        let position = buf.writing_length.fetch_add(value.len(), Ordering::Relaxed);

        let buffer = unsafe { &mut *(buf.buffer.get()) };

        #[cold]
        fn reallocate_vec<'a, 'b: 'a, T>(
            buf: RwLockReadGuard<'a, AsyncVecInner<T>>,
            self_: &'b AsyncVec<T>,
            position: usize,
            len: usize,
        ) -> RwLockReadGuard<'a, AsyncVecInner<T>> {
            drop(buf);

            let mut wbuf = self_.inner.write();

            let buffer = unsafe { &mut *(wbuf.buffer.get()) };

            let cap = buffer.capacity();

            buffer.reserve(max(cap, position + len) - cap);
            unsafe {
                buffer.set_len(buffer.capacity());
            }

            RwLockWriteGuard::downgrade(wbuf)
        }

        if buffer.capacity() < (position + value.len()) {
            buf = reallocate_vec(buf, self, position, value.len());
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                (buffer.as_mut_ptr() as *mut T).add(position),
                value.len(),
            );
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_slice_mut(&self) -> &mut [T] {
        let inner = self.inner.write();

        unsafe {
            from_raw_parts_mut(
                (*inner.buffer.get()).as_mut_ptr() as *mut T,
                inner.writing_length.load(Ordering::Relaxed),
            )
        }
    }
}
