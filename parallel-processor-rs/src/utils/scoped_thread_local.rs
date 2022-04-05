use dashmap::DashMap;
use lazy_static::lazy_static;
use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::ThreadId;

struct ThreadVarRef {
    taken: AtomicBool,
    val: UnsafeCell<Box<dyn Any>>,
}
unsafe impl Sync for ThreadVarRef {}
unsafe impl Send for ThreadVarRef {}

lazy_static! {
    static ref THREADS_MAP: DashMap<u64, DashMap<ThreadId, ThreadVarRef>> = DashMap::new();
}
static THREAD_LOCAL_VAR_INDEX: AtomicU64 = AtomicU64::new(0);

pub struct ThreadLocalVariable<'a, T> {
    var: &'a mut T,
    index: u64,
    _not_send_sync: PhantomData<*mut ()>,
}

impl<'a, T> Deref for ThreadLocalVariable<'a, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.var
    }
}

impl<'a, T> DerefMut for ThreadLocalVariable<'a, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.var
    }
}

impl<'a, T> Drop for ThreadLocalVariable<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        let obj_entry = THREADS_MAP.get(&self.index).unwrap();
        obj_entry
            .get(&std::thread::current().id())
            .unwrap()
            .taken
            .store(false, Ordering::Relaxed);
    }
}

pub struct ScopedThreadLocal<T: 'static> {
    index: u64,
    alloc: Box<dyn Fn() -> T + Send + Sync>,
}

impl<T: 'static> Drop for ScopedThreadLocal<T> {
    fn drop(&mut self) {
        THREADS_MAP.remove(&self.index);
    }
}

impl<T: 'static> ScopedThreadLocal<T> {
    pub fn new<F: Fn() -> T + Send + Sync + 'static>(alloc: F) -> Self {
        let index = THREAD_LOCAL_VAR_INDEX.fetch_add(1, Ordering::Relaxed);
        THREADS_MAP.insert(index, DashMap::new());

        Self {
            index,
            alloc: Box::new(alloc),
        }
    }

    pub fn get(&self) -> ThreadLocalVariable<T> {
        let obj_entry = THREADS_MAP.get(&self.index).unwrap();
        let entry = obj_entry
            .entry(std::thread::current().id())
            .or_insert_with(|| ThreadVarRef {
                taken: AtomicBool::new(false),
                val: UnsafeCell::new(Box::new((self.alloc)())),
            });

        if entry.taken.swap(true, Ordering::Relaxed) {
            panic!("Thread local variable taken multiple times, aborting!");
        }

        ThreadLocalVariable {
            var: unsafe { (*entry.val.get()).downcast_mut::<T>().unwrap() },
            index: self.index,
            _not_send_sync: PhantomData,
        }
    }
}
