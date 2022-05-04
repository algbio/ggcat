use crate::execution_manager::executor::Executor;
use crossbeam::channel::*;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

pub trait PoolObjectTrait: 'static {
    type InitData: Clone + Sync + Send;

    fn allocate_new(init_data: &Self::InitData) -> Self;
    fn reset(&mut self);
}

impl<T: PoolObjectTrait> PoolObjectTrait for Box<T> {
    type InitData = T::InitData;

    fn allocate_new(init_data: &Self::InitData) -> Self {
        Box::new(T::allocate_new(init_data))
    }
    fn reset(&mut self) {
        T::reset(self);
    }
}

pub struct ObjectsPool<T> {
    queue: Receiver<T>,
    pub(crate) returner: Arc<(Sender<T>, AtomicU64)>,
    allocate_fn: Box<dyn (Fn() -> T) + Sync + Send>,
    max_count: u64,
}

impl<T: PoolObjectTrait> ObjectsPool<T> {
    pub fn new(cap: usize, init_data: T::InitData) -> Self {
        let channel = bounded(cap);

        Self {
            queue: channel.1,
            returner: Arc::new((channel.0, AtomicU64::new(0))),
            allocate_fn: Box::new(move || T::allocate_new(&init_data)),
            max_count: cap as u64,
        }
    }

    pub fn alloc_object(&self) -> PoolObject<T> {
        self.returner.1.fetch_add(1, Ordering::Relaxed);
        match self.queue.try_recv() {
            Ok(mut el) => {
                el.reset();
                PoolObject::from_element(el, self)
            }
            Err(_) => {
                // println!(
                //     "Allocate scratch element {} => {}/{}",
                //     std::any::type_name::<T>(),
                //     self.returner.1.load(Ordering::Relaxed),
                //     self.max_count
                // );
                PoolObject::from_element((self.allocate_fn)(), self)
            }
        }
    }

    pub fn get_available_items(&self) -> i64 {
        (self.max_count as i64) - (self.returner.1.load(Ordering::Relaxed) as i64)
    }

    pub fn wait_for_item(&self) {
        let _ = self.returner.0.try_send(self.queue.recv().unwrap());
    }
}

pub struct PoolSender<T> {
    sender: Sender<T>,
}

impl<T> PoolSender<T> {
    pub fn send_data(&self, data: T) {
        self.sender.send(data);
    }
}

pub struct PoolObject<T> {
    pub(crate) value: MaybeUninit<T>,
    pub(crate) returner: Option<Arc<(Sender<T>, AtomicU64)>>,
}

impl<T> PoolObject<T> {
    fn from_element(value: T, pool: &ObjectsPool<T>) -> Self {
        Self {
            value: MaybeUninit::new(value),
            returner: Some(pool.returner.clone()),
        }
    }

    pub fn new_simple(value: T) -> Self {
        Self {
            value: MaybeUninit::new(value),
            returner: None,
        }
    }
}

impl<T> Deref for PoolObject<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.value.assume_init_ref() }
    }
}

impl<T> DerefMut for PoolObject<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.value.assume_init_mut() }
    }
}

impl<T> Drop for PoolObject<T> {
    fn drop(&mut self) {
        if let Some(returner) = &self.returner {
            returner.1.fetch_sub(1, Ordering::Relaxed);
            let _ = returner
                .0
                .try_send(unsafe { self.value.assume_init_read() });
        } else {
            unsafe { self.value.assume_init_drop() }
        }
    }
}
