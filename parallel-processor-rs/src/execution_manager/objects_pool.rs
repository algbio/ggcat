use crate::execution_manager::executor::Executor;
use crossbeam::channel::*;
use std::mem::MaybeUninit;
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
    pub(crate) channel: (Sender<T>, Receiver<T>),
    allocate_fn: Box<dyn (Fn() -> T) + Sync + Send>,
    allocated_count: AtomicU64,
    max_count: u64,
    strict_capacity: bool,
}

impl<T: PoolObjectTrait> ObjectsPool<T> {
    pub fn new(cap: usize, strict_capacity: bool, init_data: T::InitData) -> Self {
        let channel = bounded(cap);

        Self {
            channel,
            allocate_fn: Box::new(move || T::allocate_new(&init_data)),
            allocated_count: AtomicU64::new(0),
            max_count: cap as u64,
            strict_capacity,
        }
    }

    pub fn alloc_object(&self) -> PoolObject<T> {
        match self.channel.1.try_recv() {
            Ok(mut el) => {
                el.reset();
                PoolObject::from_element(el, self)
            }
            Err(_) => {
                if !self.strict_capacity
                    || self.allocated_count.fetch_add(1, Ordering::Relaxed) < self.max_count
                {
                    PoolObject::from_element((self.allocate_fn)(), self)
                } else {
                    let mut el = self.channel.1.recv().unwrap();
                    el.reset();
                    PoolObject::from_element(el, self)
                }
            }
        }
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
    pub(crate) ref_pool: Option<Sender<T>>,
}

impl<T> PoolObject<T> {
    fn from_element(value: T, pool: &ObjectsPool<T>) -> Self {
        Self {
            value: MaybeUninit::new(value),
            ref_pool: Some(pool.channel.0.clone()),
        }
    }

    pub fn new_simple(value: T) -> Self {
        Self {
            value: MaybeUninit::new(value),
            ref_pool: None,
        }
    }

    #[inline(always)]
    pub fn get_value(&self) -> &T {
        unsafe { self.value.assume_init_ref() }
    }

    #[inline(always)]
    pub fn get_value_mut(&mut self) -> &mut T {
        unsafe { self.value.assume_init_mut() }
    }
}

impl<T> Drop for PoolObject<T> {
    fn drop(&mut self) {
        if let Some(pool_channel) = &mut self.ref_pool {
            let _ = pool_channel.send(unsafe { self.value.assume_init_read() });
        } else {
            unsafe { self.value.assume_init_drop() }
        }
    }
}
