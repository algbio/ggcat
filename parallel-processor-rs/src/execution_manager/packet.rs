use crate::execution_manager::executor::{Executor, ExecutorAddress, OutputPacketTrait};
use crossbeam::channel::*;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

pub struct PacketsPool<T> {
    channel: (Sender<T>, Receiver<T>),
}

impl<T: OutputPacketTrait> PacketsPool<T> {
    pub fn new(cap: usize, init_data: T::InitData) -> Self {
        let channel = bounded(cap);

        for _ in 0..cap {
            channel.0.send(T::allocate_new(&init_data)).unwrap();
        }

        Self { channel }
    }

    pub fn alloc_packet(self: &Arc<Self>) -> Packet<T> {
        Packet {
            value: MaybeUninit::new(self.channel.1.recv().unwrap()),
            ref_pool: Some(Arc::downgrade(self)),
        }
    }
}

impl<T> PacketsPool<T> {
    fn return_element(&self, element: T) {
        self.channel.0.send(element).unwrap();
    }
}

pub struct Packet<T> {
    value: MaybeUninit<T>,
    ref_pool: Option<Weak<PacketsPool<T>>>,
}

impl<T> Packet<T> {
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

impl<T> Drop for Packet<T> {
    fn drop(&mut self) {
        if let Some(pool) = &mut self.ref_pool {
            if let Some(pool) = pool.upgrade() {
                pool.return_element(unsafe { self.value.assume_init_read() });
            }
        }
    }
}
