use crossbeam::channel::*;
use std::mem::MaybeUninit;

pub struct PacketsPool<T> {
    channel: (Sender<T>, Receiver<T>),
}

impl<T> PacketsPool<T> {
    pub fn new(cap: usize) -> Self {
        Self {
            channel: bounded(cap),
        }
    }

    fn return_element(&self, element: T) {
        self.channel.0.send(element).unwrap();
    }
}

pub struct Packet<T> {
    value: MaybeUninit<T>,
    ref_pool: PacketsPool<T>,
}

impl<T> Packet<T> {
    pub fn get_value(&mut self) -> &mut T {
        unsafe { self.value.assume_init_mut() }
    }
}

impl<T> Drop for Packet<T> {
    fn drop(&mut self) {
        unsafe {
            self.ref_pool.return_element(self.value.assume_init());
        }
    }
}
