use crate::memory_fs::FAKE_FLUSH;
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

pub trait CanSend: DecrementOnDrop {}
pub trait CanRecv: DecrementOnDrop {}
pub trait DecrementOnDrop {
    const DECREMENT: bool;
}

pub struct UnlockedMode(());
impl CanSend for UnlockedMode {}
impl CanRecv for UnlockedMode {}
impl DecrementOnDrop for UnlockedMode {
    const DECREMENT: bool = true;
}

pub struct SendMode(());
impl CanSend for SendMode {}
impl DecrementOnDrop for SendMode {
    const DECREMENT: bool = true;
}

pub struct RecvMode(());
impl CanRecv for RecvMode {}
impl DecrementOnDrop for RecvMode {
    const DECREMENT: bool = false;
}

struct _Channel<T> {
    data: Mutex<VecDeque<T>>,
    condvar: Condvar,
    senders_active: AtomicUsize,
}

pub struct Channel<M: DecrementOnDrop, T> {
    channel: Arc<_Channel<T>>,
    _phantom: PhantomData<M>,
}

impl<M: DecrementOnDrop, T> Channel<M, T> {
    pub fn len(&self) -> usize {
        self.channel.data.lock().len()
    }
}

impl<T> Channel<UnlockedMode, T> {
    pub fn new(capacity: usize) -> Channel<UnlockedMode, T> {
        Self {
            channel: Arc::new(_Channel {
                data: Mutex::new(VecDeque::with_capacity(capacity)),
                condvar: Condvar::new(),
                senders_active: AtomicUsize::new(1),
            }),
            _phantom: Default::default(),
        }
    }

    pub fn sender(&self) -> Channel<SendMode, T> {
        self.channel.senders_active.fetch_add(1, Ordering::Relaxed);
        Channel {
            channel: self.channel.clone(),
            _phantom: PhantomData,
        }
    }

    pub fn receiver(&self) -> Channel<RecvMode, T> {
        Channel {
            channel: self.channel.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Clone for Channel<UnlockedMode, T> {
    fn clone(&self) -> Self {
        self.channel.senders_active.fetch_add(1, Ordering::Relaxed);
        Self {
            channel: self.channel.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<M: CanSend, T> Channel<M, T> {
    pub fn send(&self, el: T) {
        let mut channel = self.channel.data.lock();
        while channel.capacity() == channel.len() {
            self.channel.condvar.wait(&mut channel);
        }
        channel.push_back(el);
        if channel.len() == 1 {
            self.channel.condvar.notify_one();
        }
    }

    pub fn send_force(&self, el: T) {
        self.channel.data.lock().push_back(el)
    }

    pub fn send_opt(&self, el: T) -> bool {
        let mut channel = self.channel.data.lock();
        if channel.len() <= channel.capacity() {
            channel.push_back(el);
            true
        } else {
            false
        }
    }
}

impl<M: CanRecv, T> Channel<M, T> {
    pub fn recv_force(&self) -> T {
        self.channel.data.lock().pop_back().unwrap()
    }

    pub fn recv_opt(&self) -> Option<T> {
        self.channel.data.lock().pop_back()
    }

    pub fn recv(&self) -> Result<T, ()> {
        if unsafe { FAKE_FLUSH } {
            assert!(!self.channel.data.is_locked());
        }

        let mut channel = self.channel.data.lock();
        while channel.len() == 0 {
            if self.channel.senders_active.load(Ordering::Relaxed) == 0 {
                return Err(());
            }
            self.channel.condvar.wait(&mut channel);
        }
        let el = channel.pop_back().unwrap();
        if channel.len() == channel.capacity() - 1 {
            self.channel.condvar.notify_one();
        }
        Ok(el)
    }
}

impl<M: DecrementOnDrop, T> Drop for Channel<M, T> {
    fn drop(&mut self) {
        if M::DECREMENT {
            self.channel.senders_active.fetch_sub(1, Ordering::Relaxed);
            self.channel.condvar.notify_all();
        }
    }
}
