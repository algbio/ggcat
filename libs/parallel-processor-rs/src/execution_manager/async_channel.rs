use crate::execution_manager::objects_pool::PoolObjectTrait;
use crossbeam::queue::SegQueue;
use parking_lot::{Condvar, Mutex};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub struct ReceiverFuture<'a, T: Sync + Send + 'static> {
    internal: &'a AsyncChannelInternal<T>,
    stream_index: u64,
}

impl<'a, T: Sync + Send + 'static> Future for ReceiverFuture<'a, T> {
    type Output = Result<T, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.internal.packets.pop() {
            None => {
                if self.internal.stream_index.load(Ordering::Relaxed) != self.stream_index {
                    Poll::Ready(Err(()))
                } else {
                    self.internal.waiting_list.push(cx.waker().clone());
                    Poll::Pending
                }
            }
            Some(value) => Poll::Ready(Ok(value)),
        }
    }
}

struct AsyncChannelInternal<T: Sync + Send + 'static> {
    packets: SegQueue<T>,
    waiting_list: SegQueue<Waker>,
    blocking_mutex: Mutex<()>,
    blocking_condvar: Condvar,
    max_capacity: usize,
    stream_index: AtomicU64,
}

pub(crate) struct AsyncChannel<T: Sync + Send + 'static> {
    internal: Arc<AsyncChannelInternal<T>>,
    stream_index: AtomicU64,
}

impl<T: Sync + Send + 'static> Clone for AsyncChannel<T> {
    fn clone(&self) -> Self {
        Self {
            internal: self.internal.clone(),
            stream_index: AtomicU64::new(self.stream_index.load(Ordering::Relaxed)),
        }
    }
}

impl<T: Sync + Send + 'static> AsyncChannel<T> {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            internal: Arc::new(AsyncChannelInternal {
                packets: SegQueue::new(),
                waiting_list: SegQueue::new(),
                blocking_mutex: Mutex::new(()),
                blocking_condvar: Condvar::new(),
                max_capacity,
                stream_index: AtomicU64::new(0),
            }),
            stream_index: AtomicU64::new(0),
        }
    }

    pub fn recv(&self) -> ReceiverFuture<T> {
        ReceiverFuture {
            internal: &self.internal,
            stream_index: self.stream_index.load(Ordering::Relaxed),
        }
    }

    pub fn try_recv(&self) -> Option<T> {
        self.internal.packets.pop()
    }

    pub fn recv_blocking(&self) -> Result<T, ()> {
        match self.internal.packets.pop() {
            None => {
                let stream_index = self.stream_index.load(Ordering::Relaxed);
                let mut lock_mutex = self.internal.blocking_mutex.lock();
                loop {
                    if self.internal.stream_index.load(Ordering::Relaxed) != stream_index {
                        return Err(());
                    }
                    if let Some(packet) = self.internal.packets.pop() {
                        return Ok(packet);
                    }
                    self.internal.blocking_condvar.wait(&mut lock_mutex);
                }
            }
            Some(packet) => Ok(packet),
        }
    }

    pub fn reopen(&self) {
        self.stream_index.store(
            self.internal.stream_index.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }

    pub fn release(&self) {
        self.internal.stream_index.fetch_add(1, Ordering::SeqCst) + 1;

        while let Some(waker) = self.internal.waiting_list.pop() {
            waker.wake();
        }
        self.internal.blocking_condvar.notify_all();
    }

    pub fn send(&self, value: T, limit_size: bool) {
        let packets_len = self.internal.packets.len();
        if !limit_size || packets_len < self.internal.max_capacity {
            self.internal.packets.push(value);

            for _ in 0..self.internal.packets.len() {
                if let Some(waker) = self.internal.waiting_list.pop() {
                    waker.wake();
                } else {
                    break;
                }
            }
            if packets_len == 0 {
                self.internal.blocking_condvar.notify_one();
            } else {
                self.internal.blocking_condvar.notify_all();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.internal.packets.len()
    }
}
