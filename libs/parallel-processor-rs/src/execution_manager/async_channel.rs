use crate::execution_manager::objects_pool::PoolObjectTrait;
use crossbeam::queue::SegQueue;
use parking_lot::{Condvar, Mutex};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub struct ReceiverFuture<'a, T: Sync + Send + 'static, const CHANNELS_COUNT: usize> {
    internal: &'a AsyncChannelInternal<T, CHANNELS_COUNT>,
    offset: usize,
    stream_index: u64,
}

#[inline(always)]
fn try_get_item<T: Sync + Send + 'static, const CHANNELS_COUNT: usize>(
    internal: &AsyncChannelInternal<T, CHANNELS_COUNT>,
    offset: usize,
) -> Option<T> {
    if let Some(packet) = internal.packets[offset..]
        .iter()
        .map(|ch| ch.pop())
        .filter(|p| p.is_some())
        .next()
        .flatten()
    {
        return Some(packet);
    }
    internal.packets[..offset]
        .iter()
        .map(|ch| ch.pop())
        .filter(|p| p.is_some())
        .next()
        .flatten()
}

impl<'a, T: Sync + Send + 'static, const CHANNELS_COUNT: usize> Future
    for ReceiverFuture<'a, T, CHANNELS_COUNT>
{
    type Output = Result<T, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match try_get_item(self.internal, self.offset) {
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

struct AsyncChannelInternal<T: Sync + Send + 'static, const CHANNELS_COUNT: usize> {
    packets: [SegQueue<T>; CHANNELS_COUNT],
    waiting_list: SegQueue<Waker>,
    blocking_mutex: Mutex<()>,
    blocking_condvar: Condvar,
    max_capacity: usize,
    stream_index: AtomicU64,
}

pub(crate) struct MultiplePriorityAsyncChannel<
    T: Sync + Send + 'static,
    const CHANNELS_COUNT: usize,
> {
    internal: Arc<AsyncChannelInternal<T, CHANNELS_COUNT>>,
    stream_index: AtomicU64,
}

impl<T: Sync + Send + 'static, const CHANNELS_COUNT: usize> Clone
    for MultiplePriorityAsyncChannel<T, CHANNELS_COUNT>
{
    fn clone(&self) -> Self {
        Self {
            internal: self.internal.clone(),
            stream_index: AtomicU64::new(self.stream_index.load(Ordering::Relaxed)),
        }
    }
}

impl<T: Sync + Send + 'static, const CHANNELS_COUNT: usize>
    MultiplePriorityAsyncChannel<T, CHANNELS_COUNT>
{
    pub fn new(max_capacity: usize) -> Self {
        Self {
            internal: Arc::new(AsyncChannelInternal {
                packets: [(); CHANNELS_COUNT].map(|_| SegQueue::new()),
                waiting_list: SegQueue::new(),
                blocking_mutex: Mutex::new(()),
                blocking_condvar: Condvar::new(),
                max_capacity,
                stream_index: AtomicU64::new(0),
            }),
            stream_index: AtomicU64::new(0),
        }
    }

    pub fn recv(&self) -> ReceiverFuture<T, CHANNELS_COUNT> {
        self.recv_offset(0)
    }

    pub fn recv_offset(&self, offset: usize) -> ReceiverFuture<T, CHANNELS_COUNT> {
        ReceiverFuture {
            internal: &self.internal,
            offset,
            stream_index: self.stream_index.load(Ordering::Relaxed),
        }
    }

    pub fn try_recv(&self) -> Option<T> {
        try_get_item(&self.internal, 0)
    }

    pub fn recv_blocking(&self) -> Result<T, ()> {
        match try_get_item(&self.internal, 0) {
            None => {
                let stream_index = self.stream_index.load(Ordering::Relaxed);
                let mut lock_mutex = self.internal.blocking_mutex.lock();
                loop {
                    if self.internal.stream_index.load(Ordering::Relaxed) != stream_index {
                        return Err(());
                    }
                    if let Some(packet) = try_get_item(&self.internal, 0) {
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

    pub fn send_with_priority(&self, value: T, priority: usize, limit_size: bool) {
        let packets_len: usize = self.internal.packets.iter().map(|p| p.len()).sum();
        if !limit_size || packets_len < self.internal.max_capacity {
            self.internal.packets[priority].push(value);

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

pub(crate) type AsyncChannel<T> = MultiplePriorityAsyncChannel<T, 1>;

impl<T: Sync + Send + 'static> AsyncChannel<T> {
    pub fn send(&self, value: T, limit_size: bool) {
        self.send_with_priority(value, 0, limit_size);
    }
}

pub(crate) type DoublePriorityAsyncChannel<T> = MultiplePriorityAsyncChannel<T, 2>;

impl<T: Sync + Send + 'static> DoublePriorityAsyncChannel<T> {
    pub fn send(&self, value: T, limit_size: bool, high_priority: bool) {
        self.send_with_priority(value, if high_priority { 0 } else { 1 }, limit_size);
    }
}
