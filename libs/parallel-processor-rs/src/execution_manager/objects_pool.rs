use crate::execution_manager::async_channel::AsyncChannel;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait PoolObjectTrait: Send + Sync + 'static {
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

pub struct ObjectsPool<T: Sync + Send + 'static> {
    queue: AsyncChannel<T>,
    pub(crate) returner: Arc<(AsyncChannel<T>, AtomicU64)>,
    allocate_fn: Box<dyn (Fn() -> T) + Sync + Send>,
    max_count: u64,
}

pub trait PoolReturner<T: Send + Sync>: Send + Sync {
    fn return_element(&self, el: T);
}

impl<T: PoolObjectTrait> PoolReturner<T> for (AsyncChannel<T>, AtomicU64) {
    fn return_element(&self, mut el: T) {
        self.1.fetch_sub(1, Ordering::Relaxed);
        el.reset();
        self.0.send(el, true);
    }
}

impl<T: PoolObjectTrait> ObjectsPool<T> {
    pub fn new(cap: usize, init_data: T::InitData) -> Self {
        let channel = AsyncChannel::new(cap);

        Self {
            queue: channel.clone(),
            returner: Arc::new((channel, AtomicU64::new(0))),
            allocate_fn: Box::new(move || T::allocate_new(&init_data)),
            max_count: cap as u64,
        }
    }

    #[inline(always)]
    async fn alloc_wait(&self) -> Result<T, ()> {
        self.queue.recv().await
    }

    #[inline(always)]
    fn alloc_wait_blocking(&self) -> Result<T, ()> {
        self.queue.recv_blocking()
    }

    pub async fn alloc_object(&self) -> PoolObject<T> {
        let el_count = self.returner.1.fetch_add(1, Ordering::Relaxed);

        if el_count >= self.max_count {
            return PoolObject::from_element(self.alloc_wait().await.unwrap(), self);
        }

        match self.queue.try_recv() {
            Some(el) => PoolObject::from_element(el, self),
            None => PoolObject::from_element((self.allocate_fn)(), self),
        }
    }

    pub fn alloc_object_blocking(&self) -> PoolObject<T> {
        let el_count = self.returner.1.fetch_add(1, Ordering::Relaxed);

        if el_count >= self.max_count {
            return PoolObject::from_element(self.alloc_wait_blocking().unwrap(), self);
        }

        match self.queue.try_recv() {
            Some(el) => PoolObject::from_element(el, self),
            None => PoolObject::from_element((self.allocate_fn)(), self),
        }
    }

    pub fn alloc_object_force(&self) -> PoolObject<T> {
        self.returner.1.fetch_add(1, Ordering::Relaxed);
        match self.queue.try_recv() {
            Some(el) => PoolObject::from_element(el, self),
            None => PoolObject::from_element((self.allocate_fn)(), self),
        }
    }

    pub fn get_available_items(&self) -> i64 {
        (self.max_count as i64) - (self.returner.1.load(Ordering::Relaxed) as i64)
    }

    pub fn get_allocated_items(&self) -> i64 {
        self.returner.1.load(Ordering::Relaxed) as i64
    }

    // pub fn wait_for_item_timeout(&self, timeout: Duration) {
    //     if let Ok(recv) = self.queue.recv_timeout(timeout) {
    //         let _ = self.returner.0.try_send(recv);
    //     }
    // }
}

pub struct PoolObject<T: Send + Sync> {
    pub(crate) value: ManuallyDrop<T>,
    pub(crate) returner: Option<Arc<dyn PoolReturner<T>>>,
}

impl<T: PoolObjectTrait> PoolObject<T> {
    fn from_element(value: T, pool: &ObjectsPool<T>) -> Self {
        Self {
            value: ManuallyDrop::new(value),
            returner: Some(pool.returner.clone()),
        }
    }
}

impl<T: Send + Sync> PoolObject<T> {
    pub fn new_simple(value: T) -> Self {
        Self {
            value: ManuallyDrop::new(value),
            returner: None,
        }
    }
}

impl<T: Send + Sync> Deref for PoolObject<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.value.deref()
    }
}

impl<T: Send + Sync> DerefMut for PoolObject<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.deref_mut()
    }
}

impl<T: Send + Sync> Drop for PoolObject<T> {
    fn drop(&mut self) {
        if let Some(returner) = &self.returner {
            returner.return_element(unsafe { ManuallyDrop::take(&mut self.value) });
        } else {
            unsafe { ManuallyDrop::drop(&mut self.value) }
        }
    }
}
