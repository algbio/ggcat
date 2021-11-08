use crossbeam::queue::*;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub trait PoolableObject {
    type AllocData: Copy + Clone;

    fn allocate(data: Self::AllocData) -> Self;
    fn reinitialize(&mut self);
}

pub struct FlexiblePool<T: PoolableObject> {
    objects: Arc<(SegQueue<T>, T::AllocData)>,
}

impl<T: PoolableObject> Clone for FlexiblePool<T> {
    fn clone(&self) -> Self {
        Self {
            objects: self.objects.clone(),
        }
    }
}

pub struct FlexiblePoolReference<T: PoolableObject> {
    pool: FlexiblePool<T>,
    object: ManuallyDrop<T>,
}

impl<T: PoolableObject> Deref for FlexiblePoolReference<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.object.deref()
    }
}

impl<T: PoolableObject> Drop for FlexiblePoolReference<T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.pool
            .release_object(unsafe { std::ptr::read(self.object.deref() as *const T) })
    }
}

impl<T: PoolableObject> DerefMut for FlexiblePoolReference<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.deref_mut()
    }
}

impl<T: PoolableObject> FlexiblePool<T> {
    pub fn new(init_data: T::AllocData) -> Self {
        Self {
            objects: Arc::new((SegQueue::new(), init_data)),
        }
    }

    pub fn take_object(&self) -> FlexiblePoolReference<T> {
        FlexiblePoolReference {
            pool: self.clone(),
            object: ManuallyDrop::new(match self.objects.0.pop() {
                None => T::allocate(self.objects.1),
                Some(el) => el,
            }),
        }
    }

    pub fn release_object(&self, mut el: T) {
        T::reinitialize(&mut el);
        self.objects.0.push(el);
    }

    pub fn take_object_owned(&self) -> T {
        match self.objects.0.pop() {
            None => T::allocate(self.objects.1),
            Some(el) => el,
        }
    }
}
