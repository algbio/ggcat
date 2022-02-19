use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};

pub struct OwnedDrop<X> {
    val: MaybeUninit<X>,
    was_taken: bool,
}

impl<X> OwnedDrop<X> {
    #[inline(always)]
    pub fn new(val: X) -> Self {
        Self {
            val: MaybeUninit::new(val),
            was_taken: false,
        }
    }

    pub unsafe fn take(&mut self) -> X {
        self.was_taken = true;
        std::ptr::read(self.val.assume_init_mut() as *const X)
    }
}

impl<X> Deref for OwnedDrop<X> {
    type Target = X;

    fn deref(&self) -> &Self::Target {
        unsafe { self.val.assume_init_ref() }
    }
}

impl<X> DerefMut for OwnedDrop<X> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.val.assume_init_mut() }
    }
}

impl<X> Drop for OwnedDrop<X> {
    fn drop(&mut self) {
        if !self.was_taken {
            unsafe {
                std::mem::drop(std::ptr::read(self.val.assume_init_ref() as *const X));
            }
        }
    }
}
