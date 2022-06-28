use parking_lot::lock_api::RawMutex;
use parking_lot::{Mutex, MutexGuard};
use std::mem::MaybeUninit;
use std::ops::DerefMut;

pub struct NightlyUtils(());

impl NightlyUtils {
    #[inline]
    pub fn box_new_uninit_slice<T>(size: usize) -> Box<[MaybeUninit<T>]> {
        #[cfg(not(feature = "nightly"))]
        {
            let mut vec = Vec::with_capacity(size);
            unsafe {
                vec.set_len(size);
            }
            vec.into_boxed_slice()
        }
        #[cfg(feature = "nightly")]
        Box::new_uninit_slice(size)
    }

    #[inline(always)]
    pub unsafe fn box_assume_init<T>(box_: Box<[MaybeUninit<T>]>) -> Box<[T]> {
        #[cfg(not(feature = "nightly"))]
        {
            std::mem::transmute(box_)
        }
        #[cfg(feature = "nightly")]
        {
            box_.assume_init()
        }
    }

    #[inline]
    pub fn box_new_zeroed_slice<T>(size: usize) -> Box<[MaybeUninit<T>]> {
        #[cfg(not(feature = "nightly"))]
        {
            let mut vec = Vec::with_capacity(size);
            unsafe {
                vec.set_len(size);
                std::ptr::write_bytes(vec.as_mut_ptr(), 0, size);
            }
            vec.into_boxed_slice()
        }
        #[cfg(feature = "nightly")]
        Box::new_zeroed_slice(size)
    }

    #[inline]
    pub unsafe fn box_new_uninit_slice_assume_init<T: Copy>(size: usize) -> Box<[T]> {
        #[cfg(not(feature = "nightly"))]
        {
            let mut vec = Vec::with_capacity(size);
            vec.set_len(size);
            vec.into_boxed_slice()
        }
        #[cfg(feature = "nightly")]
        Box::new_uninit_slice(size).assume_init()
    }

    pub fn mutex_get_or_init<'a, T>(
        m: &'a mut MutexGuard<Option<T>>,
        init_fn: fn() -> T,
    ) -> &'a mut T {
        if m.is_none() {
            *m.deref_mut() = Some(init_fn());
        }
        m.as_mut().unwrap()
    }

    pub const fn new_mutex<T>(val: T) -> Mutex<T> {
        Mutex::const_new(parking_lot::RawMutex::INIT, val)
    }
}
