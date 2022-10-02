#[macro_use]
pub mod debug_functions;
pub mod fast_rand_bool;
pub mod owned_drop;
pub mod resource_counter;
pub mod vec_slice;

use std::cmp::{max, min};
use std::sync::atomic::AtomicUsize;

pub struct Utils;

pub static DEBUG_LEVEL: AtomicUsize = AtomicUsize::new(0);

const C_INV_LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

#[macro_export]
macro_rules! panic_debug {
    ($($arg:tt)*) => {
        #[cfg(feature = "debug")]
        panic!($($arg)*);
        #[cfg(not(feature = "debug"))]
        unsafe { std::hint::unreachable_unchecked() }
    };
}

pub fn compute_best_m(k: usize) -> usize {
    let upper_bound = k / 5;
    let lower_bound = min(
        k - 1,
        max(
            2,
            (((k as f64).log(2.0) * 2.5).ceil() as usize).next_power_of_two() / 2,
        ),
    );

    max(upper_bound, lower_bound)
}

impl Utils {
    #[inline(always)]
    pub fn compress_base(base: u8) -> u8 {
        (base >> 1) & 0x3
    }

    #[inline(always)]
    pub fn decompress_base(cbase: u8) -> u8 {
        C_INV_LETTERS[cbase as usize]
    }

    #[inline(always)]
    pub fn conditional_rc_base(cbase: u8, do_rc: bool) -> u8 {
        cbase ^ if do_rc { 2 } else { 0 }
    }
}
