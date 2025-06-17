#[macro_use]
pub mod debug_functions;
pub mod fast_rand_bool;
pub mod inline_vec;
pub mod owned_drop;
pub mod resize_containers;
pub mod resource_counter;
pub mod vec_slice;

use std::cmp::max;

pub struct Utils;

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
    match k {
        0..=13 => max(k / 2, k - 4),
        14..=15 => 9,
        16..=21 => 10,
        22..=30 => 11,
        31..=37 => 12,
        38..=42 => 13,
        43..=64 => 14,
        _ => ((k as f64) / 4.0).round() as usize,
    }
}

impl Utils {
    #[inline(always)]
    pub fn compress_base(base: u8) -> u8 {
        (base >> 1) & 0x3
    }

    #[inline(always)]
    pub fn compress_base_complement(base: u8) -> u8 {
        (base >> 1) & 0x3 ^ 2
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
