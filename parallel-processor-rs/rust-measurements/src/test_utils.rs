//! Utility code for writing tests.

const DEFAULT_DELTA: f64 = 1e-5;

/// Check two floating point values are approximately equal
pub fn almost_eq(a: f64, b: f64) -> bool {
    almost_eq_delta(a, b, DEFAULT_DELTA)
}

/// Check two floating point values are approximately equal using some given delta (a fraction of the inputs)
pub fn almost_eq_delta(a: f64, b: f64, d: f64) -> bool {
    (abs(a - b) / a) < d
}

/// Assert two floating point values are approximately equal
pub fn assert_almost_eq(a: f64, b: f64) {
    assert_almost_eq_delta(a, b, DEFAULT_DELTA);
}

/// Assert two floating point values are approximately equal using some given delta (a fraction of the inputs)
pub fn assert_almost_eq_delta(a: f64, b: f64, d: f64) {
    if !almost_eq_delta(a, b, d) {
        panic!("assertion failed: {:?} != {:?} (within {:?})", a, b, d);
    }
}

/// This function doesn't seem to be available no `#![no_std]` so we re-
/// implement it here.
fn abs(x: f64) -> f64 {
    if x > 0.0 {
        x
    } else {
        -x
    }
}
