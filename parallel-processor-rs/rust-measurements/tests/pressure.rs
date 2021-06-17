extern crate measurements;
use measurements::prelude::*;

#[test]
fn psi() -> () {
    let p1 = measurements::Pressure::from_psi(200.0);
    let f = measurements::Force::from_pounds(200.0);
    let d = measurements::Length::from_inches(1.0);
    let a = d * d;
    let p2 = f / a;
    measurements::test_utils::assert_almost_eq(p1.as_base_units(), p2.as_base_units());
}

#[test]
fn metric() -> () {
    let p1 = measurements::Pressure::from_pascals(980.665);
    let m = measurements::Mass::from_kilograms(1.0);
    let g = measurements::Acceleration::from_meters_per_second_per_second(9.80665);
    let f = m * g;
    let d = measurements::Length::from_centimeters(10.0);
    let a = d * d;
    let p2 = f / a;
    measurements::test_utils::assert_almost_eq(p1.as_base_units(), p2.as_base_units());
}
