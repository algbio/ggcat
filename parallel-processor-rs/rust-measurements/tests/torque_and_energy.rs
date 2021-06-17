extern crate measurements;

use measurements::*;

#[test]
fn create() -> () {
    let f = Force::from_newtons(10.0);
    let d = Length::from_metres(1.0);
    let w: Energy = Energy::from(f * d);
    let t: Torque = Torque::from(f * d);
    println!("That's {} as energy", w);
    println!("That's {} as torque", t);
}

#[test]
fn divide() -> () {
    let w = Energy::from_joules(100.0);
    let d = Length::from_metres(10.0);
    let f: Force = w / d;
    test_utils::assert_almost_eq(f.as_newtons(), 10.0);

    let t = Torque::from_newton_metres(100.0);
    let d = Length::from_metres(10.0);
    let f: Force = t / d;
    test_utils::assert_almost_eq(f.as_newtons(), 10.0);
}
