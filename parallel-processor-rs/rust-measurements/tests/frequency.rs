extern crate measurements;

use measurements::test_utils::assert_almost_eq;

#[test]
fn oscillator() {
    // Sinusiodal Oscilator moves at 5 Hz across 50 mm
    let f = measurements::Frequency::from_hertz(5.0);
    let d = measurements::Distance::from_millimeters(50.0);
    // Speed = PI * Frequency * Displacement
    // Speed = PI * Displacement / Period
    let v = std::f64::consts::PI * d / f.as_period();
    // Check against https://www.spaceagecontrol.com/calcsinm.htm
    assert_almost_eq(v.as_meters_per_second(), 0.78539816339745);
}
