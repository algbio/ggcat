extern crate measurements;

fn main() {
    // Sinusiodal Oscilator moves at 5 Hz across 50 mm
    let f = measurements::Frequency::from_hertz(5.0);
    let d = measurements::Distance::from_millimeters(50.0);
    // Speed = PI * Frequency * Displacement
    // Speed = PI * Displacement / Period
    let v = std::f64::consts::PI * d / f.as_period();
    println!(
        "Maximum speed of a pendulum at {:.1} with max displacement {:.1} is {:.1}",
        f, d, v
    );
}
