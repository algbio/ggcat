extern crate measurements;
use measurements::{AngularVelocity, Power};

fn main() {
    let power = Power::from_ps(225.0);
    let peak_revs = AngularVelocity::from_rpm(6000.0);
    let torque = power / peak_revs;
    println!(
        "At {:.0} rpm, a {:.1} ({:.1} PS) engine, produces {:.1} ({:.1} lbf·ft)",
        peak_revs.as_rpm(),
        power,
        power.as_ps(),
        torque,
        torque.as_pound_foot()
    );
}
