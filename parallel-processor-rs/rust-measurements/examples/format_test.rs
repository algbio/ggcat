extern crate measurements;
use measurements::*;

fn main() {
    for power in -12..12 {
        let val: f64 = 123.456 * (10f64.powf(f64::from(power)));
        println!("10^{}...", power);
        println!("Temp of {0:.3} outside", Temperature::from_kelvin(val));
        println!("Distance of {0:.3}", Length::from_meters(val));
        println!("Pressure of {0:.3}", Pressure::from_millibars(val));
        println!("Volume of {0:.3}", Volume::from_litres(val));
        println!("Mass of {0:.3}", Mass::from_kilograms(val));
        println!("Speed of {0:.3}", Speed::from_meters_per_second(val));
        println!(
            "Acceleration of {0:.3}",
            Acceleration::from_meters_per_second_per_second(val)
        );
        println!("Energy of {0:.3}", Energy::from_joules(val));
        println!("Power of {0:.3}", Power::from_watts(val));
        println!("Force of {0:.3}", Force::from_newtons(val));
        println!("Force of {0:.3}", Torque::from_newton_metres(val));
        println!(
            "Force of {0:.3}",
            AngularVelocity::from_radians_per_second(val)
        );
        println!("Data size is {0:.3}", Data::from_octets(val));
    }
}
