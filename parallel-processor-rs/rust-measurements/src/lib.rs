//! # Measurements
//!
//! Measurements is a crate that lets you represent physical quantities, such
//! as Lengths, Masses, Pressures, etc. Each quantity has a series of
//! functions that allow you to convert to and from common units. You can also
//! perform arithmetic on the quantities - for example you can divide a Force
//! by an Area to get a Pressure.

#![feature(const_fn_floating_point_arithmetic)]
#![deny(warnings, missing_docs)]
#![cfg_attr(feature = "no_std", no_std)]

#[cfg(feature = "no_std")]
use core as std;
#[cfg(feature = "no_std")]
use core::time;

#[cfg(not(feature = "no_std"))]
use std::time;

#[cfg(feature = "serde")]
#[macro_use]
extern crate serde;

#[cfg(feature = "from_str")]
extern crate regex;

use std::f64::consts::PI;

#[macro_use]
mod measurement;
pub use measurement::Measurement;

pub mod length;
pub use length::{Distance, Length};

pub mod temperature;
pub use temperature::{Temperature, TemperatureDelta};

pub mod humidity;
pub use humidity::Humidity;

pub mod mass;
pub use mass::Mass;

pub mod volume;
pub use volume::Volume;

pub mod density;
pub use density::Density;

pub mod pressure;
pub use pressure::Pressure;

pub mod speed;
pub use speed::Speed;

pub mod acceleration;
pub use acceleration::Acceleration;

pub mod energy;
pub use energy::Energy;

pub mod power;
pub use power::Power;

pub mod voltage;
pub use voltage::Voltage;

pub mod current;
pub use current::Current;

pub mod resistance;
pub use resistance::Resistance;

pub mod force;
pub use force::Force;

pub mod area;
pub use area::Area;

pub mod angle;
pub use angle::Angle;

pub mod frequency;
pub use frequency::Frequency;

pub mod angular_velocity;
pub use angular_velocity::AngularVelocity;

pub mod torque;
pub use torque::Torque;

pub mod data;
pub use data::Data;

mod torque_energy;
pub use torque_energy::TorqueEnergy;

pub mod prelude;

pub mod test_utils;

/// For given types A, B and C, implement, using base units:
///     - A = B * C
///     - A = C * B
///     - B = A / C
///     - C = A / B
macro_rules! impl_maths {
    ($a:ty, $b:ty) => {
        impl std::ops::Mul<$b> for $b {
            type Output = $a;

            fn mul(self, rhs: $b) -> Self::Output {
                Self::Output::from_base_units(self.as_base_units() * rhs.as_base_units())
            }
        }

        impl std::ops::Div<$b> for $a {
            type Output = $b;

            fn div(self, rhs: $b) -> Self::Output {
                Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
            }
        }
    };

    ($a:ty, $b:ty, $c:ty) => {
        impl std::ops::Mul<$b> for $c {
            type Output = $a;

            fn mul(self, rhs: $b) -> Self::Output {
                Self::Output::from_base_units(self.as_base_units() * rhs.as_base_units())
            }
        }

        impl std::ops::Mul<$c> for $b {
            type Output = $a;

            fn mul(self, rhs: $c) -> Self::Output {
                Self::Output::from_base_units(self.as_base_units() * rhs.as_base_units())
            }
        }

        impl std::ops::Div<$c> for $a {
            type Output = $b;

            fn div(self, rhs: $c) -> Self::Output {
                Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
            }
        }

        impl std::ops::Div<$b> for $a {
            type Output = $c;

            fn div(self, rhs: $b) -> Self::Output {
                Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
            }
        }
    };
}

impl Measurement for time::Duration {
    fn as_base_units(&self) -> f64 {
        self.as_secs() as f64 + (f64::from(self.subsec_nanos()) * 1e-9)
    }

    fn from_base_units(units: f64) -> Self {
        let subsec_nanos = ((units * 1e9) % 1e9) as u32;
        let secs = units as u64;
        time::Duration::new(secs, subsec_nanos)
    }

    fn get_base_units_name(&self) -> &'static str {
        "s"
    }
}

impl_maths!(Area, Length);
impl_maths!(Energy, time::Duration, Power);
impl_maths!(Force, Mass, Acceleration);
impl_maths!(Force, Pressure, Area);
impl_maths!(Length, time::Duration, Speed);
impl_maths!(Power, Force, Speed);
impl_maths!(Speed, time::Duration, Acceleration);
impl_maths!(Volume, Length, Area);
impl_maths!(Power, AngularVelocity, Torque);
impl_maths!(Power, Voltage, Current);
impl_maths!(Voltage, Resistance, Current);

// Force * Distance is ambiguous. Create an ambiguous struct the user can then
// cast into either Torque or Energy.

impl_maths!(TorqueEnergy, Force, Length);

// Implement the divisions manually (the above macro only implemented the
// TorqueEnergy / X operations).

impl std::ops::Div<Length> for Torque {
    type Output = Force;

    fn div(self, rhs: Length) -> Self::Output {
        Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
    }
}

impl std::ops::Div<Force> for Torque {
    type Output = Length;

    fn div(self, rhs: Force) -> Self::Output {
        Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
    }
}

impl std::ops::Div<Length> for Energy {
    type Output = Force;

    fn div(self, rhs: Length) -> Self::Output {
        Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
    }
}

impl std::ops::Div<Force> for Energy {
    type Output = Length;

    fn div(self, rhs: Force) -> Self::Output {
        Self::Output::from_base_units(self.as_base_units() / rhs.as_base_units())
    }
}
