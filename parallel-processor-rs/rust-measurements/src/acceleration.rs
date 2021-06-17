//! Types and constants for handling acceleration.

use super::length;
use super::measurement::*;
#[cfg(feature = "from_str")]
use regex::Regex;
#[cfg(feature = "from_str")]
use std::str::FromStr;

/// The `Acceleration` struct can be used to deal with Accelerations in a common way.
/// Common metric and imperial units are supported.
///
/// # Example
///
/// ```
/// extern crate measurements;
/// use measurements::{Acceleration, Length, Speed};
/// use std::time::Duration;
///
/// fn main() {
///     // Standing quarter mile in 10.0 dead, at 120.0 mph
///     let track = Length::from_miles(0.25);
///     let finish = Speed::from_miles_per_hour(120.0);
///     let time = Duration::new(10, 0);
///     let accel: Acceleration = finish / time;
///     println!("You accelerated over {} at an average of {}", track, accel);
///}
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Acceleration {
    meters_per_second_per_second: f64,
}

impl Acceleration {
    /// Create a new Acceleration from a floating point value in meters per second per second
    pub fn from_meters_per_second_per_second(meters_per_second_per_second: f64) -> Acceleration {
        Acceleration {
            meters_per_second_per_second,
        }
    }

    /// Create a new Acceleration from a floating point value in metres per second per second
    pub fn from_metres_per_second_per_second(metres_per_second_per_second: f64) -> Acceleration {
        Acceleration::from_meters_per_second_per_second(metres_per_second_per_second)
    }

    /// Create a new Acceleration from a floating point value in feet per second per second
    pub fn from_feet_per_second_per_second(feet_per_second_per_second: f64) -> Acceleration {
        Acceleration::from_metres_per_second_per_second(
            feet_per_second_per_second / length::METER_FEET_FACTOR,
        )
    }

    /// Convert this Acceleration to a value in meters per second per second
    pub fn as_meters_per_second_per_second(&self) -> f64 {
        self.meters_per_second_per_second
    }

    /// Convert this Acceleration to a value in metres per second per second
    pub fn as_metres_per_second_per_second(&self) -> f64 {
        self.as_meters_per_second_per_second()
    }

    /// Convert this Acceleration to a value in feet per second per second
    pub fn as_feet_per_second_per_second(&self) -> f64 {
        self.meters_per_second_per_second * length::METER_FEET_FACTOR
    }
}

impl Measurement for Acceleration {
    fn as_base_units(&self) -> f64 {
        self.meters_per_second_per_second
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_meters_per_second_per_second(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "m/s\u{00B2}"
    }
}

#[cfg(feature = "from_str")]
impl FromStr for Acceleration {
    type Err = std::num::ParseFloatError;

    /// Create a new Acceleration from a string
    /// Plain numbers in string are considered to be meters per second
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        if val.is_empty() {
            return Ok(Acceleration::from_metres_per_second_per_second(0.0));
        }

        let re = Regex::new(r"(?i)\s*([0-9.]*)\s?([ftmps -1]{1,6})\s*$").unwrap();
        if let Some(caps) = re.captures(val) {
            let float_val = caps.get(1).unwrap().as_str();
            return Ok(
                match caps.get(2).unwrap().as_str().to_lowercase().as_str() {
                    "m/s" | "m s-1" => {
                        Acceleration::from_meters_per_second_per_second(float_val.parse::<f64>()?)
                    }
                    "ft/s" | "fps" | "ft s-1" => {
                        Acceleration::from_feet_per_second_per_second(float_val.parse::<f64>()?)
                    }
                    _ => Acceleration::from_meters_per_second_per_second(val.parse::<f64>()?),
                },
            );
        }

        Ok(Acceleration::from_meters_per_second_per_second(
            val.parse::<f64>()?,
        ))
    }
}

implement_measurement! { Acceleration }

#[cfg(test)]
mod test {

    use super::*;
    use speed::Speed;
    use test_utils::assert_almost_eq;

    // Metric
    #[test]
    fn speed_over_time() {
        let s1 = Speed::from_meters_per_second(10.0);
        let t1 = ::time::Duration::new(5, 0);
        let i1 = s1 / t1;
        let r1 = i1.as_meters_per_second_per_second();
        assert_almost_eq(r1, 2.0);
    }

    // Traits
    #[test]
    fn add() {
        let a = Acceleration::from_meters_per_second_per_second(2.0);
        let b = Acceleration::from_meters_per_second_per_second(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_meters_per_second_per_second(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Acceleration::from_meters_per_second_per_second(2.0);
        let b = Acceleration::from_meters_per_second_per_second(4.0);
        let c = a - b;
        assert_almost_eq(c.as_meters_per_second_per_second(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Acceleration::from_meters_per_second_per_second(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_meters_per_second_per_second(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Acceleration::from_meters_per_second_per_second(2.0);
        let b = Acceleration::from_meters_per_second_per_second(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_meters_per_second_per_second(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Acceleration::from_meters_per_second_per_second(2.0);
        let b = Acceleration::from_meters_per_second_per_second(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Acceleration::from_meters_per_second_per_second(2.0);
        let b = Acceleration::from_meters_per_second_per_second(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Acceleration::from_meters_per_second_per_second(2.0);
        let b = Acceleration::from_meters_per_second_per_second(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn meters_per_second_str() {
        let t = Acceleration::from_str(" 12.0m/s");
        assert!(t.is_ok());
        let o = t.unwrap().as_meters_per_second_per_second();
        assert_almost_eq(12.0, o);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn meters_per_second_minus_str() {
        let t = Acceleration::from_str("12.0 m s-1");
        assert!(t.is_ok());
        let o = t.unwrap().as_meters_per_second_per_second();
        assert_almost_eq(12.0, o);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn feet_per_second_str() {
        let t = Acceleration::from_str(" 12.0ft/s");
        assert!(t.is_ok());
        let o = t.unwrap().as_feet_per_second_per_second();
        assert_almost_eq(12.0, o);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn feet_per_second_fps_str() {
        let t = Acceleration::from_str(" 12.0fps");
        assert!(t.is_ok());
        let o = t.unwrap().as_feet_per_second_per_second();
        assert_almost_eq(12.0, o);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn feet_per_second_minus_str() {
        let t = Acceleration::from_str("12.0 ft s-1");
        assert!(t.is_ok());
        let o = t.unwrap().as_feet_per_second_per_second();
        assert_almost_eq(12.0, o);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn number_str() {
        let t = Acceleration::from_str("100.5");
        assert!(t.is_ok());

        let o = t.unwrap().as_meters_per_second_per_second();
        assert_almost_eq(o, 100.5);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn invalid_str() {
        let t = Acceleration::from_str("abcd");
        assert!(t.is_err());
    }
}
