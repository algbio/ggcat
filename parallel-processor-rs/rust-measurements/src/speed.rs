//! Types and constants for handling speed.

use super::measurement::*;
use super::*;

/// Number of seconds in a minute
pub const SECONDS_MINUTES_FACTOR: f64 = 60.0;
/// Number of minutes in a hour
pub const MINUTES_HOURS_FACTOR: f64 = 60.0;
/// Number of seconds in a hour
pub const SECONDS_HOURS_FACTOR: f64 = 60.0 * 60.0;

/// The `Speed` struct can be used to deal with speeds in a common way.
/// Common metric and imperial units are supported.
///
/// # Example
///
/// ```
/// use measurements::Speed;
///
/// let light = Speed::from_meters_per_second(300_000_000.0);
/// let mph = light.as_miles_per_hour();
/// println!("The speed of light is {} mph.", mph);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Speed {
    meters_per_second: f64,
}

impl Speed {
    /// Create a new Speed from a floating point number of m/s
    pub fn from_meters_per_second(meters_per_second: f64) -> Speed {
        Speed { meters_per_second }
    }

    /// Create a new Speed from a floating point number of m/s
    pub fn from_metres_per_second(metres_per_second: f64) -> Speed {
        Speed::from_meters_per_second(metres_per_second)
    }

    /// Create a new Speed from a floating point number of km/h (kph)
    pub fn from_kilometers_per_hour(kilometers_per_hour: f64) -> Speed {
        Speed::from_meters_per_second(
            (kilometers_per_hour / length::METER_KILOMETER_FACTOR) / SECONDS_HOURS_FACTOR,
        )
    }

    /// Create a new Speed from a floating point number of km/h (kph)
    pub fn from_kilometres_per_hour(kilometres_per_hour: f64) -> Speed {
        Speed::from_kilometers_per_hour(kilometres_per_hour)
    }

    /// Create a new Speed from a floating point number of miles/hour (mph)
    pub fn from_miles_per_hour(miles_per_hour: f64) -> Speed {
        Speed::from_meters_per_second((miles_per_hour * 1609.0) / 3600.0)
    }

    /// Convert this speed to a floating point number of m/s
    pub fn as_meters_per_second(&self) -> f64 {
        self.meters_per_second
    }

    /// Convert this speed to a floating point number of m/s
    pub fn as_metres_per_second(&self) -> f64 {
        self.as_meters_per_second()
    }

    /// Convert this speed to a floating point number of km/hour (kph)
    pub fn as_kilometers_per_hour(&self) -> f64 {
        (self.meters_per_second / 1000.0) * 3600.0
    }

    /// Convert this speed to a floating point number of km/hour (kph)
    pub fn as_kilometres_per_hour(&self) -> f64 {
        self.as_kilometers_per_hour()
    }

    /// Convert this speed to a floating point number of miles/hour (mph)
    pub fn as_miles_per_hour(&self) -> f64 {
        (self.meters_per_second / 1609.0) * 3600.0
    }
}

impl Measurement for Speed {
    fn as_base_units(&self) -> f64 {
        self.meters_per_second
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_meters_per_second(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "m/s"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("nm/s", 1e-9),
            ("\u{00B5}m/s", 1e-6),
            ("mm/s", 1e-3),
            ("m/s", 1e0),
            ("km/s", 1e3),
            ("thousand km/s", 1e6),
            ("million km/s", 1e9),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Speed }

#[cfg(test)]
mod test {
    use length::Length;
    use speed::*;
    use test_utils::assert_almost_eq;
    use time::Duration;

    // Metric
    #[test]
    fn kilometers_per_hour() {
        let i1 = Speed::from_meters_per_second(100.0);
        let r1 = i1.as_kilometers_per_hour();

        let i2 = Speed::from_kilometers_per_hour(100.0);
        let r2 = i2.as_meters_per_second();

        assert_almost_eq(r1, 360.0);
        assert_almost_eq(r2, 27.7777777777);
    }

    #[test]
    fn length_over_time() {
        let l1 = Length::from_meters(3.0);
        let t1 = Duration::new(1, 500_000_000);
        let i1 = l1 / t1;
        let r1 = i1.as_meters_per_second();
        assert_almost_eq(r1, 2.0);
    }

    #[test]
    fn acceleration() {
        // To get to 100 m/s at 50 m/s/s takes 2.0 seconds
        let s = Length::from_meters(100.0) / Duration::new(1, 0);
        let a = Length::from_meters(50.0) / Duration::new(1, 0) / Duration::new(1, 0);
        let t = s / a;
        assert_eq!(t, Duration::new(2, 0));
    }

    #[test]
    fn kilometres_per_hour() {
        let i1 = Speed::from_metres_per_second(100.0);
        let r1 = i1.as_kilometres_per_hour();

        let i2 = Speed::from_kilometres_per_hour(100.0);
        let r2 = i2.as_metres_per_second();

        assert_almost_eq(r1, 360.0);
        assert_almost_eq(r2, 27.7777777777);
    }

    // Imperial
    #[test]
    fn miles_per_hour() {
        let i1 = Speed::from_meters_per_second(100.0);
        let r1 = i1.as_miles_per_hour();

        let i2 = Speed::from_miles_per_hour(100.0);
        let r2 = i2.as_meters_per_second();

        assert_almost_eq(r1, 223.7414543194530764449968924798);
        assert_almost_eq(r2, 44.694444444444444444444444444444);
    }

    // Traits
    #[test]
    fn add() {
        let a = Speed::from_meters_per_second(2.0);
        let b = Speed::from_meters_per_second(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_meters_per_second(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Speed::from_meters_per_second(2.0);
        let b = Speed::from_meters_per_second(4.0);
        let c = a - b;
        assert_almost_eq(c.as_meters_per_second(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Speed::from_meters_per_second(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_meters_per_second(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Speed::from_meters_per_second(2.0);
        let b = Speed::from_meters_per_second(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_meters_per_second(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Speed::from_meters_per_second(2.0);
        let b = Speed::from_meters_per_second(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Speed::from_meters_per_second(2.0);
        let b = Speed::from_meters_per_second(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Speed::from_meters_per_second(2.0);
        let b = Speed::from_meters_per_second(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
