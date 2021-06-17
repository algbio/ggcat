//! Types and constants for handling frequencies.

use super::measurement::*;
use time;

/// Number of nanohertz in a Hz
pub const HERTZ_NANOHERTZ_FACTOR: f64 = 1e9;
/// Number of µHz in a Hz
pub const HERTZ_MICROHERTZ_FACTOR: f64 = 1e6;
/// Number of mHz in a Hz
pub const HERTZ_MILLIHERTZ_FACTOR: f64 = 1e3;
/// Number of kHz in a Hz
pub const HERTZ_KILOHERTZ_FACTOR: f64 = 1e-3;
/// Number of MHz in a Hz
pub const HERTZ_MEGAHERTZ_FACTOR: f64 = 1e-6;
/// Number of GHz in a Hz
pub const HERTZ_GIGAHERTZ_FACTOR: f64 = 1e-9;
/// Number of THz in a Hz
pub const HERTZ_TERAHERTZ_FACTOR: f64 = 1e-12;

/// The Frequency struct can be used to deal with frequencies in a common way.
/// Common SI prefixes are supported.
///
/// # Example
///
/// ```
/// use measurements::Frequency;
///
/// let radio_station = Frequency::from_hertz(101.5e6);
/// println!("Tune to {}.", radio_station);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Frequency {
    hertz: f64,
}

/// Distance is a synonym for Frequency
pub type Distance = Frequency;

impl Frequency {
    /// Create a new Frequency from a floating point value in hertz
    pub fn from_hertz(hertz: f64) -> Self {
        Frequency { hertz }
    }

    /// Create a new Frequency from a floating point value in Nanohertz.
    pub fn from_nanohertz(nanohertz: f64) -> Self {
        Self::from_hertz(nanohertz / HERTZ_NANOHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value in Microhertz.
    pub fn from_microhertz(microhertz: f64) -> Self {
        Self::from_hertz(microhertz / HERTZ_MICROHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value in Millihertz.
    pub fn from_millihertz(millihertz: f64) -> Self {
        Self::from_hertz(millihertz / HERTZ_MILLIHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value in Kilohertz (kHz).
    pub fn from_kilohertz(kilohertz: f64) -> Self {
        Self::from_hertz(kilohertz / HERTZ_KILOHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value in Megahertz (MHz).
    pub fn from_megahertz(megahertz: f64) -> Self {
        Self::from_hertz(megahertz / HERTZ_MEGAHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value in Gigahertz (GHz).
    pub fn from_gigahertz(gigahertz: f64) -> Self {
        Self::from_hertz(gigahertz / HERTZ_GIGAHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value in Terahertz (THz).
    pub fn from_terahertz(terahertz: f64) -> Self {
        Self::from_hertz(terahertz / HERTZ_TERAHERTZ_FACTOR)
    }

    /// Create a new Frequency from a floating point value of the period in seconds.
    pub fn from_period(period: time::Duration) -> Self {
        Self::from_hertz(1.0 / period.as_base_units())
    }

    /// Convert this Frequency to a floating point value in Nanohertz
    pub fn as_nanohertz(&self) -> f64 {
        self.hertz * HERTZ_NANOHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value in Microhertz
    pub fn as_microhertz(&self) -> f64 {
        self.hertz * HERTZ_MICROHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value in Millihertz
    pub fn as_millihertz(&self) -> f64 {
        self.hertz * HERTZ_MILLIHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value in Hertz (Hz)
    pub fn as_hertz(&self) -> f64 {
        self.hertz
    }

    /// Convert this Frequency to a floating point value in Kilohertz (kHz)
    pub fn as_kilohertz(&self) -> f64 {
        self.hertz * HERTZ_KILOHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value in Megahertz (MHz)
    pub fn as_megahertz(&self) -> f64 {
        self.hertz * HERTZ_MEGAHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value in gigahertz (GHz)
    pub fn as_gigahertz(&self) -> f64 {
        self.hertz * HERTZ_GIGAHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value in terahertz (THz)
    pub fn as_terahertz(&self) -> f64 {
        self.hertz * HERTZ_TERAHERTZ_FACTOR
    }

    /// Convert this Frequency to a floating point value of the period in seconds.
    pub fn as_period(&self) -> time::Duration {
        time::Duration::from_base_units(1.0 / self.hertz)
    }
}

impl Measurement for Frequency {
    fn as_base_units(&self) -> f64 {
        self.hertz
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_hertz(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "Hz"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("nHz", 1e-9),
            ("\u{00B5}Hz", 1e-6),
            ("mHz", 1e-3),
            ("Hz", 1e0),
            ("kHz", 1e3),
            ("MHz", 1e6),
            ("GHz", 1e9),
            ("THz", 1e12),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Frequency }

#[cfg(test)]
mod test {
    use super::*;
    use test_utils::assert_almost_eq;
    use time;

    #[test]
    pub fn hertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_hertz();
        assert_almost_eq(r1, 100.0);
    }

    #[test]
    pub fn nanohertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_nanohertz();
        let i2 = Frequency::from_nanohertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e+11);
        assert_almost_eq(r2, 1e-7);
    }

    #[test]
    pub fn microhertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_microhertz();
        let i2 = Frequency::from_microhertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e+8);
        assert_almost_eq(r2, 1e-4);
    }

    #[test]
    pub fn millihertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_millihertz();
        let i2 = Frequency::from_millihertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e+5);
        assert_almost_eq(r2, 1e-1);
    }

    #[test]
    pub fn kilohertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_kilohertz();
        let i2 = Frequency::from_kilohertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e-1);
        assert_almost_eq(r2, 1e+5);
    }

    #[test]
    pub fn megahertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_megahertz();
        let i2 = Frequency::from_megahertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e-4);
        assert_almost_eq(r2, 1e+8);
    }

    #[test]
    pub fn gigahertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_gigahertz();
        let i2 = Frequency::from_gigahertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e-7);
        assert_almost_eq(r2, 1e+11);
    }

    #[test]
    pub fn terahertz() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_terahertz();
        let i2 = Frequency::from_terahertz(100.0);
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e-10);
        assert_almost_eq(r2, 1e+14);
    }

    #[test]
    pub fn period() {
        let i1 = Frequency::from_hertz(100.0);
        let r1 = i1.as_period().as_base_units();
        let i2 = Frequency::from_period(time::Duration::new(100, 0));
        let r2 = i2.as_hertz();
        assert_almost_eq(r1, 1e-2);
        assert_almost_eq(r2, 1e-2);
    }

    // Traits
    #[test]
    fn add() {
        let a = Frequency::from_hertz(2.0);
        let b = Frequency::from_hertz(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_hertz(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Frequency::from_hertz(2.0);
        let b = Frequency::from_hertz(4.0);
        let c = a - b;
        assert_almost_eq(c.as_hertz(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Frequency::from_hertz(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_hertz(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Frequency::from_hertz(2.0);
        let b = Frequency::from_hertz(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_hertz(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Frequency::from_hertz(2.0);
        let b = Frequency::from_hertz(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Frequency::from_hertz(2.0);
        let b = Frequency::from_hertz(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Frequency::from_hertz(2.0);
        let b = Frequency::from_hertz(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
