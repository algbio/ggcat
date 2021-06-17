//! Types and constants for handling power.

use super::measurement::*;

/// Number of horsepower in a watt
pub const WATT_HORSEPOWER_FACTOR: f64 = 1.0 / 745.6998715822702;
/// Number of BTU/min in a watt
pub const WATT_BTU_MIN_FACTOR: f64 = 1.0 / 17.58426666666667;
/// Number of kW in a W
pub const WATT_KILOWATT_FACTOR: f64 = 1e-3;
/// Number of mW in a W
pub const WATT_MILLIWATT_FACTOR: f64 = 1e3;
/// Number of µW in a W
pub const WATT_MICROWATT_FACTOR: f64 = 1e6;
/// Number of pferdstarken (PS) in a W
pub const WATT_PS_FACTOR: f64 = 1.0 / 735.499;

/// The `Power` struct can be used to deal with energies in a common way.
/// Common metric and imperial units are supported.
///
/// # Example
///
/// ```
/// use measurements::Power;
///
/// let power = Power::from_horsepower(100.0);
/// let k_w = power.as_kilowatts();
/// println!("A 100.0 hp car produces {} kW", k_w);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Power {
    watts: f64,
}

impl Power {
    /// Create a new Power from a floating point value in Watts
    pub fn from_watts(watts: f64) -> Power {
        Power { watts }
    }

    /// Create a new Power from a floating point value in milliwatts
    pub fn from_milliwatts(milliwatts: f64) -> Power {
        Self::from_watts(milliwatts / WATT_MILLIWATT_FACTOR)
    }

    /// Create a new Power from a floating point value in microwatts
    pub fn from_microwatts(microwatts: f64) -> Power {
        Self::from_watts(microwatts / WATT_MICROWATT_FACTOR)
    }

    /// Create a new Power from a floating point value in horsepower (hp)
    pub fn from_horsepower(horsepower: f64) -> Power {
        Self::from_watts(horsepower / WATT_HORSEPOWER_FACTOR)
    }

    /// Create a new Power from a floating point value in metric horsepower (PS)
    pub fn from_ps(ps: f64) -> Power {
        Self::from_watts(ps / WATT_PS_FACTOR)
    }

    /// Create a new Power from a floating point value in metric horsepower (PS)
    pub fn from_metric_horsepower(metric_horsepower: f64) -> Power {
        Self::from_watts(metric_horsepower / WATT_PS_FACTOR)
    }

    /// Create a new Power from a floating point value in BTU/mjn
    pub fn from_btu_per_minute(btu_per_minute: f64) -> Power {
        Self::from_watts(btu_per_minute / WATT_BTU_MIN_FACTOR)
    }

    /// Create a new Power from a floating point value in Kilowatts (kW)
    pub fn from_kilowatts(kw: f64) -> Power {
        Self::from_watts(kw / WATT_KILOWATT_FACTOR)
    }

    /// Convert this Power into a floating point value in Watts
    pub fn as_watts(&self) -> f64 {
        self.watts
    }

    /// Convert this Power into a floating point value in horsepower (hp)
    pub fn as_horsepower(&self) -> f64 {
        self.watts * WATT_HORSEPOWER_FACTOR
    }

    /// Convert this Power into a floating point value in metric horsepower (PS)
    pub fn as_ps(&self) -> f64 {
        self.watts * WATT_PS_FACTOR
    }

    /// Convert this Power into a floating point value in metric horsepower (PS)
    pub fn as_metric_horsepower(&self) -> f64 {
        self.watts * WATT_PS_FACTOR
    }

    /// Convert this Power into a floating point value in BTU/min
    pub fn as_btu_per_minute(&self) -> f64 {
        self.watts * WATT_BTU_MIN_FACTOR
    }

    /// Convert this Power into a floating point value in kilowatts (kW)
    pub fn as_kilowatts(&self) -> f64 {
        self.watts * WATT_KILOWATT_FACTOR
    }

    /// Convert this Power into a floating point value in milliwatts (mW)
    pub fn as_milliwatts(&self) -> f64 {
        self.watts * WATT_MILLIWATT_FACTOR
    }

    /// Convert this Power into a floating point value in microwatts (µW)
    pub fn as_microwatts(&self) -> f64 {
        self.watts * WATT_MICROWATT_FACTOR
    }
}

impl Measurement for Power {
    fn as_base_units(&self) -> f64 {
        self.watts
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_watts(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "W"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to Largest
        let list = [
            ("fW", 1e-15),
            ("pW", 1e-12),
            ("nW", 1e-9),
            ("\u{00B5}W", 1e-6),
            ("mW", 1e-3),
            ("W", 1e0),
            ("kW", 1e3),
            ("MW", 1e6),
            ("GW", 1e9),
            ("TW", 1e12),
            ("PW", 1e15),
            ("EW", 1e18),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Power }

#[cfg(test)]
mod test {
    use current::*;
    use power::*;
    use test_utils::assert_almost_eq;
    use voltage::*;

    #[test]
    pub fn as_btu_per_minute() {
        let i1 = Power::from_btu_per_minute(100.0);
        let r1 = i1.as_watts();

        let i2 = Power::from_watts(100.0);
        let r2 = i2.as_btu_per_minute();

        assert_almost_eq(r1, 1758.426666666667);
        assert_almost_eq(r2, 5.686901927480627);
    }

    #[test]
    pub fn as_horsepower() {
        let i1 = Power::from_horsepower(100.0);
        let r1 = i1.as_watts();

        let i2 = Power::from_watts(100.0);
        let r2 = i2.as_horsepower();

        assert_almost_eq(r1, 74569.98715822702);
        assert_almost_eq(r2, 0.1341022089595028);
    }

    #[test]
    pub fn as_kilowatts() {
        let i1 = Power::from_kilowatts(100.0);
        let r1 = i1.as_watts();

        let i2 = Power::from_watts(100.0);
        let r2 = i2.as_kilowatts();

        assert_almost_eq(r1, 100000.0);
        assert_almost_eq(r2, 0.1);
    }

    #[test]
    pub fn as_milliwatts() {
        let i1 = Power::from_milliwatts(100.0);
        let r1 = i1.as_watts();

        let i2 = Power::from_watts(100.0);
        let r2 = i2.as_milliwatts();

        assert_almost_eq(r1, 0.1);
        assert_almost_eq(r2, 100_000.0);
    }

    #[test]
    pub fn as_microwatts() {
        let i1 = Power::from_microwatts(100.0);
        let r1 = i1.as_milliwatts();

        let i2 = Power::from_milliwatts(100.0);
        let r2 = i2.as_microwatts();

        assert_almost_eq(r1, 0.1);
        assert_almost_eq(r2, 100_000.0);
    }

    // Traits
    #[test]
    fn add() {
        let a = Power::from_watts(2.0);
        let b = Power::from_watts(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_watts(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Power::from_watts(2.0);
        let b = Power::from_watts(4.0);
        let c = a - b;
        assert_almost_eq(c.as_watts(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Power::from_watts(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_watts(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Power::from_watts(2.0);
        let b = Power::from_watts(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_watts(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Power::from_watts(2.0);
        let b = Power::from_watts(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Power::from_watts(2.0);
        let b = Power::from_watts(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Power::from_watts(2.0);
        let b = Power::from_watts(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }

    #[test]
    fn mul_voltage_current() {
        let u = Voltage::from_volts(230.0);
        let i = Current::from_amperes(10.0);
        let p = u * i;
        assert_almost_eq(p.as_kilowatts(), 2.3);
    }

    #[test]
    fn div_voltage() {
        let u = Voltage::from_volts(230.0);
        let p = Power::from_kilowatts(2.3);
        let i = p / u;
        assert_eq!(i.as_amperes(), 10.0);
    }

    #[test]
    fn div_current() {
        let i = Current::from_amperes(10.0);
        let p = Power::from_kilowatts(2.3);
        let u = p / i;
        assert_eq!(u.as_volts(), 230.0);
    }
}
