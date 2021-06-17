//! Types and constants for handling voltage.

use super::measurement::*;

/// The `Voltage` struct can be used to deal with electric potential difference
/// in a common way.
///
/// # Example
///
/// ```
/// use measurements::Voltage;
///
/// let volts = Voltage::from_millivolts(1500.0);
/// let m_v = volts.as_millivolts();
/// let k_v = volts.as_kilovolts();
/// println!("A 1.5 V battery has {} mV or {} kV", m_v, k_v);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Voltage {
    volts: f64,
}

impl Voltage {
    /// Create a new Voltage from a floating point value in Volts
    pub fn from_volts(volts: f64) -> Self {
        Voltage { volts }
    }

    /// Create a new Voltage from a floating point value in Microvolts
    pub fn from_microvolts(microvolts: f64) -> Self {
        Self::from_volts(microvolts / 1_000_000.0)
    }

    /// Create a new Voltage from a floating point value in Millivolts
    pub fn from_millivolts(millivolts: f64) -> Self {
        Self::from_volts(millivolts / 1000.0)
    }

    /// Create a new Voltage from a floating point value in Kilovolts
    pub fn from_kilovolts(kilovolts: f64) -> Self {
        Self::from_volts(kilovolts * 1000.0)
    }

    /// Convert this Voltage into a floating point value in Volts
    pub fn as_volts(&self) -> f64 {
        self.volts
    }

    /// Convert this Voltage into a floating point value in Microvolts
    pub fn as_microvolts(&self) -> f64 {
        self.volts * 1_000_000.0
    }

    /// Convert this Voltage into a floating point value in Millivolts
    pub fn as_millivolts(&self) -> f64 {
        self.volts * 1000.0
    }

    /// Convert this Voltage into a floating point value in Kilovolts
    pub fn as_kilovolts(&self) -> f64 {
        self.volts / 1000.0
    }
}

impl Measurement for Voltage {
    fn as_base_units(&self) -> f64 {
        self.volts
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_volts(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "V"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to Largest
        let list = [
            ("fV", 1e-15),
            ("pV", 1e-12),
            ("nV", 1e-9),
            ("\u{00B5}V", 1e-6),
            ("mV", 1e-3),
            ("V", 1e0),
            ("kV", 1e3),
            ("MV", 1e6),
            ("GV", 1e9),
            ("TV", 1e12),
            ("PV", 1e15),
            ("EV", 1e18),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Voltage }

#[cfg(test)]
mod test {
    use current::*;
    use resistance::*;
    use test_utils::assert_almost_eq;
    use voltage::*;

    #[test]
    pub fn as_kilovolts() {
        let u = Voltage::from_volts(10_000.0);
        assert_almost_eq(u.as_kilovolts(), 10.0);
    }

    #[test]
    pub fn as_volts() {
        let u = Voltage::from_kilovolts(1.234);
        assert_almost_eq(u.as_volts(), 1234.0);
    }

    #[test]
    pub fn as_microvolts() {
        let u = Voltage::from_volts(1.234567);
        assert_almost_eq(u.as_microvolts(), 1234567.0);
    }

    #[test]
    pub fn as_millivolts() {
        let u = Voltage::from_volts(1.234);
        assert_almost_eq(u.as_millivolts(), 1234.0);
    }

    // Traits
    #[test]
    fn add() {
        let a = Voltage::from_volts(2.0);
        let b = Voltage::from_millivolts(4000.0);
        let c = a + b;
        assert_almost_eq(c.as_volts(), 6.0);
    }

    #[test]
    fn sub() {
        let a = Voltage::from_volts(2.0);
        let b = Voltage::from_millivolts(4000.0);
        let c = a - b;
        assert_almost_eq(c.as_volts(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Voltage::from_millivolts(2000.0);
        let b = 4.0 * a;
        assert_almost_eq(b.as_volts(), 8.0);
    }

    #[test]
    fn div() {
        let a = Voltage::from_volts(2.0);
        let b = Voltage::from_millivolts(4000.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_volts(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Voltage::from_kilovolts(1.0);
        let b = Voltage::from_millivolts(1_000_000.0);
        let c = Voltage::from_microvolts(1_000_000_000.0);
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(b, c);
    }

    #[test]
    fn neq() {
        let a = Voltage::from_volts(2.0);
        let b = Voltage::from_millivolts(2.0);
        assert_ne!(a, b);
    }

    #[test]
    fn cmp() {
        let a = Voltage::from_volts(2.0);
        let b = Voltage::from_volts(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }

    #[test]
    fn mul_resistance_current() {
        let r = Resistance::from_ohms(470.0);
        let i = Current::from_amperes(2.0);
        let u = r * i;
        assert_eq!(u.as_volts(), 940.0);
    }

    #[test]
    fn div_resistance() {
        let r = Resistance::from_ohms(470.0);
        let u = Voltage::from_kilovolts(4.7);
        let i = u / r;
        assert_eq!(i.as_amperes(), 10.0);
    }

    #[test]
    fn div_current() {
        let i = Current::from_amperes(10.0);
        let u = Voltage::from_kilovolts(4.7);
        let r = u / i;
        assert_eq!(r.as_ohms(), 470.0);
    }
}
