//! Types and constants for handling electrical resistance.

use super::measurement::*;

/// The `Resistance` struct can be used to deal with electrical resistance in a
/// common way.
///
/// # Example
///
/// ```
/// use measurements::Resistance;
///
/// let r = Resistance::from_kiloohms(4.7);
/// let o = r.as_ohms();
/// let mo = r.as_megaohms();
/// println!("A 4.7 kΩ resistor has {} Ω or {} MΩ", o, mo);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Resistance {
    ohms: f64,
}

impl Resistance {
    /// Create a new Resistance from a floating point value in ohms
    pub fn from_ohms(ohms: f64) -> Self {
        Resistance { ohms }
    }

    /// Create a new Resistance from a floating point value in kiloohms
    pub fn from_kiloohms(kiloohms: f64) -> Self {
        Self::from_ohms(kiloohms * 1000.0)
    }

    /// Create a new Resistance from a floating point value in milliohms
    pub fn from_megaohms(megaohms: f64) -> Self {
        Self::from_ohms(megaohms * 1000.0 * 1000.0)
    }

    /// Convert this Resistance into a floating point value in ohms
    pub fn as_ohms(&self) -> f64 {
        self.ohms
    }

    /// Convert this Resistance into a floating point value in kiloohms
    pub fn as_kiloohms(&self) -> f64 {
        self.ohms / 1000.0
    }

    /// Convert this Resistance into a floating point value in milliohms
    pub fn as_megaohms(&self) -> f64 {
        self.ohms / 1000.0 / 1000.0
    }
}

impl Measurement for Resistance {
    fn as_base_units(&self) -> f64 {
        self.ohms
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_ohms(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "\u{2126}"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to Largest
        let list = [
            ("f\u{2126}", 1e-15),
            ("p\u{2126}", 1e-12),
            ("n\u{2126}", 1e-9),
            ("\u{00B5}\u{2126}", 1e-6),
            ("m\u{2126}", 1e-3),
            ("\u{2126}", 1e0),
            ("k\u{2126}", 1e3),
            ("M\u{2126}", 1e6),
            ("G\u{2126}", 1e9),
            ("T\u{2126}", 1e12),
            ("P\u{2126}", 1e15),
            ("E\u{2126}", 1e18),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Resistance }

#[cfg(test)]
mod test {
    use resistance::*;
    use test_utils::assert_almost_eq;

    #[test]
    pub fn as_ohms() {
        let u = Resistance::from_kiloohms(1.234);
        assert_almost_eq(u.as_ohms(), 1234.0);
    }

    #[test]
    pub fn as_kiloohms() {
        let u = Resistance::from_ohms(10_000.0);
        assert_almost_eq(u.as_kiloohms(), 10.0);
    }

    #[test]
    pub fn as_megaohms() {
        let u = Resistance::from_ohms(1_234_567.0);
        assert_almost_eq(u.as_megaohms(), 1.234567);
    }

    // Traits
    #[test]
    fn add() {
        let a = Resistance::from_ohms(2000.0);
        let b = Resistance::from_kiloohms(4.0);
        let c = a + b;
        assert_almost_eq(c.as_kiloohms(), 6.0);
    }

    #[test]
    fn sub() {
        let a = Resistance::from_megaohms(2.0);
        let b = Resistance::from_kiloohms(4000.0);
        let c = a - b;
        assert_almost_eq(c.as_ohms(), -2_000_000.0);
    }

    #[test]
    fn mul() {
        let a = Resistance::from_ohms(2000.0);
        let b = 4.0 * a;
        assert_almost_eq(b.as_kiloohms(), 8.0);
    }

    #[test]
    fn div() {
        let a = Resistance::from_kiloohms(2.0);
        let b = Resistance::from_ohms(4000.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_ohms(), 1000.0);
    }

    #[test]
    fn eq() {
        let a = Resistance::from_ohms(1_000_000.0);
        let b = Resistance::from_megaohms(1.0);
        assert_eq!(a, b);
    }

    #[test]
    fn neq() {
        let a = Resistance::from_ohms(2.0);
        let b = Resistance::from_megaohms(2.0);
        assert_ne!(a, b);
    }

    #[test]
    fn cmp() {
        let a = Resistance::from_ohms(2.0);
        let b = Resistance::from_ohms(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
