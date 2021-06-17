//! Types and constants for handling electrical current.

use super::measurement::*;

/// The `Current` struct can be used to deal with electric potential difference
/// in a common way.
///
/// # Example
///
/// ```
/// use measurements::Current;
///
/// let amperes = Current::from_milliamperes(35.0);
/// let a = amperes.as_amperes();
/// let u_a = amperes.as_microamperes();
/// println!("35 mA correspond to {} A or {} µA", a, u_a);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Current {
    amperes: f64,
}

impl Current {
    /// Create a new Current from a floating point value in amperes
    pub fn from_amperes(amperes: f64) -> Self {
        Current { amperes }
    }

    /// Create a new Current from a floating point value in milliamperes
    pub fn from_milliamperes(milliamperes: f64) -> Self {
        Self::from_amperes(milliamperes / 1000.0)
    }

    /// Create a new Current from a floating point value in microamperes
    pub fn from_microamperes(microamperes: f64) -> Self {
        Self::from_amperes(microamperes / 1000.0 / 1000.0)
    }

    /// Create a new Current from a floating point value in nanoamperes
    pub fn from_nanoamperes(nanoamperes: f64) -> Self {
        Self::from_amperes(nanoamperes / 1_000_000_000.0)
    }

    /// Convert this Current into a floating point value in amperes
    pub fn as_amperes(&self) -> f64 {
        self.amperes
    }

    /// Convert this Current into a floating point value in milliamperes
    pub fn as_milliamperes(&self) -> f64 {
        self.amperes * 1000.0
    }

    /// Convert this Current into a floating point value in microamperes
    pub fn as_microamperes(&self) -> f64 {
        self.amperes * 1000.0 * 1000.0
    }

    /// Convert this Current into a floating point value in nanoamperes
    pub fn as_nanoamperes(&self) -> f64 {
        self.amperes * 1_000_000_000.0
    }
}

impl Measurement for Current {
    fn as_base_units(&self) -> f64 {
        self.amperes
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_amperes(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "A"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to Largest
        let list = [
            ("fA", 1e-15),
            ("pA", 1e-12),
            ("nA", 1e-9),
            ("\u{00B5}A", 1e-6),
            ("mA", 1e-3),
            ("A", 1e0),
            ("kA", 1e3),
            ("MA", 1e6),
            ("GA", 1e9),
            ("TA", 1e12),
            ("PA", 1e15),
            ("EA", 1e18),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Current }

#[cfg(test)]
mod test {
    use current::*;
    use test_utils::assert_almost_eq;

    #[test]
    pub fn as_amperes() {
        let u = Current::from_milliamperes(1234.0);
        assert_almost_eq(u.as_amperes(), 1.234);
    }

    #[test]
    pub fn as_milliamperes() {
        let u = Current::from_amperes(1.234);
        assert_almost_eq(u.as_milliamperes(), 1234.0);
    }

    #[test]
    pub fn as_microamperes() {
        let u = Current::from_amperes(0.001);
        assert_almost_eq(u.as_microamperes(), 1000.0);
    }

    #[test]
    pub fn as_nanoamperes() {
        let u = Current::from_amperes(0.000001);
        assert_almost_eq(u.as_nanoamperes(), 1000.0);
    }

    // Traits
    #[test]
    fn add() {
        let a = Current::from_amperes(2.0);
        let b = Current::from_milliamperes(4000.0);
        let c = a + b;
        assert_almost_eq(c.as_amperes(), 6.0);
    }

    #[test]
    fn sub() {
        let a = Current::from_amperes(2.0);
        let b = Current::from_milliamperes(4000.0);
        let c = a - b;
        assert_almost_eq(c.as_amperes(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Current::from_milliamperes(2000.0);
        let b = 4.0 * a;
        assert_almost_eq(b.as_amperes(), 8.0);
    }

    #[test]
    fn div() {
        let a = Current::from_amperes(2.0);
        let b = Current::from_milliamperes(4000.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_amperes(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Current::from_microamperes(1_000_000.0);
        let b = Current::from_milliamperes(1_000.0);
        let c = Current::from_nanoamperes(1_000_000_000.0);
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(a, c);
    }

    #[test]
    fn neq() {
        let a = Current::from_amperes(2.0);
        let b = Current::from_milliamperes(2.0);
        assert_ne!(a, b);
    }

    #[test]
    fn cmp() {
        let a = Current::from_amperes(2.0);
        let b = Current::from_amperes(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
