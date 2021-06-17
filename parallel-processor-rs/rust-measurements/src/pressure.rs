//! Types and constants for handling pressure.

use super::measurement::*;

/// Number of Pascals in an atomosphere
pub const PASCAL_ATMOSPHERE_FACTOR: f64 = 101_325.0;
/// Number of Pascals in a hectopascal
pub const PASCAL_HECTOPASCAL_FACTOR: f64 = 100.0;
/// Number of Pascals in a kilopascal
pub const PASCAL_KILOPASCAL_FACTOR: f64 = 1000.0;
/// Number of Pascals in a millibar
pub const PASCAL_MILLIBAR_FACTOR: f64 = 100.0;
/// Number of Pascals in a Bar
pub const PASCAL_BAR_FACTOR: f64 = 100_000.0;
/// Number of Pascals in a PSI
pub const PASCAL_PSI_FACTOR: f64 = 6894.76;

/// The `Pressure` struct can be used to deal with presssures in a common way.
/// Common metric and imperial units are supported.
///
/// # Example
///
/// ```
/// use measurements::Pressure;
///
/// let earth = Pressure::from_atmospheres(1.0);
/// let mbar = earth.as_millibars();
/// println!("Atmospheric pressure is {} mbar.", mbar);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Pressure {
    pascals: f64,
}

impl Pressure {
    /// Create new Pressure from floating point value in Pascals (Pa)
    pub fn from_pascals(pascals: f64) -> Pressure {
        Pressure { pascals }
    }

    /// Create new Pressure from floating point value in hectopascals (hPA)
    pub fn from_hectopascals(hectopascals: f64) -> Pressure {
        Self::from_pascals(hectopascals * PASCAL_HECTOPASCAL_FACTOR)
    }

    /// Create new Pressure from floating point value in millibars (mBar)
    pub fn from_millibars(millibars: f64) -> Pressure {
        Self::from_pascals(millibars * PASCAL_MILLIBAR_FACTOR)
    }

    /// Create new Pressure from floating point value in kilopascals (kPa)
    pub fn from_kilopascals(kilopascals: f64) -> Pressure {
        Self::from_pascals(kilopascals * PASCAL_KILOPASCAL_FACTOR)
    }

    /// Create new Pressure from floating point value in psi
    pub fn from_psi(psi: f64) -> Pressure {
        Self::from_pascals(psi * PASCAL_PSI_FACTOR)
    }

    /// Create new Pressure from floating point value in Bar
    pub fn from_bars(bars: f64) -> Pressure {
        Self::from_pascals(bars * PASCAL_BAR_FACTOR)
    }

    /// Create new Pressure from floating point value in Atmospheres
    pub fn from_atmospheres(atmospheres: f64) -> Pressure {
        Self::from_pascals(atmospheres * PASCAL_ATMOSPHERE_FACTOR)
    }

    /// Convert this Pressure into a floating point value in Pascals
    pub fn as_pascals(&self) -> f64 {
        self.pascals
    }

    /// Convert this Pressure into a floating point value in hectopascals (hPA)
    pub fn as_hectopascals(&self) -> f64 {
        self.pascals / PASCAL_HECTOPASCAL_FACTOR
    }

    /// Convert this Pressure into a floating point value in millibars (mBar)
    pub fn as_millibars(&self) -> f64 {
        self.pascals / PASCAL_MILLIBAR_FACTOR
    }

    /// Convert this Pressure into a floating point value in kilopascals (kPA)
    pub fn as_kilopascals(&self) -> f64 {
        self.pascals / PASCAL_KILOPASCAL_FACTOR
    }

    /// Convert this Pressure into a floating point value in pounds per square-inch (psi)
    pub fn as_psi(&self) -> f64 {
        self.pascals / PASCAL_PSI_FACTOR
    }

    /// Convert this Pressure into a floating point value in Bar
    pub fn as_bars(&self) -> f64 {
        self.pascals / PASCAL_BAR_FACTOR
    }

    /// Convert this Pressure into a floating point value in Atmospheres
    pub fn as_atmospheres(&self) -> f64 {
        self.pascals / PASCAL_ATMOSPHERE_FACTOR
    }
}

impl Measurement for Pressure {
    fn as_base_units(&self) -> f64 {
        self.pascals
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_pascals(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "Pa"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        let list = [
            ("mPa", 1e-3),
            ("Pa", 1e0),
            ("hPa", 1e2),
            ("kPa", 1e3),
            ("MPa", 1e6),
            ("GPa", 1e9),
            ("TPa", 1e12),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Pressure }

#[cfg(test)]
mod test {
    use super::*;
    use test_utils::assert_almost_eq;

    #[test]
    fn hectopascals() {
        let t = Pressure::from_pascals(100.0);
        let o = t.as_hectopascals();
        assert_almost_eq(o, 1.0);

        let t = Pressure::from_hectopascals(100.0);
        let o = t.as_pascals();
        assert_almost_eq(o, 10000.0);
    }
    #[test]
    fn millibars() {
        let t = Pressure::from_pascals(100.0);
        let o = t.as_millibars();
        assert_almost_eq(o, 1.0);

        let t = Pressure::from_millibars(100.0);
        let o = t.as_pascals();
        assert_almost_eq(o, 10000.0);
    }
    #[test]
    fn kilopascals() {
        let t = Pressure::from_pascals(100.0);
        let o = t.as_kilopascals();
        assert_almost_eq(o, 0.1);

        let t = Pressure::from_kilopascals(100.0);
        let o = t.as_pascals();
        assert_almost_eq(o, 100000.0);
    }
    #[test]
    fn bars() {
        let t = Pressure::from_pascals(100.0);
        let o = t.as_bars();
        assert_almost_eq(o, 0.001);

        let t = Pressure::from_bars(100.0);
        let o = t.as_pascals();
        assert_almost_eq(o, 1e+7);
    }
    #[test]
    fn atmospheres() {
        let t = Pressure::from_pascals(100.0);
        let o = t.as_atmospheres();
        assert_almost_eq(o, 0.000986923);

        let t = Pressure::from_atmospheres(100.0);
        let o = t.as_pascals();
        assert_almost_eq(o, 10132497.2616919);
    }

    #[test]
    fn psi() {
        let t = Pressure::from_pascals(100.0);
        let o = t.as_psi();
        assert_almost_eq(o, 0.0145038);

        let t = Pressure::from_psi(100.0);
        let o = t.as_pascals();
        assert_almost_eq(o, 689476.9760513823);
    }

    // Traits
    #[test]
    fn add() {
        let a = Pressure::from_pascals(2.0);
        let b = Pressure::from_pascals(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_pascals(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Pressure::from_pascals(2.0);
        let b = Pressure::from_pascals(4.0);
        let c = a - b;
        assert_almost_eq(c.as_pascals(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Pressure::from_pascals(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_pascals(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Pressure::from_pascals(2.0);
        let b = Pressure::from_pascals(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_pascals(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Pressure::from_pascals(2.0);
        let b = Pressure::from_pascals(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Pressure::from_pascals(2.0);
        let b = Pressure::from_pascals(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Pressure::from_pascals(2.0);
        let b = Pressure::from_pascals(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
