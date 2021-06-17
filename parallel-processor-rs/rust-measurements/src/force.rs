//! Types and constants for handling force.

use super::measurement::*;

/// Number of POUNDS force in a Newton
pub const POUNDS_PER_NEWTON: f64 = 0.224809;
/// Number of POUNDALS in a Newton.  A poundal is the force necessary to
/// accelerate 1 pound-mass at 1 foot per second per second.
pub const POUNDALS_PER_NEWTON: f64 = 7.2330;
/// Number of KILOPONDS in a Newton
pub const KILOPONDS_PER_NEWTON: f64 = 1.0 / 9.80665;
/// Number of DYNES in a Newton
pub const DYNES_PER_NEWTON: f64 = 1e5;

/// The `Force` struct can be used to deal with force in a common way.
///
/// #Example
///
/// ```
/// use measurements::Force;
/// use measurements::Mass;
/// use measurements::Acceleration;
///
/// let metric_ton = Mass::from_metric_tons(1.0);
/// let gravity = Acceleration::from_meters_per_second_per_second(9.81);
/// let force: Force = metric_ton * gravity; // F=ma
/// println!(
///     "One metric ton exerts a force of {} due to gravity",
///     force);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Force {
    newtons: f64,
}

impl Force {
    /// Create a Force from a floating point value in Newtons
    pub fn from_newtons(newtons: f64) -> Self {
        Force { newtons }
    }

    /// Create a Force from a floating point value in Micronewtons
    pub fn from_micronewtons(micronewtons: f64) -> Self {
        Self::from_newtons(micronewtons / 1e6)
    }

    /// Create a Force from a floating point value in Millinewtons
    pub fn from_millinewtons(millinewtons: f64) -> Self {
        Self::from_newtons(millinewtons / 1e3)
    }

    /// Create a Force from a floating point value in pounds
    pub fn from_pounds(pounds: f64) -> Self {
        Self::from_newtons(pounds / POUNDS_PER_NEWTON)
    }

    /// Create a Force from a floating point value in poundals
    pub fn from_poundals(poundals: f64) -> Self {
        Self::from_newtons(poundals / POUNDALS_PER_NEWTON)
    }

    /// Create a Force from a floating point value in kiloponds
    pub fn from_kiloponds(kiloponds: f64) -> Self {
        Self::from_newtons(kiloponds / KILOPONDS_PER_NEWTON)
    }

    /// Create a Force from a floating point value in Dynes
    pub fn from_dynes(dynes: f64) -> Self {
        Self::from_newtons(dynes / DYNES_PER_NEWTON)
    }

    /// Convert this Force into a floating point value in Micronewtons
    pub fn as_micronewtons(&self) -> f64 {
        self.newtons * 1e6
    }

    /// Convert this Force into a floating point value in Milliewtons
    pub fn as_millinewtons(&self) -> f64 {
        self.newtons * 1e3
    }

    /// Convert this Force into a floating point value in Newtons
    pub fn as_newtons(&self) -> f64 {
        self.newtons
    }

    /// Convert this Force into a floating point value in pound-force (lb.f)
    pub fn as_pounds(&self) -> f64 {
        self.newtons * POUNDS_PER_NEWTON
    }

    /// Convert this Force into a floating point value in poundals
    pub fn as_poundals(&self) -> f64 {
        self.newtons * POUNDALS_PER_NEWTON
    }

    /// Convert this Force into a floating point value in kiloponds
    pub fn as_kiloponds(&self) -> f64 {
        self.newtons * KILOPONDS_PER_NEWTON
    }

    /// Convert this Force into a floating point value in dynes
    pub fn as_dynes(&self) -> f64 {
        self.newtons * DYNES_PER_NEWTON
    }
}

impl Measurement for Force {
    fn as_base_units(&self) -> f64 {
        self.newtons
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_newtons(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "N"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("nN", 1e-9),
            ("\u{00B5}N", 1e-6),
            ("mN", 1e-3),
            ("N", 1e0),
            ("kN", 1e3),
            ("MN", 1e6),
            ("GN", 1e9),
            ("TN", 1e12),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Force }

#[cfg(test)]
mod test {
    use force::*;
    use test_utils::assert_almost_eq;

    #[test]
    pub fn newtons() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_newtons();

        assert_almost_eq(r1, 100.0);
    }

    #[test]
    pub fn micronewtons() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_micronewtons();

        let i2 = Force::from_micronewtons(100.0);
        let r2 = i2.as_newtons();

        assert_almost_eq(r1, 1e8);
        assert_almost_eq(r2, 1e-4);
    }

    #[test]
    pub fn millinewtons() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_millinewtons();

        let i2 = Force::from_millinewtons(100.0);
        let r2 = i2.as_newtons();

        assert_almost_eq(r1, 1e5);
        assert_almost_eq(r2, 1e-1);
    }

    #[test]
    pub fn pounds() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_pounds();

        let i2 = Force::from_pounds(100.0);
        let r2 = i2.as_newtons();

        assert_almost_eq(r1, 22.480886300718);
        assert_almost_eq(r2, 444.822);
    }

    #[test]
    pub fn poundals() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_poundals();

        let i2 = Force::from_poundals(100.0);
        let r2 = i2.as_newtons();

        assert_almost_eq(r1, 723.3016238104);
        assert_almost_eq(r2, 13.8255);
    }

    #[test]
    pub fn kiloponds() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_kiloponds();

        let i2 = Force::from_kiloponds(100.0);
        let r2 = i2.as_newtons();

        assert_almost_eq(r1, 10.1972);
        assert_almost_eq(r2, 980.665);
    }

    #[test]
    pub fn dynes() {
        let i1 = Force::from_newtons(100.0);
        let r1 = i1.as_dynes();

        let i2 = Force::from_dynes(100.0);
        let r2 = i2.as_newtons();

        assert_almost_eq(r1, 1e7);
        assert_almost_eq(r2, 0.001);
    }

    #[test]
    fn add() {
        let a = Force::from_newtons(2.0);
        let b = Force::from_newtons(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_newtons(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Force::from_newtons(2.0);
        let b = Force::from_newtons(4.0);
        let c = a - b;
        assert_almost_eq(c.as_newtons(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Force::from_newtons(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_newtons(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Force::from_newtons(2.0);
        let b = Force::from_newtons(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_newtons(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Force::from_newtons(2.0);
        let b = Force::from_newtons(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Force::from_newtons(2.0);
        let b = Force::from_newtons(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Force::from_newtons(2.0);
        let b = Force::from_newtons(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
