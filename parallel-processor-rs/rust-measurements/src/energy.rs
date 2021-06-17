//! Types and constants for handling energy.

use super::measurement::*;

/// The `Energy` struct can be used to deal with energies in a common way.
/// Common metric and imperial units are supported.
///
/// # Example
///
/// ```
/// use measurements::Energy;
///
/// let energy = Energy::from_kcalories(2500.0);
/// println!("Some say a health adult male should consume {} per day", energy);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Energy {
    joules: f64,
}

impl Energy {
    /// Create a new Energy from a floating point value in Joules (or watt-seconds)
    pub fn from_joules(joules: f64) -> Energy {
        Energy { joules }
    }

    /// Create a new Energy from a floating point value in Kilocalories (often just called calories)
    pub fn from_kcalories(kcalories: f64) -> Energy {
        Self::from_joules(kcalories * 4186.8)
    }

    /// Create a new Energy from a floating point value in British Thermal Units
    pub fn from_btu(btu: f64) -> Energy {
        Self::from_joules(btu * 1055.056)
    }

    /// Create a new Energy from a floating point value in electron Volts (eV).
    pub fn from_e_v(e_v: f64) -> Energy {
        Self::from_joules(e_v / 6.241_509_479_607_718e18)
    }

    /// Create a new Energy from a floating point value in Watt-hours (Wh)
    pub fn from_watt_hours(wh: f64) -> Energy {
        Self::from_joules(wh * 3600.0)
    }

    /// Create a new Energy from a floating point value in Kilowatt-Hours (kWh)
    pub fn from_kilowatt_hours(kwh: f64) -> Energy {
        Self::from_joules(kwh * 3600.0 * 1000.0)
    }

    /// Convert this Energy into a floating point value in Joules (or watt-seconds)
    pub fn as_joules(&self) -> f64 {
        self.joules
    }

    /// Convert this Energy into a floating point value in Kilocalories (often just called calories)
    pub fn as_kcalories(&self) -> f64 {
        self.joules / 4186.8
    }

    /// Convert this Energy into a floating point value in British Thermal Units
    pub fn as_btu(&self) -> f64 {
        self.joules / 1055.056
    }

    /// Convert this Energy into a floating point value in electron volts (eV)
    pub fn as_e_v(&self) -> f64 {
        self.joules * 6.241_509_479_607_718e18
    }

    /// Convert this Energy into a floating point value in Watt-hours (Wh)
    pub fn as_watt_hours(&self) -> f64 {
        self.joules / 3600.0
    }

    /// Convert this Energy into a floating point value in kilowatt-hours (kWh)
    pub fn as_kilowatt_hours(&self) -> f64 {
        self.joules / (3600.0 * 1000.0)
    }
}

impl Measurement for Energy {
    fn as_base_units(&self) -> f64 {
        self.joules
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_joules(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "J"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to Largest
        let list = [
            ("fJ", 1e-15),
            ("pJ", 1e-12),
            ("nJ", 1e-9),
            ("\u{00B5}J", 1e-6),
            ("mJ", 1e-3),
            ("J", 1e0),
            ("kJ", 1e3),
            ("MJ", 1e6),
            ("GJ", 1e9),
            ("TJ", 1e12),
            ("PJ", 1e15),
            ("EJ", 1e18),
        ];
        self.pick_appropriate_units(&list)
    }
}

implement_measurement! { Energy }

#[cfg(test)]
mod test {
    use energy::*;
    use test_utils::assert_almost_eq;

    #[test]
    pub fn as_kcalories() {
        let i1 = Energy::from_kcalories(100.0);
        let r1 = i1.as_joules();

        let i2 = Energy::from_joules(100.0);
        let r2 = i2.as_kcalories();

        assert_almost_eq(r1, 418680.0);
        assert_almost_eq(r2, 0.0238845896627496);
    }

    #[test]
    pub fn as_btu() {
        let i1 = Energy::from_btu(100.0);
        let r1 = i1.as_joules();

        let i2 = Energy::from_joules(100.0);
        let r2 = i2.as_btu();

        assert_almost_eq(r1, 105505.6);
        assert_almost_eq(r2, 0.0947816987913438);
    }

    #[test]
    pub fn as_e_v() {
        let i1 = Energy::from_e_v(100.0);
        let r1 = i1.as_joules();

        let i2 = Energy::from_joules(100.0);
        let r2 = i2.as_e_v();

        assert_almost_eq(r1, 1.60217653e-17);
        assert_almost_eq(r2, 6.241509479607718e+20);
    }

    #[test]
    pub fn as_watt_hours() {
        let i1 = Energy::from_watt_hours(100.0);
        let r1 = i1.as_joules();

        let i2 = Energy::from_joules(100.0);
        let r2 = i2.as_watt_hours();

        assert_almost_eq(r1, 360000.0);
        assert_almost_eq(r2, 0.02777777777777777777777777777778);
    }

    #[test]
    pub fn as_kilowatt_hours() {
        let i1 = Energy::from_kilowatt_hours(100.0);
        let r1 = i1.as_joules();

        let i2 = Energy::from_joules(100.0);
        let r2 = i2.as_kilowatt_hours();

        assert_almost_eq(r1, 360000000.0);
        assert_almost_eq(r2, 2.777777777777777777777777777778e-5);
    }

    // Traits
    #[test]
    fn add() {
        let a = Energy::from_joules(2.0);
        let b = Energy::from_joules(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_joules(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Energy::from_joules(2.0);
        let b = Energy::from_joules(4.0);
        let c = a - b;
        assert_almost_eq(c.as_joules(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Energy::from_joules(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_joules(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Energy::from_joules(2.0);
        let b = Energy::from_joules(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_joules(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Energy::from_joules(2.0);
        let b = Energy::from_joules(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Energy::from_joules(2.0);
        let b = Energy::from_joules(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Energy::from_joules(2.0);
        let b = Energy::from_joules(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
