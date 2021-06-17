//! Types and constants for handling masses.

use super::measurement::*;
#[cfg(feature = "from_str")]
use regex::Regex;
#[cfg(feature = "from_str")]
use std::str::FromStr;

// Constants, metric

/// Number of ng in a kg
pub const KILOGRAM_NANOGRAM_FACTOR: f64 = 1e12;
/// Number of µg in a kg
pub const KILOGRAM_MICROGRAM_FACTOR: f64 = 1e9;
/// Number of mg in a kg
pub const KILOGRAM_MILLIGRAM_FACTOR: f64 = 1e6;
/// Number of g in a kg
pub const KILOGRAM_GRAM_FACTOR: f64 = 1e3;
/// Number of Tonnes in a kg
pub const KILOGRAM_TONNE_FACTOR: f64 = 1e-3;
/// Number of carats in a kg
pub const KILOGRAM_CARAT_FACTOR: f64 = 5000.0;

// Constants, imperial

/// Number of Grains in a kg
pub const KILOGRAM_GRAINS_FACTOR: f64 = KILOGRAM_MILLIGRAM_FACTOR / 64.79891;
/// Number of Pennyweights in a kg
pub const KILOGRAM_PENNYWEIGHTS_FACTOR: f64 = KILOGRAM_GRAINS_FACTOR / 24.0;
/// Number of Avoirdupois Ounces in a kg
pub const KILOGRAM_OUNCES_FACTOR: f64 = KILOGRAM_POUNDS_FACTOR * 16.0;
/// Number of Troy Ounces in a kg
pub const KILOGRAM_TROY_OUNCES_FACTOR: f64 = KILOGRAM_GRAM_FACTOR / 31.1034768;
/// Number of Avoirdupois Pounds in a kg
pub const KILOGRAM_POUNDS_FACTOR: f64 = 1.0 / 0.45359237;
/// Number of Troy Pounds in a kg
pub const KILOGRAM_TROY_POUNDS_FACTOR: f64 = KILOGRAM_TROY_OUNCES_FACTOR / 12.0;
/// Number of Avoirdupois Stone in a kg
pub const KILOGRAM_STONES_FACTOR: f64 = KILOGRAM_POUNDS_FACTOR / 14.0;
/// Number of Short (US) Tons in a kg
pub const KILOGRAM_SHORT_TONS_FACTOR: f64 = KILOGRAM_POUNDS_FACTOR / 2000.0;
/// Number of Long (international) Tons in a kg
pub const KILOGRAM_LONG_TONS_FACTOR: f64 = KILOGRAM_POUNDS_FACTOR / 2240.0;

/// The Mass struct can be used to deal with mass in a common way. Metric,
/// avoirdupois imperial and troy imperial units are supported.
///
/// #Example
///
/// ```
/// use measurements::Mass;
///
/// let metric_ton = Mass::from_metric_tons(1.0);
/// let united_states_tons = metric_ton.as_short_tons();
/// let united_states_pounds = metric_ton.as_pounds();
/// println!(
///     "One metric ton is {} U.S. tons - that's {} pounds!",
///     united_states_tons, united_states_pounds);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Mass {
    kilograms: f64,
}

impl Mass {
    /// Create a Mass from a floating point value in kilograms
    pub fn from_kilograms(kilograms: f64) -> Self {
        Mass { kilograms }
    }

    /// Create a Mass from a floating point value in micrograms
    pub fn from_micrograms(micrograms: f64) -> Self {
        Self::from_kilograms(micrograms / KILOGRAM_MICROGRAM_FACTOR)
    }

    /// Create a Mass from a floating point value in milligrams
    pub fn from_milligrams(milligrams: f64) -> Self {
        Self::from_kilograms(milligrams / KILOGRAM_MILLIGRAM_FACTOR)
    }

    /// Create a Mass from a floating point value in carats
    pub fn from_carats(carats: f64) -> Self {
        Self::from_kilograms(carats / KILOGRAM_CARAT_FACTOR)
    }

    /// Create a Mass from a floating point value in grams
    pub fn from_grams(grams: f64) -> Self {
        Self::from_kilograms(grams / KILOGRAM_GRAM_FACTOR)
    }

    /// Create a Mass from a floating point value in metric tonnes
    pub fn from_metric_tons(metric_tons: f64) -> Self {
        Self::from_kilograms(metric_tons / KILOGRAM_TONNE_FACTOR)
    }

    /// Create a Mass from a floating point value in metric tonnes
    pub fn from_tonnes(metric_tons: f64) -> Self {
        Self::from_kilograms(metric_tons / KILOGRAM_TONNE_FACTOR)
    }

    /// Create a Mass from a floating point value in grains
    pub fn from_grains(grains: f64) -> Self {
        Self::from_kilograms(grains / KILOGRAM_GRAINS_FACTOR)
    }

    /// Create a Mass from a floating point value in pennyweights
    pub fn from_pennyweights(pennyweights: f64) -> Self {
        Self::from_kilograms(pennyweights / KILOGRAM_PENNYWEIGHTS_FACTOR)
    }

    /// Create a Mass from a floating point value in ounces
    pub fn from_ounces(ounces: f64) -> Self {
        Self::from_kilograms(ounces / KILOGRAM_OUNCES_FACTOR)
    }

    /// Create a Mass from a floating point value in troy_ounces
    pub fn from_troy_ounces(troy_ounces: f64) -> Self {
        Self::from_kilograms(troy_ounces / KILOGRAM_TROY_OUNCES_FACTOR)
    }

    /// Create a Mass from a floating point value in Pounds (lbs)
    pub fn from_pounds(pounds: f64) -> Self {
        Self::from_kilograms(pounds / KILOGRAM_POUNDS_FACTOR)
    }

    /// Create a Mass from a floating point value in Troy Pounds
    pub fn from_troy_pounds(troy_pounds: f64) -> Self {
        Self::from_kilograms(troy_pounds / KILOGRAM_TROY_POUNDS_FACTOR)
    }

    /// Create a Mass from a floating point value in Stone (st.)
    pub fn from_stones(stones: f64) -> Self {
        Self::from_kilograms(stones / KILOGRAM_STONES_FACTOR)
    }

    /// Create a Mass from a floating point value in short (US) tons
    pub fn from_short_tons(short_tons: f64) -> Self {
        Self::from_kilograms(short_tons / KILOGRAM_SHORT_TONS_FACTOR)
    }

    /// Create a Mass from a floating point value in long (imperial) tons
    pub fn from_long_tons(long_tons: f64) -> Self {
        Self::from_kilograms(long_tons / KILOGRAM_LONG_TONS_FACTOR)
    }

    /// Convert this Mass to a floating point value in micrograms
    pub fn as_micrograms(&self) -> f64 {
        self.kilograms * KILOGRAM_MICROGRAM_FACTOR
    }

    /// Convert this Mass to a floating point value in milligrams
    pub fn as_milligrams(&self) -> f64 {
        self.kilograms * KILOGRAM_MILLIGRAM_FACTOR
    }

    /// Convert this Mass to a floating point value in carats
    pub fn as_carats(&self) -> f64 {
        self.kilograms * KILOGRAM_CARAT_FACTOR
    }

    /// Convert this Mass to a floating point value in grams
    pub fn as_grams(&self) -> f64 {
        self.kilograms * KILOGRAM_GRAM_FACTOR
    }

    /// Convert this Mass to a floating point value in kilograms (kg)
    pub fn as_kilograms(&self) -> f64 {
        self.kilograms
    }

    /// Convert this Mass to a floating point value in metric Tonnes
    pub fn as_metric_tons(&self) -> f64 {
        self.kilograms * KILOGRAM_TONNE_FACTOR
    }

    /// Convert this Mass to a floating point value in metric Tonnes
    pub fn as_tonnes(&self) -> f64 {
        self.kilograms * KILOGRAM_TONNE_FACTOR
    }

    /// Convert this Mass to a floating point value in Grains
    pub fn as_grains(&self) -> f64 {
        self.kilograms * KILOGRAM_GRAINS_FACTOR
    }

    /// Convert this Mass to a floating point value in Pennyweights
    pub fn as_pennyweights(&self) -> f64 {
        self.kilograms * KILOGRAM_PENNYWEIGHTS_FACTOR
    }

    /// Convert this Mass to a floating point value in Ounces (oz)
    pub fn as_ounces(&self) -> f64 {
        self.kilograms * KILOGRAM_OUNCES_FACTOR
    }

    /// Convert this Mass to a floating point value in Pounds (lbs)
    pub fn as_pounds(&self) -> f64 {
        self.kilograms * KILOGRAM_POUNDS_FACTOR
    }

    /// Convert this Mass to a floating point value in Troy Ounces
    pub fn as_troy_ounces(&self) -> f64 {
        self.kilograms * KILOGRAM_TROY_OUNCES_FACTOR
    }

    /// Convert this Mass to a floating point value in Troy Pounds
    pub fn as_troy_pounds(&self) -> f64 {
        self.kilograms * KILOGRAM_TROY_POUNDS_FACTOR
    }

    /// Convert this Mass to a floating point value in Stone (st.)
    pub fn as_stones(&self) -> f64 {
        self.kilograms * KILOGRAM_STONES_FACTOR
    }

    /// Convert this Mass to a floating point value in short (US) Tons
    pub fn as_short_tons(&self) -> f64 {
        self.kilograms * KILOGRAM_SHORT_TONS_FACTOR
    }

    /// Convert this Mass to a floating point value in long (international) Tons
    pub fn as_long_tons(&self) -> f64 {
        self.kilograms * KILOGRAM_LONG_TONS_FACTOR
    }
}

impl Measurement for Mass {
    fn as_base_units(&self) -> f64 {
        self.kilograms
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_kilograms(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "kg"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("ng", 1e-12),
            ("\u{00B5}g", 1e-9),
            ("mg", 1e-6),
            ("g", 1e-3),
            ("kg", 1e0),
            ("tonnes", 1e3),
            ("thousand tonnes", 1e6),
            ("million tonnes", 1e9),
        ];
        self.pick_appropriate_units(&list)
    }
}

#[cfg(feature = "from_str")]
impl FromStr for Mass {
    type Err = std::num::ParseFloatError;

    /// Create a new Mass from a string
    /// Plain numbers in string are considered to be Kilograms
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        if val.is_empty() {
            return Ok(Mass::from_kilograms(0.0));
        }

        let re = Regex::new(r"(?i)\s*([0-9.]*)\s?([a-zμ]{1,3})\s*$").unwrap();
        if let Some(caps) = re.captures(val) {
            let float_val = caps.get(1).unwrap().as_str();
            return Ok(
                match caps.get(2).unwrap().as_str().to_lowercase().as_str() {
                    "ug" | "μg" => Mass::from_micrograms(float_val.parse::<f64>()?),
                    "mg" => Mass::from_milligrams(float_val.parse::<f64>()?),
                    "ct" => Mass::from_carats(float_val.parse::<f64>()?),
                    "g" => Mass::from_grams(float_val.parse::<f64>()?),
                    "kg" => Mass::from_kilograms(float_val.parse::<f64>()?),
                    "t" => Mass::from_metric_tons(float_val.parse::<f64>()?),
                    "gr" => Mass::from_grains(float_val.parse::<f64>()?),
                    "dwt" => Mass::from_pennyweights(float_val.parse::<f64>()?),
                    "oz" => Mass::from_ounces(float_val.parse::<f64>()?),
                    "st" => Mass::from_stones(float_val.parse::<f64>()?),
                    "lbs" => Mass::from_pounds(float_val.parse::<f64>()?),
                    _ => Mass::from_grams(float_val.parse::<f64>()?),
                },
            );
        }

        Ok(Mass::from_kilograms(val.parse::<f64>()?))
    }
}

implement_measurement! { Mass }

#[cfg(test)]
mod test {
    use mass::*;
    use test_utils::assert_almost_eq;

    // Mass Units
    // Metric
    #[test]
    fn kilograms() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    fn micrograms() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_micrograms();
        assert_almost_eq(o, 1e+11);

        let t = Mass::from_micrograms(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 1e-7);
    }

    #[test]
    fn milligrams() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_milligrams();
        assert_almost_eq(o, 1e+8);

        let t = Mass::from_milligrams(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 0.0001);
    }

    #[test]
    fn carats() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_carats();
        assert_almost_eq(o, 500000.0);

        let t = Mass::from_carats(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 0.02);
    }

    #[test]
    fn grams() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_grams();
        assert_almost_eq(o, 100000.0);

        let t = Mass::from_grams(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 0.1);
    }

    #[test]
    fn metric_tons() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_metric_tons();
        assert_almost_eq(o, 0.1);

        let t = Mass::from_metric_tons(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 100000.0);
    }

    // Imperial
    #[test]
    fn grains() {
        let t = Mass::from_kilograms(1.0);
        let o = t.as_grains();
        assert_almost_eq(o, 15432.358);

        let t = Mass::from_grains(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 0.0064798911);
    }

    #[test]
    fn pennyweights() {
        let t = Mass::from_kilograms(1.0);
        let o = t.as_pennyweights();
        assert_almost_eq(o, 643.01493);

        let t = Mass::from_pennyweights(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 0.15551738);
    }

    #[test]
    fn ounces() {
        let t = Mass::from_kilograms(1.0);
        let o = t.as_ounces();
        assert_almost_eq(o, 35.273962);

        let t = Mass::from_ounces(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 2.8349523);
    }

    #[test]
    fn troy_ounces() {
        let t = Mass::from_kilograms(1.0);
        let o = t.as_troy_ounces();
        assert_almost_eq(o, 32.150747);

        let t = Mass::from_troy_ounces(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 3.1103476);
    }

    #[test]
    fn pounds() {
        let t = Mass::from_kilograms(1.0);
        let o = t.as_pounds();
        assert_almost_eq(o, 2.2046228);

        let t = Mass::from_pounds(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 45.359233);
    }

    #[test]
    fn troy_pounds() {
        let t = Mass::from_kilograms(1.0);
        let o = t.as_troy_pounds();
        assert_almost_eq(o, 2.6792289);

        let t = Mass::from_troy_pounds(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 37.324172);
    }

    #[test]
    fn stones() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_stones();
        assert_almost_eq(o, 15.74730);

        let t = Mass::from_stones(100.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 635.02934);
    }

    #[test]
    fn short_tons() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_short_tons();
        assert_almost_eq(o, 0.11023113);

        let t = Mass::from_short_tons(1.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 907.18475);
    }

    #[test]
    fn long_tons() {
        let t = Mass::from_kilograms(100.0);
        let o = t.as_long_tons();
        assert_almost_eq(o, 0.098420653);

        let t = Mass::from_long_tons(1.0);
        let o = t.as_kilograms();
        assert_almost_eq(o, 1016.0469);
    }

    // Traits
    #[test]
    fn add() {
        let a = Mass::from_kilograms(2.0);
        let b = Mass::from_kilograms(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_kilograms(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Mass::from_kilograms(2.0);
        let b = Mass::from_kilograms(4.0);
        let c = a - b;
        assert_almost_eq(c.as_kilograms(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Mass::from_kilograms(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_kilograms(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Mass::from_kilograms(2.0);
        let b = Mass::from_kilograms(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_kilograms(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Mass::from_kilograms(2.0);
        let b = Mass::from_kilograms(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Mass::from_kilograms(2.0);
        let b = Mass::from_kilograms(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Mass::from_kilograms(2.0);
        let b = Mass::from_kilograms(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn number_str() {
        let t = Mass::from_str("100.5");
        assert!(t.is_ok());
        let o = t.unwrap().as_kilograms();
        assert_almost_eq(o, 100.5);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn micrograms_from_string() {
        assert_almost_eq(123.0, Mass::from_str(" 123ug ").unwrap().as_micrograms());
        assert_almost_eq(123.0, Mass::from_str("123 ug ").unwrap().as_micrograms());
        assert_almost_eq(123.0, Mass::from_str("  123μg").unwrap().as_micrograms());
        assert_almost_eq(123.0, Mass::from_str("123 μg").unwrap().as_micrograms());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn milligrams_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123mg").unwrap().as_milligrams());
        assert_almost_eq(123.0, Mass::from_str("123 mg").unwrap().as_milligrams());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn carats_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123ct").unwrap().as_carats());
        assert_almost_eq(123.0, Mass::from_str("123 ct").unwrap().as_carats());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn grams_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123g").unwrap().as_grams());
        assert_almost_eq(123.0, Mass::from_str("123 g").unwrap().as_grams());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn kilograms_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123kg").unwrap().as_kilograms());
        assert_almost_eq(123.0, Mass::from_str("123 kg").unwrap().as_kilograms());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn tonnes_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123T").unwrap().as_tonnes());
        assert_almost_eq(123.0, Mass::from_str("123 T").unwrap().as_tonnes());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn grains_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123gr").unwrap().as_grains());
        assert_almost_eq(123.0, Mass::from_str("123 gr").unwrap().as_grains());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn pennyweights_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123dwt").unwrap().as_pennyweights());
        assert_almost_eq(123.0, Mass::from_str("123 dwt").unwrap().as_pennyweights());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn ounces_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123oz").unwrap().as_ounces());
        assert_almost_eq(123.0, Mass::from_str("123 oz").unwrap().as_ounces());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn pounds_from_string() {
        assert_almost_eq(123.0, Mass::from_str("123lbs").unwrap().as_pounds());
        assert_almost_eq(123.0, Mass::from_str("123 lbs").unwrap().as_pounds());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn invalid_str() {
        let t = Mass::from_str("abcd");
        assert!(t.is_err());
    }
}
