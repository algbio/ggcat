//! Types and constants for handling volumes (that is, three-dimensional space, not loudness).

use super::measurement::*;
#[cfg(feature = "from_str")]
use regex::Regex;
#[cfg(feature = "from_str")]
use std::str::FromStr;

/// The `Volume` struct can be used to deal with volumes in a common way.
///
/// #Example
///
/// ```
/// use measurements::Volume;
///
/// let gallon = Volume::from_gallons(1.0);
/// let pint = Volume::from_pints(1.0);
/// let beers = gallon / pint;
/// println!("A gallon of beer will pour {} pints!", beers);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Volume {
    liters: f64,
}

/// Number of Milliliters in a litre
pub const LITER_MILLILITERS_FACTOR: f64 = 1000.0;
/// Number of Cubic Centimeters in a litre
pub const LITER_CUBIC_CENTIMETER_FACTOR: f64 = 1000.0;
/// Number of Cubic Meters in a litre
pub const LITER_CUBIC_METER_FACTOR: f64 = 1.0 / 1000.0;
/// Number of Drops in a litre
pub const LITER_DROP_FACTOR: f64 = 15419.6298055;
/// Number of (US) Fluid Drams in a litre
pub const LITER_DRAM_FACTOR: f64 = 270.510351863;
/// Number of Teaspoons in a litre
pub const LITER_TEASPOONS_FACTOR: f64 = 202.8841362;
/// Number of Tablespoons in a litre
pub const LITER_TABLESPOONS_FACTOR: f64 = 67.6280454;
/// Number of Cubic Inches in a litre
pub const LITER_CUBIC_INCHES_FACTOR: f64 = 61.0237440947;
/// Number of Fluid Ounces (UK) in a litre
pub const LITER_FLUID_OUNCES_UK_FACTOR: f64 = 35.19506424;
/// Number of Fluid Ounces (US) in a litre
pub const LITER_FLUID_OUNCES_FACTOR: f64 = 33.8140227;
/// Number of Cups in a litre
pub const LITER_CUP_FACTOR: f64 = 4.226752838;
/// Number of Pints in a litre
pub const LITER_PINTS_FACTOR: f64 = 2.11337641887;
/// Number of Pints (UK) in a litre
pub const LITER_PINTS_UK_FACTOR: f64 = 1.75975398639;
/// Number of Quarts in a litre
pub const LITER_QUARTS_FACTOR: f64 = 1.05668820943;
/// Number of Gallons in a litre
pub const LITER_GALLONS_FACTOR: f64 = 0.264172052358;
/// Number of Gallons (UK) in a litre
pub const LITER_GALLONS_UK_FACTOR: f64 = 0.219969248299;
/// Number of Cubic Feet in a litre
pub const LITER_CUBIC_FEET_FACTOR: f64 = 0.0353146667215;
/// Number of Cubic Yards in a litre
pub const LITER_CUBIC_YARD_FACTOR: f64 = 0.0013079506193;

impl Volume {
    /// Create a new Volume from a floating point value in Liters (l)
    pub fn from_liters(liters: f64) -> Self {
        Volume { liters }
    }

    /// Create a new Volume from a floating point value in Litres (l)
    pub fn from_litres(liters: f64) -> Self {
        Self::from_liters(liters)
    }

    /// Create a new Volume from a floating point value in Cubic Centimeters (cc or cm³)
    pub fn from_cubic_centimeters(cubic_centimeters: f64) -> Self {
        Self::from_liters(cubic_centimeters / LITER_CUBIC_CENTIMETER_FACTOR)
    }

    /// Create a new Volume from a floating point value in Cubic Centimetres (cc or cm³)
    pub fn from_cubic_centimetres(cubic_centimeters: f64) -> Self {
        Self::from_cubic_centimeters(cubic_centimeters)
    }

    /// Create a new Volume from a floating point value in Milliliters (ml)
    pub fn from_milliliters(milliliters: f64) -> Self {
        Self::from_liters(milliliters / LITER_MILLILITERS_FACTOR)
    }

    /// Create a new Volume from a floating point value in Millilitres (ml)
    pub fn from_millilitres(milliliters: f64) -> Self {
        Self::from_milliliters(milliliters)
    }

    /// Create a new Volume from a floating point value in Cubic Meters (m³)
    pub fn from_cubic_meters(cubic_meters: f64) -> Self {
        Self::from_liters(cubic_meters / LITER_CUBIC_METER_FACTOR)
    }

    /// Create a new Volume from a floating point value in Cubic Metres (m³)
    pub fn from_cubic_metres(cubic_meters: f64) -> Self {
        Self::from_cubic_meters(cubic_meters)
    }

    /// Create a new Volume from a floating point value in Drops
    pub fn from_drops(drops: f64) -> Self {
        Self::from_liters(drops / LITER_DROP_FACTOR)
    }

    /// Create a new Volume from a floating point value in US Fluid Drams
    pub fn from_drams(drams: f64) -> Self {
        Self::from_liters(drams / LITER_DRAM_FACTOR)
    }

    /// Create a new Volume from a floating point value in Teaspoons (tsp)
    pub fn from_teaspoons(teaspoons: f64) -> Self {
        Self::from_liters(teaspoons / LITER_TEASPOONS_FACTOR)
    }

    /// Create a new Volume from a floating point value in Tablespoons (tbsp)
    pub fn from_tablespoons(tablespoons: f64) -> Self {
        Self::from_liters(tablespoons / LITER_TABLESPOONS_FACTOR)
    }

    /// Create a new Volume from a floating point value in UK Fluid Ounces (fl oz)
    pub fn from_fluid_ounces_uk(fluid_ounces_uk: f64) -> Self {
        Self::from_liters(fluid_ounces_uk / LITER_FLUID_OUNCES_UK_FACTOR)
    }

    /// Create a new Volume from a floating point value in US Fluid Ounces (fl oz)
    pub fn from_fluid_ounces(fluid_ounces: f64) -> Self {
        Self::from_liters(fluid_ounces / LITER_FLUID_OUNCES_FACTOR)
    }

    /// Create a new Volume from a floating point value in Cubic Inches (cu in or in³)
    pub fn from_cubic_inches(cubic_inches: f64) -> Self {
        Self::from_liters(cubic_inches / LITER_CUBIC_INCHES_FACTOR)
    }

    /// Create a new Volume from a floating point value in Cups
    pub fn from_cups(cups: f64) -> Self {
        Self::from_liters(cups / LITER_CUP_FACTOR)
    }

    /// Create a new Volume from a floating point value in US Pints
    pub fn from_pints(pints: f64) -> Self {
        Self::from_liters(pints / LITER_PINTS_FACTOR)
    }

    /// Create a new Volume from a floating point value in UK Pints
    pub fn from_pints_uk(pints_uk: f64) -> Self {
        Self::from_liters(pints_uk / LITER_PINTS_UK_FACTOR)
    }

    /// Create a new Volume from a floating point value in Quarts
    pub fn from_quarts(quarts: f64) -> Self {
        Self::from_liters(quarts / LITER_QUARTS_FACTOR)
    }

    /// Create a new Volume from a floating point value in US Gallons (gal US)
    pub fn from_gallons(gallons: f64) -> Self {
        Self::from_liters(gallons / LITER_GALLONS_FACTOR)
    }

    /// Create a new Volume from a floating point value in UK/Imperial Gallons (gal)
    pub fn from_gallons_uk(gallons_uk: f64) -> Self {
        Self::from_liters(gallons_uk / LITER_GALLONS_UK_FACTOR)
    }

    /// Create a new Volume from a floating point value in Cubic Feet (ft³)
    pub fn from_cubic_feet(cubic_feet: f64) -> Self {
        Self::from_liters(cubic_feet / LITER_CUBIC_FEET_FACTOR)
    }

    /// Create a new Volume from a floating point value in Cubic Yards (yd³)
    pub fn from_cubic_yards(cubic_yards: f64) -> Self {
        Self::from_liters(cubic_yards / LITER_CUBIC_YARD_FACTOR)
    }

    /// Convert Volume to a floating point value in Cubic Centimeters (cc or cm³)
    pub fn as_cubic_centimeters(&self) -> f64 {
        self.liters * LITER_CUBIC_CENTIMETER_FACTOR
    }

    /// Convert Volume to a floating point value in Cubic Centimetres (cc or cm³)
    pub fn as_cubic_centimetres(&self) -> f64 {
        self.as_cubic_centimeters()
    }

    /// Convert Volume to a floating point value in Milliliters (ml)
    pub fn as_milliliters(&self) -> f64 {
        self.liters * LITER_MILLILITERS_FACTOR
    }

    /// Convert Volume to a floating point value in Millilitres (ml)
    pub fn as_millilitres(&self) -> f64 {
        self.as_milliliters()
    }

    /// Convert Volume to a floating point value in Liters (l)
    pub fn as_liters(&self) -> f64 {
        self.liters
    }

    /// Convert Volume to a floating point value in Litres (l)
    pub fn as_litres(&self) -> f64 {
        self.as_liters()
    }

    /// Convert Volume to a floating point value in Cubic Meters (m³)
    pub fn as_cubic_meters(&self) -> f64 {
        self.liters * LITER_CUBIC_METER_FACTOR
    }

    /// Convert Volume to a floating point value in Cubic Metres (m³)
    pub fn as_cubic_metres(&self) -> f64 {
        self.as_cubic_meters()
    }

    /// Convert Volume to a floating point value in Drops
    pub fn as_drops(&self) -> f64 {
        self.liters * LITER_DROP_FACTOR
    }

    /// Convert Volume to a floating point value in US Fluid Drams
    pub fn as_drams(&self) -> f64 {
        self.liters * LITER_DRAM_FACTOR
    }

    /// Convert Volume to a floating point value in Teaspoons (tsp)
    pub fn as_teaspoons(&self) -> f64 {
        self.liters * LITER_TEASPOONS_FACTOR
    }

    /// Convert Volume to a floating point value in Tablespoons (tbsp)
    pub fn as_tablespoons(&self) -> f64 {
        self.liters * LITER_TABLESPOONS_FACTOR
    }

    /// Convert Volume to a floating point value in Cubic Inches (cu in or in³)
    pub fn as_cubic_inches(&self) -> f64 {
        self.liters * LITER_CUBIC_INCHES_FACTOR
    }

    /// Convert Volume to a floating point value in UK Fluid Ounces (fl oz)
    pub fn as_fluid_ounces_uk(&self) -> f64 {
        self.liters * LITER_FLUID_OUNCES_UK_FACTOR
    }

    /// Convert Volume to a floating point value in US Fluid Ounces (fl oz)
    pub fn as_fluid_ounces(&self) -> f64 {
        self.liters * LITER_FLUID_OUNCES_FACTOR
    }

    /// Convert Volume to a floating point value in Cups
    pub fn as_cups(&self) -> f64 {
        self.liters * LITER_CUP_FACTOR
    }

    /// Convert Volume to a floating point value in US Pints
    pub fn as_pints(&self) -> f64 {
        self.liters * LITER_PINTS_FACTOR
    }

    /// Convert Volume to a floating point value in UK Pints
    pub fn as_pints_uk(&self) -> f64 {
        self.liters * LITER_PINTS_UK_FACTOR
    }

    /// Convert Volume to a floating point value in Quarts
    pub fn as_quarts(&self) -> f64 {
        self.liters * LITER_QUARTS_FACTOR
    }

    /// Convert Volume to a floating point value in US Gallons (gal us)
    pub fn as_gallons(&self) -> f64 {
        self.liters * LITER_GALLONS_FACTOR
    }

    /// Convert Volume to a floating point value in UK/Imperial Gallons (gal)
    pub fn as_gallons_uk(&self) -> f64 {
        self.liters * LITER_GALLONS_UK_FACTOR
    }

    /// Convert Volume to a floating point value in Cubic Feet (ft³)
    pub fn as_cubic_feet(&self) -> f64 {
        self.liters * LITER_CUBIC_FEET_FACTOR
    }

    /// Convert Volume to a floating point value in Cubic Yards (yd³)
    pub fn as_cubic_yards(&self) -> f64 {
        self.liters * LITER_CUBIC_YARD_FACTOR
    }
}

impl Measurement for Volume {
    fn as_base_units(&self) -> f64 {
        self.liters
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_liters(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "l"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("pl", 1e-12),
            ("nl", 1e-9),
            ("\u{00B5}l", 1e-6),
            ("ml", 1e-3),
            ("l", 1e0),
            ("m\u{00B3}", 1e3),
            ("km\u{00B3}", 1e12),
        ];
        self.pick_appropriate_units(&list)
    }
}

#[cfg(feature = "from_str")]
impl FromStr for Volume {
    type Err = std::num::ParseFloatError;

    /// Create a new Volume from a string
    /// Plain numbers in string are considered to be Liters. Defaults for units with US
    /// and UK variants are considered to be in US without specific "imp" prefix.
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        if val.is_empty() {
            return Ok(Volume::from_liters(0.0));
        }

        let re = Regex::new(r"(?i)\s*([0-9.]*)\s?([a-z .]{1,10}[0-9³]{0,1})\s*$").unwrap();
        if let Some(caps) = re.captures(val) {
            let float_val = caps.get(1).unwrap().as_str();
            return Ok(
                match caps.get(2).unwrap().as_str().to_lowercase().as_str() {
                    "cm3" | "cm\u{00b3}" => {
                        Volume::from_cubic_centimeters(float_val.parse::<f64>()?)
                    }
                    "ft3" | "ft\u{00b3}" => Volume::from_cubic_feet(float_val.parse::<f64>()?),
                    "yd3" | "yd\u{00b3}" => Volume::from_cubic_yards(float_val.parse::<f64>()?),
                    "in3" | "in\u{00b3}" => Volume::from_cubic_inches(float_val.parse::<f64>()?),
                    "gal" | "us gal" => Volume::from_gallons(float_val.parse::<f64>()?),
                    "imp gal" => Volume::from_gallons_uk(float_val.parse::<f64>()?),
                    "cup" => Volume::from_cups(float_val.parse::<f64>()?),
                    "tsp" => Volume::from_teaspoons(float_val.parse::<f64>()?),
                    "tbsp" | "t." => Volume::from_tablespoons(float_val.parse::<f64>()?),
                    "ml" => Volume::from_milliliters(float_val.parse::<f64>()?),
                    "us fl oz" | "fl oz" => Volume::from_fluid_ounces(float_val.parse::<f64>()?),
                    "imp fl oz" => Volume::from_fluid_ounces_uk(float_val.parse::<f64>()?),
                    "m3" | "m\u{00b3}" => Volume::from_cubic_meters(float_val.parse::<f64>()?),
                    "gt" | "gtt" => Volume::from_drops(float_val.parse::<f64>()?),
                    "dr" => Volume::from_drams(float_val.parse::<f64>()?),
                    "l" => Volume::from_litres(float_val.parse::<f64>()?),
                    "qt" => Volume::from_quarts(float_val.parse::<f64>()?),
                    "us pt" | "us p" | "p" | "pt" => Volume::from_pints(float_val.parse::<f64>()?),
                    "imp pt" | "imp p" => Volume::from_pints_uk(float_val.parse::<f64>()?),
                    _ => Volume::from_litres(val.parse::<f64>()?),
                },
            );
        }

        Ok(Volume::from_liters(val.parse::<f64>()?))
    }
}

implement_measurement! { Volume }

#[cfg(test)]
mod test {
    use test_utils::assert_almost_eq;
    use volume::*;

    // Volume Units
    // Metric
    #[test]
    fn litres() {
        let t = Volume::from_litres(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    fn cubic_centimeters() {
        let t = Volume::from_litres(1.0);
        let o = t.as_cubic_centimeters();
        assert_almost_eq(o, 1000.0);

        let t = Volume::from_cubic_centimeters(1000.0);
        let o = t.as_litres();
        assert_almost_eq(o, 1.0);
    }

    #[test]
    fn milliliters() {
        let t = Volume::from_litres(1.0);
        let o = t.as_milliliters();
        assert_almost_eq(o, 1000.0);

        let t = Volume::from_milliliters(1000.0);
        let o = t.as_litres();
        assert_almost_eq(o, 1.0);
    }

    #[test]
    fn cubic_meters() {
        let t = Volume::from_litres(100.0);
        let o = t.as_cubic_meters();
        assert_almost_eq(o, 0.1);

        let t = Volume::from_cubic_meters(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 100000.0);
    }

    // Imperial
    #[test]
    fn drops() {
        let t = Volume::from_litres(100.0);
        let o = t.as_drops();
        assert_almost_eq(o, 1541962.98055);

        let t = Volume::from_drops(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 0.00648524);
    }

    #[test]
    fn drams() {
        let t = Volume::from_litres(100.0);
        let o = t.as_drams();
        assert_almost_eq(o, 27051.0351863);

        let t = Volume::from_drams(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 0.36967162);
    }

    #[test]
    fn teaspoons() {
        let t = Volume::from_litres(100.0);
        let o = t.as_teaspoons();
        assert_almost_eq(o, 20288.41362);

        let t = Volume::from_teaspoons(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 0.492892159402);
    }

    #[test]
    fn tablespoons() {
        let t = Volume::from_litres(100.0);
        let o = t.as_tablespoons();
        assert_almost_eq(o, 6762.80454);

        let t = Volume::from_tablespoons(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 1.47867647821);
    }

    #[test]
    fn cubic_inches() {
        let t = Volume::from_litres(100.0);
        let o = t.as_cubic_inches();
        assert_almost_eq(o, 6102.37440947);

        let t = Volume::from_cubic_inches(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 1.6387064);
    }

    #[test]
    fn fluid_ounces_uk() {
        let t = Volume::from_litres(100.0);
        let o = t.as_fluid_ounces_uk();
        assert_almost_eq(o, 3519.506424);

        let t = Volume::from_fluid_ounces_uk(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 2.84130750034);
    }

    #[test]
    fn fluid_ounces() {
        let t = Volume::from_litres(100.0);
        let o = t.as_fluid_ounces();
        assert_almost_eq(o, 3381.40227);

        let t = Volume::from_fluid_ounces(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 2.95735295641);
    }

    #[test]
    fn cups() {
        let t = Volume::from_litres(100.0);
        let o = t.as_cups();
        assert_almost_eq(o, 422.6752838);

        let t = Volume::from_cups(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 23.6588236485);
    }

    #[test]
    fn pints() {
        let t = Volume::from_litres(100.0);
        let o = t.as_pints();
        assert_almost_eq(o, 211.337641887);

        let t = Volume::from_pints(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 47.3176473);
    }

    #[test]
    fn pints_uk() {
        let t = Volume::from_litres(100.0);
        let o = t.as_pints_uk();
        assert_almost_eq(o, 175.975398639);

        let t = Volume::from_pints_uk(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 56.826125);
    }

    #[test]
    fn quarts() {
        let t = Volume::from_litres(100.0);
        let o = t.as_quarts();
        assert_almost_eq(o, 105.668820943);

        let t = Volume::from_quarts(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 94.6352946);
    }

    #[test]
    fn gallons() {
        let t = Volume::from_litres(100.0);
        let o = t.as_gallons();
        assert_almost_eq(o, 26.4172052358);

        let t = Volume::from_gallons(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 378.5411784);
    }

    #[test]
    fn gallons_uk() {
        let t = Volume::from_litres(100.0);
        let o = t.as_gallons_uk();
        assert_almost_eq(o, 21.9969248299);

        let t = Volume::from_gallons_uk(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 454.609);
    }

    #[test]
    fn cubic_feet() {
        let t = Volume::from_litres(100.0);
        let o = t.as_cubic_feet();
        assert_almost_eq(o, 3.53146667215);

        let t = Volume::from_cubic_feet(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 2831.6846592);
    }

    #[test]
    fn cubic_yards() {
        let t = Volume::from_litres(100.0);
        let o = t.as_cubic_yards();
        assert_almost_eq(o, 0.13079506193);

        let t = Volume::from_cubic_yards(100.0);
        let o = t.as_litres();
        assert_almost_eq(o, 76455.4857992);
    }

    // Traits
    #[test]
    fn add() {
        let a = Volume::from_litres(2.0);
        let b = Volume::from_litres(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_litres(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Volume::from_litres(2.0);
        let b = Volume::from_litres(4.0);
        let c = a - b;
        assert_almost_eq(c.as_litres(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Volume::from_litres(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_litres(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Volume::from_litres(2.0);
        let b = Volume::from_litres(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_litres(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Volume::from_litres(2.0);
        let b = Volume::from_litres(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Volume::from_litres(2.0);
        let b = Volume::from_litres(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Volume::from_litres(2.0);
        let b = Volume::from_litres(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn empty_val_from_str() {
        let v = Volume::from_str("");
        assert!(v.is_ok());
        assert_eq!(0.0, v.unwrap().as_litres());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn cubic_centimeters_from_str() {
        let v = Volume::from_str(" 10cm3");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_cubic_centimeters());

        let v2 = Volume::from_str("10 cm³ ");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_cubic_centimetres());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn cubic_feet_from_str() {
        let v = Volume::from_str(" 10 ft3");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_cubic_feet());

        let v2 = Volume::from_str(" 10ft³ ");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_cubic_feet());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn cubic_yard_from_str() {
        let v = Volume::from_str(" 10 yd3");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_cubic_yards());

        let v2 = Volume::from_str(" 10yd³ ");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_cubic_yards());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn cubic_inches_from_str() {
        let v = Volume::from_str(" 10 in3");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_cubic_inches());

        let v2 = Volume::from_str("10in³ ");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_cubic_inches());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn gallons_from_str() {
        let v = Volume::from_str(" 10 gal");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_gallons());

        let v2 = Volume::from_str("10 US gal");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_gallons());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn uk_gallons_from_str() {
        let v = Volume::from_str(" 10 imp gal");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_gallons_uk());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn cups_from_str() {
        let v = Volume::from_str("10cup");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_cups());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn teaspoons_from_str() {
        let v = Volume::from_str("10tsp");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_teaspoons());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn tablespoons_from_str() {
        let v = Volume::from_str("10 tbsp");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_tablespoons());

        let v2 = Volume::from_str("10T.");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_tablespoons());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn milliliters_from_str() {
        let v = Volume::from_str("10ml");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_milliliters());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn fluid_ounces_from_str() {
        let v = Volume::from_str("10 US fl oz");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_fluid_ounces());

        let v2 = Volume::from_str("10 fl oz");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_fluid_ounces());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn fluid_ounces_uk_from_str() {
        let v = Volume::from_str("10imp fl oz");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_fluid_ounces_uk());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn cubic_meters_from_str() {
        let v = Volume::from_str("10 m3");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_cubic_meters());

        let v2 = Volume::from_str("10m³");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_cubic_meters());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn drops_from_str() {
        let v = Volume::from_str("10 gt");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_drops());

        let v2 = Volume::from_str("10gtt");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_drops());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn drams_from_str() {
        let v = Volume::from_str("10dr");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_drams());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn liters_from_str() {
        let v = Volume::from_str("10L");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_liters());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn quarts_from_str() {
        let v = Volume::from_str("10qt");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_quarts());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn pints_from_str() {
        let v = Volume::from_str("10 US pt");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_pints());

        let v2 = Volume::from_str("10 US p");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_pints());

        let v3 = Volume::from_str("10p");
        assert!(v3.is_ok());
        assert_almost_eq(10.0, v3.unwrap().as_pints());

        let v4 = Volume::from_str("10pt");
        assert!(v4.is_ok());
        assert_almost_eq(10.0, v4.unwrap().as_pints());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn pints_uk_from_str() {
        let v = Volume::from_str("10 imp pt");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_pints_uk());

        let v2 = Volume::from_str("10 imp p");
        assert!(v2.is_ok());
        assert_almost_eq(10.0, v2.unwrap().as_pints_uk());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn default_from_str() {
        let v = Volume::from_str("10");
        assert!(v.is_ok());
        assert_almost_eq(10.0, v.unwrap().as_liters());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn invalid_from_str() {
        let v = Volume::from_str("10abcd");
        assert_eq!(false, v.is_ok());
    }
}
