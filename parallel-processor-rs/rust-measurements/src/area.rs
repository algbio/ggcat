//! Types and constants for handling areas.

use super::length;
use super::measurement::*;
#[cfg(feature = "from_str")]
use regex::Regex;
#[cfg(feature = "from_str")]
use std::str::FromStr;

/// Number of acres in a square meter
const SQUARE_METER_ACRE_FACTOR: f64 = 1.0 / 4046.86;

/// The `Area` struct can be used to deal with areas in a common way.
/// Common metric and imperial units are supported.
///
/// # Example
///
/// ```
/// use measurements::Area;
///
/// let football_field = Area::from_square_meters(7140.0);
/// let acres = football_field.as_acres();
/// println!("There are {} acres in a football field.", acres);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Area {
    square_meters: f64,
}

impl Area {
    /// Create new area from floating point value in Square Meters
    pub fn from_square_meters(square_meters: f64) -> Self {
        Area { square_meters }
    }

    /// Create new area from floating point value in Square Metres
    pub fn from_square_metres(square_metres: f64) -> Self {
        Self::from_square_meters(square_metres)
    }

    /// Create new area from floating point value in Square Nanometers
    pub fn from_square_nanometers(square_nanometers: f64) -> Self {
        Self::from_square_meters(
            square_nanometers / (length::METER_NANOMETER_FACTOR * length::METER_NANOMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Nanometres
    pub fn from_square_nanometres(square_nanometres: f64) -> Self {
        Self::from_square_nanometers(square_nanometres)
    }

    /// Create new area from floating point value in Square Micrometers
    pub fn from_square_micrometers(square_micrometers: f64) -> Self {
        Self::from_square_meters(
            square_micrometers
                / (length::METER_MICROMETER_FACTOR * length::METER_MICROMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Micrometres
    pub fn from_square_micrometres(square_micrometres: f64) -> Self {
        Self::from_square_micrometers(square_micrometres)
    }

    /// Create new area from floating point value in Square Millimeters
    pub fn from_square_millimeters(square_millimeters: f64) -> Self {
        Self::from_square_meters(
            square_millimeters
                / (length::METER_MILLIMETER_FACTOR * length::METER_MILLIMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Millimetres
    pub fn from_square_millimetres(square_millimetres: f64) -> Self {
        Self::from_square_millimeters(square_millimetres)
    }

    /// Create new area from floating point value in Square Centimeters
    pub fn from_square_centimeters(square_centimeters: f64) -> Self {
        Self::from_square_meters(
            square_centimeters
                / (length::METER_CENTIMETER_FACTOR * length::METER_CENTIMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Centimetres
    pub fn from_square_centimetres(square_centimetres: f64) -> Self {
        Self::from_square_centimeters(square_centimetres)
    }

    /// Create new area from floating point value in Square Decimeters
    pub fn from_square_decimeters(square_decimeters: f64) -> Self {
        Self::from_square_meters(
            square_decimeters / (length::METER_DECIMETER_FACTOR * length::METER_DECIMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Decimetres
    pub fn from_square_decimetres(square_decimetres: f64) -> Self {
        Self::from_square_decimeters(square_decimetres)
    }

    /// Create new area from floating point value in Square Hectometers
    pub fn from_square_hectometers(square_hectometers: f64) -> Self {
        Self::from_square_meters(
            square_hectometers
                / (length::METER_HECTOMETER_FACTOR * length::METER_HECTOMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Hectometres
    pub fn from_square_hectometres(square_hectometres: f64) -> Self {
        Self::from_square_hectometers(square_hectometres)
    }

    /// Create new area from floating point value in Hectares
    pub fn from_hectares(hectares: f64) -> Self {
        Self::from_square_hectometers(hectares)
    }

    /// Create new area from floating point value in Square Kilometers
    pub fn from_square_kilometers(square_kilometers: f64) -> Self {
        Self::from_square_meters(
            square_kilometers / (length::METER_KILOMETER_FACTOR * length::METER_KILOMETER_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Kilometres
    pub fn from_square_kilometres(square_kilometres: f64) -> Self {
        Self::from_square_kilometers(square_kilometres)
    }

    /// Create new area from floating point value in Square Inches
    pub fn from_square_inches(square_inches: f64) -> Self {
        Self::from_square_meters(
            square_inches / (length::METER_INCH_FACTOR * length::METER_INCH_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Feet
    pub fn from_square_feet(square_feet: f64) -> Self {
        Self::from_square_meters(
            square_feet / (length::METER_FEET_FACTOR * length::METER_FEET_FACTOR),
        )
    }

    /// Create new area from floating point value in Square Yards
    pub fn from_square_yards(square_yards: f64) -> Self {
        Self::from_square_meters(
            square_yards / (length::METER_YARD_FACTOR * length::METER_YARD_FACTOR),
        )
    }

    /// Create new area from floating point value in Acres
    pub fn from_acres(acres: f64) -> Self {
        Self::from_square_meters(acres / SQUARE_METER_ACRE_FACTOR)
    }

    /// Create new area from floating point value in Square Miles
    pub fn from_square_miles(square_miles: f64) -> Self {
        Self::from_square_meters(
            square_miles / (length::METER_MILE_FACTOR * length::METER_MILE_FACTOR),
        )
    }

    /// Convert this Area to a floating point value in Square Nanometers
    pub fn as_square_nanometers(&self) -> f64 {
        self.square_meters * (length::METER_NANOMETER_FACTOR * length::METER_NANOMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Nanometres
    pub fn as_square_nanometres(&self) -> f64 {
        self.as_square_nanometers()
    }

    /// Convert this Area to a floating point value in Square Micrometers
    pub fn as_square_micrometers(&self) -> f64 {
        self.square_meters * (length::METER_MICROMETER_FACTOR * length::METER_MICROMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Micrometres
    pub fn as_square_micrometres(&self) -> f64 {
        self.as_square_micrometers()
    }

    /// Convert this Area to a floating point value in Square Millimeters
    pub fn as_square_millimeters(&self) -> f64 {
        self.square_meters * (length::METER_MILLIMETER_FACTOR * length::METER_MILLIMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Millimetres
    pub fn as_square_millimetres(&self) -> f64 {
        self.as_square_millimeters()
    }

    /// Convert this Area to a floating point value in Square Centimeters
    pub fn as_square_centimeters(&self) -> f64 {
        self.square_meters * (length::METER_CENTIMETER_FACTOR * length::METER_CENTIMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Centimetres
    pub fn as_square_centimetres(&self) -> f64 {
        self.as_square_centimeters()
    }

    /// Convert this Area to a floating point value in Square Meters
    pub fn as_square_meters(&self) -> f64 {
        self.square_meters
    }

    /// Convert this Area to a floating point value in Square Metres
    pub fn as_square_metres(&self) -> f64 {
        self.as_square_meters()
    }

    /// Convert this Area to a floating point value in Square Decimeters
    pub fn as_square_decimeters(&self) -> f64 {
        self.square_meters * (length::METER_DECIMETER_FACTOR * length::METER_DECIMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Decimetres
    pub fn as_square_decimetres(&self) -> f64 {
        self.as_square_decimeters()
    }

    /// Convert this Area to a floating point value in Square Hectometers
    pub fn as_square_hectometers(&self) -> f64 {
        self.square_meters * (length::METER_HECTOMETER_FACTOR * length::METER_HECTOMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Hectometres
    pub fn as_square_hectometres(&self) -> f64 {
        self.as_square_hectometers()
    }

    /// Convert this Area to a floating point value in Hectares
    pub fn as_hectares(&self) -> f64 {
        self.as_square_hectometers()
    }

    /// Convert this Area to a floating point value in Square Kilometers
    pub fn as_square_kilometers(&self) -> f64 {
        self.square_meters * (length::METER_KILOMETER_FACTOR * length::METER_KILOMETER_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Kilometres
    pub fn as_square_kilometres(&self) -> f64 {
        self.as_square_kilometers()
    }

    /// Convert this Area to a floating point value in Square Inches
    pub fn as_square_inches(&self) -> f64 {
        self.square_meters * (length::METER_INCH_FACTOR * length::METER_INCH_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Feet
    pub fn as_square_feet(&self) -> f64 {
        self.square_meters * (length::METER_FEET_FACTOR * length::METER_FEET_FACTOR)
    }

    /// Convert this Area to a floating point value in Square Yards
    pub fn as_square_yards(&self) -> f64 {
        self.square_meters * (length::METER_YARD_FACTOR * length::METER_YARD_FACTOR)
    }

    /// Convert this Area to a floating point value in Acres
    pub fn as_acres(&self) -> f64 {
        self.square_meters * SQUARE_METER_ACRE_FACTOR
    }

    /// Convert this Area to a floating point value in Square Miles
    pub fn as_square_miles(&self) -> f64 {
        self.square_meters * (length::METER_MILE_FACTOR * length::METER_MILE_FACTOR)
    }
}

impl Measurement for Area {
    fn as_base_units(&self) -> f64 {
        self.square_meters
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_square_meters(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "m\u{00B2}"
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("nm\u{00B2}", 1e-18),
            ("\u{00B5}m\u{00B2}", 1e-12),
            ("mm\u{00B2}", 1e-6),
            ("cm\u{00B2}", 1e-4),
            ("m\u{00B2}", 1e0),
            ("km\u{00B2}", 1e6),
            ("thousand km\u{00B2}", 1e9),
            ("million km\u{00B2}", 1e12),
        ];
        self.pick_appropriate_units(&list)
    }
}

#[cfg(feature = "from_str")]
impl FromStr for Area {
    type Err = std::num::ParseFloatError;

    /// Create a new Area from a string
    /// Plain numbers in string are considered to be square meters
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        if val.is_empty() {
            return Ok(Area::from_square_meters(0.0));
        }

        let re = Regex::new(r"(?i)\s*([0-9.]*)\s?([a-z2\u{00B2}\u{00B5} ]{1,5})\s*$").unwrap();
        if let Some(caps) = re.captures(val) {
            println!("{:?}", caps);
            let float_val = caps.get(1).unwrap().as_str();
            return Ok(
                match caps.get(2).unwrap().as_str().trim().to_lowercase().as_str() {
                    "nm\u{00B2}" | "nm2" => Area::from_square_nanometers(float_val.parse::<f64>()?),
                    "\u{00B5}m\u{00B2}" | "\u{00B5}m2" | "um\u{00B2}" | "um2" => {
                        Area::from_square_micrometers(float_val.parse::<f64>()?)
                    }
                    "mm\u{00B2}" | "mm2" => {
                        Area::from_square_millimeters(float_val.parse::<f64>()?)
                    }
                    "cm\u{00B2}" | "cm2" => {
                        Area::from_square_centimeters(float_val.parse::<f64>()?)
                    }
                    "dm\u{00B2}" | "dm2" => Area::from_square_decimeters(float_val.parse::<f64>()?),
                    "m\u{00B2}" | "m2" => Area::from_square_meters(float_val.parse::<f64>()?),
                    "km\u{00B2}" | "km2" => Area::from_square_kilometers(float_val.parse::<f64>()?),
                    "ha" | "hm\u{00B2}" | "hm2" => Area::from_hectares(float_val.parse::<f64>()?),
                    "acre" | "ac" => Area::from_acres(float_val.parse::<f64>()?),
                    "ft\u{00B2}" | "ft2" | "sq ft" => {
                        Area::from_square_feet(float_val.parse::<f64>()?)
                    }
                    "yd\u{00B2}" | "yd2" | "sq yd" => {
                        Area::from_square_yards(float_val.parse::<f64>()?)
                    }
                    "mi\u{00B2}" | "mi2" | "sq mi" => {
                        Area::from_square_miles(float_val.parse::<f64>()?)
                    }
                    _ => Area::from_square_meters(val.parse::<f64>()?),
                },
            );
        }

        Ok(Area::from_square_meters(val.parse::<f64>()?))
    }
}

implement_measurement! { Area }

#[cfg(test)]
mod test {
    use area::*;
    use test_utils::assert_almost_eq;

    #[test]
    fn square_meters() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_meters();
        let i2 = Area::from_square_meters(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 100.0);
        assert_almost_eq(r2, 100.0);
    }

    #[test]
    fn square_metres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_metres();
        let i2 = Area::from_square_metres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 100.0);
        assert_almost_eq(r2, 100.0);
    }

    #[test]
    fn square_nanometers() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_nanometers();
        let i2 = Area::from_square_nanometers(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e20);
        assert_almost_eq(r2, 1e-16);
    }

    #[test]
    fn square_nanometres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_nanometres();
        let i2 = Area::from_square_nanometres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e20);
        assert_almost_eq(r2, 1e-16);
    }

    #[test]
    fn square_micrometers() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_micrometers();
        let i2 = Area::from_square_micrometers(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e14);
        assert_almost_eq(r2, 1e-10);
    }

    #[test]
    fn square_micrometres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_micrometres();
        let i2 = Area::from_square_micrometres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e14);
        assert_almost_eq(r2, 1e-10);
    }

    #[test]
    fn square_millimeters() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_millimeters();
        let i2 = Area::from_square_millimeters(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e8);
        assert_almost_eq(r2, 1e-4);
    }

    #[test]
    fn square_millimetres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_millimetres();
        let i2 = Area::from_square_millimetres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e8);
        assert_almost_eq(r2, 1e-4);
    }

    #[test]
    fn square_centimeters() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_centimeters();
        let i2 = Area::from_square_centimeters(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e6);
        assert_almost_eq(r2, 1e-2);
    }

    #[test]
    fn square_centimetres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_centimetres();
        let i2 = Area::from_square_centimetres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e6);
        assert_almost_eq(r2, 1e-2);
    }

    #[test]
    fn square_decimeters() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_decimeters();
        let i2 = Area::from_square_decimeters(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e4);
        assert_almost_eq(r2, 1.0);
    }

    #[test]
    fn square_decimetres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_decimetres();
        let i2 = Area::from_square_decimetres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e4);
        assert_almost_eq(r2, 1.0);
    }

    #[test]
    fn square_hectometers() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_hectometers();
        let i2 = Area::from_square_hectometers(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e-2);
        assert_almost_eq(r2, 1e6);
    }

    #[test]
    fn square_hectometres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_hectometres();
        let i2 = Area::from_square_hectometres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e-2);
        assert_almost_eq(r2, 1e6);
    }

    #[test]
    fn hectares() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_hectares();
        let i2 = Area::from_hectares(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e-2);
        assert_almost_eq(r2, 1e6);
    }

    #[test]
    fn square_kilometers() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_kilometers();
        let i2 = Area::from_square_kilometers(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e-4);
        assert_almost_eq(r2, 1e8);
    }

    #[test]
    fn square_kilometres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_kilometres();
        let i2 = Area::from_square_kilometres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1e-4);
        assert_almost_eq(r2, 1e8);
    }

    #[test]
    fn square_inches() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_inches();
        let i2 = Area::from_square_inches(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 155000.0);
        assert_almost_eq(r2, 0.06451587097);
    }

    #[test]
    fn square_feet() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_feet();
        let i2 = Area::from_square_feet(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 1076.39);
        assert_almost_eq(r2, 9.2902950097728);
    }

    #[test]
    fn square_yards() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_yards();
        let i2 = Area::from_square_yards(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 119.599);
        assert_almost_eq(r2, 83.612732764187);
    }

    #[test]
    fn acres() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_acres();
        let i2 = Area::from_acres(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 0.0247105);
        assert_almost_eq(r2, 404686.0);
    }

    #[test]
    fn square_miles() {
        let i1 = Area::from_square_meters(100.0);
        let r1 = i1.as_square_miles();
        let i2 = Area::from_square_miles(100.0);
        let r2 = i2.as_square_meters();
        assert_almost_eq(r1, 3.86102e-5);
        assert_almost_eq(r2, 258998704.7);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn empty_str() {
        let t = Area::from_str("");
        assert!(t.is_ok());
        let o = t.unwrap().as_square_meters();
        assert_eq!(o, 0.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn invalid_str() {
        let t = Area::from_str("abcd");
        assert!(t.is_err());
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_nanometer_str() {
        let t = Area::from_str(" 100.0 nm2");
        assert_almost_eq(t.unwrap().as_square_nanometers(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_micrometer_str() {
        let t = Area::from_str(" 100.0 um2");
        assert_almost_eq(t.unwrap().as_square_micrometers(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_millimeter_str() {
        let t = Area::from_str(" 100.0 mm2");
        assert_almost_eq(t.unwrap().as_square_millimeters(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_centimeter_str() {
        let t = Area::from_str(" 100.0 cm2 ");
        assert_almost_eq(t.unwrap().as_square_centimeters(), 100.0);
    }
    #[test]
    #[cfg(feature = "from_str")]
    fn square_decimeter_str() {
        let t = Area::from_str(" 100.0 dm2");
        assert_almost_eq(t.unwrap().as_square_decimeters(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_meter_str() {
        let t = Area::from_str(" 100.0 m2 ");
        assert_almost_eq(t.unwrap().as_square_meters(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_hectometer_str() {
        let t = Area::from_str(" 100.0 ha ");
        assert_almost_eq(t.unwrap().as_square_hectometers(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_kilometer_str() {
        let t = Area::from_str(" 100.0 km\u{00B2} ");
        assert_almost_eq(t.unwrap().as_square_kilometers(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_feet_str() {
        let t = Area::from_str(" 100.0 ft\u{00B2} ");
        assert_almost_eq(t.unwrap().as_square_feet(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_yard_str() {
        let t = Area::from_str(" 100.0 sq yd ");
        assert_almost_eq(t.unwrap().as_square_yards(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn square_mile_str() {
        let t = Area::from_str(" 100.0 sq mi ");
        assert_almost_eq(t.unwrap().as_square_miles(), 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn acre_str() {
        let t = Area::from_str(" 100.0 acre ");
        assert_almost_eq(t.unwrap().as_acres(), 100.0);
    }
}
