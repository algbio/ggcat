//! Types and constants for handling temperature.

use super::measurement::*;
#[cfg(feature = "from_str")]
use regex::Regex;
#[cfg(feature = "from_str")]
use std::str::FromStr;

/// The `Temperature` struct can be used to deal with absolute temperatures in
/// a common way.
///
/// # Example
///
/// ```
/// use measurements::Temperature;
///
/// let boiling_water = Temperature::from_celsius(100.0);
/// let fahrenheit = boiling_water.as_fahrenheit();
/// println!("Boiling water measures at {} degrees fahrenheit.", fahrenheit);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Temperature {
    degrees_kelvin: f64,
}

/// The `TemperatureDelta` struct can be used to deal with differences between
/// temperatures in a common way.
///
/// # Example
///
/// ```
/// use measurements::{Temperature, TemperatureDelta};
///
/// let boiling_water = Temperature::from_celsius(100.0);
/// let frozen_water = Temperature::from_celsius(0.0);
/// let difference: TemperatureDelta = boiling_water - frozen_water;
/// println!("Boiling water is {} above freezing.", difference);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct TemperatureDelta {
    kelvin_degrees: f64,
}

impl TemperatureDelta {
    /// Create a new TemperatureDelta from a floating point value in Kelvin
    pub fn from_kelvin(kelvin_degrees: f64) -> Self {
        TemperatureDelta { kelvin_degrees }
    }

    /// Create a new TemperatureDelta from a floating point value in Celsius
    pub fn from_celsius(celsius_degrees: f64) -> Self {
        TemperatureDelta::from_kelvin(celsius_degrees)
    }

    /// Create a new TemperatureDelta from a floating point value in Fahrenheit
    pub fn from_fahrenheit(farenheit_degrees: f64) -> Self {
        TemperatureDelta {
            kelvin_degrees: farenheit_degrees / 1.8,
        }
    }

    /// Create a new TemperatureDelta from a floating point value in Rankine
    pub fn from_rankine(rankine_degrees: f64) -> Self {
        TemperatureDelta {
            kelvin_degrees: rankine_degrees / 1.8,
        }
    }

    /// Convert this TemperatureDelta to a floating point value in Kelvin
    pub fn as_kelvin(&self) -> f64 {
        self.kelvin_degrees
    }

    /// Convert this TemperatureDelta to a floating point value in Celsius
    pub fn as_celsius(&self) -> f64 {
        self.kelvin_degrees
    }

    /// Convert this TemperatureDelta to a floating point value in Fahrenheit
    pub fn as_fahrenheit(&self) -> f64 {
        self.kelvin_degrees * 1.8
    }

    /// Convert this TemperatureDelta to a floating point value in Rankine
    pub fn as_rankine(&self) -> f64 {
        self.kelvin_degrees * 1.8
    }
}

impl Temperature {
    /// Create a new Temperature from a floating point value in Kelvin
    pub fn from_kelvin(degrees_kelvin: f64) -> Self {
        Temperature { degrees_kelvin }
    }

    /// Create a new Temperature from a floating point value in Celsius
    pub fn from_celsius(degrees_celsius: f64) -> Self {
        Self::from_kelvin(degrees_celsius + 273.15)
    }

    /// Create a new Temperature from a floating point value in Fahrenheit
    pub fn from_fahrenheit(degrees_fahrenheit: f64) -> Self {
        Self::from_kelvin((degrees_fahrenheit - 32.0) / 1.8 + 273.15)
    }

    /// Create a new Temperature from a floating point value in Rankine
    pub fn from_rankine(degrees_rankine: f64) -> Self {
        Self::from_kelvin((degrees_rankine - 491.67) / 1.8 + 273.15)
    }

    /// Convert this absolute Temperature to a floating point value in Kelvin
    pub fn as_kelvin(&self) -> f64 {
        self.degrees_kelvin
    }

    /// Convert this absolute Temperature to a floating point value in Celsius
    pub fn as_celsius(&self) -> f64 {
        self.degrees_kelvin - 273.15
    }

    /// Convert this absolute Temperature to a floating point value in Fahrenheit
    pub fn as_fahrenheit(&self) -> f64 {
        (self.degrees_kelvin - 273.15) * 1.8 + 32.0
    }

    /// Convert this absolute Temperature to a floating point value in Rankine
    pub fn as_rankine(&self) -> f64 {
        (self.degrees_kelvin - 273.15) * 1.8 + 491.67
    }
}

impl Measurement for Temperature {
    fn as_base_units(&self) -> f64 {
        self.degrees_kelvin
    }

    fn from_base_units(degrees_kelvin: f64) -> Self {
        Self::from_kelvin(degrees_kelvin)
    }

    fn get_base_units_name(&self) -> &'static str {
        "K"
    }
}

impl Measurement for TemperatureDelta {
    fn as_base_units(&self) -> f64 {
        self.kelvin_degrees
    }

    fn from_base_units(kelvin_degrees: f64) -> Self {
        Self::from_kelvin(kelvin_degrees)
    }

    fn get_base_units_name(&self) -> &'static str {
        "K"
    }
}

impl ::std::ops::Add<TemperatureDelta> for Temperature {
    type Output = Temperature;

    fn add(self, other: TemperatureDelta) -> Temperature {
        Temperature::from_kelvin(self.degrees_kelvin + other.kelvin_degrees)
    }
}

impl ::std::ops::Add<Temperature> for TemperatureDelta {
    type Output = Temperature;

    fn add(self, other: Temperature) -> Temperature {
        other + self
    }
}

impl ::std::ops::Sub<TemperatureDelta> for Temperature {
    type Output = Temperature;

    fn sub(self, other: TemperatureDelta) -> Temperature {
        Temperature::from_kelvin(self.degrees_kelvin - other.kelvin_degrees)
    }
}

impl ::std::ops::Sub<Temperature> for Temperature {
    type Output = TemperatureDelta;

    fn sub(self, other: Temperature) -> TemperatureDelta {
        TemperatureDelta::from_kelvin(self.degrees_kelvin - other.degrees_kelvin)
    }
}

impl ::std::cmp::Eq for Temperature {}
impl ::std::cmp::PartialEq for Temperature {
    fn eq(&self, other: &Self) -> bool {
        self.as_base_units() == other.as_base_units()
    }
}

impl ::std::cmp::PartialOrd for Temperature {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        self.as_base_units().partial_cmp(&other.as_base_units())
    }
}

#[cfg(feature = "from_str")]
impl FromStr for Temperature {
    type Err = std::num::ParseFloatError;

    /// Create a new Temperature from a string
    /// Plain numbers in string are considered to be Celsius
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        if val.is_empty() {
            return Ok(Temperature::from_celsius(0.0));
        }

        let re = Regex::new(r"\s*([0-9.]*)\s?(deg|\u{00B0}|)?\s?([fckrFCKR]{1})\s*$").unwrap();
        if let Some(caps) = re.captures(val) {
            let float_val = caps.get(1).unwrap().as_str();
            return Ok(
                match caps.get(3).unwrap().as_str().to_uppercase().as_str() {
                    "F" => Temperature::from_fahrenheit(float_val.parse::<f64>()?),
                    "C" => Temperature::from_celsius(float_val.parse::<f64>()?),
                    "K" => Temperature::from_kelvin(float_val.parse::<f64>()?),
                    "R" => Temperature::from_rankine(float_val.parse::<f64>()?),
                    _ => Temperature::from_celsius(val.parse::<f64>()?),
                },
            );
        }

        Ok(Temperature::from_celsius(val.parse::<f64>()?))
    }
}

implement_display!(Temperature);
implement_measurement!(TemperatureDelta);

#[cfg(test)]
mod test {
    use temperature::*;
    use test_utils::assert_almost_eq;

    // Temperature Units
    #[test]
    fn kelvin() {
        let t = Temperature::from_kelvin(100.0);
        let o = t.as_kelvin();

        assert_almost_eq(o, 100.0);
    }

    #[test]
    fn celsius() {
        let t = Temperature::from_kelvin(100.0);
        let o = t.as_celsius();

        assert_almost_eq(o, -173.15);
    }

    #[test]
    fn fahrenheit() {
        let t = Temperature::from_kelvin(100.0);
        let o = t.as_fahrenheit();

        assert_almost_eq(o, -279.67);
    }

    #[test]
    fn rankine() {
        let t = Temperature::from_kelvin(100.0);
        let o = t.as_rankine();

        assert_almost_eq(o, 180.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn empty_str() {
        let t = Temperature::from_str("");
        assert!(t.is_ok());

        let o = t.unwrap().as_celsius();
        assert_eq!(o, 0.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn celsius_str() {
        let t = Temperature::from_str("100C");
        assert!(t.is_ok());

        let o = t.unwrap().as_celsius();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn celsius_space_str() {
        let t = Temperature::from_str("100 C");
        assert!(t.is_ok());

        let o = t.unwrap().as_celsius();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn celsius_degree_str() {
        let t = Temperature::from_str("100°C");
        assert!(t.is_ok());

        let o = t.unwrap().as_celsius();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn fahrenheit_str() {
        let t = Temperature::from_str("100F");
        assert!(t.is_ok());

        let o = t.unwrap().as_fahrenheit();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn fahrenheit_lc_str() {
        let t = Temperature::from_str("100 f");
        assert!(t.is_ok());

        let o = t.unwrap().as_fahrenheit();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn fahrenheit_degree_str() {
        let t = Temperature::from_str("100 deg f");
        assert!(t.is_ok());

        let o = t.unwrap().as_fahrenheit();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn rankine_str() {
        let t = Temperature::from_str("100R");
        assert!(t.is_ok());

        let o = t.unwrap().as_rankine();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn rankine_degree_str() {
        let t = Temperature::from_str("100 °R");
        assert!(t.is_ok());

        let o = t.unwrap().as_rankine();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn number_str() {
        let t = Temperature::from_str("100.5");
        assert!(t.is_ok());

        let o = t.unwrap().as_celsius();
        assert_almost_eq(o, 100.5);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn invalid_str() {
        let t = Temperature::from_str("abcd");
        assert!(t.is_err());
    }

    // Traits
    #[test]
    fn add() {
        let a = Temperature::from_kelvin(2.0);
        let b = TemperatureDelta::from_kelvin(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_kelvin(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn add2() {
        let a = TemperatureDelta::from_kelvin(2.0);
        let b = TemperatureDelta::from_kelvin(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_kelvin(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Temperature::from_kelvin(4.0);
        let b = TemperatureDelta::from_kelvin(2.0);
        let c = a - b;
        assert_almost_eq(c.as_kelvin(), 2.0);
    }

    #[test]
    fn sub2() {
        let a = Temperature::from_fahrenheit(212.0);
        let b = Temperature::from_celsius(75.0);
        let c = a - b;
        assert_almost_eq(c.as_kelvin(), 25.0);
    }

    #[test]
    fn sub3() {
        let a = TemperatureDelta::from_fahrenheit(180.0);
        let b = TemperatureDelta::from_celsius(75.0);
        let c = a - b;
        assert_almost_eq(c.as_kelvin(), 25.0);
    }

    #[test]
    fn mul() {
        let a = TemperatureDelta::from_celsius(5.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_celsius(), 10.0);
        assert_eq!(b, c);
    }

    #[test]
    fn eq() {
        let a = Temperature::from_kelvin(2.0);
        let b = Temperature::from_kelvin(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Temperature::from_kelvin(2.0);
        let b = Temperature::from_kelvin(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Temperature::from_kelvin(2.0);
        let b = Temperature::from_kelvin(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
