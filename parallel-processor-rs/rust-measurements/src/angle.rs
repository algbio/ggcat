//! Types and constants for handling angles

use super::measurement::*;

#[cfg(feature = "from_str")]
use regex::Regex;
#[cfg(feature = "from_str")]
use std::str::FromStr;

/// The 'Angle' struct can be used to deal with angles in a common way.
///
/// # Example
///
/// ```
/// use measurements::Angle;
///
/// let whole_cake = Angle::from_degrees(360.0);
/// let pieces = 6.0;
/// let slice = whole_cake / pieces;
/// println!("Each slice will be {} degrees", slice.as_degrees());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Angle {
    radians: f64,
}

impl Angle {
    /// Create a new Angle from a floating point value in degrees
    pub fn from_degrees(degrees: f64) -> Self {
        Angle::from_radians(degrees * ::PI / 180.0)
    }

    /// Create a new Angle from a floating point value in radians
    pub fn from_radians(radians: f64) -> Self {
        Angle { radians }
    }

    /// Convert this Angle to a floating point value in degrees
    pub fn as_degrees(&self) -> f64 {
        self.radians * 180.0 / ::PI
    }

    /// Convert this Angle to a floating point value in radians
    pub fn as_radians(&self) -> f64 {
        self.radians
    }

    /// Calculate the cosine of this angle
    #[cfg(not(feature = "no_std"))]
    pub fn cos(&self) -> f64 {
        self.radians.cos()
    }

    /// Calculate the sine of this angle
    #[cfg(not(feature = "no_std"))]
    pub fn sin(&self) -> f64 {
        self.radians.sin()
    }

    /// Calculate the sine and cosine of this angle
    #[cfg(not(feature = "no_std"))]
    pub fn sin_cos(&self) -> (f64, f64) {
        self.radians.sin_cos()
    }

    /// Calculate the tangent of this angle
    #[cfg(not(feature = "no_std"))]
    pub fn tan(&self) -> f64 {
        self.radians.tan()
    }

    /// Calculate the arcsine of a number
    #[cfg(not(feature = "no_std"))]
    pub fn asin(num: f64) -> Self {
        Angle::from_radians(num.asin())
    }

    /// Calculate the arccosine of a number
    #[cfg(not(feature = "no_std"))]
    pub fn acos(num: f64) -> Self {
        Angle::from_radians(num.acos())
    }

    /// Calculate the arctangent of a number
    #[cfg(not(feature = "no_std"))]
    pub fn atan(num: f64) -> Self {
        Angle::from_radians(num.atan())
    }
}

impl Measurement for Angle {
    fn as_base_units(&self) -> f64 {
        self.radians
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_radians(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "rad"
    }
}

#[cfg(feature = "from_str")]
impl FromStr for Angle {
    type Err = std::num::ParseFloatError;

    /// Create a new Angle from a string
    /// Plain numbers in string are considered to be plain degrees
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        if val.is_empty() {
            return Ok(Angle::from_degrees(0.0));
        }

        let re = Regex::new(r"(?i)\s*([0-9.]*)\s?(deg|\u{00B0}|rad)\s*$").unwrap();
        if let Some(caps) = re.captures(val) {
            let float_val = caps.get(1).unwrap().as_str();
            return Ok(
                match caps.get(2).unwrap().as_str().to_lowercase().as_str() {
                    "deg" | "\u{00B0}" => Angle::from_degrees(float_val.parse::<f64>()?),
                    "rad" => Angle::from_radians(float_val.parse::<f64>()?),
                    _ => Angle::from_degrees(val.parse::<f64>()?),
                },
            );
        }

        Ok(Angle::from_degrees(val.parse::<f64>()?))
    }
}

implement_measurement! { Angle }

#[cfg(test)]
mod test {
    use angle::*;
    use std::f64::consts::PI;
    use test_utils::assert_almost_eq;

    #[test]
    fn radians() {
        let i1 = Angle::from_degrees(360.0);
        let r1 = i1.as_radians();
        let i2 = Angle::from_radians(PI);
        let r2 = i2.as_degrees();
        assert_almost_eq(r1, 2.0 * PI);
        assert_almost_eq(r2, 180.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn angle_from_str() {
        let t = Angle::from_str("100 deg");
        assert!(t.is_ok());

        let o = t.unwrap().as_degrees();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn angle_from_degree_str() {
        let t = Angle::from_str("100°");
        assert!(t.is_ok());

        let o = t.unwrap().as_degrees();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn angle_from_radian_str() {
        let t = Angle::from_str("100rad");
        assert!(t.is_ok());

        let o = t.unwrap().as_radians();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn default_str() {
        let t = Angle::from_str("100");
        assert!(t.is_ok());

        let o = t.unwrap().as_degrees();
        assert_almost_eq(o, 100.0);
    }

    #[test]
    #[cfg(feature = "from_str")]
    fn invalid_str() {
        let t = Angle::from_str("abcd");
        assert!(t.is_err());
    }
}
