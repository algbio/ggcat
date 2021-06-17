//! Types and constants for handling density.

use super::measurement::*;
use mass::Mass;
use volume::Volume;

// Constants, metric
/// Number of pound per cubic foot in 1 kilograms per cubic meter
pub const LBCF_KGCM_FACTOR: f64 = 0.062427973725314;

/// The `Density` struct can be used to deal with Densities in a common way, to enable mass,
/// volume and density calculations and unit conversions.
///
/// # Example1 - calculating volume from units of mass and density
///
/// ```
/// extern crate measurements;
/// use measurements::{Density, Mass, Volume};
///
/// fn main() {
///    // Q: A 12 stone man hops into a brimming full bath, completely emersing himself.
///    // How many gallons of water spill on the floor?
///    // (Assume The human body is roughly about as dense as water - 1 gm/cm³)
///    //
///    let body_density: Density = Mass::from_grams(1.0) / Volume:: from_cubic_centimetres(1.0);
///    let mans_weight = Mass::from_stones(12.0);
///    let water_volume = mans_weight / body_density;
///    println!("{} gallons of water spilled on the floor", water_volume.as_gallons());
///}
/// ```
/// # Example2 - converting to ad-hoc units of density
///
/// ```
/// extern crate measurements;
/// use measurements::{Density, Mass, Volume};
///
/// fn main() {
///    // Q: what is 3 grams per litre in units of ounces per quart?
///    //
///    let density: Density = Mass::from_grams(3.0) / Volume:: from_litres(1.0);
///    let ounces = (density * Volume::from_quarts(1.0)).as_ounces();
///    println!("Answer is {} ounces per quart", ounces);
///}
/// ```

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Density {
    kilograms_per_cubic_meter: f64,
}

impl Density {
    /// Create a new Density from a floating point value in kilograms per cubic meter
    pub fn from_kilograms_per_cubic_meter(kilograms_per_cubic_meter: f64) -> Density {
        Density {
            kilograms_per_cubic_meter,
        }
    }

    /// Create a new Density from a floating point value in pounds per cubic feet
    pub fn from_pounds_per_cubic_feet(pounds_per_cubic_foot: f64) -> Density {
        Density::from_kilograms_per_cubic_meter(pounds_per_cubic_foot / LBCF_KGCM_FACTOR)
    }

    /// Convert this Density to a value in kilograms per cubic meter
    pub fn as_kilograms_per_cubic_meter(&self) -> f64 {
        self.kilograms_per_cubic_meter
    }

    /// Convert this Density to a value in pounds per cubic feet
    pub fn as_pounds_per_cubic_feet(&self) -> f64 {
        self.kilograms_per_cubic_meter * LBCF_KGCM_FACTOR
    }
}

// mass / volume = density
impl ::std::ops::Div<Volume> for Mass {
    type Output = Density;

    fn div(self, other: Volume) -> Density {
        Density::from_base_units(self.as_base_units() / other.as_cubic_meters())
    }
}

// mass / density = volume
impl ::std::ops::Div<Density> for Mass {
    type Output = Volume;

    fn div(self, other: Density) -> Volume {
        Volume::from_cubic_meters(self.as_base_units() / other.as_base_units())
    }
}

// volume * density = mass
impl ::std::ops::Mul<Density> for Volume {
    type Output = Mass;

    fn mul(self, other: Density) -> Mass {
        Mass::from_base_units(self.as_cubic_meters() * other.as_base_units())
    }
}

// density * volume = mass
impl ::std::ops::Mul<Volume> for Density {
    type Output = Mass;

    fn mul(self, other: Volume) -> Mass {
        Mass::from_base_units(self.as_base_units() * other.as_cubic_meters())
    }
}

impl Measurement for Density {
    fn as_base_units(&self) -> f64 {
        self.kilograms_per_cubic_meter
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_kilograms_per_cubic_meter(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "kg/m\u{00B3}"
    }
}

implement_measurement! { Density }

#[cfg(test)]
mod test {

    use super::*;
    use test_utils::assert_almost_eq;

    // Metric
    #[test]
    fn mass_over_volume() {
        let v1 = Volume::from_cubic_meters(10.0);
        let m1 = Mass::from_kilograms(5.0);
        let i1 = m1 / v1;
        let r1 = i1.as_kilograms_per_cubic_meter();
        assert_almost_eq(r1, 0.5);
    }
    #[test]
    fn mass_over_density() {
        let m1 = Mass::from_kilograms(5.0);
        let d1 = Density::from_kilograms_per_cubic_meter(10.0);
        let i1 = m1 / d1;
        let r1 = i1.as_cubic_meters();
        assert_almost_eq(r1, 0.5);
    }
    #[test]
    fn volume_times_density() {
        let v1 = Volume::from_cubic_meters(5.0);
        let d1 = Density::from_kilograms_per_cubic_meter(10.0);
        let i1 = v1 * d1;
        let r1 = i1.as_kilograms();
        assert_almost_eq(r1, 50.0);
    }
    #[test]
    fn density_times_volume() {
        let v1 = Volume::from_cubic_meters(5.0);
        let d1 = Density::from_kilograms_per_cubic_meter(10.0);
        let i1 = v1 * d1;
        let r1 = i1.as_kilograms();
        assert_almost_eq(r1, 50.0);
    }
    #[test]
    fn cvt_pcf_to_kgcm() {
        let a = Density::from_kilograms_per_cubic_meter(1.0);
        let b = Density::from_pounds_per_cubic_feet(0.062428);
        assert_almost_eq(
            a.as_kilograms_per_cubic_meter(),
            b.as_kilograms_per_cubic_meter(),
        );
        assert_almost_eq(a.as_pounds_per_cubic_feet(), b.as_pounds_per_cubic_feet());
    }

    // Traits
    #[test]
    fn add() {
        let a = Density::from_kilograms_per_cubic_meter(2.0);
        let b = Density::from_kilograms_per_cubic_meter(4.0);
        let c = a + b;
        let d = b + a;
        assert_almost_eq(c.as_kilograms_per_cubic_meter(), 6.0);
        assert_eq!(c, d);
    }

    #[test]
    fn sub() {
        let a = Density::from_kilograms_per_cubic_meter(2.0);
        let b = Density::from_kilograms_per_cubic_meter(4.0);
        let c = a - b;
        assert_almost_eq(c.as_kilograms_per_cubic_meter(), -2.0);
    }

    #[test]
    fn mul() {
        let a = Density::from_kilograms_per_cubic_meter(3.0);
        let b = a * 2.0;
        let c = 2.0 * a;
        assert_almost_eq(b.as_kilograms_per_cubic_meter(), 6.0);
        assert_eq!(b, c);
    }

    #[test]
    fn div() {
        let a = Density::from_kilograms_per_cubic_meter(2.0);
        let b = Density::from_kilograms_per_cubic_meter(4.0);
        let c = a / b;
        let d = a / 2.0;
        assert_almost_eq(c, 0.5);
        assert_almost_eq(d.as_kilograms_per_cubic_meter(), 1.0);
    }

    #[test]
    fn eq() {
        let a = Density::from_kilograms_per_cubic_meter(2.0);
        let b = Density::from_kilograms_per_cubic_meter(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = Density::from_kilograms_per_cubic_meter(2.0);
        let b = Density::from_kilograms_per_cubic_meter(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = Density::from_kilograms_per_cubic_meter(2.0);
        let b = Density::from_kilograms_per_cubic_meter(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
