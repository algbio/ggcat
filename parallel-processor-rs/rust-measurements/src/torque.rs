//! Types and constants for handling torque

use super::measurement::*;

/// Number of pound-foot in a newton-metre
const NEWTON_METRE_POUND_FOOT_FACTOR: f64 = 0.73756326522588;

/// The 'Torque' struct can be used to deal with torque in a common way.
///
/// # Example
///
/// ```
/// use measurements::Torque;
///
/// let engine_torque = Torque::from_pound_foot(250.0);
/// println!("In metric, that's {} Nm", engine_torque.as_newton_metres());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Torque {
    newton_metres: f64,
}

impl Torque {
    /// Create a new Torque from a floating point value in newton metres
    pub fn from_newton_metres(newton_metres: f64) -> Self {
        Torque { newton_metres }
    }

    /// Create a new Torque from a floating point value in newton meters
    pub fn from_newton_meters(newton_meters: f64) -> Self {
        Torque::from_newton_metres(newton_meters)
    }

    /// Create a new Torque from a floating point value in pound-foot (lbf.ft)
    pub fn from_pound_foot(pound_foot: f64) -> Self {
        Torque::from_newton_metres(pound_foot / NEWTON_METRE_POUND_FOOT_FACTOR)
    }

    /// Convert this Torque to a floating point value in newton metres
    pub fn as_newton_metres(&self) -> f64 {
        self.newton_metres
    }

    /// Convert this Torque to a floating point value in newton meters
    pub fn as_newton_meters(&self) -> f64 {
        self.newton_metres
    }

    /// Convert this Torque to a floating point value in pound-foot (lbf-ft)
    pub fn as_pound_foot(&self) -> f64 {
        self.newton_metres * NEWTON_METRE_POUND_FOOT_FACTOR
    }
}

impl Measurement for Torque {
    fn as_base_units(&self) -> f64 {
        self.newton_metres
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_newton_metres(units)
    }

    fn get_base_units_name(&self) -> &'static str {
        "Nm"
    }
}

implement_measurement! { Torque }

#[cfg(test)]
mod test {
    use super::*;
    use test_utils::assert_almost_eq;

    #[test]
    fn lbf_ft() {
        let i1 = Torque::from_pound_foot(250.0);
        let r1 = i1.as_newton_metres();
        let i2 = Torque::from_newton_metres(300.0);
        let r2 = i2.as_pound_foot();
        assert_almost_eq(r1, 338.954);
        assert_almost_eq(r2, 221.269);
    }
}
