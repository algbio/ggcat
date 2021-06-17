//! Implements a bridging structure to distinguish between Torque and Energy

use super::*;

/// If you multiply a Force by a Length, we can't tell if you're
/// pushing something along (which requires Energy) or rotating
/// something (which creates a Torque). This struct is what results
/// from the multiplication, and you have to then convert
/// it to whichever you want.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TorqueEnergy {
    newton_metres: f64,
}

impl std::convert::From<TorqueEnergy> for Torque {
    fn from(t: TorqueEnergy) -> Torque {
        Torque::from_newton_metres(t.newton_metres)
    }
}

impl std::convert::From<TorqueEnergy> for Energy {
    fn from(t: TorqueEnergy) -> Energy {
        Energy::from_joules(t.newton_metres)
    }
}

impl Measurement for TorqueEnergy {
    fn as_base_units(&self) -> f64 {
        self.newton_metres
    }

    fn from_base_units(units: f64) -> Self {
        TorqueEnergy {
            newton_metres: units,
        }
    }

    fn get_base_units_name(&self) -> &'static str {
        "Nm||J"
    }
}
