extern crate measurements;

use measurements::{mass::Mass, test_utils::assert_almost_eq, Measurement};

// Macro for testing `get_appropriate_units()`.
// Specify the name of the test, the initial value (in kg) to be
// passed in and the units that should be returned when
// `get_appropriate_units()` is called.
// An additional factor term may be given for cases when a
// unit conversion takes place.
macro_rules! test_from_kg {
    ($name:ident, $value:expr, $unit:expr) => {
        #[test]
        fn $name() {
            let mass = Mass::from_kilograms($value);
            let (unit, v) = mass.get_appropriate_units();
            assert_eq!(unit, $unit);
            assert_almost_eq($value, v);
        }
    };
    ($name:ident, $value:expr, $unit:expr, $factor:expr) => {
        #[test]
        fn $name() {
            let mass = Mass::from_kilograms($value);
            let (unit, v) = mass.get_appropriate_units();
            assert_eq!(unit, $unit);
            assert_almost_eq($value * $factor, v);
        }
    };
}

test_from_kg!(one_kg_keeps_unit, 1.0, "kg");
test_from_kg!(minus_one_kg_keeps_unit, -1.0, "kg");
test_from_kg!(more_than_one_kg_keeps_unit, 1.0 + 0.001, "kg");
test_from_kg!(
    less_than_one_kg_changes_unit_to_grams,
    1.0 - 0.001,
    "g",
    1000.0
);
test_from_kg!(
    one_thousand_kg_changes_unit_to_tonnes,
    1000.0,
    "tonnes",
    0.001
);
test_from_kg!(
    one_thousandth_of_kg_changes_unit_to_grams,
    0.001,
    "g",
    1000.0
);
