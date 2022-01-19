//! Data measurements utils, taken from https://github.com/jocull/rust-measurements.git

/// This is a special macro that creates the code to implement
/// `std::fmt::Display`.
#[macro_export]
macro_rules! implement_display {
    ($($t:ty)*) => ($(

        impl ::std::fmt::Display for $t {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                let (unit, value) = self.get_appropriate_units();
                value.fmt(f)?;      // Value
                write!(f, "\u{00A0}{}", unit)
            }
        }
    )*)
}

/// This is a special macro that creates the code to implement
/// operator and comparison overrides.
macro_rules! implement_measurement {
    ($($t:ty)*) => ($(

        implement_display!( $t );

        impl ::std::ops::Add for $t {
            type Output = Self;

            fn add(self, rhs: Self) -> Self {
                Self::from_base_units(self.as_base_units() + rhs.as_base_units())
            }
        }

        impl ::std::ops::Sub for $t {
            type Output = Self;

            fn sub(self, rhs: Self) -> Self {
                Self::from_base_units(self.as_base_units() - rhs.as_base_units())
            }
        }

        // Dividing a `$t` by another `$t` returns a ratio.
        //
        impl ::std::ops::Div<$t> for $t {
            type Output = f64;

            fn div(self, rhs: Self) -> f64 {
                self.as_base_units() / rhs.as_base_units()
            }
        }

        // Dividing a `$t` by a factor returns a new portion of the measurement.
        //
        impl ::std::ops::Div<f64> for $t {
            type Output = Self;

            fn div(self, rhs: f64) -> Self {
                Self::from_base_units(self.as_base_units() / rhs)
            }
        }

        // Multiplying a `$t` by a factor increases (or decreases) that
        // measurement a number of times.
        impl ::std::ops::Mul<f64> for $t {
            type Output = Self;

            fn mul(self, rhs: f64) -> Self {
                Self::from_base_units(self.as_base_units() * rhs)
            }
        }

        // Multiplying `$t` by a factor is commutative
        impl ::std::ops::Mul<$t> for f64 {
            type Output = $t;

            fn mul(self, rhs: $t) -> $t {
                rhs * self
            }
        }

        impl ::std::cmp::Eq for $t { }
        impl ::std::cmp::PartialEq for $t {
            fn eq(&self, other: &Self) -> bool {
                self.as_base_units() == other.as_base_units()
            }
        }

        impl ::std::cmp::PartialOrd for $t {
            fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
                self.as_base_units().partial_cmp(&other.as_base_units())
            }
        }
    )*)
}

// Types and constants for handling amounts of data (in octets, or bits).

/// The `Data` struct can be used to deal with computer information in a common way.
/// Common legacy and SI units are supported.
///
/// # Example
///
/// ```
/// use measurements::Data;
///
/// let file_size = Data::from_mebioctets(2.5);
/// let octets = file_size.as_octets();
/// println!("There are {} octets in that file.", octets);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct MemoryDataSize {
    /// Number of octets
    pub octets: f64,
}

impl MemoryDataSize {
    // Constants
    pub const OCTET_BIT_FACTOR: f64 = 0.125;

    // Constants, legacy
    pub const OCTET_KILOOCTET_FACTOR: u64 = 1000;
    pub const OCTET_MEGAOCTET_FACTOR: u64 = 1000 * 1000;
    pub const OCTET_GIGAOCTET_FACTOR: u64 = 1000 * 1000 * 1000;
    pub const OCTET_TERAOCTET_FACTOR: u64 = 1000 * 1000 * 1000 * 1000;

    // Constants, SI
    pub const OCTET_KIBIOCTET_FACTOR: u64 = 1024;
    pub const OCTET_MEBIOCTET_FACTOR: u64 = 1024 * 1024;
    pub const OCTET_GIBIOCTET_FACTOR: u64 = 1024 * 1024 * 1024;
    pub const OCTET_TEBIOCTET_FACTOR: u64 = 1024 * 1024 * 1024 * 1024;

    /// Create new Data from floating point value in Octets
    pub const fn from_octets(octets: f64) -> Self {
        MemoryDataSize { octets }
    }

    /// Create new Data from floating point value in Bits
    pub fn from_bits(bits: f64) -> Self {
        Self::from_octets(bits * Self::OCTET_BIT_FACTOR)
    }

    // Inputs, legacy
    /// Create new Data from floating point value in Kilooctets (1000 octets)
    pub const fn from_kilooctets(kilooctets: u64) -> Self {
        Self::from_octets((kilooctets * Self::OCTET_KILOOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Megaoctets (1e6 octets)
    pub const fn from_megaoctets(megaoctets: u64) -> Self {
        Self::from_octets((megaoctets * Self::OCTET_MEGAOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Gigaoctets (1e9 octets)
    pub const fn from_gigaoctets(gigaoctets: u64) -> Self {
        Self::from_octets((gigaoctets * Self::OCTET_GIGAOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Teraoctets (1e12 octets)
    pub const fn from_teraoctets(teraoctets: u64) -> Self {
        Self::from_octets((teraoctets * Self::OCTET_TERAOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Kibioctets (1024 octets)
    pub const fn from_kibioctets(kibioctets: u64) -> Self {
        Self::from_octets((kibioctets * Self::OCTET_KIBIOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Mebioctets (1024**2 octets)
    pub const fn from_mebioctets(mebioctets: u64) -> Self {
        Self::from_octets((mebioctets * Self::OCTET_MEBIOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Gibioctets (1024**3 octets)
    pub const fn from_gibioctets(gibioctets: u64) -> Self {
        Self::from_octets((gibioctets * Self::OCTET_GIBIOCTET_FACTOR) as f64)
    }

    /// Create new Data from floating point value in Tebioctets (1024**4 octets)
    pub const fn from_tebioctets(tebioctets: u64) -> Self {
        Self::from_octets((tebioctets * Self::OCTET_TEBIOCTET_FACTOR) as f64)
    }

    /// Convert this Data to a floating point value in Octets
    pub fn as_octets(&self) -> f64 {
        self.octets
    }

    /// Convert this Data to a floating point value in Bits
    pub fn as_bits(&self) -> f64 {
        self.octets / Self::OCTET_BIT_FACTOR
    }

    /// Convert this Data to a floating point value in Kilooctets (1000 octets)
    pub fn as_kilooctets(&self) -> f64 {
        self.octets / (Self::OCTET_KILOOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Megaoctets (1e6 octets)
    pub fn as_megaoctets(&self) -> f64 {
        self.octets / (Self::OCTET_MEGAOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Gigaoctets (1e9 octets)
    pub fn as_gigaoctets(&self) -> f64 {
        self.octets / (Self::OCTET_GIGAOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Teraoctets (1e12 octets)
    pub fn as_teraoctets(&self) -> f64 {
        self.octets / (Self::OCTET_TERAOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Kibioctets (1024 octets)
    pub fn as_kibioctets(&self) -> f64 {
        self.octets / (Self::OCTET_KIBIOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Mebioctets (1024**2 octets)
    pub fn as_mebioctets(&self) -> f64 {
        self.octets / (Self::OCTET_MEBIOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Gibioctets (1024**3 octets)
    pub fn as_gibioctets(&self) -> f64 {
        self.octets / (Self::OCTET_GIBIOCTET_FACTOR as f64)
    }

    /// Convert this Data to a floating point value in Tebioctets (1024**4 octets)
    pub fn as_tebioctets(&self) -> f64 {
        self.octets / (Self::OCTET_TEBIOCTET_FACTOR as f64)
    }

    fn as_base_units(&self) -> f64 {
        self.octets
    }

    fn from_base_units(units: f64) -> Self {
        Self::from_octets(units)
    }

    fn get_appropriate_units(&self) -> (&'static str, f64) {
        // Smallest to largest
        let list = [
            ("octets", 1.0),
            ("KiB", 1024.0),
            ("MiB", 1024.0 * 1024.0),
            ("GiB", 1024.0 * 1024.0 * 1024.0),
            ("TiB", 1024.0 * 1024.0 * 1024.0 * 1024.0),
            ("PiB", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
            ("EiB", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ];
        self.pick_appropriate_units(&list)
    }

    /// Given a list of units and their scale relative to the base unit,
    /// select the most appropriate one.
    ///
    /// The list must be smallest to largest, e.g. ("nanometre", 10-9) to
    /// ("kilometre", 10e3)
    fn pick_appropriate_units(&self, list: &[(&'static str, f64)]) -> (&'static str, f64) {
        for &(unit, ref scale) in list.iter().rev() {
            let value = self.as_base_units() / scale;
            if value >= 1.0 || value <= -1.0 {
                return (unit, value);
            }
        }
        (list[0].0, self.as_base_units() / list[0].1)
    }

    pub fn as_bytes(&self) -> usize {
        self.as_base_units() as usize
    }
    pub fn from_bytes(bytes: usize) -> Self {
        Self::from_base_units(bytes as f64)
    }
    pub fn max(self, other: Self) -> Self {
        Self::from_base_units(self.as_base_units().max(other.as_base_units()))
    }
}

implement_measurement! { MemoryDataSize }

#[cfg(test)]
mod test {
    use crate::memory_data_size::MemoryDataSize;

    const DEFAULT_DELTA: f64 = 1e-5;

    /// Check two floating point values are approximately equal using some given delta (a fraction of the inputs)
    fn almost_eq_delta(a: f64, b: f64, d: f64) -> bool {
        ((a - b).abs() / a) < d
    }

    /// Assert two floating point values are approximately equal using some given delta (a fraction of the inputs)
    fn assert_almost_eq_delta(a: f64, b: f64, d: f64) {
        if !almost_eq_delta(a, b, d) {
            panic!("assertion failed: {:?} != {:?} (within {:?})", a, b, d);
        }
    }

    /// Assert two floating point values are approximately equal
    fn assert_almost_eq(a: f64, b: f64) {
        assert_almost_eq_delta(a, b, DEFAULT_DELTA);
    }

    // Metric
    #[test]
    fn bits() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_bits();

        let i2 = MemoryDataSize::from_bits(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 800.0);
        assert_almost_eq(r2, 12.5);
    }

    #[test]
    fn kilooctet() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_kilooctets();

        let i2 = MemoryDataSize::from_kilooctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 0.1);
        assert_almost_eq(r2, 1e5);
    }

    #[test]
    fn megaoctet() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_megaoctets();

        let i2 = MemoryDataSize::from_megaoctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 0.0001);
        assert_almost_eq(r2, 1e8);
    }

    #[test]
    fn gigaoctet() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_gigaoctets();

        let i2 = MemoryDataSize::from_gigaoctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 1e-7);
        assert_almost_eq(r2, 1e11);
    }

    #[test]
    fn teraoctet() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_teraoctets();

        let i2 = MemoryDataSize::from_teraoctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 1e-10);
        assert_almost_eq(r2, 1e14);
    }

    // Imperial
    #[test]
    fn kibioctet() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_kibioctets();

        let i2 = MemoryDataSize::from_kibioctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 0.09765625);
        assert_almost_eq(r2, 102400.0);
    }

    #[test]
    fn mebioctet() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_mebioctets();

        let i2 = MemoryDataSize::from_mebioctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 9.536743e-5);
        assert_almost_eq(r2, 104857600.0);
    }

    #[test]
    fn gibioctets() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_gibioctets();

        let i2 = MemoryDataSize::from_gibioctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 9.313226e-8);
        assert_almost_eq(r2, 107374182400.0);
    }

    #[test]
    fn tebioctets() {
        let i1 = MemoryDataSize::from_octets(100.0);
        let r1 = i1.as_tebioctets();

        let i2 = MemoryDataSize::from_tebioctets(100.0);
        let r2 = i2.as_octets();

        assert_almost_eq(r1, 9.094947e-11);
        assert_almost_eq(r2, 109951162777600.0);
    }

    // Traits
    #[test]
    fn add() {
        let a = MemoryDataSize::from_octets(2.0);
        let b = MemoryDataSize::from_octets(4.0);
        let c = a + b;
        assert_almost_eq(c.as_octets(), 6.0);
    }

    #[test]
    fn sub() {
        let a = MemoryDataSize::from_octets(2.0);
        let b = MemoryDataSize::from_octets(4.0);
        let c = a - b;
        assert_almost_eq(c.as_octets(), -2.0);
    }

    #[test]
    fn mul() {
        let b = MemoryDataSize::from_octets(4.0);
        let d = b * 2.0;
        assert_almost_eq(d.as_octets(), 8.0);
    }

    #[test]
    fn div() {
        let b = MemoryDataSize::from_octets(4.0);
        let d = b / 2.0;
        assert_almost_eq(d.as_octets(), 2.0);
    }

    #[test]
    fn eq() {
        let a = MemoryDataSize::from_octets(2.0);
        let b = MemoryDataSize::from_octets(2.0);
        assert_eq!(a == b, true);
    }

    #[test]
    fn neq() {
        let a = MemoryDataSize::from_octets(2.0);
        let b = MemoryDataSize::from_octets(4.0);
        assert_eq!(a == b, false);
    }

    #[test]
    fn cmp() {
        let a = MemoryDataSize::from_octets(2.0);
        let b = MemoryDataSize::from_octets(4.0);
        assert_eq!(a < b, true);
        assert_eq!(a <= b, true);
        assert_eq!(a > b, false);
        assert_eq!(a >= b, false);
    }
}
