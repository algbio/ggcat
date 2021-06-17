# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

Added this Changelog.

### Added

- Humidity and density in [#27](https://github.com/rust-embedded-community/rust-measurements/pull/27)

### Changed

- Merged in [#36](https://github.com/rust-embedded-community/rust-measurements/pull/36) to adjust bounds on `Measurements::pick_appropriate_units()`, which changes the return value for cases when the value is 1.0.

## [0.10.2]

Merged in [#17](https://github.com/thejpster/rust-measurements/pull/17) to add:

* nanoamps
* microwatts
* milliwatts
* microvolts

## [0.10.1]

The tests now work correctly after the `#[no_std]` move.

## [0.10.0]

Now builds for `#[no_std]` targets.

## [0.9.0]

Adds:

* Electrical Current (in Amps)
* Electrical Resistance (in Ohms)
* Corresponding updates to Power and Voltage

## [0.8.0]

Added the Voltage type, in Volts.

## [0.7.0]

Uses the [time](https://crates.io/crates/time) crate, to avoid `std::time` (which isn't available with `#[no_std]`).

## [0.6.0]

Adds:

* Angles
* Angular Velocity
* Area
* Data (bytes, etc)
* Force
* Frequency
* Mass
* Metric Horsepower (PS)

Changed Pressure to be in Pascals. Also add a bunch of tests and doc fixes.

[Unreleased]: https://github.com/thejpster/rust-measurements/compare/v0.10.2...HEAD
[0.10.2]: https://github.com/thejpster/rust-measurements/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/thejpster/rust-measurements/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/thejpster/rust-measurements/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/thejpster/rust-measurements/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/thejpster/rust-measurements/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/thejpster/rust-measurements/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/thejpster/rust-measurements/compare/v0.2.1...v0.6.0
