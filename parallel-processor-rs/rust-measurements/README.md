# Type-safe units of measure for Rust

[![Build Status](https://github.com/rust-embedded-community/rust-measurements/workflows/Build/badge.svg)](https://github.com/rust-embedded-community/rust-measurements/actions?query=workflow%3ABuild)

### Why should I care? I already have numbers...

Working with units can be very error prone.
If one person is working in feet and one person is working in meters, what happens?

Doing all of your math in raw numerical types can be unsafe and downright confusing.
What can we do to help?

### Typed measurements to the rescue!

Working in typed measurements increases safety by dealing with what you really care about: units of measure.

Conversions to and from different units are simple, and operator overrides allow you to work with the measurements directly.

### Currently available measurement types

- Acceleration
- Angle
- Angular Velocity
- Area
- Current
- Data (bytes, etc)
- Density
- Energy
- Force
- Frequency
- Length
- Humidity
- Mass
- Power
- Pressure
- Resistance
- Speed
- Temperature
- Torque
- Voltage
- Volume

### Examples

In your Cargo.toml add the dependency...

```toml
[dependencies]
measurements = "^0.10.2"
```

In your code...

```rust
extern crate measurements;

use measurements::{Length, Pressure, Temperature, Volume, Weight};

fn main() {
    // Lengths!
    let football_field = Length::from_yards(100.0);
    let meters = football_field.as_meters();
    println!("There are {} meters in a football field.", meters);

    /// Temperatures!
    let boiling_water = Temperature::from_celsius(100.0);
    let fahrenheit = boiling_water.as_fahrenheit();
    println!("Boiling water measures at {} degrees fahrenheit.", fahrenheit);

    // Weights!
    let metric_ton = Weight::from_metric_tons(1.0);
    let united_states_tons = metric_ton.as_short_tons();
    let united_states_pounds = metric_ton.as_pounds();
    println!("One metric ton is {} U.S. tons - that's {} pounds!", united_states_tons, united_states_pounds);

    // Volumes!
    let gallon = Volume::from_gallons(1.0);
    let pint = Volume::from_pints(1.0);
    let beers = gallon / pint;
    println!("A gallon of beer will pour {:.1} pints!", beers);

    // Pressures!
    let atmosphere = Pressure::from_atmospheres(1.0);
    println!("Earth's atmosphere is usually {} psi", atmosphere.as_psi());
}
```

### Features

The crate contains few features to disable or enable certain functionalities:

* no_std
    * Removes functionality that Rust std library provides
* from_str
    * Allows creating measurement units from string input

--------------------------------------

**References**

I am by no means a measurement or math expert, I simply wanted to do something useful while learning Rust. Thank you to these sites and their authors for the great reference material used in building this library.

  - http://www.metric-conversions.org
  - http://www.conversion-metric.org
