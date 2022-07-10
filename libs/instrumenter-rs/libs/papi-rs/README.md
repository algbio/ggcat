# rust-papi

Rust bindings for PAPI.

[PAPI] is a library for accessing hardware performance counters on a variety of hardware.

## Building

rust-papi uses rustpkg. To build rust-papi, from your local repository, do:

```
rustpkg build papi
```

This will create the library and a program at `bin/papi` that demonstrates that rust-papi is working.

You can also run the test suite:

```
RUST_THREADS=1 rustpkg test papi
```

The `RUST_THREADS=1` is necessary to work around thread safety issues in PAPI.

## Using rust-papi

Here is a simple program that does some computation and reports the number of instructions executed.

```rust
extern mod papi;

fn main() {
    let counters = papi::CounterSet::new([papi::PAPI_TOT_INS]);

    let start = counters.read();
    let x = fib(14);
    let stop = counters.accum();

    println!("Computed fib(14) = {:d} in {:d} instructions.",
             x, stop[0] - start[0]);
}

fn fib(n: int) -> int {
    if n < 2 { 1 }
    else { fib(n - 1) + fib(n - 2) }
}
```

[PAPI]: http://icl.cs.utk.edu/papi/
