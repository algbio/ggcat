extern crate papi = "papi#0.3pre";

fn fib(n: int) -> int {
    if n < 2 { 1 }
    else { fib(n - 1) + fib(n - 2) }
}

#[test]
fn is_initialized() {
    unsafe { papi::is_initialized() };
}

#[test]
fn num_counters() {
    unsafe { papi::num_counters() };
}

#[test]
fn counter_set() {
    unsafe {
        let mut counters = papi::CounterSet::new([papi::PAPI_TOT_INS,
                                                  papi::PAPI_TOT_CYC]);
    
        let _start = counters.read();
        let _x = fib(14);
        let _stop = counters.accum();
    }
}