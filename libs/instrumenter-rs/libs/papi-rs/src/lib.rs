//! This package provides bindings to the PAPI performance counters
//! library.
#![feature(thread_id_value)]

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(non_upper_case_globals)]
mod bindings;
pub mod counter;
pub mod events_set;

use std::os::raw::{c_int, c_ulong};

use bindings::*;

const fn papi_version_number(maj: u32, min: u32, rev: u32, inc: u32) -> u32 {
    (maj << 24) | (min << 16) | (rev << 8) | inc
}

const PAPI_VERSION: u32 = papi_version_number(6, 0, 0, 1);
const PAPI_VER_CURRENT: c_int = (PAPI_VERSION & 0xffff0000) as c_int;

#[link(name = "papi")]
extern "C" {}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PapiError {
    code: i32,
}

pub(crate) fn check_error(code: i32) -> Result<(), PapiError> {
    if code == (PAPI_OK as i32) {
        Ok(())
    } else {
        Err(PapiError { code })
    }
}

extern "C" fn get_thread_id() -> c_ulong {
    std::thread::current().id().as_u64().get() as c_ulong
}

pub fn initialize(multithread: bool) -> Result<(), PapiError> {
    unsafe {
        let version = PAPI_library_init(PAPI_VER_CURRENT);
        if version != PAPI_VER_CURRENT {
            return Err(PapiError { code: version });
        }

        if multithread {
            check_error(PAPI_thread_init(Some(get_thread_id)))?;
        }
    }

    Ok(())
}

pub fn is_initialized() -> bool {
    unsafe { check_error(PAPI_is_initialized()).is_ok() }
}

// The only reasonable action for counters_in_use is to
// retry. Otherwise, you might as well just fail yourself.
#[derive(PartialEq, Eq)]
pub enum Action {
    Retry,
}

#[cfg(test)]
mod tests {
    use counter::Counter;
    use events_set::EventsSet;
    use PAPI_EINVAL;

    #[test]
    fn test_fib() {
        crate::initialize(true).unwrap();

        let counters = vec![
            Counter::from_name("ix86arch::INSTRUCTION_RETIRED").unwrap(),
            Counter::from_name("ix86arch::MISPREDICTED_BRANCH_RETIRED").unwrap(),
        ];

        let mut event_set = EventsSet::new(&counters).unwrap();
        let mut second_set = event_set.try_clone().unwrap();

        for fv in 1..45 {
            event_set.start().unwrap();
            second_set.start().unwrap();
            let x = fib(fv);
            second_set.stop().unwrap();
            let counters = event_set.stop().unwrap();
            println!(
                "Computed fib({}) = {} in {} instructions [mispredicted: {}].",
                fv, x, counters[0], counters[1]
            );
        }
    }

    fn fib(n: isize) -> isize {
        if n < 2 {
            1
        } else {
            fib(n - 1) + fib(n - 2)
        }
    }
}
