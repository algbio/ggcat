use crate::check_error;
use std::ffi::CString;
use std::os::raw::c_int;
use {PAPI_event_name_to_code, PapiError};

#[derive(Clone)]
#[allow(dead_code)]
pub struct Counter {
    pub(crate) name: String,
    pub(crate) code: c_int,
}

impl Counter {
    pub fn from_name(name: &str) -> Result<Counter, PapiError> {
        let evt_name = CString::new(name.to_string()).unwrap();
        let mut code = 0;

        unsafe {
            check_error(PAPI_event_name_to_code(
                evt_name.as_ptr(),
                &mut code as *mut _,
            ))?;
        }

        Ok(Counter {
            name: name.to_string(),
            code,
        })
    }
}
