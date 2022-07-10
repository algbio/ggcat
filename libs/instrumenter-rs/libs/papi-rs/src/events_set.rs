use counter::Counter;
use std::os::raw::{c_int, c_longlong};
use std::sync::Arc;
use PAPI_destroy_eventset;
use {check_error, PAPI_create_eventset};
use {PAPI_accum, PAPI_read, PAPI_start, PAPI_stop, PAPI_NULL};
use {PAPI_add_event, PapiError};

pub struct EventsSet {
    counter_defs: Arc<Vec<Counter>>,
    counter_values: Vec<c_longlong>,
    evt_set_id: c_int,
}

impl EventsSet {
    fn create_event(counters: &[Counter]) -> Result<c_int, PapiError> {
        let mut evt_set_id = PAPI_NULL;

        unsafe { check_error(PAPI_create_eventset(&mut evt_set_id as *mut c_int))? };

        for evt in counters {
            unsafe {
                check_error(PAPI_add_event(evt_set_id, evt.code))?;
            }
        }

        Ok(evt_set_id)
    }

    pub fn new(counters: &[Counter]) -> Result<Self, PapiError> {
        Ok(Self {
            counter_defs: Arc::new(counters.to_vec()),
            counter_values: vec![0; counters.len()],
            evt_set_id: Self::create_event(counters)?,
        })
    }

    pub fn len(&self) -> usize {
        self.counter_defs.len()
    }

    pub fn try_clone(&self) -> Result<Self, PapiError> {
        Ok(Self {
            counter_defs: self.counter_defs.clone(),
            counter_values: vec![0; self.counter_defs.len()],
            evt_set_id: Self::create_event(&self.counter_defs)?,
        })
    }

    pub fn start(&mut self) -> Result<(), PapiError> {
        unsafe { check_error(PAPI_start(self.evt_set_id)) }
    }

    pub fn read(&mut self) -> Result<&[c_longlong], PapiError> {
        unsafe {
            check_error(PAPI_read(self.evt_set_id, self.counter_values.as_mut_ptr()))?;
        }
        Ok(&self.counter_values)
    }

    pub fn read_into<'a>(
        &self,
        counters: &'a mut [c_longlong],
    ) -> Result<&'a [c_longlong], PapiError> {
        assert_eq!(counters.len(), self.counter_defs.len());
        unsafe {
            check_error(PAPI_read(self.evt_set_id, counters.as_mut_ptr()))?;
        }
        Ok(counters)
    }

    pub fn accum(&mut self) -> Result<&[c_longlong], PapiError> {
        unsafe {
            check_error(PAPI_accum(
                self.evt_set_id,
                self.counter_values.as_mut_ptr(),
            ))?;
        }
        Ok(&self.counter_values)
    }

    pub fn stop(&mut self) -> Result<&[c_longlong], PapiError> {
        unsafe {
            check_error(PAPI_stop(self.evt_set_id, self.counter_values.as_mut_ptr()))?;
        }
        Ok(&self.counter_values)
    }
}

impl Drop for EventsSet {
    fn drop(&mut self) {
        let _ = self.stop();
        unsafe {
            PAPI_destroy_eventset(&mut self.evt_set_id as *mut c_int);
        }
    }
}
