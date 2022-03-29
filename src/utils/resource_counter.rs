use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

pub struct ResourceCounter {
    counter: Mutex<i64>,
    condvar: Condvar,
}

impl ResourceCounter {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            counter: Mutex::new(limit as i64),
            condvar: Condvar::new(),
        })
    }

    pub fn allocate_blocking(self: &Arc<Self>, count: u64) {
        let mut counter = self.counter.lock();
        loop {
            if *counter >= count as i64 {
                *counter -= count as i64;
                break;
            } else {
                self.condvar.wait(&mut counter);
            }
        }
    }

    pub fn allocate_overflow(self: &Arc<Self>, count: u64) {
        let mut counter = self.counter.lock();
        *counter -= count as i64;
    }

    pub fn deallocate(self: &Arc<Self>, count: u64, notify_amount: u64) {
        let mut counter = self.counter.lock();
        *counter += count as i64;
        if *counter >= notify_amount as i64 {
            self.condvar.notify_all();
        }
    }
}
