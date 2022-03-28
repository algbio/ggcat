use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

pub struct ResourceCounter {
    counter: Mutex<u64>,
    condvar: Condvar,
}

impl ResourceCounter {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            counter: Mutex::new(limit),
            condvar: Condvar::new(),
        })
    }

    pub fn allocate_blocking(self: &Arc<Self>, count: u64) {
        let mut counter = self.counter.lock();
        loop {
            if *counter >= count {
                *counter -= count;
                break;
            } else {
                self.condvar.wait(&mut counter);
            }
        }
    }

    pub fn deallocate(self: &Arc<Self>, count: u64, notify_amount: u64) {
        let mut counter = self.counter.lock();
        *counter += count;
        if *counter >= notify_amount {
            self.condvar.notify_all();
        }
    }
}
