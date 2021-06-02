// use std::sync::atomic::AtomicUsize;
// use std::sync::{Condvar, Mutex, MutexGuard};
//
// pub struct Semaphore {
//     counter: Mutex<usize>,
//     condvar: Condvar,
// }
//
// impl Semaphore {
//     pub fn new(capacity: usize) -> Self {
//         Self {
//             counter: Mutex::new(0),
//             condvar: Condvar::new(),
//         }
//     }
//
//     pub fn increment(&self) {
//         let mut value = self.counter.lock().unwrap();
//         *value += 1;
//         self.condvar.notify_one();
//     }
//     pub fn decrement(&self) {
//         let mut value = self.counter.lock().unwrap();
//         while *value == 0 {
//             value = self.condvar.wait(value).unwrap();
//         }
//         *value -= 1;
//     }
// }
