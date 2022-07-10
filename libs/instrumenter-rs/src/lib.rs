pub mod instr_span;
mod subscriber;
pub mod tracking_allocator;

use crate::instr_span::InstrSpan;
use crate::subscriber::InstrSubscriber;
pub use instrumenter_derive::track;
use papi::counter::Counter;
use papi::events_set::EventsSet;
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
#[doc(hidden)]
pub use tracing as private__;
use tracing::Metadata;

thread_local! {
    pub(crate) static EVENTS_SET: RefCell<Option<EventsSet>> = RefCell::new(None);
}

pub(crate) static EVENTS_LIST: RwLock<Vec<Counter>> = RwLock::new(vec![]);

pub(crate) static SPANS_LIST: Mutex<Vec<InstrSpan>> = Mutex::new(vec![]);

pub struct InstrumenterGuard(());

static IS_RUNNING: AtomicBool = AtomicBool::new(false);
static JOIN_HANDLE: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

static mut GLOBAL_SUBSCRIBER: Option<Arc<InstrSubscriber>> = None;

impl Drop for InstrumenterGuard {
    fn drop(&mut self) {
        IS_RUNNING.store(false, Ordering::SeqCst);
        JOIN_HANDLE.lock().take().unwrap().join().unwrap();
    }
}

#[macro_export]
macro_rules! trace_block {
    ($name:ident) => {
        let span = $crate::private__::span!($crate::private__::Level::INFO, stringify!($name));
        let _guard = span.enter();
    };
    ($name:ident, $drop_name:ident) => {
        let span = $crate::private__::span!($crate::private__::Level::INFO, stringify!($name));
        let $drop_name = span.enter();
    };
}

#[macro_export]
macro_rules! setup_allocator {
    () => {
        #[global_allocator]
        static TRACKING_ALLOCATOR: $crate::tracking_allocator::TrackingAllocator =
            $crate::tracking_allocator::TrackingAllocator;
    };
}

pub(crate) fn get_subscriber() -> Option<&'static InstrSubscriber> {
    unsafe { GLOBAL_SUBSCRIBER.as_ref().map(|x| x.deref()) }
}

pub fn initialize_tracing(
    dest_file: impl AsRef<Path>,
    hw_counter_names: &[&str],
) -> InstrumenterGuard {
    papi::initialize(true).unwrap();
    *EVENTS_LIST.write() = hw_counter_names
        .iter()
        .map(|cn| Counter::from_name(cn).unwrap())
        .collect();

    let mut file = BufWriter::new(File::create(dest_file).unwrap());
    IS_RUNNING.store(true, Ordering::SeqCst);

    *JOIN_HANDLE.lock() = Some(
        std::thread::Builder::new()
            .name("tracing-saver".to_string())
            .spawn(move || {
                let mut vec = Vec::new();

                while IS_RUNNING.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(100));

                    std::mem::swap(&mut vec, SPANS_LIST.lock().deref_mut());
                    if vec.len() > 0 {
                        for span in vec.drain(..) {
                            serde_json::to_writer(&mut file, &span).unwrap();
                            writeln!(file, "").unwrap();
                        }
                        file.flush().unwrap();
                        vec.clear();
                    }
                }
            })
            .unwrap(),
    );

    let subscriber = Arc::new(InstrSubscriber::new());

    unsafe {
        GLOBAL_SUBSCRIBER = Some(subscriber.clone());
    }

    tracing::subscriber::set_global_default(subscriber).unwrap();
    InstrumenterGuard(())
}

#[cfg(test)]
mod tests {
    use crate as instrumenter;
    use crate::initialize_tracing;
    use papi::counter::Counter;
    use papi::events_set::EventsSet;
    use std::fmt::Debug;
    use std::time::Duration;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Record};
    use tracing::{Event, Id, Instrument, Metadata, Subscriber};

    #[crate::track(name = "ASDF")]
    fn traced_fn(arg1: usize) {
        tracing::info!("inside my_function!");
        second_traced_fn(arg1 * 2);
    }

    #[crate::track(name = "ASDF1")]
    fn second_traced_fn(arg1: usize) {
        let mut x = 0.0;
        for i in 0..100000000 {
            x *= arg1 as f64 * (i as f64).cos();
        }
        println!("X: {}", x);
    }
    #[test]
    fn trace_test() {
        let _guard = initialize_tracing(
            "/tmp/tracing.dat",
            &[
                "ix86arch::INSTRUCTION_RETIRED",
                "ix86arch::MISPREDICTED_BRANCH_RETIRED",
            ],
        );

        // let mut handles = Vec::new();

        // for i in 0..128 {
        //     handles.push(std::thread::spawn(move || {
        traced_fn(3);
        //     }));
        // }
        // handles.into_iter().for_each(|h| h.join().unwrap());

        std::thread::sleep(Duration::from_secs(5));
        //
        // tracing::subscriber::with_default(collector, || {
        //     traced_fn(132);
        //     traced_fn(1232);
        //     traced_fn(123);
        // });

        // unsafe {
        //     let counters = papi::CounterSet::new([papi::PAPI_TOT_INS]);
        //
        //     let start = counters.read();
        //     let x = fib(14);
        //     let stop = counters.accum();
        //
        //     println!("Computed fib(14) = {} in {} instructions.",
        //              x, stop[0] - start[0]);
        // }
    }
}
