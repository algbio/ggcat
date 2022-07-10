#[cfg(feature = "enabled")]
pub mod instr_span;

#[cfg(feature = "enabled")]
mod subscriber;
#[cfg(feature = "enabled")]
pub mod tracking_allocator;

#[doc(hidden)]
#[cfg(feature = "enabled")]
pub use tracing as private__;

#[cfg(feature = "enabled")]
mod enabled {
    use crate::instr_span::InstrSpan;
    use crate::subscriber::InstrSubscriber;
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
    macro_rules! global_setup_instrumenter {
        () => {
            pub use instrumenter::private__ as tracing;
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

    #[macro_export]
    macro_rules! use_instrumenter {
        () => {
            use instrumenter::private__ as tracing;
        };
    }
}

#[cfg(not(feature = "enabled"))]
mod not_enabled {
    use std::path::Path;

    pub fn initialize_tracing(_dest_file: impl AsRef<Path>, _hw_counter_names: &[&str]) -> () {}

    #[macro_export]
    macro_rules! use_instrumenter {
        () => {};
    }

    #[macro_export]
    macro_rules! global_setup_instrumenter {
        () => {};
    }
}

#[cfg(feature = "enabled")]
pub use enabled::initialize_tracing;

#[cfg(not(feature = "enabled"))]
pub use not_enabled::initialize_tracing;

pub use instrumenter_derive::track;
