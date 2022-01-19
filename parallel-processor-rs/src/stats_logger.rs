use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub struct StatsLogger {
    stats_file: UnsafeCell<Option<Mutex<File>>>,
    stats: UnsafeCell<Option<Mutex<HashMap<&'static str, (f64, StatMode)>>>>,
    time: UnsafeCell<Option<Instant>>,
    started: AtomicBool,
    interval_ms: usize,
}

unsafe impl Sync for StatsLogger {}

pub struct StatRaiiCounter {
    #[cfg(not(feature = "no-stats"))]
    name: &'static str,
}

impl StatRaiiCounter {
    #[inline(always)]
    pub fn create(stat_name: &'static str) -> Self {
        #[cfg(not(feature = "no-stats"))]
        {
            DEFAULT_STATS_LOGGER.update_stat(stat_name, 1.0, StatMode::Sum);
            Self { name: stat_name }
        }

        #[cfg(feature = "no-stats")]
        Self {}
    }
}

impl Drop for StatRaiiCounter {
    fn drop(&mut self) {
        #[cfg(not(feature = "no-stats"))]
        DEFAULT_STATS_LOGGER.update_stat(self.name, -1.0, StatMode::Sum);
    }
}

#[macro_export]
macro_rules! update_stat {
    ($name:expr, $value:expr, $mode:expr) => {
        #[cfg(not(feature = "no-stats"))]
        {
            use $crate::stats_logger::StatMode;
            $crate::stats_logger::DEFAULT_STATS_LOGGER.update_stat($name, $value, $mode);
        }
    };
}

#[derive(Copy, Clone)]
pub enum StatMode {
    Replace,
    Sum,
}

impl StatsLogger {
    pub const fn new(interval_ms: usize) -> Self {
        Self {
            stats_file: UnsafeCell::new(None),
            stats: UnsafeCell::new(None),
            time: UnsafeCell::new(None),
            started: AtomicBool::new(false),
            interval_ms,
        }
    }

    pub fn init(&'static self, path: impl AsRef<Path>) {
        #[cfg(not(feature = "no-stats"))]
        {
            if self.started.swap(true, Ordering::Relaxed) {
                return;
            }
            unsafe {
                *self.stats_file.get() = Some(Mutex::new(File::create(path).unwrap()));
                *self.stats.get() = Some(Mutex::new(HashMap::new()));
                *self.time.get() = Some(Instant::now());
            }
            thread::spawn(move || loop {
                sleep(Duration::from_millis(self.interval_ms as u64));

                let mut entries: Vec<_>;
                let mut map_lock = unsafe { (*self.stats.get()).as_mut().unwrap().lock() };
                entries = map_lock.iter().map(|(e, f)| (*e, f.0)).collect();

                *map_lock = map_lock
                    .iter()
                    .map(|(e, f)| (*e, *f))
                    .filter(|x| match x.1 .1 {
                        StatMode::Replace => false,
                        StatMode::Sum => x.1 .0 != 0.0,
                    })
                    .collect();

                drop(map_lock);

                let mut values = Vec::new();

                let time = unsafe { (*self.time.get()).as_ref().unwrap().elapsed() };

                entries.sort_by_key(|x| x.0);

                for (name, value) in entries {
                    let _ = writeln!(values, "{};{:.2};{:.2?}", name, value, time);
                }
                unsafe {
                    let _ = (*self.stats_file.get())
                        .as_mut()
                        .unwrap()
                        .lock()
                        .write_all(values.as_slice());
                }
            });
        }
    }

    #[inline(never)] // To allow tracking in profiler
    #[cfg(not(feature = "no-stats"))]
    pub fn update_stat(&self, name: &'static str, value: f64, mode: StatMode) {
        unsafe {
            if !self.started.load(Ordering::Relaxed) {
                return;
            }
            let mut map_lock = (*self.stats.get()).as_mut().unwrap().lock();

            let val = map_lock.entry(name).or_insert((0.0, mode));
            match mode {
                StatMode::Replace => {
                    val.0 = value;
                }
                StatMode::Sum => {
                    val.0 += value;
                }
            }
        }
    }
}

pub static DEFAULT_STATS_LOGGER: StatsLogger = StatsLogger::new(500);
