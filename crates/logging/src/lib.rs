pub mod stats;

use std::fmt::{Debug, Display};

use parking_lot::Mutex;

#[repr(u8)]
pub enum MessageLevel {
    Info = 0,
    Warning = 1,
    Error = 2,
    UnrecoverableError = 3,
}

static MESSAGES_CALLBACK: Mutex<Option<fn(MessageLevel, &str)>> = Mutex::new(None);

pub fn setup_logging_callback(callback: fn(MessageLevel, &str)) {
    let mut messages_callback = MESSAGES_CALLBACK.lock();
    *messages_callback = Some(callback);
}

pub fn log(level: MessageLevel, message: &str) {
    let messages_callback = MESSAGES_CALLBACK.lock();
    if let Some(callback) = &*messages_callback {
        callback(level, message);
    } else {
        if let MessageLevel::UnrecoverableError = level {
            panic!("{}", message);
        } else {
            println!("{}", message);
        }
    }
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::log($crate::MessageLevel::Info, &format!($($arg)*));
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::log($crate::MessageLevel::Warning, &format!($($arg)*));
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::log($crate::MessageLevel::Error, &format!($($arg)*));
    };
}

pub trait UnrecoverableErrorLogging {
    fn log_unrecoverable_error(self, message: &str) -> Self;
    fn log_unrecoverable_error_with_data<D: Display>(self, message: &str, data: D) -> Self;
}

impl<T, E: Debug> UnrecoverableErrorLogging for std::result::Result<T, E> {
    fn log_unrecoverable_error(self, message: &str) -> Self {
        if let Err(err) = &self {
            log(
                MessageLevel::UnrecoverableError,
                &format!("{}: {:?}", message, err),
            );
        }
        self
    }

    fn log_unrecoverable_error_with_data<D: Display>(self, message: &str, data: D) -> Self {
        if let Err(err) = &self {
            log(
                MessageLevel::UnrecoverableError,
                &format!("{} [{}]: {:?}", message, data, err),
            );
        }
        self
    }
}
