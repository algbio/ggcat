#![deny(clippy::all)]
#![deny(clippy::cargo)]

//! A small library to get memory usage and elapsed CPU time.
//!
//! * Supports Windows, Linux and macOS.
//! * Async interface, uses `tokio::fs` for file operations
//!
//! ```rust
//! use simple_process_stats::ProcessStats;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let process_stats = ProcessStats::get().await.expect("could not get stats for running process");
//! println!("{:?}", process_stats);
//! // ProcessStats {
//! //     cpu_time_user: 421.875ms,
//! //     cpu_time_kernel: 102.332ms,
//! //     memory_usage_bytes: 3420160,
//! // }
//! # }
//! ```
//!
//! On Linux, this library reads `/proc/self/stat` and uses the `sysconf` libc function.
//!
//! On Windows, the library uses `GetCurrentProcess` combined with `GetProcessTimes` and `K32GetProcessMemoryInfo`.
//!
//! On macOS, this library uses `proc_pidinfo` from `libproc` (and current process ID is determined via `libc`).

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Holds the retrieved basic statistics about the running process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessStats {
    /// How much time this process has spent executing in user mode since it was started
    pub cpu_time_user: Duration,
    /// How much time this process has spent executing in kernel mode since it was started
    pub cpu_time_kernel: Duration,
    /// Size of the "resident" memory the process has allocated, in bytes.
    pub memory_usage_bytes: u64,
}

impl ProcessStats {
    /// Get the statistics using the OS-specific method.
    #[cfg(target_os = "windows")]
    pub fn get() -> Result<ProcessStats, Error> {
        windows::get_info()
    }

    /// Get the statistics using the OS-specific method.
    #[cfg(target_os = "linux")]
    pub fn get() -> Result<ProcessStats, Error> {
        linux::get_info()
    }

    /// Get the statistics using the OS-specific method.
    #[cfg(target_os = "macos")]
    pub fn get() -> Result<ProcessStats, Error> {
        macos::get_info()
    }
}

/// An error that occurred while trying to get the process stats.
#[derive(Error, Debug)]
pub enum Error {
    /// A file's contents could not be read successfully. The file that could not be read is specified by
    /// the `PathBuf` parameter.
    #[error("Failed to read from file `{0}`: {1}")]
    FileRead(PathBuf, std::io::Error),
    /// A file's contents were in an unexpected format
    #[error("File contents are in unexpected format")]
    FileContentsMalformed,

    /// A system-native function returned an error code.
    #[error("Call to system-native API errored: {0}")]
    SystemCall(std::io::Error),
}
