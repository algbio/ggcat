use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct InstrMetadata {
    /// The name of the span described by this metadata.
    pub name: String,

    /// The part of the system that the span that this metadata describes
    /// occurred in.
    pub target: String,

    /// The name of the Rust module where the span occurred, or `None` if this
    /// could not be determined.
    pub module_path: Option<String>,

    /// The name of the source code file where the span occurred, or `None` if
    /// this could not be determined.
    pub file: Option<String>,

    /// The line number in the source code file where the span occurred, or
    /// `None` if this could not be determined.
    pub line: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InstrSpan {
    pub thread_id: u64,
    pub start_time: Duration,
    pub end_time: Duration,
    pub executon_time: Duration,
    pub user_time: Duration,
    pub enter_count: usize,
    pub counters: Vec<u64>,
    pub parameters: Vec<(String, i128)>,
    pub meta: InstrMetadata,
    pub max_own_memory: usize,
    pub max_own_items: usize,

    pub current_tot_memory: usize,
    pub current_tot_items: usize,

    pub max_memory: usize,
}
