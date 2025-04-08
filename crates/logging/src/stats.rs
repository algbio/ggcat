pub mod macros;

use std::{
    io::BufWriter,
    path::{Path, PathBuf},
    time::Duration,
};

use parking_lot::Mutex;
use serde::Serialize;

#[derive(Clone, Copy, Default)]
pub struct PrintableDuration(pub Duration);

impl From<Duration> for PrintableDuration {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl std::fmt::Debug for PrintableDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secs = self.0.as_secs();
        let millis = self.0.subsec_nanos();
        write!(f, "{}.{}", secs, millis)
    }
}

impl Serialize for PrintableDuration {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let secs = self.0.as_secs_f64();
        serializer.serialize_f64(secs)
    }
}

#[derive(Default, Serialize)]
pub struct SubbucketReport {
    pub subbucket_index: usize,
    pub super_kmers_count: usize,
    pub start_time: PrintableDuration,
    pub reset_capacity_time: PrintableDuration,
    pub end_reset_capacity_time: PrintableDuration,
    // pub sequences_count: usize,
}

#[derive(Default, Serialize)]
pub struct InputFileStats {
    pub file_name: PathBuf,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
}

#[derive(Default, Serialize)]
pub struct CompactReport {
    pub bucket_index: usize,
    pub input_files: Vec<InputFileStats>,
    pub output_file: PathBuf,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
    pub subbucket_reports: Vec<SubbucketReport>,
}

#[derive(Default, Serialize)]
pub struct ProcessBatchReport {
    pub total_sequences: usize,
    pub elapsed_time: PrintableDuration,
}

#[derive(Default, Serialize)]
pub struct BucketMergeInputStat {
    pub bucket_index: usize,
    pub is_resplit: bool,
    pub input_subbucket_sizes: Vec<usize>,
    // pub process_batches: Vec<ProcessBatchReport>,
    // pub final_execution_repprt: Option<ProcessBatchReport>,
}

#[derive(Default, Serialize)]
pub struct InputChunkStats {
    pub index: usize,
    pub sequences_count: usize,
    pub sequences_size: usize,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
    pub finished_send_time: PrintableDuration,
    pub is_last: bool,
}

#[derive(Default, Serialize)]
pub struct CompactCheckpointStats {
    pub trigger_time: PrintableDuration,
    pub buckets: Vec<u16>,
}

#[derive(Default, Serialize)]
pub struct AssemblerStats {
    pub preprocess_time: PrintableDuration,
    /// The buffers that hold input sequences
    pub input_chunks: Vec<InputChunkStats>,
    /// The checkpoints that determine the compaction start
    pub compact_checkpoints: Vec<CompactCheckpointStats>,
    /// The compaction reports (every step)
    pub compact_reports: Vec<CompactReport>,
    /// Info about preliminary buckets analysis in the kmers merge phase
    pub bucket_input_merge_stats: Vec<BucketMergeInputStat>,
    // pub resplit_stats
    // pub rewrite_stats
    // pub kmers_merge_stats
    // pub kmers_merge_extension_stats
}

impl AssemblerStats {
    pub const fn empty() -> Self {
        Self {
            preprocess_time: PrintableDuration(Duration::from_secs(0)),
            input_chunks: Vec::new(),
            compact_checkpoints: Vec::new(),
            compact_reports: Vec::new(),
            bucket_input_merge_stats: Vec::new(),
        }
    }
}

#[derive(Serialize)]
pub struct StatsManager {
    #[serde(skip)]
    pub start_time: Option<std::time::Instant>,
    pub input_counter: usize,
    pub assembler: AssemblerStats,
}

impl StatsManager {
    pub const fn empty() -> Self {
        Self {
            start_time: None,
            input_counter: 0,
            assembler: AssemblerStats::empty(),
        }
    }
}

pub static STATS: Mutex<StatsManager> = Mutex::new(StatsManager::empty());

pub fn write_stats(output_file: &Path) {
    let output_file = output_file.with_extension("json");
    let file = BufWriter::new(std::fs::File::create(output_file).unwrap());
    let stats = STATS.lock();
    serde_json::to_writer(file, &*stats).unwrap();
}
