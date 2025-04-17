pub mod macros;

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    io::BufWriter,
    ops::AddAssign,
    path::{Path, PathBuf},
    time::Duration,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StatId(u64);

#[derive(Clone, Copy, Default)]
pub struct PrintableDuration(pub Duration);

impl From<Duration> for PrintableDuration {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl AddAssign<Duration> for PrintableDuration {
    fn add_assign(&mut self, other: Duration) {
        self.0.add_assign(other);
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
    pub file_size: usize,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
}

#[derive(Default, Serialize)]
pub struct CompactReport {
    pub report_id: StatId,
    pub bucket_index: usize,
    pub input_files: Vec<InputFileStats>,
    pub output_file: PathBuf,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
    pub subbucket_reports: Vec<SubbucketReport>,
    pub input_total_size: usize,
    pub output_total_size: usize,
    pub compression_ratio: f64,
}

#[derive(Clone, Default, Serialize)]
pub struct KmersMergeBucketReport {
    pub report_id: StatId,
    pub total_sequences: usize,
    pub start_time: PrintableDuration,
    pub elapsed_processor_time: PrintableDuration,
    pub end_processor_time: PrintableDuration,

    pub sequences_sizes: u64,
    pub all_kmers_count: u64,

    pub start_finalize_time: PrintableDuration,
    pub end_finalize_time: PrintableDuration,
    pub output_kmers_count: u64,
}

#[derive(Default, Serialize)]
pub struct BucketMergeInputStat {
    pub id: StatId,
    pub bucket_index: usize,
    pub is_resplit: bool,
    pub input_subbucket_sizes: Vec<usize>,
}

#[derive(Default, Serialize)]
pub struct InputChunkStats {
    pub id: StatId,
    pub index: usize,
    pub sequences_count: usize,
    pub sequences_size: usize,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
    pub finished_send_time: PrintableDuration,
    pub is_last: bool,
}

#[derive(Default, Serialize)]
pub struct InputChunkProcessStats {
    pub id: StatId,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
    pub thread_id: StatId,
}

#[derive(Default, Serialize)]
pub struct CompactCheckpointStats {
    pub trigger_time: PrintableDuration,
    pub buckets: Vec<u16>,
    pub trigger_input_chunk: StatId,
    pub thread_id: StatId,
}

#[derive(Default, Serialize)]
pub struct ResplitInfo {
    pub resplit_id: StatId,
    pub bucket_index: usize,
}

#[derive(Default, Serialize)]
pub struct ResplitFinalInfo {
    pub resplit_id: StatId,
    pub start_time: PrintableDuration,
    pub end_time: PrintableDuration,
    pub out_files: Vec<PathBuf>,
}

#[derive(Default, Serialize)]
pub struct RewriteInfo {
    pub rewrite_id: StatId,
    pub bucket_index: usize,
    pub file_name: PathBuf,
}

#[derive(Default, Serialize)]
pub struct ProcessOnlineInfo {
    pub process_id: StatId,
    pub bucket_index: usize,
}

#[derive(Default, Serialize)]
pub struct KmersTransformInputBucketStats {
    pub stat_id: StatId,
    pub paths: Vec<PathBuf>,
    pub sub_bucket_counters: Vec<u64>,
    pub compaction_delta: i64,
    pub compacted: bool,
    pub resplitted: bool,
    pub rewritten: bool,
    pub allow_online_processing: bool,
    pub queue: BinaryHeap<(Reverse<u64>, usize, bool)>,
    pub resplits: Vec<ResplitInfo>,
    pub processed: Vec<ProcessOnlineInfo>,
    pub rewrites: Vec<RewriteInfo>,
    pub threads_ratio: f64,
    pub addr_concurrency: usize,
    pub chunks_concurrency: usize,
    pub concurrency: usize,
    pub total_file_size: usize,
    pub used_hash_bits: usize,
}

#[derive(Serialize)]
pub struct KmersTransformStats {
    pub buckets: Vec<KmersTransformInputBucketStats>,
    pub resplits: Vec<ResplitFinalInfo>,
}

impl KmersTransformStats {
    pub const fn empty() -> Self {
        Self {
            buckets: vec![],
            resplits: vec![],
        }
    }
}

#[derive(Default, Serialize)]
pub struct AssemblerStats {
    pub preprocess_time: PrintableDuration,
    /// The buffers that hold input sequences
    pub input_chunks: Vec<InputChunkStats>,

    /// The processing stats of the input chunks
    pub input_process_stats: Vec<InputChunkProcessStats>,

    /// The checkpoints that determine the compaction start
    pub compact_checkpoints: Vec<CompactCheckpointStats>,
    /// The compaction reports (every step)
    pub compact_reports: Vec<CompactReport>,
    /// Info about preliminary buckets analysis in the kmers merge phase
    pub bucket_input_merge_stats: Vec<BucketMergeInputStat>,

    pub kmers_merge_stats: Vec<KmersMergeBucketReport>,
    // pub kmers_merge_stats
    // pub kmers_merge_extension_stats
}

impl AssemblerStats {
    pub const fn empty() -> Self {
        Self {
            preprocess_time: PrintableDuration(Duration::from_secs(0)),
            input_chunks: Vec::new(),
            input_process_stats: Vec::new(),
            compact_checkpoints: Vec::new(),
            compact_reports: Vec::new(),
            bucket_input_merge_stats: Vec::new(),
            kmers_merge_stats: Vec::new(),
        }
    }
}

#[derive(Serialize)]
pub struct StatsManager {
    #[serde(skip)]
    pub start_time: Option<std::time::Instant>,
    pub input_counter: usize,
    pub assembler: AssemblerStats,
    pub transform: KmersTransformStats,
}

impl StatsManager {
    pub const fn empty() -> Self {
        Self {
            start_time: None,
            input_counter: 0,
            assembler: AssemblerStats::empty(),
            transform: KmersTransformStats::empty(),
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
