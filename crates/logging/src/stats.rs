use std::time::Duration;

pub struct CompactReport {
    pub input_files: Vec<String>,
    pub output_file: String,
    pub elapsed_time: Duration,
}

pub struct ProcessBatchReport {
    pub total_sequences: usize,
    pub elapsed_time: Duration,
}

pub struct BucketStat {
    pub index: usize,
    pub chunks_count: usize,
    pub total_size: usize,
    pub sequences_count: usize,
    pub kmers_count: usize,
    pub process_batches: Vec<ProcessBatchReport>,
    pub final_execution_repprt: Option<ProcessBatchReport>,
    pub compact_reports: Vec<CompactReport>,
    pub resplit_info: Vec<BucketStat>,
    pub rewrite_info: Vec<BucketStat>,
}

pub struct AssemblerStats {
    pub bucket_stats: Vec<BucketStat>,
}
