use parallel_processor::enable_counters_logging;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::max;
use std::env::temp_dir;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub struct GGCATConfig {
    /// Directory for temporary files
    pub temp_dir: Option<PathBuf>,
    /// Maximum suggested memory usage (GB)
    /// The tool will try use only up to this GB of memory to store temporary files
    /// without writing to disk. This usage does not include the needed memory for the processing steps.
    /// GGCAT can allocate extra memory for files if the current memory is not enough to complete the current operation
    pub memory: f64,
    /// Use all the given memory before writing to disk
    pub prefer_memory: bool,

    /// The total threads to be used
    pub total_threads_count: usize,

    pub stats_file: Option<PathBuf>,
}

static IS_INITIALIZED: AtomicBool = AtomicBool::new(false);
pub struct GGCATInstance(());

impl GGCATInstance {
    pub fn create(config: GGCATConfig) -> Self {
        if IS_INITIALIZED.swap(true, Ordering::SeqCst) {
            return GGCATInstance(());
        }

        // Increase the maximum allowed number of open files
        fdlimit::raise_fd_limit();

        config::PREFER_MEMORY.store(config.prefer_memory, Ordering::Relaxed);

        rayon::ThreadPoolBuilder::new()
            .num_threads(config.total_threads_count)
            .thread_name(|i| format!("rayon-thread-{}", i))
            .build_global()
            .unwrap();

        if let Some(temp_dir) = &config.temp_dir {
            create_dir_all(temp_dir).unwrap();
        } else {
            todo!("Force memory-only usage")
        }

        if let Some(stats_file) = config.stats_file {
            enable_counters_logging(stats_file, Duration::from_millis(1000), |val| {
                val["phase"] = PHASES_TIMES_MONITOR.read().get_phase_desc().into();
            });
        }

        MemoryFs::init(
            MemoryDataSize::from_bytes(
                (config.memory * (MemoryDataSize::OCTET_GIBIOCTET_FACTOR as f64)) as usize,
            ),
            config::FLUSH_QUEUE_FACTOR * config.total_threads_count,
            max(1, config.total_threads_count / 4),
            8192,
        );
        GGCATInstance(())
    }
}

pub fn build_graph(
    // The input files
    input_files: Vec<String>,

    // The output file
    output_file: String,

    // Specifies the k-mers length
    kmer_length: usize,
    // The threads to be used
    threads_count: usize,
    // Treats reverse complementary kmers as different
    forward_only: bool,
    // Overrides the default m-mers (minimizers) length
    minimizer_length: usize,

    // Enable colors
    colors: bool,

    // Minimum multiplicity required to keep a kmer
    min_multiplicity: usize,

    // Generate maximal unitigs connections references, in BCALM2 format L:<+/->:<other id>:<+/->
    generate_maximal_unitigs_links: bool,
    // // Generate greedy matchtigs instead of maximal unitigs
    // greedy_matchtigs: bool,
    //
    // // Generate eulertigs instead of maximal unitigs
    // eulertigs: bool,
    //
    // // Generate pathtigs instead of maximal unitigs
    // pathtigs: bool,
) {
}

pub fn query_graph(
    // The input graph
    input_graph: String,
    // The input query as a .fasta file
    input_query: String,

    // The output file
    output_file_prefix: String,

    // Specifies the k-mers length
    kmer_length: usize,
    // The threads to be used
    threads_count: usize,
    // Treats reverse complementary kmers as different
    forward_only: bool,
    // Overrides the default m-mers (minimizers) length
    minimizer_length: usize,

    // Enable colors
    colors: bool,
) {
}

fn read_graph(
    // The input graph
    input_graph: String,
    // Enable colors
    colors: bool,
    output_file_prefix: String,
) {
}
