mod utils;

use colors::colors_manager::ColorsManager;
use colors::{
    bundles::multifile_building::ColorBundleMultifileBuilding, non_colored::NonColoredManager,
};
use config::ColorIndexType;
use hashes::MinimizerHashFunctionFactory;
use hashes::{cn_nthash::CanonicalNtHashIteratorFactory, fw_nthash::ForwardNtHashIteratorFactory};
pub use io::sequences_stream::general::GeneralSequenceBlockData;
use parallel_processor::enable_counters_logging;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use querier::ColoredQueryOutputFormat;
use std::cmp::max;
use std::fs::create_dir_all;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

pub mod debug {
    use crate::utils::HashType;
    use assembler::AssemblerStartingStep;
    pub use config::KEEP_FILES as DEBUG_KEEP_FILES;
    use parking_lot::Mutex;
    use querier::QuerierStartingStep;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    pub static DEBUG_ASSEMBLER_FIRST_STEP: Mutex<AssemblerStartingStep> =
        Mutex::new(AssemblerStartingStep::MinimizerBucketing);
    pub static DEBUG_ASSEMBLER_LAST_STEP: Mutex<AssemblerStartingStep> =
        Mutex::new(AssemblerStartingStep::MaximalUnitigsLinks);

    pub static DEBUG_QUERIER_FIRST_STEP: Mutex<QuerierStartingStep> =
        Mutex::new(QuerierStartingStep::MinimizerBucketing);

    pub static DEBUG_HASH_TYPE: Mutex<HashType> = Mutex::new(HashType::Auto);

    pub static DEBUG_LINK_PHASE_ITERATION_START_STEP: AtomicUsize = AtomicUsize::new(0);
    pub static DEBUG_ONLY_BSTATS: AtomicBool = AtomicBool::new(false);

    pub static BUCKETS_COUNT_FORCE: Mutex<Option<usize>> = Mutex::new(None);
}

#[derive(Clone)]
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

    /// The default lz4 compression level for the intermediate files
    pub intermediate_compression_level: Option<u32>,

    pub stats_file: Option<PathBuf>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ExtraElaboration {
    None,
    /// Generate maximal unitigs connections references, in BCALM2 format L:<+/->:<other id>:<+/->
    UnitigLinks,
    /// Generate greedy matchtigs
    GreedyMatchtigs,
    /// Generate eulertigs
    Eulertigs,
    /// Generate pathtigs
    Pathtigs,
}

static INSTANCE: Mutex<Option<GGCATInstance>> = Mutex::new(None);
#[derive(Clone)]
pub struct GGCATInstance(GGCATConfig);

impl GGCATInstance {
    pub fn create(config: GGCATConfig) -> Self {
        let mut instance = INSTANCE.lock();

        if let Some(instance) = instance.deref() {
            return instance.clone();
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

        if let Some(stats_file) = &config.stats_file {
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
        *instance = Some(GGCATInstance(config));
        return instance.clone().unwrap();
    }

    pub fn build_graph(
        &self,
        // The input files
        input_files: Vec<GeneralSequenceBlockData>,

        // The output file
        output_file: PathBuf,

        // The names of the colors, ordered by color index
        color_names: Option<Vec<String>>,

        // Specifies the k-mers length
        kmer_length: usize,
        // The threads to be used
        threads_count: usize,
        // Treats reverse complementary kmers as different
        forward_only: bool,
        // Overrides the default m-mers (minimizers) length
        minimizer_length: Option<usize>,

        // Enable colors
        colors: bool,

        // Minimum multiplicity required to keep a kmer
        min_multiplicity: usize,

        extra_elab: ExtraElaboration,
    ) {
        let bucketing_hash_dispatch = if forward_only {
            <ForwardNtHashIteratorFactory as MinimizerHashFunctionFactory>::DYNAMIC_DISPATCH_ID
        } else {
            <CanonicalNtHashIteratorFactory as MinimizerHashFunctionFactory>::DYNAMIC_DISPATCH_ID
        };

        let merging_hash_dispatch = utils::get_hash_static_id(
            debug::DEBUG_HASH_TYPE.lock().clone(),
            kmer_length,
            forward_only,
        );

        let colors_hash = if colors {
            ColorBundleMultifileBuilding::DYNAMIC_DISPATCH_ID
        } else {
            NonColoredManager::DYNAMIC_DISPATCH_ID
        };

        let temp_dir = self
            .0
            .temp_dir
            .as_ref()
            .map(|t| t.join(&format!("build_graph_{}", uuid::Uuid::new_v4())));

        assembler::dynamic_dispatch::run_assembler(
            (bucketing_hash_dispatch, merging_hash_dispatch, colors_hash),
            kmer_length,
            minimizer_length.unwrap_or(::utils::compute_best_m(kmer_length)),
            debug::DEBUG_ASSEMBLER_FIRST_STEP.lock().clone(),
            debug::DEBUG_ASSEMBLER_LAST_STEP.lock().clone(),
            input_files,
            color_names.unwrap_or(vec![]),
            output_file,
            temp_dir,
            threads_count,
            min_multiplicity,
            *debug::BUCKETS_COUNT_FORCE.lock(),
            Some(debug::DEBUG_LINK_PHASE_ITERATION_START_STEP.load(Ordering::Relaxed)),
            self.0.intermediate_compression_level,
            extra_elab == ExtraElaboration::UnitigLinks,
            match extra_elab {
                ExtraElaboration::GreedyMatchtigs => Some(assembler::MatchtigMode::GreedyTigs),
                ExtraElaboration::Eulertigs => Some(assembler::MatchtigMode::EulerTigs),
                ExtraElaboration::Pathtigs => Some(assembler::MatchtigMode::PathTigs),
                _ => None,
            },
            debug::DEBUG_ONLY_BSTATS.load(Ordering::Relaxed),
        );
    }

    pub fn query_graph(
        &self,
        // The input graph
        input_graph: PathBuf,
        // The input query as a .fasta file
        input_query: PathBuf,

        // The output file
        output_file_prefix: PathBuf,

        // Specifies the k-mers length
        kmer_length: usize,
        // The threads to be used
        threads_count: usize,
        // Treats reverse complementary kmers as different
        forward_only: bool,
        // Overrides the default m-mers (minimizers) length
        minimizer_length: Option<usize>,

        // Enable colors
        colors: bool,

        // Query output format
        color_output_format: ColoredQueryOutputFormat,
    ) {
        let bucketing_hash_dispatch = if forward_only {
            <ForwardNtHashIteratorFactory as MinimizerHashFunctionFactory>::DYNAMIC_DISPATCH_ID
        } else {
            <CanonicalNtHashIteratorFactory as MinimizerHashFunctionFactory>::DYNAMIC_DISPATCH_ID
        };

        let merging_hash_dispatch = utils::get_hash_static_id(
            debug::DEBUG_HASH_TYPE.lock().clone(),
            kmer_length,
            forward_only,
        );

        let colors_hash = if colors {
            ColorBundleMultifileBuilding::DYNAMIC_DISPATCH_ID
        } else {
            NonColoredManager::DYNAMIC_DISPATCH_ID
        };

        querier::dynamic_dispatch::run_query(
            (bucketing_hash_dispatch, merging_hash_dispatch, colors_hash),
            kmer_length,
            minimizer_length.unwrap_or(::utils::compute_best_m(kmer_length)),
            debug::DEBUG_QUERIER_FIRST_STEP.lock().clone(),
            input_graph,
            input_query,
            output_file_prefix,
            self.0.temp_dir.clone(),
            *debug::BUCKETS_COUNT_FORCE.lock(),
            threads_count,
            self.0.intermediate_compression_level,
            color_output_format,
        )
    }

    pub fn dump_colors(
        // The input graph
        input_colormap: impl AsRef<Path>,
    ) -> impl Iterator<Item = String> {
        use colors::colors_manager::ColorMapReader;
        use colors::storage::deserializer::ColorsDeserializer;
        use colors::DefaultColorsSerializer;

        let colors_deserializer =
            ColorsDeserializer::<DefaultColorsSerializer>::new(input_colormap, true);

        (0..colors_deserializer.colors_count()).map(move |i| {
            colors_deserializer
                .get_color_name(i as ColorIndexType, true)
                .to_string()
        })
    }

    // fn read_graph(
    //     // The input graph
    //     input_graph: String,
    //     // Enable colors
    //     colors: bool,
    //     output_file_prefix: String,
    // ) {
    // }
}
