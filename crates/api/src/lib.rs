mod utils;

use ::utils::assembler_phases::AssemblerPhase;
use colors::bundles::graph_querying::ColorBundleGraphQuerying;
use colors::colors_manager::ColorsManager;
use colors::{
    bundles::multifile_building::ColorBundleMultifileBuilding, non_colored::NonColoredManager,
};
use config::KEEP_FILES;
pub use ggcat_logging::MessageLevel;
use ggcat_logging::{UnrecoverableErrorLogging, info, warn};
use io::concurrent::structured_sequences::StructuredSequenceBackendWrapper;
use io::concurrent::structured_sequences::fasta::FastaWriterWrapper;
use io::concurrent::structured_sequences::gfa::{GFAWriterWrapperV1, GFAWriterWrapperV2};
use io::sequences_stream::GenericSequencesStream;
use io::sequences_stream::fasta::FastaFileSequencesStream;
use parallel_processor::enable_counters_logging;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::cmp::max;
use std::fs::create_dir_all;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

pub use crate::utils::HashType;
pub use config::ColorIndexType;
pub use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
pub use io::sequences_stream::{
    SequenceInfo,
    general::{DynamicSequencesStream, GeneralSequenceBlockData},
};
pub use querier::ColoredQueryOutputFormat;

pub mod debug {
    use crate::utils::HashType;
    pub use config::KEEP_FILES as DEBUG_KEEP_FILES;
    use parking_lot::Mutex;
    use querier::QuerierStartingStep;
    use std::sync::atomic::AtomicBool;
    use utils::assembler_phases::AssemblerPhase;

    pub static DEBUG_ASSEMBLER_FIRST_STEP: Mutex<AssemblerPhase> =
        Mutex::new(AssemblerPhase::MinimizerBucketing);
    pub static DEBUG_ASSEMBLER_LAST_STEP: Mutex<AssemblerPhase> =
        Mutex::new(AssemblerPhase::MaximalUnitigsLinks);

    pub static DEBUG_QUERIER_FIRST_STEP: Mutex<QuerierStartingStep> =
        Mutex::new(QuerierStartingStep::MinimizerBucketing);

    pub static DEBUG_HASH_TYPE: Mutex<HashType> = Mutex::new(HashType::Auto);

    pub static DEBUG_ONLY_BSTATS: AtomicBool = AtomicBool::new(false);

    pub static BUCKETS_COUNT_LOG_FORCE: Mutex<Option<usize>> = Mutex::new(None);
}

#[derive(Clone, Copy, Debug)]
pub enum LoggingMode {
    Log,
    ForceStdout,
}

#[derive(Clone, Copy, Debug)]
pub enum GfaVersion {
    V1,
    V2,
}

/// Main config of GGCAT. This config is global and should be passed to GGCATInstance::create
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

    /// The path to an optional json-formatted real time stats file
    pub stats_file: Option<PathBuf>,

    /// The messages callback, if present, no output will be automatically written to stdout
    pub messages_callback: Option<fn(MessageLevel, &str)>,
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
    /// Generate simplitigs
    FastSimplitigs,
    /// Generate fast eulertigs
    FastEulertigs,
}

static INSTANCE: Mutex<Option<&'static GGCATInstance>> = Mutex::new(None);

pub struct GGCATInstance(GGCATConfig);

fn create_tempdir(base_path: Option<PathBuf>) -> Option<PathBuf> {
    base_path.map(|t| {
        let temp_dir = t.join(&format!("build_graph_{}", uuid::Uuid::new_v4()));
        let _ = create_dir_all(&temp_dir);
        temp_dir
    })
}

fn remove_tempdir(temp_dir: Option<PathBuf>) {
    if let Some(temp_dir) = temp_dir {
        MemoryFs::remove_directory(&temp_dir, true);
        let _ = std::fs::remove_dir_all(temp_dir);
    }
}

/// Main GGCAT struct. It's a singleton and can be created by passing a GGCATConfig.
/// Successive calls to create will return the same instance, ignoring the new configuration.
impl GGCATInstance {
    /// Creates a new GGCATInstance. If an instance already exists, it will be returned, ignoring the new config.
    pub fn create(config: GGCATConfig) -> anyhow::Result<&'static Self> {
        let mut instance = INSTANCE.lock();

        if let Some(instance) = instance.deref() {
            return Ok(instance);
        }

        parallel_processor::set_logger_function(|level, message| {
            ggcat_logging::log(
                match level {
                    parallel_processor::LogLevel::Info => MessageLevel::Info,
                    parallel_processor::LogLevel::Warning => MessageLevel::Warning,
                    parallel_processor::LogLevel::Error => MessageLevel::Error,
                },
                &message,
            );
        });

        // Increase the maximum allowed number of open files
        if let Err(err) = fdlimit::raise_fd_limit() {
            ggcat_logging::warn!(
                "WARNING: Failed to increase the maximum number of open files: {}",
                err
            );
        }

        config::PREFER_MEMORY.store(config.prefer_memory, Ordering::Relaxed);

        if let Some(callback) = config.messages_callback {
            ggcat_logging::setup_logging_callback(callback);
        }

        rayon::ThreadPoolBuilder::new()
            .num_threads(config.total_threads_count)
            .thread_name(|i| format!("rayon-thread-{}", i))
            .build_global()
            .log_unrecoverable_error("Cannot initialize rayon thread pool")?;

        if let Some(temp_dir) = &config.temp_dir {
            create_dir_all(temp_dir).log_unrecoverable_error_with_data(
                "Cannot create temporary directory",
                temp_dir.display(),
            )?;
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
        *instance = Some(Box::leak(Box::new(GGCATInstance(config))));
        return Ok(instance.unwrap());
    }

    /// Builds a new graph from the given input streams, with the specified parameters
    pub fn build_graph(
        &self,
        // The input streams
        input_streams: Vec<GeneralSequenceBlockData>,

        // The output file
        output_file: PathBuf,

        // The names of the colors, ordered by color index
        color_names: Option<&[String]>,

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

        gfa_output_version: Option<GfaVersion>,
    ) -> anyhow::Result<PathBuf> {
        let merging_hash_dispatch = utils::get_hash_static_id(
            debug::DEBUG_HASH_TYPE.lock().clone(),
            kmer_length,
            forward_only,
        );

        let colors_hash = if colors {
            ColorBundleMultifileBuilding::dynamic_dispatch_id()
        } else {
            NonColoredManager::dynamic_dispatch_id()
        };

        if gfa_output_version.is_some() && colors {
            anyhow::bail!("GFA output is not supported with colors");
        }

        let output_mode = match gfa_output_version {
            None => FastaWriterWrapper::dynamic_dispatch_id(),
            Some(GfaVersion::V1) => GFAWriterWrapperV1::dynamic_dispatch_id(),
            Some(GfaVersion::V2) => GFAWriterWrapperV2::dynamic_dispatch_id(),
        };

        let first_step = debug::DEBUG_ASSEMBLER_FIRST_STEP.lock().clone();
        let last_step = debug::DEBUG_ASSEMBLER_LAST_STEP.lock().clone();
        let temp_dir = if first_step == AssemblerPhase::default() {
            create_tempdir(self.0.temp_dir.clone())
        } else {
            Some(
                self.0
                    .temp_dir
                    .clone()
                    .expect("while testing successive phases the temp dir must be specified"),
            )
        };

        if let Some(temp_dir) = &temp_dir {
            info!("Temporary directory: {}", temp_dir.display());
        } else {
            warn!("No temporary directory specified, writing to cwd!");
        }

        let output_file = assembler::dynamic_dispatch::run_assembler(
            (merging_hash_dispatch, colors_hash, output_mode),
            kmer_length,
            minimizer_length.unwrap_or(::utils::compute_best_m(kmer_length)),
            first_step,
            last_step,
            input_streams,
            color_names.unwrap_or(&[]),
            output_file,
            temp_dir.clone(),
            threads_count,
            min_multiplicity,
            *debug::BUCKETS_COUNT_LOG_FORCE.lock(),
            self.0.intermediate_compression_level,
            extra_elab == ExtraElaboration::UnitigLinks,
            match extra_elab {
                ExtraElaboration::GreedyMatchtigs => Some(assembler::MatchtigMode::GreedyTigs),
                ExtraElaboration::Eulertigs => Some(assembler::MatchtigMode::EulerTigs),
                ExtraElaboration::Pathtigs => Some(assembler::MatchtigMode::PathTigs),
                ExtraElaboration::FastSimplitigs => Some(assembler::MatchtigMode::FastSimpliTigs),
                ExtraElaboration::FastEulertigs => Some(assembler::MatchtigMode::FastEulerTigs),
                _ => None,
            },
            debug::DEBUG_ONLY_BSTATS.load(Ordering::Relaxed),
            forward_only,
        )?;

        if last_step == AssemblerPhase::BuildUnitigs && !KEEP_FILES.load(Ordering::Relaxed) {
            remove_tempdir(temp_dir);
        } else {
            info!("Keeping temp dir at {:?}", temp_dir);
        }

        Ok(output_file)
    }

    /// Queries a (optionally) colored graph with a specific set of sequences as queries
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
    ) -> anyhow::Result<PathBuf> {
        let merging_hash_dispatch = utils::get_hash_static_id(
            debug::DEBUG_HASH_TYPE.lock().clone(),
            kmer_length,
            forward_only,
        );

        let colors_hash = if colors {
            ColorBundleGraphQuerying::dynamic_dispatch_id()
        } else {
            NonColoredManager::dynamic_dispatch_id()
        };

        let temp_dir = create_tempdir(self.0.temp_dir.clone());

        let output_file = querier::dynamic_dispatch::run_query(
            (merging_hash_dispatch, colors_hash),
            kmer_length,
            minimizer_length.unwrap_or(::utils::compute_best_m(kmer_length)),
            debug::DEBUG_QUERIER_FIRST_STEP.lock().clone(),
            input_graph,
            input_query,
            output_file_prefix,
            temp_dir.clone(),
            *debug::BUCKETS_COUNT_LOG_FORCE.lock(),
            threads_count,
            self.0.intermediate_compression_level,
            color_output_format,
        )?;

        remove_tempdir(temp_dir);

        Ok(output_file)
    }

    /// Obtains the standard colormap file path from a graph file path
    pub fn get_colormap_file(graph_file: impl AsRef<Path>) -> PathBuf {
        graph_file.as_ref().with_extension("colors.dat")
    }

    /// Returns an iterator over the color names in the given graph.
    /// The color indexes returned from the dump_unitigs function
    /// can be used to index this (collected) iterator.
    pub fn dump_colors(
        // The input colormap
        input_colormap: impl AsRef<Path>,
    ) -> anyhow::Result<impl Iterator<Item = String>> {
        use colors::DefaultColorsSerializer;
        use colors::colors_manager::ColorMapReader;
        use colors::storage::deserializer::ColorsDeserializer;

        let colors_deserializer =
            ColorsDeserializer::<DefaultColorsSerializer>::new(input_colormap, true)?;

        Ok((0..colors_deserializer.colors_count()).map(move |i| {
            colors_deserializer
                .get_color_name(i as ColorIndexType, true)
                .to_string()
        }))
    }

    /// Queries specified color subsets of the colormap, returning
    /// the color indices corresponding to the colors of each subset
    pub fn query_colormap(
        &self,
        // The input colormap
        colormap_file: PathBuf,
        // The subsets to be queried
        subsets: Vec<ColorIndexType>,
        // Call the output function from a single thread at a time,
        // avoiding the need for synchronization in the user code
        single_thread_output_function: bool,
        output_function: impl Fn(ColorIndexType, &[ColorIndexType]) + Send + Sync,
    ) -> anyhow::Result<()> {
        dumper::dump_colormap_query(
            colormap_file,
            subsets,
            single_thread_output_function,
            output_function,
        )
    }

    /// Dumps the unitigs of the given graph, optionally with colors
    /// It's not guaranteed that maximal unitigs are returned, as only kmers with the same colors subset
    /// are returned as whole unitigs to speedup colormap reading times
    pub fn dump_unitigs(
        &self,
        graph_input: impl AsRef<Path>,
        // Specifies the k-mers length
        kmer_length: usize,
        // Overrides the default m-mers (minimizers) length
        minimizer_length: Option<usize>,
        colors: bool,
        // The threads to be used
        threads_count: usize,

        // Call the output function from a single thread at a time,
        // avoiding the need for synchronization in the user code
        single_thread_output_function: bool,
        output_function: impl Fn(&[u8], &[ColorIndexType], bool) + Send + Sync,
    ) -> anyhow::Result<()> {
        let temp_dir = create_tempdir(self.0.temp_dir.clone());

        if colors {
            dumper::dump_unitigs(
                kmer_length,
                minimizer_length.unwrap_or(::utils::compute_best_m(kmer_length)),
                &graph_input,
                temp_dir.clone(),
                *debug::BUCKETS_COUNT_LOG_FORCE.lock(),
                threads_count,
                single_thread_output_function,
                self.0.intermediate_compression_level,
                output_function,
            )?;
        } else {
            FastaFileSequencesStream::new().read_block(
                &(graph_input.as_ref().to_path_buf(), None),
                false,
                Some(kmer_length - 1),
                |seq, _info| {
                    output_function(seq.ident_data, &[], false);
                },
            );
        }

        remove_tempdir(temp_dir);
        Ok(())
    }
}
