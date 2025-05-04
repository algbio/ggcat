use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{mem::transmute, path::PathBuf};

use ggcat_api::{
    ColorIndexType, DnaSequence, DnaSequencesFileType, DynamicSequencesStream, GfaVersion,
    SequenceInfo,
};
use ggcat_api::{ExtraElaboration, GGCATConfig, GGCATInstance, GeneralSequenceBlockData};

#[repr(transparent)]
struct GGCATInstanceFFI(GGCATInstance);

static FFI_MESSAGES_CALLBACK_PTR: AtomicUsize = AtomicUsize::new(0);

fn ggcat_create(config: ffi::GGCATConfigFFI) -> *const GGCATInstanceFFI {
    FFI_MESSAGES_CALLBACK_PTR.store(config.messages_callback, Ordering::SeqCst);

    let instance = GGCATInstance::create(GGCATConfig {
        temp_dir: if config.use_temp_dir {
            Some(PathBuf::from(config.temp_dir))
        } else {
            None
        },
        memory: config.memory,
        prefer_memory: config.prefer_memory,
        total_threads_count: config.total_threads_count,
        intermediate_compression_level: if config.intermediate_compression_level != u32::MAX {
            Some(config.intermediate_compression_level)
        } else {
            None
        },
        stats_file: if config.use_stats_file {
            Some(PathBuf::from(config.stats_file))
        } else {
            None
        },
        messages_callback: if config.messages_callback == 0 {
            None
        } else {
            Some(|lvl, str| {
                let ptr = FFI_MESSAGES_CALLBACK_PTR.load(Ordering::SeqCst);
                if ptr != 0 {
                    let cb: extern "C" fn(u8, *const i8) =
                        unsafe { std::mem::transmute(ptr as *const ()) };
                    let cstring = std::ffi::CString::new(str).unwrap();
                    cb(lvl as u8, cstring.as_ptr());
                }
            })
        },
        enable_disk_optimization: config.enable_disk_optimization,
    })
    .ok();
    unsafe { std::mem::transmute(instance) }
}

fn ggcat_build(
    instance: &'static GGCATInstanceFFI,
    // The input blocks
    input_blocks: Vec<GeneralSequenceBlockData>,

    // The output file
    output_file: String,

    // The names of the colors, ordered by color index
    color_names: &[String],

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

    // Extra elaboration step
    extra_elab: usize,

    // Output the result in GFA format
    gfa_output_version: u32,

    // Enables the disk optimization
    enable_disk_optimization: bool,
) -> String {
    const EXTRA_ELABORATION_STEP_NONE: usize = 0;
    const EXTRA_ELABORATION_STEP_UNITIG_LINKS: usize = 1;
    const EXTRA_ELABORATION_STEP_GREEDY_MATCHTIGS: usize = 2;
    const EXTRA_ELABORATION_STEP_EULERTIGS: usize = 3;
    const EXTRA_ELABORATION_STEP_PATHTIGS: usize = 4;

    instance
        .0
        .build_graph(
            input_blocks,
            PathBuf::from(output_file),
            if color_names.len() > 0 {
                Some(color_names)
            } else {
                None
            },
            kmer_length,
            threads_count,
            forward_only,
            if minimizer_length == usize::MAX {
                None
            } else {
                Some(minimizer_length)
            },
            colors,
            min_multiplicity,
            match extra_elab {
                EXTRA_ELABORATION_STEP_NONE => ExtraElaboration::None,
                EXTRA_ELABORATION_STEP_UNITIG_LINKS => ExtraElaboration::UnitigLinks,
                EXTRA_ELABORATION_STEP_GREEDY_MATCHTIGS => ExtraElaboration::GreedyMatchtigs,
                EXTRA_ELABORATION_STEP_EULERTIGS => ExtraElaboration::Eulertigs,
                EXTRA_ELABORATION_STEP_PATHTIGS => ExtraElaboration::Pathtigs,
                _ => panic!("Invalid extra_elab value: {}", extra_elab),
            },
            match gfa_output_version {
                0 => None,
                1 => Some(GfaVersion::V1),
                2 => Some(GfaVersion::V2),
                _ => panic!("Invalid gfa_output_version value: {}", gfa_output_version),
            },
            enable_disk_optimization,
        )
        .unwrap_or_default()
        .to_str()
        .unwrap()
        .to_string()
}

fn ggcat_build_from_files(
    instance: &'static GGCATInstanceFFI,
    // The input files
    input_files: &[String],

    // The output file
    output_file: String,

    // The names of the colors, ordered by color index
    color_names: &[String],

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

    // Extra elaboration step
    extra_elab: usize,

    // Output the result in GFA format
    gfa_output_version: u32,

    // Enables the disk optimization
    enable_disk_optimization: bool,
) -> String {
    ggcat_build(
        instance,
        input_files
            .iter()
            .map(|f| {
                if f.ends_with(".gfa") {
                    todo!("GFA support is not implemented yet");
                    // GeneralSequenceBlockData::GFA() // PathBuf::from(f))
                } else {
                    GeneralSequenceBlockData::FASTA((PathBuf::from(f), None))
                }
            })
            .collect(),
        output_file,
        color_names,
        kmer_length,
        threads_count,
        forward_only,
        minimizer_length,
        colors,
        min_multiplicity,
        extra_elab,
        gfa_output_version,
        enable_disk_optimization,
    )
}

fn ggcat_build_from_streams(
    instance: &'static GGCATInstanceFFI,
    // The input streams
    input_streams: &[ffi::InputStreamFFI],

    // The output file
    output_file: String,

    // The names of the colors, ordered by color index
    color_names: &[String],

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

    // Extra elaboration step
    extra_elab: usize,

    // Output the result in GFA format
    gfa_output_version: u32,

    // Enables the disk optimization
    enable_disk_optimization: bool,
) -> String {
    struct SequencesStreamFFI {
        // extern "C" void (*read_block)(uintptr_t block, bool copy_ident_data, size_t partial_read_copyback, uintptr_t callback, uintptr_t callback_context);
        //      with void callback(uintptr_t callback_context, DnaSequenceFFI sequence, SequenceInfoFFI info);
        virtual_read_block: extern "C" fn(
            block: usize,
            copy_ident_data: bool,
            partial_read_copyback: usize,
            callback: extern "C" fn(
                callback_context: usize,
                sequence: DnaSequenceFFI,
                info: SequenceInfoFFI,
            ),
            callback_context: usize,
        ),
        // extern "C" uint64_t (*estimated_base_count)(uintptr_t block);
        virtual_estimated_base_count: extern "C" fn(block: usize) -> u64,
    }

    impl DynamicSequencesStream for SequencesStreamFFI {
        fn read_block(
            &self,
            block: usize,
            copy_ident_data: bool,
            partial_read_copyback: Option<usize>,
            mut callback: &mut dyn FnMut(DnaSequence, SequenceInfo),
        ) {
            extern "C" fn callback_wrapper(
                callback_ptr: usize,
                sequence: DnaSequenceFFI,
                info: SequenceInfoFFI,
            ) {
                let callback = unsafe {
                    &mut *(callback_ptr as *mut &mut dyn FnMut(DnaSequence, SequenceInfo))
                };
                callback(
                    DnaSequence {
                        ident_data: unsafe {
                            from_raw_parts(sequence.ident_data, sequence.ident_data_len)
                        },
                        seq: unsafe { from_raw_parts(sequence.seq, sequence.seq_len) },
                        format: match sequence.format {
                            DnaSequencesFileTypeFFI::FASTA => DnaSequencesFileType::FASTA,
                            DnaSequencesFileTypeFFI::FASTQ => DnaSequencesFileType::FASTQ,
                            DnaSequencesFileTypeFFI::GFA => DnaSequencesFileType::GFA,
                            DnaSequencesFileTypeFFI::BINARY => DnaSequencesFileType::BINARY,
                        },
                    },
                    SequenceInfo {
                        color: Some(info.color),
                    },
                );
            }

            let callback_ptr = (&mut callback) as *mut _ as usize;
            (self.virtual_read_block)(
                block,
                copy_ident_data,
                partial_read_copyback.unwrap_or(0),
                callback_wrapper,
                callback_ptr,
            );
        }

        fn estimated_base_count(&self, block: usize) -> u64 {
            (self.virtual_estimated_base_count)(block)
        }
    }

    ggcat_build(
        instance,
        input_streams
            .iter()
            .map(|s| {
                GeneralSequenceBlockData::Dynamic((
                    Arc::new(unsafe {
                        SequencesStreamFFI {
                            virtual_read_block: transmute(s.virtual_read_block),
                            virtual_estimated_base_count: transmute(s.virtual_estimated_base_count),
                        }
                    }),
                    s.block_data,
                ))
            })
            .collect(),
        output_file,
        color_names,
        kmer_length,
        threads_count,
        forward_only,
        minimizer_length,
        colors,
        min_multiplicity,
        extra_elab,
        gfa_output_version,
        enable_disk_optimization,
    )
}

/// Queries a (optionally) colored graph with a specific set of sequences as queries
fn ggcat_query_graph(
    instance: &'static GGCATInstanceFFI,

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

    // Query output format
    color_output_format: usize,
) -> String {
    const COLORED_QUERY_OUTPUT_FORMAT_JSON_LINES_WITH_NUMBERS: usize = 0;
    const COLORED_QUERY_OUTPUT_FORMAT_JSON_LINES_WITH_NAMES: usize = 1;

    instance
        .0
        .query_graph(
            PathBuf::from(input_graph),
            PathBuf::from(input_query),
            PathBuf::from(output_file_prefix),
            kmer_length,
            threads_count,
            forward_only,
            if minimizer_length == usize::MAX {
                None
            } else {
                Some(minimizer_length)
            },
            colors,
            match color_output_format {
                COLORED_QUERY_OUTPUT_FORMAT_JSON_LINES_WITH_NUMBERS => {
                    ggcat_api::ColoredQueryOutputFormat::JsonLinesWithNumbers
                }
                COLORED_QUERY_OUTPUT_FORMAT_JSON_LINES_WITH_NAMES => {
                    ggcat_api::ColoredQueryOutputFormat::JsonLinesWithNames
                }
                _ => panic!("Invalid color_output_format value: {}", color_output_format),
            },
        )
        .unwrap_or_default()
        .to_str()
        .unwrap()
        .to_string()
}

/// Obtains the standard colormap file path from a graph file path
pub fn ggcat_get_colormap_file(graph_file: String) -> String {
    GGCATInstance::get_colormap_file(PathBuf::from(graph_file))
        .to_str()
        .unwrap()
        .to_string()
}

/// Returns a vector of color names in the given graph.
/// The color indexes returned from the dump_unitigs function
/// can be used to index this vector.
pub fn ggcat_dump_colors(
    // The input colormap
    input_colormap: String,
) -> Vec<String> {
    GGCATInstance::dump_colors(input_colormap)
        .map(|v| v.collect())
        .unwrap_or_default()
}

/// Dumps the unitigs of the given graph, optionally with colors
/// It's not guaranteed that maximal unitigs are returned, as only kmers with the same colors subset
/// are returned as whole unitigs to speedup colormap reading times
fn ggcat_dump_unitigs(
    instance: &'static GGCATInstanceFFI,
    // The input graph
    graph_input: String,
    // Specifies the k-mers length
    kmer_length: usize,
    // Overrides the default m-mers (minimizers) length
    minimizer_length: usize,
    // Enable colors
    colors: bool,
    // The threads to be used
    threads_count: usize,

    // Call the output function from a single thread at a time,
    // avoiding the need for synchronization in the user code
    single_thread_output_function: bool,

    output_function_context: usize,
    output_function_ptr: usize,
) {
    let output_function: extern "C" fn(usize, usize, usize, usize, usize, bool) =
        unsafe { transmute(output_function_ptr) };

    let _ = instance.0.dump_unitigs(
        PathBuf::from(graph_input),
        kmer_length,
        if minimizer_length == usize::MAX {
            None
        } else {
            Some(minimizer_length)
        },
        colors,
        threads_count,
        single_thread_output_function,
        |sequence, colors, same_colors| {
            output_function(
                output_function_context,
                sequence.as_ptr() as usize,
                sequence.len(),
                colors.as_ptr() as usize,
                colors.len(),
                same_colors,
            );
        },
    );
}

/// Queries specified color subsets of the colormap, returning
/// the color indices corresponding to the colors of each subset
fn ggcat_query_colormap(
    instance: &'static GGCATInstanceFFI,
    // The input colormap
    colormap: String,
    // The subsets to be queried
    subsets: Vec<ColorIndexType>,
    // Call the output function from a single thread at a time,
    // avoiding the need for synchronization in the user code
    single_thread_output_function: bool,

    output_function_context: usize,
    output_function_ptr: usize,
) {
    let output_function: extern "C" fn(usize, u32, usize, usize) =
        unsafe { transmute(output_function_ptr) };

    let _ = instance.0.query_colormap(
        PathBuf::from(colormap),
        subsets,
        single_thread_output_function,
        |subset, colors| {
            output_function(
                output_function_context,
                subset,
                colors.as_ptr() as usize,
                colors.len(),
            );
        },
    );
}

static_assertions::assert_eq_size!(ColorIndexType, u32);

#[derive(Debug)]
#[repr(C)]
pub enum DnaSequencesFileTypeFFI {
    FASTA = 0,
    FASTQ = 1,
    GFA = 2,
    BINARY = 3,
}

#[repr(C)]
pub struct DnaSequenceFFI {
    pub ident_data: *const u8,
    pub ident_data_len: usize,
    pub seq: *const u8,
    pub seq_len: usize,
    pub format: DnaSequencesFileTypeFFI,
}

#[repr(C)]
pub struct SequenceInfoFFI {
    pub color: u32, // ColorIndexType
}

#[cxx::bridge]
mod ffi {

    /// Main config of GGCAT. This config is global and should be passed to GGCATInstance::create
    pub struct GGCATConfigFFI {
        /// If false, a memory only mode is attempted. May crash for large input data if there is no enough RAM memory.
        pub use_temp_dir: bool,

        /// Directory for temporary files
        pub temp_dir: String,

        /// Maximum suggested memory usage (GB)
        /// The tool will try use only up to this GB of memory to store temporary files
        /// without writing to disk. This usage does not include the needed memory for the processing steps.
        /// GGCAT can allocate extra memory for files if the current memory is not enough to complete the current operation
        pub memory: f64,

        /// Use all the given memory before writing to disk
        pub prefer_memory: bool,

        /// The total threads to be used
        pub total_threads_count: usize,

        /// The default lz4 compression level for the intermediate files, -1 to use default values
        pub intermediate_compression_level: u32,

        /// True if the stats file should be created
        pub use_stats_file: bool,
        /// The path to an optional json-formatted real time stats file
        pub stats_file: String,

        /// Function pointer with signature void (uint8_t, const char *) receiving messages
        pub messages_callback: usize,

        /// Output the result in the specified version of GFA format, 0 outputs in FASTA (default)
        /// Supported V1 and V2
        pub gfa_output_version: u32,

        /// Enables the disk optimization
        pub enable_disk_optimization: bool,
    }

    pub struct InputStreamFFI {
        // extern "C" void (*read_block)(uintptr_t block, bool copy_ident_data, size_t partial_read_copyback, uintptr_t callback, uintptr_t callback_context);
        //      with void callback(uintptr_t callback_context, DnaSequenceFFI sequence, SequenceInfoFFI info);
        pub virtual_read_block: usize,
        // extern "C" size_t (*estimated_base_count)(uintptr_t block);
        pub virtual_estimated_base_count: usize,
        pub block_data: usize,
    }

    extern "Rust" {
        type GGCATInstanceFFI;

        /// Creates a new GGCATInstance. If an instance already exists, it will be returned, ignoring the new config.
        fn ggcat_create(config: GGCATConfigFFI) -> *const GGCATInstanceFFI;

        /// Builds a new graph from the given input files, with the specified parameters
        fn ggcat_build_from_files(
            instance: &'static GGCATInstanceFFI,
            // The input files
            input_files: &[String],

            // The output file
            output_file: String,

            // The names of the colors, ordered by color index
            color_names: &[String],

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

            // Extra elaboration step
            extra_elab: usize,

            // Output the result in GFA format
            gfa_output_version: u32,

            // Enables the disk optimization
            enable_disk_optimization: bool,
        ) -> String;

        /// Builds a new graph from the given input streams, with the specified parameters
        fn ggcat_build_from_streams(
            instance: &'static GGCATInstanceFFI,
            // The input streams
            input_streams: &[InputStreamFFI],

            // The output file
            output_file: String,

            // The names of the colors, ordered by color index
            color_names: &[String],

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

            // Extra elaboration step
            extra_elab: usize,

            // Output the result in GFA format
            gfa_output_version: u32,

            // Enables the disk optimization
            enable_disk_optimization: bool,
        ) -> String;

        /// Queries a (optionally) colored graph with a specific set of sequences as queries
        fn ggcat_query_graph(
            instance: &'static GGCATInstanceFFI,

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

            // Query output format
            color_output_format: usize,
        ) -> String;

        fn ggcat_get_colormap_file(graph_file: String) -> String;

        /// Returns a vector of color names in the given graph.
        /// The color indexes returned from the dump_unitigs function
        /// can be used to index this vector.
        pub fn ggcat_dump_colors(
            // The input colormap
            input_colormap: String,
        ) -> Vec<String>;

        /// Dumps the unitigs of the given graph, optionally with colors
        /// It's not guaranteed that maximal unitigs are returned, as only kmers with the same colors subset
        /// are returned as whole unitigs to speedup colormap reading times
        fn ggcat_dump_unitigs(
            instance: &'static GGCATInstanceFFI,
            // The input graph
            graph_input: String,
            // Specifies the k-mers length
            kmer_length: usize,
            // Overrides the default m-mers (minimizers) length
            minimizer_length: usize,
            // Enable colors
            colors: bool,
            // The threads to be used
            threads_count: usize,
            // Call the output function from a single thread at a time,
            // avoiding the need for synchronization in the user code
            single_thread_output_function: bool,

            output_function_context: usize,
            // extern "C" fn(context: usize, seq_ptr: usize, seq_len: usize, col_ptr: usize, col_len: usize, same_colors: bool),
            output_function_ptr: usize,
        );

        /// Queries specified color subsets of the colormap, returning
        /// the color indices corresponding to the colors of each subset
        fn ggcat_query_colormap(
            instance: &'static GGCATInstanceFFI,
            // The input colormap
            colormap: String,
            // The subsets to be queried
            subsets: Vec<u32>,
            // Call the output function from a single thread at a time,
            // avoiding the need for synchronization in the user code
            single_thread_output_function: bool,

            output_function_context: usize,
            output_function_ptr: usize,
        );
    }
}
