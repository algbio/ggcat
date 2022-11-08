pub use ffi::GGCATConfig;

pub fn initialize(config: GGCATConfig) {}

pub fn build_graph(
    // The input files
    input: Vec<String>,
    // Enable colors
    colors: bool,

    // Minimum multiplicity required to keep a kmer
    min_multiplicity: usize,

    // The output file
    output_file: String,

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
    // Enable colors
    colors: bool,
    output_file_prefix: String,
) {
}

fn get_unitigs_stream() {}

#[cxx::bridge]
mod ffi {

    pub struct GGCATConfig {
        /// Specifies the k-mers length
        klen: usize,
        /// Directory for temporary files
        temp_dir: String,
        /// The threads to be used
        threads_count: usize,
        /// Treats reverse complementary kmers as different
        forward_only: bool,
        /// Overrides the default m-mers (minimizers) length
        mlen: usize,
        /// Maximum memory usage (GB)
        memory: f64,
        /// Use all the given memory before writing to disk
        prefer_memory: bool,
    }

    extern "Rust" {
        // Zero or more opaque types which both languages can pass around but
        // only Rust can see the fields.
        // Functions implemented in Rust.
        // fn print_test(buf: &[u8]) -> &[u8];
    }
}
