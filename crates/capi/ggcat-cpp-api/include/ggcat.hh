#include <cstddef>
#include <string>
#include <vector>

namespace ggcat
{

    template <typename T>
    struct Slice
    {
        T *data;
        size_t size;
        static inline Slice<T> empty()
        {
            return Slice<T>{nullptr, 0};
        }

        Slice(T *data, size_t size) : data(data), size(size) {}
    };

    enum ExtraElaborationStep
    {
        ExtraElaborationStep_None = 0,
        /// Generate maximal unitigs connections references, in BCALM2 format L:<+/->:<other id>:<+/->
        ExtraElaborationStep_UnitigLinks = 1,
        /// Generate greedy matchtigs
        ExtraElaborationStep_GreedyMatchtigs = 2,
        /// Generate eulertigs
        ExtraElaborationStep_Eulertigs = 3,
        /// Generate pathtigs
        ExtraElaborationStep_Pathtigs = 4,
    };

    enum ColoredQueryOutputFormat
    {
        ColoredQueryOutputFormat_JsonLinesWithNumbers = 0,
        ColoredQueryOutputFormat_JsonLinesWithNames = 1,
    };

    // Main config of GGCAT. This config is global and should be passed to GGCATInstance::create
    struct GGCATConfig final
    {
        // If false, a memory only mode is attempted. May crash for large input data if there is no enough RAM memory.
        bool use_temp_dir;
        // Directory for temporary files
        std::string temp_dir;
        // Maximum suggested memory usage (GB)
        // The tool will try use only up to this GB of memory to store temporary files
        // without writing to disk. This usage does not include the needed memory for the processing steps.
        // GGCAT can allocate extra memory for files if the current memory is not enough to complete the current operation
        double memory;
        // Use all the given memory before writing to disk
        bool prefer_memory;
        // The total threads to be used
        std::size_t total_threads_count;
        // The default lz4 compression level for the intermediate files, -1 to use default values
        std::uint32_t intermediate_compression_level;
        // True if the stats file should be created
        bool use_stats_file;
        // The path to an optional json-formatted real time stats file
        std::string stats_file;
    };

    class GGCATInstance
    {

    private:
        static GGCATInstance instance;

        void dump_unitigs_internal(
            std::string graph_input,
            size_t kmer_length,
            size_t minimizer_length,
            bool colors,
            size_t threads_count,
            uintptr_t context,
            uintptr_t output_function);

        template <typename F>
        static void output_function_bridge(
            uintptr_t context,
            uintptr_t seq_ptr,
            size_t seq_len,
            uintptr_t col_ptr,
            size_t col_len, bool same_color)
        {
            F *output_function = reinterpret_cast<F *>(context);
            (*output_function)(
                Slice<char>((char *)seq_ptr, seq_len),
                Slice<uint32_t>((uint32_t *)col_ptr, col_len),
                same_color);
        }

    public:
        static GGCATInstance *create(GGCATConfig config);

        /// Builds a new graph from the given input files, with the specified parameters
        std::string build_graph_from_files(
            // The input files
            Slice<std::string> input_files,

            // The output file
            std::string output_file,

            // Specifies the k-mers length
            size_t kmer_length,

            // The threads to be used
            size_t threads_count,

            // Treats reverse complementary kmers as different
            bool forward_only = false,

            // Minimum multiplicity required to keep a kmer
            size_t min_multiplicity = 1,

            // Extra elaboration step
            ExtraElaborationStep extra_elab = ExtraElaborationStep_None,

            // Enable colors
            bool colors = false,

            // The names of the colors, ordered by color index
            Slice<std::string> color_names = Slice<std::string>::empty(),

            // Overrides the default m-mers (minimizers) length
            size_t minimizer_length = -1);

        /// Queries a (optionally) colored graph with a specific set of sequences as queries
        std::string query_graph(
            // The input graph
            std::string input_graph,
            // The input query as a .fasta file
            std::string input_query,

            // The output file
            std::string output_file_prefix,

            // Specifies the k-mers length
            size_t kmer_length,
            // The threads to be used
            size_t threads_count,
            // Treats reverse complementary kmers as different
            bool forward_only = false,
            // Enable colors
            bool colors = false,
            // Query output format
            size_t color_output_format = ColoredQueryOutputFormat_JsonLinesWithNumbers,
            // Overrides the default m-mers (minimizers) length
            size_t minimizer_length = -1);

        // Obtains the standard colormap file path from a graph file path
        static std::string get_colormap_file(std::string graph_file);

        /// Returns a vector of color names in the given graph.
        /// The color indexes returned from the dump_unitigs function
        /// can be used to index this vector.
        static std::vector<std::string> dump_colors(
            // The input colormap
            std::string input_colormap);

        /// Dumps the unitigs of the given graph, optionally with colors
        /// It's not guaranteed that maximal unitigs are returned, as only kmers with the same colors subset
        /// are returned as whole unitigs to speedup colormap reading times
        template <typename F>
        void dump_unitigs(
            std::string graph_input,
            // Specifies the k-mers length
            size_t kmer_length,

            // The threads to be used
            size_t threads_count,
            // The callback to be called for each unitig, arguments: (Slice<char> seq, Slice<uint32_t> colors, bool same_color)
            F output_function,
            // Enable colors
            bool colors = false,
            // Overrides the default m-mers (minimizers) length
            size_t minimizer_length = -1)
        {
            this->dump_unitigs_internal(graph_input,
                                        kmer_length,
                                        minimizer_length,
                                        colors,
                                        threads_count,
                                        (uintptr_t)&output_function, (uintptr_t)GGCATInstance::output_function_bridge<F>);
        }
    };
}