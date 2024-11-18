/* clang-format off */
#include <cstddef>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>

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

        Slice(T *data, size_t size) : // Avoid passing a null pointer to rust, as slices pointers are not allowed to be null
                                      data(data ? data : reinterpret_cast<T *>(UINTPTR_MAX)), size(size)
        {
        }

        T *begin() { return data; };
        T *end() { return data + size; };
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

    enum DnaSequencesFileType
    {
        DnaSequencesFileType_FASTA = 0,
        DnaSequencesFileType_FASTQ = 1,
        DnaSequencesFileType_GFA = 2,
        DnaSequencesFileType_BINARY = 3,
    };

    enum MessageLevel {
        MessageLevel_Info = 0,
        MessageLevel_Warning = 1,
        MessageLevel_Error = 2,
        MessageLevel_UnrecoverableError = 3
    };

    struct DnaSequence
    {
        Slice<char> ident_data;
        Slice<char> seq;
        DnaSequencesFileType format;
    };

    struct SequenceInfo
    {
        uint32_t color; // ColorIndexType
    };

    class StreamReader
    {
    public:
        virtual void read_block(
            void *block,
            bool copy_ident_data,
            size_t partial_read_copyback,
            void (*callback)(DnaSequence sequence, SequenceInfo info)) = 0;

        // Non virtual
        // void estimated_base_count(void *block);
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

        // The messages callback, if not null no info will be printed to stdout
        void (*messages_callback)(MessageLevel level, const char *message);
    };

    struct __InputStreamBlockData
    {
        void (*read_block)(
            uintptr_t block,
            bool copy_ident_data,
            size_t partial_read_copyback,
            void (*callback)(
                uintptr_t callback_context,
                DnaSequence sequence,
                SequenceInfo info),
            uintptr_t callback_context);

        uint64_t (*estimated_base_count)(uintptr_t block);
        uintptr_t block_data;
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
            bool single_thread_output_function,
            uintptr_t context,
            uintptr_t output_function);

        void query_colormap_internal(
            std::string colormap_file,
            uintptr_t subsets_ptr,
            size_t subsets_len,
            bool single_thread_output_function,
            uintptr_t context,
            uintptr_t output_function);

        template <typename F>
        static void unitigs_dump_output_function_bridge(
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

        template <typename F>
        static void colormap_query_output_function_bridge(
            uintptr_t context,
            uint32_t col_subset,
            uintptr_t col_ptr,
            size_t col_len)
        {
            F *output_function = reinterpret_cast<F *>(context);
            (*output_function)(
                col_subset,
                Slice<uint32_t>((uint32_t *)col_ptr, col_len)
            );
        }

        std::string build_graph_internal_ffi(
            Slice<__InputStreamBlockData> input_streams,
            std::string output_file,
            size_t kmer_length,
            size_t threads_count,
            bool forward_only,
            size_t min_multiplicity,
            ExtraElaborationStep extra_elab,
            bool colors,
            Slice<std::string> color_names,
            size_t minimizer_length,
            bool output_gfa);

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
            size_t minimizer_length = -1,
            
            // Output the result as a GFA file
            bool gfa_output = false);

        /// Builds a new graph from the given input streams, with the specified parameters
        template <typename S>
        std::string build_graph_from_streams(
            // The input streams
            Slice<void *> input_streams,

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
            size_t minimizer_length = -1,
            
            // Outputs the result as GFA
            bool output_gfa = false)
        {

            thread_local std::unique_ptr<StreamReader> stream_reader = nullptr;
            thread_local std::pair<void (*)(uintptr_t, DnaSequence, SequenceInfo), uintptr_t> callback_data;

            std::vector<__InputStreamBlockData> input_stream_blocks;

            for (size_t i = 0; i < input_streams.size; i++)
            {
                input_stream_blocks.push_back({[](uintptr_t block, bool copy_ident_data, size_t partial_read_copyback, void (*callback)(uintptr_t callback_context, DnaSequence sequence, SequenceInfo info), uintptr_t callback_context)
                                               {
                                                   callback_data.first = callback;
                                                   callback_data.second = callback_context;

                                                   if (stream_reader == nullptr)
                                                   {
                                                       stream_reader = std::unique_ptr<S>(new S());
                                                   }

                                                   StreamReader *local_stream_reader = stream_reader.get();

                                                   local_stream_reader->read_block((void *)block, copy_ident_data, partial_read_copyback, [](DnaSequence sequence, SequenceInfo info)
                                                                                   { callback_data.first(callback_data.second, sequence, info); });
                                               },
                                               [](uintptr_t block) -> uint64_t
                                               {
                                                   return S::estimated_base_count((void *)block);
                                               },
                                               (uintptr_t)input_streams.data[i]});
            }

            return build_graph_internal_ffi(Slice<__InputStreamBlockData>(input_stream_blocks.data(),
                                                                          input_stream_blocks.size()),
                                            output_file,
                                            kmer_length,
                                            threads_count,
                                            forward_only,
                                            min_multiplicity,
                                            extra_elab,
                                            colors,
                                            color_names,
                                            minimizer_length, 
                                            output_gfa);
        }

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

            // Call the output function from a single thread at a time,
            // avoiding the need for synchronization in the user code
            bool single_thread_output_function,

            // The callback to be called for each unitig, arguments: (Slice<char> seq, Slice<uint32_t> colors, bool same_color)
            F output_function,
            // Enable colors
            bool colors = false,
            // Overrides the default m-mers (minimizers) length
            size_t minimizer_length = -1)
        {
            auto bridge_ptr = GGCATInstance::unitigs_dump_output_function_bridge<F>;
            this->dump_unitigs_internal(graph_input,
                                        kmer_length,
                                        minimizer_length,
                                        colors,
                                        threads_count,
                                        single_thread_output_function,
                                        (uintptr_t)&output_function, reinterpret_cast<uintptr_t>(bridge_ptr));
        }

        /// Queries specified color subsets of the colormap, returning
        /// the color indices corresponding to the colors of each subset
        template <typename F>
        void query_colormap(
            std::string colormap_file,
            // Color subsets to be queried
            uint32_t *subsets,
            size_t subsets_count,
            // Call the output function from a single thread at a time,
            // avoiding the need for synchronization in the user code
            bool single_thread_output_function,

            // The callback to be called for each unitig, arguments: (Slice<char> seq, Slice<uint32_t> colors, bool same_color)
            F output_function
        )
        {
            auto bridge_ptr = GGCATInstance::colormap_query_output_function_bridge<F>;
            this->query_colormap_internal(colormap_file,
                                        (uintptr_t)subsets,
                                        subsets_count,
                                        single_thread_output_function,
                                        (uintptr_t)&output_function, reinterpret_cast<uintptr_t>(bridge_ptr));
        }
    };
}
/* clang-format on */