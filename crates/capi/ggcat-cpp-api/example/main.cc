#include <iostream>
#include <ggcat.hh>
#include <mutex>

using namespace ggcat;

int main(int argc, char const *argv[])
{
    GGCATConfig config;

    config.use_temp_dir = true;
    config.temp_dir = "/tmp",
    config.memory = 2.0,
    config.prefer_memory = true,
    config.total_threads_count = 16,
    config.intermediate_compression_level = -1,

    config.use_stats_file = false;
    config.stats_file = "";

    GGCATInstance *instance = GGCATInstance::create(config);

    std::vector<std::string> input_files = {
        "../../../../example-inputs/sal1.fa",
        "../../../../example-inputs/sal2.fa",
        "../../../../example-inputs/sal3.fa",
    };
    std::string graph_file = "/tmp/sal-dbg.fa";

    std::vector<std::string> color_names = {
        "sal1.fa",
        "sal2.fa",
        "sal3.fa",
    };

    size_t k = 31;
    size_t threads_count = 16;

    std::string output_file = instance->build_graph_from_files(
        Slice<std::string>(input_files.data(), input_files.size()),
        graph_file,
        k,
        threads_count,
        false,
        1,
        ExtraElaborationStep_UnitigLinks,
        true,
        Slice<std::string>(color_names.data(), color_names.size()),
        -1);

    std::string input_query = "../../../../example-inputs/query.fa";

    std::string output_query = instance->query_graph(
        graph_file,
        input_query,
        "/tmp/query-results",
        k,
        threads_count,
        false,
        true,
        ColoredQueryOutputFormat_JsonLinesWithNames);

    std::cout << "Output query file: " << output_query << std::endl;

    std::mutex print_kmer_lock;

    auto file_color_names =
        GGCATInstance::dump_colors(GGCATInstance::get_colormap_file(graph_file));

    instance->dump_unitigs(
        graph_file,
        k,
        threads_count,
        // WARNING: this function is called asynchronously from multiple threads, so it must be thread-safe.
        // Also the same_colors boolean is referred to the previous call of this function from the current thread
        [&](Slice<char> read, Slice<uint32_t> colors, bool same_colors)
        {
            std::lock_guard<std::mutex> _lock(print_kmer_lock);
            if (read.size < 100)
            {
                std::cout << "Dump unitig '";
                std::cout.write(read.data, read.size);
                std::cout << "'" << std::endl;
            }
            else
            {
                std::cout << "Dump unitig '";
                std::cout.write(read.data, 100);
                std::cout << "...'" << std::endl;
            }

            std::cout << "\t colors: [";

            for (size_t i = 0; i < colors.size; i++)
            {
                if (i > 0)
                {
                    std::cout << ", ";
                }
                std::cout << '"' << file_color_names[colors.data[i]] << '"';
            }

            std::cout << "] same_colors: " << same_colors << std::endl;
        },
        true);

    return 0;
}
