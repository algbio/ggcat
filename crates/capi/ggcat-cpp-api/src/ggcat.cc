#include <ggcat.hh>
#include <ggcat-cpp-bindings.hh>
#include <vector>
#include <mutex>

using namespace ggcat;

GGCATInstanceFFI const *ffi_instance = nullptr;
GGCATInstance ggcat::GGCATInstance::instance = GGCATInstance();

static_assert(sizeof(size_t) == sizeof(uintptr_t), "size_t and uintptr_t must be the same size");

GGCATInstance *GGCATInstance::create(GGCATConfig config)
{
    if (ffi_instance == nullptr)
    {

        GGCATConfigFFI ffi_config;

        ffi_config.use_temp_dir = config.use_temp_dir,
        ffi_config.temp_dir = rust::String(config.temp_dir.c_str()),
        ffi_config.memory = config.memory,
        ffi_config.prefer_memory = config.prefer_memory,
        ffi_config.total_threads_count = config.total_threads_count,
        ffi_config.intermediate_compression_level = config.intermediate_compression_level,
        ffi_config.use_stats_file = config.use_stats_file,
        ffi_config.stats_file = rust::String(config.stats_file.c_str()),

        ffi_instance = &ggcat_create(ffi_config);
    }
    return &instance;
}

std::string GGCATInstance::build_graph_from_files(
    Slice<std::string> input_files,
    std::string output_file,
    size_t kmer_length,
    size_t threads_count,
    bool forward_only,
    size_t min_multiplicity,
    ExtraElaborationStep extra_elab,
    bool colors,
    Slice<std::string> color_names,
    size_t minimizer_length)
{
    std::vector<rust::String> ffi_input_files;

    for (int i = 0; i < input_files.size; i++)
    {
        ffi_input_files.push_back(rust::String(input_files.data[i].c_str()));
    }

    std::vector<rust::String> ffi_color_names;

    for (int i = 0; i < color_names.size; i++)
    {
        ffi_color_names.push_back(rust::String(color_names.data[i].c_str()));
    }

    auto rust_str = ggcat_build_from_files(*ffi_instance,
                                           rust::Slice<const rust::String>(ffi_input_files.data(), ffi_input_files.size()),
                                           rust::String(output_file.c_str()),
                                           rust::Slice<const rust::String>(ffi_color_names.data(), ffi_color_names.size()),
                                           kmer_length,
                                           threads_count,
                                           forward_only,
                                           minimizer_length,
                                           colors,
                                           min_multiplicity,
                                           extra_elab);
    return std::string(rust_str.c_str());
}

std::string GGCATInstance::query_graph(
    std::string input_graph,
    std::string input_query,
    std::string output_file_prefix,
    size_t kmer_length,
    size_t threads_count,
    bool forward_only,
    bool colors,
    size_t color_output_format,
    size_t minimizer_length)
{
    auto rust_str = ggcat_query_graph(*ffi_instance,
                                      rust::String(input_graph.c_str()),
                                      rust::String(input_query.c_str()),
                                      rust::String(output_file_prefix.c_str()),
                                      kmer_length,
                                      threads_count,
                                      forward_only,
                                      minimizer_length,
                                      colors,
                                      color_output_format);
    return std::string(rust_str.c_str());
}

std::string GGCATInstance::get_colormap_file(std::string graph_file)
{
    auto rust_str = ggcat_get_colormap_file(rust::String(graph_file.c_str()));
    return rust_str.c_str();
}

std::vector<std::string> GGCATInstance::dump_colors(
    std::string input_colormap)
{
    auto colors_vec = ggcat_dump_colors(rust::String(input_colormap.c_str()));

    std::vector<std::string> colors;

    for (auto &name : colors_vec)
    {
        colors.push_back(name.c_str());
    }

    return colors;
}

void GGCATInstance::dump_unitigs_internal(
    std::string graph_input,
    size_t kmer_length,
    size_t minimizer_length,
    bool colors,
    size_t threads_count,
    uintptr_t context,
    uintptr_t output_function)

{

    ggcat_dump_unitigs(*ffi_instance,
                       rust::String(graph_input.c_str()),
                       kmer_length,
                       minimizer_length,
                       colors,
                       threads_count,
                       context, output_function);
}

#include <iostream>
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
        "../../../example-inputs/sal1.fa",
        "../../../example-inputs/sal2.fa",
        "../../../example-inputs/sal3.fa",
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

    std::string input_query = "../../../example-inputs/query.fa";

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
