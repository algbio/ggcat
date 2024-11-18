#include <ggcat.hh>
#include "ggcat-cpp-bindings.hh"
#include <vector>

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
        ffi_config.messages_callback = (size_t)config.messages_callback;

        ffi_instance = ggcat_create(ffi_config);
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
    size_t minimizer_length,
    bool gfa_output)
{
    std::vector<rust::String> ffi_input_files;

    for (size_t i = 0; i < input_files.size; i++)
    {
        ffi_input_files.push_back(rust::String(input_files.data[i].c_str()));
    }

    std::vector<rust::String> ffi_color_names;

    for (size_t i = 0; i < color_names.size; i++)
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
                                           extra_elab,
                                           gfa_output);
    return std::string(rust_str.c_str());
}

std::string GGCATInstance::build_graph_internal_ffi(
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
    bool output_gfa)
{
    std::vector<InputStreamFFI> ffi_input_streams;

    for (size_t i = 0; i < input_streams.size; i++)
    {
        InputStreamFFI ffi_input_stream;
        ffi_input_stream.block_data = input_streams.data[i].block_data;
        ffi_input_stream.virtual_read_block = (uintptr_t)input_streams.data[i].read_block;
        ffi_input_stream.virtual_estimated_base_count = (uintptr_t)input_streams.data[i].estimated_base_count;

        ffi_input_streams.push_back(ffi_input_stream);
    }

    std::vector<rust::String> ffi_color_names;

    for (size_t i = 0; i < color_names.size; i++)
    {
        ffi_color_names.push_back(rust::String(color_names.data[i].c_str()));
    }

    auto rust_str = ggcat_build_from_streams(*ffi_instance,
                                             rust::Slice<const InputStreamFFI>(ffi_input_streams.data(), ffi_input_streams.size()),
                                             rust::String(output_file.c_str()),
                                             rust::Slice<const rust::String>(ffi_color_names.data(), ffi_color_names.size()),
                                             kmer_length,
                                             threads_count,
                                             forward_only,
                                             minimizer_length,
                                             colors,
                                             min_multiplicity,
                                             extra_elab,
                                             output_gfa);
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
    bool single_thread_output_function,
    uintptr_t context,
    uintptr_t output_function)

{

    ggcat_dump_unitigs(*ffi_instance,
                       rust::String(graph_input.c_str()),
                       kmer_length,
                       minimizer_length,
                       colors,
                       threads_count,
                       single_thread_output_function,
                       context,
                       output_function);
}

void GGCATInstance::query_colormap_internal(
    std::string colormap_file,
    uintptr_t subsets_ptr,
    size_t subsets_len,
    bool single_thread_output_function,
    uintptr_t context,
    uintptr_t output_function)

{
    auto subsets_rust_vec = rust::Vec<uint32_t>();
    subsets_rust_vec.reserve(subsets_len);
    for (size_t i = 0; i < subsets_len; i++)
    {
        subsets_rust_vec.push_back(((uint32_t *)subsets_ptr)[i]);
    }

    ggcat_query_colormap(*ffi_instance,
                         rust::String(colormap_file.c_str()),
                         subsets_rust_vec,
                         single_thread_output_function,
                         context,
                         output_function);
}