#include <ggcat.hh>
#include <ggcat-cpp-bindings.hh>
#include <vector>

using namespace ggcat;

GGCATInstanceFFI const *ffi_instance = nullptr;
GGCATInstance ggcat::GGCATInstance::instance = GGCATInstance();

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


    return ggcat_build_from_files(*ffi_instance,
                                  rust::Slice<const rust::String>(ffi_input_files.data(), ffi_input_files.size()),
                                  rust::String(output_file.c_str()), 
                                  rust::Slice<const rust::String>(ffi_color_names.data(), ffi_color_names.size()), 
                                  kmer_length, 
                                  threads_count, 
                                  forward_only,
                                  minimizer_length, 
                                  colors, 
                                  min_multiplicity, 
                                  extra_elab).c_str();
}

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


    instance->build_graph_from_files(
        Slice<std::string>(input_files.data(), input_files.size()),
        graph_file,
        31,
        16,
        false,
        1,
        ExtraElaborationStep_UnitigLinks,
        true,
        Slice<std::string>(color_names.data(), color_names.size()),
        -1);

    return 0;
}
