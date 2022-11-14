use std::path::PathBuf;

use ggcat_api::{ExtraElaboration, GGCATConfig, GGCATInstance, GeneralSequenceBlockData};

fn main() {
    let instance = GGCATInstance::create(GGCATConfig {
        temp_dir: Some(PathBuf::from("/tmp")),
        memory: 2.0,
        prefer_memory: true,
        total_threads_count: 16,
        intermediate_compression_level: None,
        stats_file: None,
    });

    instance.build_graph(
        vec![
            GeneralSequenceBlockData::FASTA(PathBuf::from("input/sal1.fa")),
            GeneralSequenceBlockData::FASTA(PathBuf::from("input/sal2.fa")),
            GeneralSequenceBlockData::FASTA(PathBuf::from("input/sal3.fa")),
        ],
        PathBuf::from("/tmp/sal-dbg.fa"),
        Some(vec![
            "sal1".to_string(),
            "sal2".to_string(),
            "sal3".to_string(),
        ]),
        31,
        16,
        false,
        None,
        true,
        1,
        ExtraElaboration::UnitigLinks,
    );

    // instance.query_graph("/tmp/sal-dbg.fa", input_query, output_file_prefix, kmer_length, threads_count, forward_only, minimizer_length, colors, color_output_format)
}
