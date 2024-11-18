use ggcat_api::{
    ColoredQueryOutputFormat, ExtraElaboration, GGCATConfig, GGCATInstance,
    GeneralSequenceBlockData,
};
use itertools::Itertools;
use std::{path::PathBuf, sync::Mutex};

fn main() {
    let instance = GGCATInstance::create(GGCATConfig {
        temp_dir: Some(PathBuf::from("/tmp")),
        memory: 2.0,
        prefer_memory: true,
        total_threads_count: 16,
        intermediate_compression_level: None,
        stats_file: None,
        messages_callback: Some(|lvl, msg| match lvl {
            ggcat_api::MessageLevel::Info => {
                println!("Info: {}", msg);
            }
            ggcat_api::MessageLevel::Warning => {
                println!("Warning: {}", msg);
            }
            ggcat_api::MessageLevel::Error => {
                println!("Error: {}", msg);
            }
            ggcat_api::MessageLevel::UnrecoverableError => {
                panic!("Unrecoverable error: {}", msg);
            }
        }),
        gfa_output: false,
    })
    .unwrap();

    let graph_file = PathBuf::from("/tmp/sal-dbg.fa");
    let k = 31;
    let threads_count = 16;

    // Example building of a colored graph from three FASTA files
    // building also bcalm2-style links across maximal unitigs
    let graph_file = instance
        .build_graph(
            vec![
                GeneralSequenceBlockData::FASTA((
                    PathBuf::from("../../../example-inputs/sal1.fa"),
                    None,
                )),
                GeneralSequenceBlockData::FASTA((
                    PathBuf::from("../../../example-inputs/sal2.fa"),
                    None,
                )),
                GeneralSequenceBlockData::FASTA((
                    PathBuf::from("../../../example-inputs/sal3.fa"),
                    None,
                )),
            ],
            graph_file.clone(),
            Some(&["sal1".to_string(), "sal2".to_string(), "sal3".to_string()]),
            k,
            threads_count,
            false,
            None,
            true,
            1,
            ExtraElaboration::UnitigLinks,
            false,
        )
        .unwrap();

    let input_query = PathBuf::from("../../../example-inputs/query.fa");

    let output_query = instance
        .query_graph(
            graph_file.clone(),
            input_query,
            PathBuf::from("/tmp/query-results"),
            k,
            threads_count,
            false,
            None,
            true,
            ColoredQueryOutputFormat::JsonLinesWithNames,
        )
        .unwrap();

    println!("Output query file: {:?}", output_query.display());

    let print_kmer_lock = Mutex::new(());

    let color_names: Vec<_> =
        GGCATInstance::dump_colors(GGCATInstance::get_colormap_file(&graph_file))
            .unwrap()
            .collect();

    instance
        .dump_unitigs(
            &graph_file,
            k,
            None,
            true,
            threads_count,
            false,
            // WARNING: this function is called asynchronously from multiple threads, so it must be thread-safe.
            // Also the same_colors boolean is referred to the previous call of this function from the current thread
            |read, colors, same_colors| {
                let _lock = print_kmer_lock.lock().unwrap();
                if read.len() < 100 {
                    println!("Dump unitig '{}'", std::str::from_utf8(read).unwrap());
                } else {
                    println!(
                        "Dump unitig '{}...'",
                        std::str::from_utf8(&read[..100]).unwrap()
                    );
                }
                println!(
                    "\t colors: {:?} same_colors: {}",
                    colors.iter().map(|c| &color_names[*c as usize]).format(" "),
                    same_colors
                );
            },
        )
        .unwrap();

    let colormap = GGCATInstance::get_colormap_file(&graph_file);
    instance
        .query_colormap(colormap, vec![0, 1, 2, 3, 4], true, |subset, colors| {
            print!("Subset: {} has colors:", subset);
            for color in colors {
                println!(" {}[{}]", color, color_names[*color as usize]);
            }
        })
        .unwrap()
}
