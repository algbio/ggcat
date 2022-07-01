use io::reads_writer::ReadsWriter;
use io::sequences_reader::SequencesReader;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct CmdRewriteArgs {
    /// The input files
    pub input: Vec<PathBuf>,

    /// The lists of input files
    #[structopt(short = "l", long = "input-lists")]
    pub input_lists: Vec<PathBuf>,

    #[structopt(short = "o", long = "output-path")]
    pub output_path: PathBuf,
}

pub fn cmd_rewrite(args: CmdRewriteArgs) {
    let mut files_list = Vec::new();

    files_list.extend(args.input.into_iter());

    for list in args.input_lists {
        let file_list = BufReader::new(File::open(list).unwrap());

        files_list.extend(file_list.lines().map(|l| PathBuf::from(l.unwrap())));
    }

    files_list.par_iter().for_each(|x| {
        let new_file_path = args.output_path.join(x.file_name().unwrap());

        let mut writer = ReadsWriter::new_compressed_gzip(new_file_path, 9);

        SequencesReader::process_file_extended(
            x,
            |f| {
                writer.add_read(f);
            },
            false,
        )
    });
}
