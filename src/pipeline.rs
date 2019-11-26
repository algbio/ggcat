use crate::reads_freezer::ReadsFreezer;
use crate::gzip_fasta_reader::GzipFastaReader;
use std::thread;
use std::io::Read;
use std::sync::Arc;
use crate::progress::Progress;

pub struct Pipeline;

impl Pipeline {

    pub fn file_freezers_to_reads(files: &[String]) -> ReadsFreezer {
        let files_ref = Vec::from(files);
        ReadsFreezer::from_generator(|writer| {
            for file in files_ref {
                println!("Reading {}", file);
                let freezer = ReadsFreezer::from_file(file);
                writer.pipe_freezer(freezer);
            }
        })
    }


    pub fn fasta_gzip_to_reads(files: &[String]) -> ReadsFreezer {
        let files_ref = Vec::from(files);
        ReadsFreezer::from_generator(|writer| {
            for file in files_ref {
                println!("Reading {}", file);
                GzipFastaReader::process_file(file, |read| {
                    writer.add_read(read);
                })
            }
        })
    }

    pub fn cut_n(freezer: ReadsFreezer, k: usize) -> ReadsFreezer {
        ReadsFreezer::from_generator(move |writer| {

            let mut progress = Progress::new();

            freezer.for_each(|read| {
                for record in read.split(|x| *x == b'N') {
                    if record.len() < k {
                        continue;
                    }
                    writer.add_read(record);
                    progress.incr(read.len() as u64);
                    progress.event(|a, c| c >= 10000000,
                                   |a, c, r| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
                }
            })
        })
    }
}

