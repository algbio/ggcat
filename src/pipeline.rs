use crate::reads_freezer::ReadsFreezer;
use crate::gzip_fasta_reader::GzipFastaReader;
use std::thread;
use std::io::Read;
use crate::progress::Progress;
use std::path::Path;

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
        ReadsFreezer::from_generator(move |writer| {

            let mut computed_size = 0u64;
            let total_size: u64 = files_ref.iter().map(|file| Path::new(file).metadata().unwrap().len()).sum();

            for (idx, file) in files_ref.iter().enumerate() {
                println!("Reading {} [{}/{} => {:.2}%] SIZE: {:.2}/{:.2}GB => {:.2}%",
                         file,
                         idx,
                         files_ref.len(),
                         (idx as f64) / (files_ref.len() as f64) * 100.0,
                         computed_size as f64 / 1024.0 / 1024.0 / 1024.0,
                         total_size as f64 / 1024.0 / 1024.0 / 1024.0,
                         (computed_size as f64) / (total_size as f64) * 100.0);
                GzipFastaReader::process_file(file.clone(), |read| {
                    writer.add_read(read);
                });
                computed_size += Path::new(&file.to_string()).metadata().unwrap().len();
            }

            println!("Finished {} SIZE: {:.2} 100%",
                     files_ref.len(),
                     total_size as f64 / 1024.0 / 1024.0 / 1024.0);
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
                }
                progress.incr(read.len() as u64);
                progress.event(|a, c| c >= 100000000,
                               |a, c, r| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
            })
        })
    }
}

