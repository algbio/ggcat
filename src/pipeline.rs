use crate::reads_freezer::ReadsFreezer;
use crate::gzip_fasta_reader::GzipFastaReader;
use std::thread;
use std::io::Read;
use crate::progress::Progress;
use std::path::Path;
use crate::utils::Utils;

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

    pub fn cut_n(freezer: &'static ReadsFreezer, k: usize) -> ReadsFreezer {
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

    pub fn make_buckets(freezer: &'static ReadsFreezer, k: usize, numbuckets: usize, base_name: &str) {
        let mut writers = vec![];

        for i in 0..numbuckets {
            let writer = ReadsFreezer::optifile_splitted(format!("{}{:03}", base_name, i));
            writers.push(writer);
        }

        Utils::thread_safespawn(move || {
            let mut progress = Progress::new();

            freezer.for_each(|read| {
                let mut hashes = nthash::NtHashIterator::new(read, k).unwrap();
                if let Some(minimum_hash) = (k..read.len()).map(|_| hashes.optim()).min() {
                    writers[minimum_hash as usize % numbuckets].add_read(read);
                }
                progress.incr(read.len() as u64);
                progress.event(|a, c| c >= 100000000,
                               |a, c, r| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
            })
        });
    }

    #[inline(always)]
    fn compute_chosen_bucket(read: &[u8], k: usize) -> Option<(u64, &[u8])> {
        let mut hashes = nthash::NtHashIterator::new(read, k).unwrap();

        const THRESHOLD_PERC: f64 = 1.0;
        const THRESHOLD_VALUE: u64 = (std::u64::MAX as f64 * THRESHOLD_PERC / 100.0) as u64;
//.filter(|v| v.0 < THRESHOLD_VALUE)
        let res = (k..read.len()).map(|idx| (hashes.optim(), idx)).min()?;
        Some((res.0, &read[res.1-k..res.1]))
    }

    pub fn save_minimals(freezer: &'static ReadsFreezer, k: usize) -> ReadsFreezer {
        ReadsFreezer::from_generator(move |writer| {
            let mut progress = Progress::new();

            freezer.for_each(|read| {
                if let Some(chosen) = Self::compute_chosen_bucket(read, k) {
                    writer.add_read(chosen.1);
                }
                progress.incr(read.len() as u64);
                progress.event(|a, c| c >= 100000000,
                               |a, c, r| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
            })
        })
    }
}

