use crate::reads_freezer::ReadsFreezer;
use crate::gzip_fasta_reader::GzipFastaReader;
use std::thread;
use std::io::Read;
use crate::progress::Progress;
use std::path::Path;
use crate::utils::Utils;
use rand::seq::IteratorRandom;
use rand::prelude::{StdRng, ThreadRng};
use rand::RngCore;
use std::collections::HashMap;
use std::ops::Range;
use crate::bloom_filter::BloomFilter;
use crate::bloom_processing::TOTAL_MEM_EXP_FIRST;
use itertools::Itertools;
use std::cmp::Ordering;

pub struct Pipeline;

pub const MINIMIZER_THRESHOLD_PERC: f64 = 1.0;
pub const MINIMIZER_THRESHOLD_VALUE: u64 = (std::u64::MAX as f64 * MINIMIZER_THRESHOLD_PERC / 100.0) as u64;

#[derive(Debug, PartialEq, Eq)]
struct ReadDefinition {
    slice: Range<usize>
}

impl Ord for ReadDefinition {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.slice.start, self.slice.end).cmp(&(other.slice.start, other.slice.end))
    }
}

impl PartialOrd for ReadDefinition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((self.slice.start, self.slice.end).cmp(&(other.slice.start, other.slice.end)))
    }
}


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KmerPosition {
    read: ReadDefinition,
    sub_position: usize,
    k: usize
}


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
                               |a, c, r, _| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
            })
        })
    }

    pub fn bloom_filter(freezer: &'static ReadsFreezer, k: usize, decimate: bool) -> BloomFilter {
        crate::bloom_processing::bloom(freezer, k, decimate, true)
    }

    pub fn bloom_check_coverage(freezer: ReadsFreezer, filter: &mut BloomFilter) {
        let mut progress = Progress::new();
        let mut total = 0usize;
        let mut part_total = 0usize;
        let mut collisions = 0usize;
        let mut unique_values = 0usize;

        let k = filter.k;
        freezer.for_each(|read| {
            let mut hashes = nthash::NtHashIterator::new(read, k).unwrap();
            for (hash, idx) in hashes.iter_enumerate() {
                let address = (hash as usize) % (1 << TOTAL_MEM_EXP_FIRST);
                if filter.increment_cell(address) {
                    collisions += 1;
                }
                else {
                    unique_values += 1;
                }
                part_total += 1;
            }
            progress.event(|t, p| p >= 1000000, |t, p, r, e| {
                total += part_total;
                println!("Rate {:.1}M bases/sec  records: {} rate: {}/{} [U:{}] => {:.2}% | {:.2}%",
                         part_total as f64 / e / 1000000.0,
                         t,
                         collisions,
                         total,
                         unique_values,
                         unique_values as f64 / total as f64 * 100.0,
                         unique_values as f64 / ((1usize << TOTAL_MEM_EXP_FIRST) as f64) * 100.0);
                part_total = 0;
            });
        })

    }

    #[inline(always)]
    fn compute_chosen_hashes<'a>(read: &'a[u8], k: usize, nbuckets: usize) -> Option<impl Iterator<Item=(usize, usize, u64)> + 'a> {
        if read.len() < k {
            return None;
        }

        let mut hashes = nthash::NtHashIterator::new(read, k).unwrap();
        let res = hashes.iter_enumerate()
            .filter(|v| v.0.rotate_left(32) < MINIMIZER_THRESHOLD_VALUE)
            .map(move |bucket| ((bucket.0 as usize) % nbuckets, bucket.1, bucket.0));
        Some(res)

    }

    #[inline(always)]
    fn compute_chosen_bucket(read: &[u8], k: usize, nbuckets: usize) -> Option<(usize, usize, u64)> {
        Self::compute_chosen_hashes(read, k, nbuckets)?.min()
//        Some((ThreadRng::default().next_u32() as usize % nbuckets, &read[0..k]))
    }

    #[inline(always)]
    fn compute_hash_from_bucket(read: &[u8], k: usize, nbuckets: usize, bucket: usize) -> Option<(usize, usize, u64)> {
        Self::compute_chosen_hashes(read, k, nbuckets)?.filter(|val| val.0 == bucket).min()
    }

    fn merge_reads<'a, I: Iterator<Item=(&'a[u8], usize)> + Clone>(reads: I, k: usize) -> Option<Vec<u8>> {
        let buffer_min_neg = reads.clone().map(|(_, pos)| pos).max().unwrap();
        let buffer_max = reads.clone().map(|(r, pos)| r.len() - pos).max().unwrap();

        let mut buffer = vec![0u8; buffer_max + buffer_min_neg];

        let mut count = 0usize;
        for (read, offset) in reads {
            for (idx, base) in read.iter().enumerate() {
                let pos = idx + buffer_min_neg - offset;
                if buffer[pos] == 0 {
                    buffer[pos] = *base;
                }
                else if buffer[pos] != *base {
                    buffer[pos] = b'N';
//                    panic!("Error!");
                }
            }
            count += 1;
        }

//        if buffer.iter().filter(|b| **b == b'N').count() > buffer.len() / 10 {
//            None
//        }
//        else {
            Some(buffer)
//        }
    }

    pub fn merge_bucket(freezer: &'static ReadsFreezer, k: usize, nbuckets: usize, bucket: usize) -> ReadsFreezer{

        ReadsFreezer::from_generator(move |writer| {
            let mut hash_map: HashMap<_, Vec<KmerPosition>> = HashMap::new();

            let mut vector_buffer = Vec::with_capacity(1024 * 1024 * 256);

            freezer.for_each(|read| {
                if let Some((_, kmer_pos, hash)) = Self::compute_chosen_bucket(read, k, nbuckets) {
                    let read_start = vector_buffer.len();

//                let kmer = &read[kmer_pos..kmer_pos];

                    vector_buffer.extend_from_slice(read);

                    let kmer_desc = KmerPosition {
                        read: ReadDefinition {
                            slice: read_start..read_start + read.len()
                        },
                        sub_position: kmer_pos,
                        k
                    };

                    if let Some(vec) = hash_map.get_mut(&hash) {
                        vec.push(kmer_desc);
                    } else {
                        hash_map.insert(hash, vec![kmer_desc]);
                    }
                }
            });

            for (kmer_hash, list) in hash_map.iter() { //.sorted() {
                if list.len() > 1 {
                    if let Some(result) = Self::merge_reads(list.iter().map(|kpos| {
                        let read_slice = &vector_buffer[kpos.read.slice.clone()];
                        (read_slice, kpos.sub_position)
                    }), k) {
                        writer.add_read(result.as_slice());
                        continue;
                    }
                }
                for element in list {
                    writer.add_read(&vector_buffer[element.read.slice.clone()]);
                }
            }
            println!("Merge completed!");
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
                if let Some(chosen) = Self::compute_chosen_bucket(read, k, numbuckets) {
                    writers[chosen.0].add_read(read);
                }
                progress.incr(read.len() as u64);
                progress.event(|a, c| c >= 100000000,
                               |a, c, r, _| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
            })
        });
    }

    pub fn save_minimals(freezer: &'static ReadsFreezer, k: usize) -> ReadsFreezer {
        ReadsFreezer::from_generator(move |writer| {
            let mut progress = Progress::new();

            freezer.for_each(|read| {
                if let Some((_, position, _)) = Self::compute_chosen_bucket(read, k, 256) {
                    writer.add_read(&read[position..position+k]);
                }
                progress.incr(read.len() as u64);
                progress.event(|a, c| c >= 100000000,
                               |a, c, r, _| println!("Read {} rate: {:.1}M/s", a, r / 1024.0 / 1024.0))
            })
        })
    }
}

