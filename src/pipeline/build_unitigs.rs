use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::hash::HashableSequence;
use crate::intermediate_storage::{
    IntermediateReadsReader, IntermediateReadsWriter, IntermediateSequencesStorage,
};
use crate::multi_thread_buckets::MultiThreadBuckets;
use crate::pipeline::links_compaction::LinkMapping;
use crate::pipeline::Pipeline;
use crate::reads_freezer::{FastaWriterConcurrentBuffer, ReadsFreezer};
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::unitig_link::{UnitigIndex, UnitigLink};
use crate::utils::Utils;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use nix::sys::ptrace::cont;
use object_pool::Pool;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::collections::HashMap;
use std::fs::File;
use std::intrinsics::unlikely;
use std::io::Cursor;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, Thread};
use std::time::{Duration, Instant};

impl Pipeline {
    pub fn build_unitigs(
        mut read_buckets_files: Vec<PathBuf>,
        mut unitig_map_files: Vec<PathBuf>,
        output_path: &Path,
        buckets_count: usize,
        k: usize,
        m: usize,
    ) -> PathBuf {
        let start_time = Instant::now();

        read_buckets_files.sort();
        unitig_map_files.sort();

        let inputs: Vec<_> = read_buckets_files
            .iter()
            .zip(unitig_map_files.iter())
            .collect();

        let mut final_unitigs_file = Mutex::new(ReadsFreezer::optfile_splitted_compressed_lz4(
            format!("{}", output_path.join("output-unitigs").display()),
        ));
        let output_file = final_unitigs_file.lock().unwrap().get_path();

        inputs.par_iter().for_each(|(read_file, unitigs_map_file)| {
            let mut tmp_final_unitigs_buffer =
                FastaWriterConcurrentBuffer::new(&final_unitigs_file, 1024 * 1024 * 8);

            assert_eq!(
                Utils::get_bucket_index(read_file),
                Utils::get_bucket_index(unitigs_map_file)
            );

            let bucket_index = Utils::get_bucket_index(read_file);

            let mappings_file = filebuffer::FileBuffer::open(unitigs_map_file).unwrap();
            let mut reader = Cursor::new(mappings_file.deref());

            let mut unitigs_hashmap = HashMap::new();
            let mut unitigs_tmp_vec = Vec::new();

            let mut counter: usize = 0;
            while let Some(link) = UnitigLink::read_from(&mut reader, &mut unitigs_tmp_vec) {
                let start_unitig = UnitigIndex::new(bucket_index, link.entry as usize);

                assert!(!unitigs_hashmap.contains_key(&start_unitig));
                unitigs_hashmap.insert(
                    start_unitig,
                    (counter, 1u8 | ((!link.flags.is_forward() as u8) << 1)),
                );

                counter += 1;

                for el in link.entries.get_slice(&unitigs_tmp_vec) {
                    if *el != start_unitig {
                        assert!(!unitigs_hashmap.contains_key(el));
                        unitigs_hashmap.insert(*el, (counter, 0u8));
                        counter += 1;
                    }
                }

                unitigs_tmp_vec.clear();
            }

            let mut final_sequences = Vec::with_capacity(counter);
            let mut temp_storage = Vec::new();
            final_sequences.resize(counter, None);

            let mut count = 0;
            IntermediateReadsReader::<UnitigIndex>::new(read_file).for_each(|index, seq| {
                if seq.to_string().as_str() == "TTTAAGGACAAGAAGATTTATCCCACCATTTCA" {
                    println!("FOUND {:?}!", index);
                }

                // if !unitigs_hashmap.contains_key(&index) {
                //     // println!(
                //     //     "Unitig: {:?} => {}",
                //     //     index,
                //     //     unitigs_hashmap.contains_key(&index)
                //     // );
                //     return;
                // }

                let &(findex, is_start) = unitigs_hashmap.get(&index).unwrap();
                final_sequences[findex] = Some((
                    CompressedReadIndipendent::from_read(&seq, &mut temp_storage),
                    is_start,
                ));
            });

            let mut temp_sequence = Vec::new();
            final_sequences.push(Some((
                CompressedRead::new_from_plain(&[], &mut temp_storage),
                1u8,
            )));

            if let Some(index) = final_sequences
                .iter()
                .enumerate()
                .filter(|x| x.1.is_none())
                .next()
                .map(|x| x.0)
            {
                println!("Index: {:?} ", index);
            }

            'uloop: for sequence in final_sequences.group_by(|a, b| b.unwrap().1 == 0) {
                let is_backwards = (sequence[0].unwrap().1 >> 1) != 0;

                temp_sequence.clear();

                let mut is_first = true;

                for upart in if is_backwards {
                    itertools::Either::Right(sequence.iter().rev())
                } else {
                    itertools::Either::Left(sequence.iter())
                } {
                    let (read, _) = upart.unwrap();

                    let compr_read = read.as_reference(&temp_storage);
                    if compr_read.bases_count() == 0 {
                        continue 'uloop;
                    }
                    if is_first {
                        temp_sequence.extend(compr_read.as_bases_iter());
                        is_first = false;
                    } else {
                        // let mut tmp = [0; 62];
                        // compr_read.sub_slice(0..62).write_to_slice(&mut tmp[..]);
                        // if &temp_sequence[temp_sequence.len() - 62..temp_sequence.len()] != &tmp[..]
                        // {
                        //     println!(
                        //         "ERROR: {} ==> {}",
                        //         std::str::from_utf8(temp_sequence.as_slice()).unwrap(),
                        //         compr_read.to_string()
                        //     )
                        // }
                        temp_sequence.extend(
                            compr_read
                                .sub_slice((k - 1)..compr_read.bases_count())
                                .as_bases_iter(),
                        );
                    }
                }

                tmp_final_unitigs_buffer.add_read(FastaSequence {
                    ident: b"SEQ",
                    seq: temp_sequence.as_slice(),
                    qual: None,
                });
            }

            tmp_final_unitigs_buffer.finalize();

            println!("Size: {}", unitigs_hashmap.len())
        });

        final_unitigs_file.into_inner().unwrap().finalize();
        output_file
    }
}
