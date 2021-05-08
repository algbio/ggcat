use crate::binary_writer::{BinaryWriter, StorageMode};
use crate::compressed_read::{CompressedRead, H_INV_LETTERS, H_LOOKUP};
use crate::hash_entry::HashEntry;
use crate::intermediate_storage::IntermediateStorage;
use crate::multi_thread_buckets::{BucketsThreadDispatcher, MultiThreadBuckets};
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use crate::sequences_reader::FastaSequence;
use crate::smart_bucket_sort::{smart_radix_sort, SortKey};
use crate::unitig_link::Direction;
use nthash::{nt_manual_roll, nt_manual_roll_rev, NtHashIterator, NtSequence};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::cmp::min;
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufWriter, Write};
use std::ops::{Index, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

pub struct RetType {
    pub sequences: Vec<PathBuf>,
    pub hashes: Vec<PathBuf>,
}

impl Pipeline {
    pub fn kmers_merge(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        min_multiplicity: usize,
        out_directory: impl AsRef<Path> + std::marker::Sync,
        k: usize,
        m: usize,
    ) -> RetType {
        let start_time = Instant::now();
        static CURRENT_BUCKETS_COUNT: AtomicU64 = AtomicU64::new(0);

        const NONE: Option<Mutex<BufWriter<File>>> = None;
        let mut hashes_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(out_directory.as_ref().join("hashes"), StorageMode::Plain),
        );

        let sequences = Mutex::new(Vec::new());

        file_inputs.par_iter().for_each(|input| {
            const VEC: Vec<HashEntry> = Vec::new();
            const MAX_HASHES_FOR_FLUSH: usize = 1024 * 64;
            let mut hashes_tmp = BucketsThreadDispatcher::new(MAX_HASHES_FOR_FLUSH, &hashes_buckets);

            // FIXME: Embed in file!
            let file_name = input.file_name().unwrap().to_string_lossy().to_string();
            let bucket_string: Vec<u8> = (&file_name[0..file_name.rfind(".").unwrap()]).as_bytes().iter()
                .map(|x| *x)
                .filter(|x| (*x as char).is_digit(10)).collect();

            let bucket_index: u64 = String::from_utf8(bucket_string).unwrap().parse().unwrap();
            println!("Processing bucket {}...", bucket_index);


            let mut kmers_cnt = 0;
            let mut kmers_unique = 0;

            let mut writer = ReadsFreezer::optfile_splitted_compressed_lz4(format!("{}/result{}", out_directory.as_ref().display(), bucket_index));
            sequences.lock().unwrap().push(writer.get_path());

            let mut read_index = 0;

            const CVEC: Vec<u8> = Vec::new();
            const CREAD: Vec<(usize, usize, usize, u64)> = Vec::new();
            let mut buckets = [CVEC; 256];
            let mut cmp_reads = [CREAD; 256];

            IntermediateStorage::new_reader(input.clone()).for_each(|x| {

                let hashes = NtHashIterator::new(x.sub_slice(0..min(x.bases_count(), k)), m).unwrap();

                let (minpos, minimizer) = hashes.iter_enumerate().min_by_key(|(i, k)| *k).unwrap();

                let bucket = (minimizer >> 12) % 256;

                let slen = buckets[bucket as usize].len();
                buckets[bucket as usize].extend_from_slice(x.cmp_slice());

                let mut sort_key: u64 = minimizer;
                cmp_reads[bucket as usize].push((slen, x.bases_count(), minpos, sort_key))
            });

            let mut m5 = 0;

            for b in 0..256 {
                #[derive(Copy, Clone)]
                struct SimpleU64Hash {
                    value: u64
                }
                impl Hasher for SimpleU64Hash {
                    fn finish(&self) -> u64 {
                        self.value
                    }

                    fn write(&mut self, bytes: &[u8]) {
                        self.value = unsafe { *(bytes.as_ptr() as *const u64) };
                    }
                }
                impl BuildHasher for SimpleU64Hash {
                    type Hasher = Self;

                    fn build_hasher(&self) -> Self::Hasher {
                        *self
                    }
                }

                let mut rcorrect_reads: Vec<(u64, usize)> = Vec::new();
                let mut rhash_map = hashbrown::HashMap::with_capacity_and_hasher(4096, SimpleU64Hash { value: 0 });

                let mut backward_seq = Vec::new();
                let mut forward_seq = Vec::new();
                forward_seq.reserve(k);

                let mut idx_str: Vec<u8> = Vec::new();

                struct Compare {}
                impl SortKey<(usize, usize, usize, u64)> for Compare {
                    fn get(value: &(usize, usize, usize, u64)) -> u64 {
                        value.3
                    }
                }

                smart_radix_sort::<_, Compare, false>(&mut cmp_reads[b], 64 - 8);

                for slice in cmp_reads[b].group_by(|a, b| a.3 == b.3) {

                    rhash_map.clear();
                    rcorrect_reads.clear();

                    let mut tot_reads = 0;
                    let mut tot_chars = 0;

                    for (read_idx, count, pos, minimizer) in slice {
                        kmers_cnt += count - k + 1;

                        let read = CompressedRead::new(
                            &buckets[b][*read_idx..*read_idx + ((*count + 3) / 4)],
                            *count,
                        );

                        let hashes = NtHashIterator::new(read, k).unwrap();

                        for (idx, hash) in hashes.iter().enumerate() {
                            let entry = rhash_map.entry(hash).or_insert((read_idx * 4 + idx, 0));
                            entry.1 += 1;
                            if entry.1 == min_multiplicity {
                                rcorrect_reads.push((hash, entry.0));
                            }
                        }
                        tot_reads += 1;
                        tot_chars += read.bases_count();
                    }

                    for (hash, read_start) in rcorrect_reads.iter() {

                        let mut read = CompressedRead::from_compressed_reads(
                            &buckets[b][..],
                            *read_start,
                            k,
                        );

                        if rhash_map.remove_entry(hash).is_none() {
                            continue;
                        }

                        backward_seq.clear();
                        unsafe {
                            forward_seq.set_len(k);
                        }

                        read.write_to_slice(&mut forward_seq[..]);

                        let mut try_extend_function = |
                            output: &mut Vec<u8>,
                            compute_hash: fn(hash: u64, klen: usize, out_h: u64, in_h: u64) -> u64,
                            out_base_index: usize
                        | {
                            let mut start_index = (*hash, 0, 0);
                            loop {
                                let mut count = 0;
                                for idx in 0..4 {
                                    let new_hash = compute_hash(start_index.0, k, unsafe { read.get_h_unchecked(0) }, H_LOOKUP[idx]);
                                    if let Some(hash) = rhash_map.remove(&new_hash) {
                                        if hash.1 >= min_multiplicity {
                                            count += 1;
                                            start_index = (new_hash, idx, hash.0);
                                        }
                                    }
                                }
                                if count == 1 {
                                    read = CompressedRead::from_compressed_reads(
                                        &buckets[b][..],
                                        start_index.2,
                                        k,
                                    );
                                    output.push(H_INV_LETTERS[start_index.1]);
                                } else {
                                    break;
                                }
                            }
                            start_index.0
                        };

                        let mut fw_hash = try_extend_function(&mut forward_seq, nt_manual_roll, 0);
                        let mut bw_hash = try_extend_function(&mut backward_seq, nt_manual_roll_rev, k - 1);

                        let out_seq = if backward_seq.len() > 0 {
                            backward_seq.reverse();
                            backward_seq.extend_from_slice(&forward_seq[..]);
                            &backward_seq[..]
                        }
                        else {
                            &forward_seq[..]
                        };

                        fw_hash = nt_manual_roll(fw_hash, k, H_LOOKUP[((out_seq[out_seq.len() - k] >> 1) & 0x3) as usize], 0).rotate_right(1);
                        bw_hash = nt_manual_roll_rev(bw_hash, k, H_LOOKUP[((out_seq[k - 1] >> 1) & 0x3) as usize], 0);

                        idx_str.clear();
                        idx_str.write_fmt(format_args!("{}", read_index));

                        writer.add_read(FastaSequence {
                            ident: &idx_str[..],
                            seq: out_seq,
                            qual: None
                        });

                        let fw_hash_sr = HashEntry {
                            hash: fw_hash,
                            bucket: bucket_index as u32,
                            entry: read_index,
                            direction: Direction::Forward
                        };
                        let fw_bucket_index = fw_hash as usize % buckets_count;
                        hashes_tmp.add_element(fw_bucket_index, &(), fw_hash_sr);
                        // fw_hash_sr.serialize_to_file(&mut hashes_buckets[]);

                        let bw_hash_sr = HashEntry {
                            hash: bw_hash,
                            bucket: bucket_index as u32,
                            entry: read_index,
                            direction: Direction::Backward
                        };
                        let bw_bucket_index = bw_hash as usize % buckets_count;
                        hashes_tmp.add_element(bw_bucket_index, &(), bw_hash_sr);

                        read_index += 1;
                    }
                }
            }
            println!(
                "[{}/{}]Kmers {}, unique: {}, ratio: {:.2}% ~~ m5: {} ratio: {:.2}% [{:?}] Time: {:?}",
                CURRENT_BUCKETS_COUNT.fetch_add(1, Ordering::Relaxed) + 1,
                buckets_count,
                kmers_cnt,
                kmers_unique,
                (kmers_unique as f32) / (kmers_cnt as f32) * 100.0,
                m5,
                (m5 as f32) / (kmers_unique as f32) * 100.0,
                buckets.iter().map(|x| x.len()).sum::<usize>() / 256, // set
                start_time.elapsed()
            );
            writer.finalize();
        });

        RetType {
            sequences: vec![],
            hashes: hashes_buckets.finalize(),
        }
    }
}
