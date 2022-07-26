use crate::structs::query_colored_counters::{ColorsRange, QueryColoredCounters};
use colors::colors_manager::ColorsManager;
use config::{DEFAULT_PREFETCH_AMOUNT, KEEP_FILES};
use flate2::Compression;
use hashbrown::HashMap;
use io::get_bucket_index;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Condvar, Mutex};
use rayon::prelude::*;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

enum QueryOutputFileWriter {
    Plain(File),
    LZ4Compressed(lz4::Encoder<File>),
    GzipCompressed(flate2::write::GzEncoder<File>),
}

impl Write for QueryOutputFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            QueryOutputFileWriter::Plain(w) => w.write(buf),
            QueryOutputFileWriter::LZ4Compressed(w) => w.write(buf),
            QueryOutputFileWriter::GzipCompressed(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            QueryOutputFileWriter::Plain(w) => w.flush(),
            QueryOutputFileWriter::LZ4Compressed(w) => w.flush(),
            QueryOutputFileWriter::GzipCompressed(w) => w.flush(),
        }
    }
}

pub fn colored_query_output<CX: ColorsManager>(
    query_input: PathBuf,
    mut colored_query_buckets: Vec<PathBuf>,
    output_file: PathBuf,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: colored query output".to_string());

    let buckets_count = colored_query_buckets.len();

    // DONE: save ranges of queries divided per buckets along with ranges of colors
    // DONE/PARTIAL: Load color ranges in the last phase and perform sparse fenwick tree updates on queries
    // Write out the result iterating the fenwick contiguous slices one by one

    static OPS_COUNT: AtomicUsize = AtomicUsize::new(0);
    static COL_COUNT: AtomicUsize = AtomicUsize::new(0);

    colored_query_buckets.reverse();
    let buckets_channel = Mutex::new(colored_query_buckets);

    let query_output_file = File::create(&output_file).unwrap();

    let query_output = Mutex::new((
        BufWriter::new(
            match output_file.extension().map(|e| e.to_str()).flatten() {
                Some("lz4") => QueryOutputFileWriter::LZ4Compressed(
                    lz4::EncoderBuilder::new()
                        .level(4)
                        .build(query_output_file)
                        .unwrap(),
                ),
                Some("gz") => QueryOutputFileWriter::GzipCompressed(
                    flate2::GzBuilder::new().write(query_output_file, Compression::default()),
                ),
                _ => QueryOutputFileWriter::Plain(query_output_file),
            },
        ),
        0,
    ));
    let output_sync_condvar = Condvar::new();

    (0..rayon::current_num_threads())
        .into_par_iter()
        .for_each(|_| {
            while let Some(input) = buckets_channel.lock().pop() {
                // TODO: Replace hashmap with vec
                let mut queries_results = HashMap::new();

                CompressedBinaryReader::new(
                    &input,
                    RemoveFileMode::Remove {
                        remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                )
                .decode_all_bucket_items::<QueryColoredCounters, _>(
                    (Vec::new(), Vec::new()),
                    &mut (),
                    |counters, _| {
                        for query in counters.queries {
                            let colors_map = queries_results
                                .entry(query.query_index)
                                .or_insert_with(|| HashMap::new());

                            assert_eq!(counters.colors.len() % 2, 0);
                            for range in counters.colors.chunks(2) {
                                let ColorsRange::Range(range) = ColorsRange::from_slice(range);

                                OPS_COUNT.fetch_add(1, Ordering::Relaxed);
                                COL_COUNT.fetch_add(range.len(), Ordering::Relaxed);

                                for color in range {
                                    *colors_map.entry(color).or_insert(0) += query.count;
                                }
                            }
                        }
                    },
                );

                let bucket_index = get_bucket_index(input);

                let mut results = queries_results.into_iter().collect::<Vec<_>>();
                results.sort_unstable_by_key(|r| r.0);

                let mut queries_lock = query_output.lock();

                let (queries_file, query_write_index) = {
                    while queries_lock.1 != bucket_index {
                        output_sync_condvar.wait(&mut queries_lock);
                    }
                    queries_lock.deref_mut()
                };

                for (query, result) in results {
                    write!(queries_file, "{}: ", query).unwrap();
                    let mut query_result = result.into_iter().collect::<Vec<_>>();
                    query_result.sort_unstable_by_key(|r| r.0);

                    for (i, q) in query_result.into_iter().enumerate() {
                        if i != 0 {
                            write!(queries_file, ", ").unwrap();
                        }
                        write!(queries_file, "{}[{}]", q.0, q.1).unwrap();
                    }
                    writeln!(queries_file).unwrap();
                }

                *query_write_index += 1;
                output_sync_condvar.notify_all();
            }
        });

    println!(
        "Operations count: {} vs real {}",
        OPS_COUNT.load(Ordering::Relaxed),
        COL_COUNT.load(Ordering::Relaxed)
    );
}
