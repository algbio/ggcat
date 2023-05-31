use crate::structs::query_colored_counters::{ColorsRange, QueryColoredCountersSerializer};
use crate::ColoredQueryOutputFormat;
use colors::colors_manager::ColorMapReader;
use colors::colors_manager::{ColorsManager, ColorsMergeManager};
use config::{
    get_compression_level_info, get_memory_mode, ColorIndexType, SwapPriority,
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, QUERIES_COUNT_MIN_BATCH,
};
use flate2::Compression;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::get_bucket_index;
use nightly_quirks::prelude::*;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Condvar, Mutex};
use rayon::prelude::*;
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

pub fn colored_query_output<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
>(
    colormap: &<CX::ColorsMergeManagerType<H, MH> as ColorsMergeManager<H, MH>>::GlobalColorsTableReader,
    mut colored_query_buckets: Vec<PathBuf>,
    output_file: PathBuf,
    temp_dir: PathBuf,
    query_kmers_count: &[u64],
    colored_query_output_format: ColoredQueryOutputFormat,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: colored query output".to_string());

    let buckets_count = colored_query_buckets.len();

    let max_bucket_queries_count = (((query_kmers_count.len() + 1) as u64)
        .nq_div_ceil(QUERIES_COUNT_MIN_BATCH)
        * QUERIES_COUNT_MIN_BATCH) as usize;

    static OPS_COUNT: AtomicUsize = AtomicUsize::new(0);
    static COL_COUNT: AtomicUsize = AtomicUsize::new(0);

    colored_query_buckets.reverse();
    let buckets_channel = Mutex::new(colored_query_buckets);

    let output_file = if output_file.extension().is_none() {
        output_file.with_extension("jsonl")
    } else {
        output_file
    };

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
            #[derive(Copy, Clone)]
            struct QueryColorListItem {
                color: ColorIndexType,
                count: u64,
                next_index: usize,
            }

            let mut queries_colors_list_pool = vec![];
            let mut queries_results =
                vec![(0u32 /* epoch */, 0usize /* list index */); max_bucket_queries_count];
            let mut temp_colors_list = vec![];

            let mut epoch = 0;

            while let Some(input) = {
                let mut lock = buckets_channel.lock();
                let element = lock.pop();
                drop(lock);
                element
            } {
                epoch += 1;
                queries_colors_list_pool.clear();

                let start_query_index =
                    get_bucket_index(&input) as usize * max_bucket_queries_count / buckets_count;

                CompressedBinaryReader::new(
                    &input,
                    RemoveFileMode::Remove {
                        remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                )
                .decode_all_bucket_items::<QueryColoredCountersSerializer, _>(
                    (Vec::new(), Vec::new()),
                    &mut (),
                    |counters, _| {
                        for query in counters.queries {
                            let (entry_epoch, colors_map_index) = &mut queries_results
                                [query.query_index as usize - start_query_index - 1];

                            if *entry_epoch != epoch {
                                *entry_epoch = epoch;
                                *colors_map_index = usize::MAX;
                            }

                            assert_eq!(counters.colors.len() % 2, 0);
                            for range in counters.colors.chunks(2) {
                                let ColorsRange::Range(range) = ColorsRange::from_slice(range);

                                OPS_COUNT.fetch_add(1, Ordering::Relaxed);
                                COL_COUNT.fetch_add(range.len(), Ordering::Relaxed);

                                for color in range {
                                    queries_colors_list_pool.push(QueryColorListItem {
                                        color,
                                        count: query.count,
                                        next_index: *colors_map_index,
                                    });
                                    *colors_map_index = queries_colors_list_pool.len() - 1;
                                }
                            }
                        }
                    },
                );

                let bucket_index = get_bucket_index(input);

                let compressed_stream = CompressedBinaryWriter::new(
                    &temp_dir.join("query-data"),
                    &(
                        get_memory_mode(SwapPriority::ColoredQueryBuckets),
                        CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                        get_compression_level_info(),
                    ),
                    bucket_index as usize,
                );

                let mut jsonline_buffer = vec![];
                for (query, mut query_colors_list_index) in
                    queries_results.iter().enumerate().filter_map(|(i, r)| {
                        if r.0 != epoch {
                            None
                        } else {
                            Some((i + start_query_index, r.1))
                        }
                    })
                {
                    jsonline_buffer.clear();
                    write!(
                        jsonline_buffer,
                        "{{\"query_index\":{}, \"matches\":{{",
                        query
                    )
                    .unwrap();

                    temp_colors_list.clear();
                    while query_colors_list_index != usize::MAX {
                        let el = &queries_colors_list_pool[query_colors_list_index];
                        temp_colors_list.push((el.color, el.count));
                        query_colors_list_index = el.next_index;
                    }
                    temp_colors_list.sort_unstable_by_key(|r| r.0);

                    for (i, qc) in temp_colors_list.nq_group_by(|a, b| a.0 == b.0).enumerate() {
                        let color_index = qc[0].0;
                        let color_presence = qc.iter().map(|x| x.1).sum::<u64>();

                        if i != 0 {
                            write!(jsonline_buffer, ",").unwrap();
                        }

                        match colored_query_output_format {
                            ColoredQueryOutputFormat::JsonLinesWithNumbers => {
                                write!(jsonline_buffer, "\"{}\"", color_index)
                            }
                            ColoredQueryOutputFormat::JsonLinesWithNames => {
                                write!(
                                    jsonline_buffer,
                                    "\"{}\"",
                                    colormap.get_color_name(color_index, true)
                                )
                            }
                        }
                        .unwrap();

                        write!(
                            jsonline_buffer,
                            ": {:.2}",
                            (color_presence as f64) / (query_kmers_count[query as usize] as f64)
                        )
                        .unwrap();
                    }
                    writeln!(jsonline_buffer, "}}}}").unwrap();
                    compressed_stream.write_data(&jsonline_buffer);
                }

                let stream_path = compressed_stream.get_path();
                compressed_stream.finalize();

                let mut decompress_stream = CompressedBinaryReader::new(
                    stream_path,
                    RemoveFileMode::Remove { remove_fs: true },
                    DEFAULT_PREFETCH_AMOUNT,
                );

                let mut queries_lock = query_output.lock();

                let (queries_file, query_write_index) = {
                    while queries_lock.1 != bucket_index {
                        output_sync_condvar.wait(&mut queries_lock);
                    }
                    queries_lock.deref_mut()
                };

                std::io::copy(&mut decompress_stream.get_single_stream(), queries_file).unwrap();

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
