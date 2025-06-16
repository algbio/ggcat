use byteorder::ReadBytesExt;
use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::SingleKmerColorDataType;
use config::{
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE, SwapPriority, get_compression_level_info,
    get_memory_mode,
};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataOwned,
};
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::readers::typed_binary_reader::TypedStreamReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{BucketsCount, ExtraBuckets, MultiThreadBuckets, SingleBucket};
use parallel_processor::fast_smart_bucket_sort::{SortKey, fast_smart_radix_sort};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::Read;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct CounterEntry<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()>> {
    pub query_index: u64,
    pub counter: u64,
    pub _phantom: PhantomData<CX>,
}

pub struct CounterEntrySerializer<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()>>(
    CX::LastData,
);

impl<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()>> BucketItemSerializer
    for CounterEntrySerializer<CX>
{
    type InputElementType<'a> = CounterEntry<CX>;
    type ExtraData = CX;
    type ExtraDataBuffer = ();
    type ReadBuffer = ();
    type ReadType<'a> = (CounterEntry<CX>, CX);
    type InitData = ();

    type CheckpointData = ();

    #[inline(always)]
    fn new(_: ()) -> Self {
        Self(Default::default())
    }

    #[inline(always)]
    fn reset(&mut self) {
        self.0 = Default::default();
    }

    #[inline(always)]
    fn write_to(
        &mut self,
        element: &Self::InputElementType<'_>,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        _: &Self::ExtraDataBuffer,
    ) {
        encode_varint(|b| bucket.extend_from_slice(b), element.query_index);
        encode_varint(|b| bucket.extend_from_slice(b), element.counter);
        extra_data.encode(bucket, self.0);
        self.0 = extra_data.obtain_last_data(self.0);
    }

    fn read_from<'a, S: Read>(
        &mut self,
        mut stream: S,
        _read_buffer: &'a mut Self::ReadBuffer,
        _: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>> {
        let query_index = decode_varint(|| stream.read_u8().ok())?;
        let counter = decode_varint(|| stream.read_u8().ok())?;
        let color = CX::decode(&mut stream, self.0)?;
        self.0 = color.obtain_last_data(self.0);
        Some((
            CounterEntry {
                query_index,
                counter,
                _phantom: PhantomData,
            },
            color,
        ))
    }

    #[inline(always)]
    fn get_size(&self, _: &Self::InputElementType<'_>, data: &Self::ExtraData) -> usize {
        VARINT_MAX_SIZE * 2 + data.max_size()
    }
}

pub fn counters_sorting<CX: ColorsManager>(
    _k: usize,
    file_counters_inputs: Vec<SingleBucket>,
    colored_buckets_path: PathBuf,
    colors_count: u64,
    output_file: PathBuf,
    query_kmers_count: &[u64],
) -> Vec<SingleBucket> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: counters sorting".to_string());

    let buckets_count =
        BucketsCount::from_power_of_two(file_counters_inputs.len(), ExtraBuckets::None);

    let final_counters = if CX::COLORS_ENABLED {
        vec![]
    } else {
        let mut counters = Vec::with_capacity(query_kmers_count.len());
        counters.extend((0..query_kmers_count.len()).map(|_| AtomicU64::new(0)));
        counters
    };

    let color_buckets = if CX::COLORS_ENABLED {
        Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            colored_buckets_path,
            None,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
                get_compression_level_info(),
            ),
            &(),
        ))
    } else {
        Arc::new(MultiThreadBuckets::EMPTY)
    };

    let thread_buffers = ScopedThreadLocal::new(move || {
        if CX::COLORS_ENABLED {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &buckets_count)
        } else {
            BucketsThreadBuffer::EMPTY
        }
    });

    file_counters_inputs.par_iter().for_each(|input| {
        let mut thread_buffer = thread_buffers.get();
        let mut colored_buckets_writer = BucketsThreadDispatcher::<
            _,
            CounterEntrySerializer<SingleKmerColorDataType<CX>>,
        >::new(&color_buckets, thread_buffer.take(), ());

        let mut counters_vec: Vec<(
            CounterEntry<SingleKmerColorDataType<CX>>,
            SingleKmerColorDataType<CX>,
        )> = Vec::new();
        let file_index = ChunkedBinaryReaderIndex::from_file(
            &input.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        );

        TypedStreamReader::get_items::<CounterEntrySerializer<SingleKmerColorDataType<CX>>>(
            None,
            (),
            file_index.into_chunks(),
            |h, _| {
                counters_vec.push(h);
            },
        );

        struct CountersCompare;
        impl<CX: SequenceExtraDataConsecutiveCompression<TempBuffer = ()>>
            SortKey<(CounterEntry<CX>, CX)> for CountersCompare
        {
            type KeyType = u64;
            const KEY_BITS: usize = std::mem::size_of::<u64>() * 8;

            fn compare(
                left: &(CounterEntry<CX>, CX),
                right: &(CounterEntry<CX>, CX),
            ) -> std::cmp::Ordering {
                left.0.query_index.cmp(&right.0.query_index)
            }

            fn get_shifted(value: &(CounterEntry<CX>, CX), rhs: u8) -> u8 {
                (value.0.query_index >> rhs) as u8
            }
        }

        fast_smart_radix_sort::<_, CountersCompare, false>(&mut counters_vec[..]);

        for query_results in counters_vec.nq_group_by_mut(|a, b| a.0.query_index == b.0.query_index)
        {
            query_results.sort_unstable_by(|x, y| x.1.cmp(&y.1));
            let query_index = query_results[0].0.query_index;

            if CX::COLORS_ENABLED {
                for entry in query_results.nq_group_by(|a, b| a.1 == b.1) {
                    let color = entry[0].1.clone();
                    colored_buckets_writer.add_element(
                        CX::get_bucket_from_color(
                            &color,
                            colors_count,
                            buckets_count.normal_buckets_count_log,
                        ),
                        &color,
                        &CounterEntry {
                            query_index,
                            counter: entry.iter().map(|e| e.0.counter).sum(),
                            _phantom: PhantomData,
                        },
                    );
                }
            } else {
                final_counters[query_index as usize - 1].store(
                    query_results.iter().map(|e| e.0.counter).sum(),
                    Ordering::Relaxed,
                );
            }
        }

        thread_buffer.put_back(colored_buckets_writer.finalize().0);
    });

    if !CX::COLORS_ENABLED {
        let output_file = if output_file.extension().is_none() {
            output_file.with_extension("csv")
        } else {
            output_file
        };

        let mut writer = csv::Writer::from_path(output_file).unwrap();
        writer
            .write_record(&[
                "query_index",
                "matched_kmers",
                "query_kmers",
                "match_percentage",
            ])
            .unwrap();

        for (query_index, (info, counter)) in query_kmers_count
            .iter()
            .zip(final_counters.iter())
            .enumerate()
        {
            writer
                .write_record(&[
                    query_index.to_string(),
                    counter.load(Ordering::Relaxed).to_string(),
                    info.to_string(),
                    format!(
                        "{:.2}",
                        (counter.load(Ordering::Relaxed) as f64 / *info as f64)
                    ),
                ])
                .unwrap();
        }
        vec![]
    } else {
        color_buckets.finalize_single()
    }
}
