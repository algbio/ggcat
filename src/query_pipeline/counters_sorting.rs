use crate::colors::colors_manager::color_types::SingleKmerColorDataType;
use crate::colors::colors_manager::ColorsManager;
use crate::config::DEFAULT_PREFETCH_AMOUNT;
use crate::io::concurrent::temp_reads::extra_data::{SequenceExtraData, SequenceExtraDataOwned};
use crate::io::sequences_reader::SequencesReader;
use crate::io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use crate::query_pipeline::QueryPipeline;
use crate::KEEP_FILES;
use byteorder::ReadBytesExt;
use parallel_processor::buckets::bucket_writer::BucketItem;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::Read;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct CounterEntry<CX: SequenceExtraData<TempBuffer = ()>> {
    pub query_index: u64,
    pub counter: u64,
    pub _phantom: PhantomData<CX>,
}

impl<CX: SequenceExtraData<TempBuffer = ()>> BucketItem for CounterEntry<CX> {
    type ExtraData = CX;
    type ExtraDataBuffer = ();
    type ReadBuffer = ();
    type ReadType<'a> = (Self, CX);

    #[inline(always)]
    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        _: &Self::ExtraDataBuffer,
    ) {
        encode_varint(|b| bucket.extend_from_slice(b), self.query_index);
        encode_varint(|b| bucket.extend_from_slice(b), self.counter);
        extra_data.encode(bucket);
    }

    fn read_from<'a, S: Read>(
        mut stream: S,
        _read_buffer: &'a mut Self::ReadBuffer,
        _: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>> {
        let query_index = decode_varint(|| stream.read_u8().ok())?;
        let counter = decode_varint(|| stream.read_u8().ok())?;
        let color = CX::decode(&mut stream)?;
        Some((
            Self {
                query_index,
                counter,
                _phantom: PhantomData,
            },
            color,
        ))
    }

    #[inline(always)]
    fn get_size(&self, data: &Self::ExtraData) -> usize {
        VARINT_MAX_SIZE * 2 + data.max_size()
    }
}

impl QueryPipeline {
    pub fn counters_sorting<CX: ColorsManager>(
        k: usize,
        query_input: PathBuf,
        file_counters_inputs: Vec<PathBuf>,
        output_file: PathBuf,
    ) {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: counters sorting".to_string());

        let mut sequences_info = vec![];

        SequencesReader::process_file_extended(
            query_input,
            |seq| {
                sequences_info.push((seq.seq.len() - k + 1) as u64);
            },
            false,
        );

        let mut final_counters = Vec::with_capacity(sequences_info.len());
        final_counters.extend((0..sequences_info.len()).map(|_| AtomicU64::new(0)));

        file_counters_inputs.par_iter().for_each(|input| {
            let mut counters_vec: Vec<(
                CounterEntry<SingleKmerColorDataType<CX>>,
                SingleKmerColorDataType<CX>,
            )> = Vec::new();
            LockFreeBinaryReader::new(
                input,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            )
            .decode_all_bucket_items::<CounterEntry<SingleKmerColorDataType<CX>>, _>(
                (),
                &mut (),
                |h, _| {
                    counters_vec.push(h);
                },
            );

            struct CountersCompare;
            impl<CX: SequenceExtraData<TempBuffer=()>> SortKey<(CounterEntry<CX>, CX)> for CountersCompare {
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

            for x in counters_vec.group_by_mut(|a, b| a.0.query_index == b.0.query_index) {
                x.sort_unstable_by(|x, y| x.1.cmp(&y.1));
                let query_index = x[0].0.query_index;

                // TODO: Save colors here
                final_counters[query_index as usize - 1]
                    .store(x.iter().map(|e| e.0.counter).sum(), Ordering::Relaxed);
            }
        });

        let mut writer = csv::Writer::from_path(output_file).unwrap();

        for (info, counter) in sequences_info.iter().zip(final_counters.iter()) {
            writer
                .write_record(&[
                    info.to_string(),
                    counter.load(Ordering::Relaxed).to_string(),
                ])
                .unwrap();
        }
    }
}
