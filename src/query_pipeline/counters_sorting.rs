use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::io::sequences_reader::SequencesReader;
use crate::io::varint::{decode_varint, encode_varint};
use crate::query_pipeline::QueryPipeline;
use crate::KEEP_FILES;
use byteorder::ReadBytesExt;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::multi_thread_buckets::BucketWriter;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::cell::UnsafeCell;
use std::io::{Cursor, Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct CounterEntry {
    pub query_index: u64,
    pub counter: u64,
}

impl SequenceExtraData for CounterEntry {
    fn decode<'a>(mut reader: &'a mut impl Read) -> Option<Self> {
        let query_index = decode_varint(|| reader.read_u8().ok())?;
        let counter = decode_varint(|| reader.read_u8().ok())?;
        Some(Self {
            query_index,
            counter,
        })
    }

    fn encode<'a>(&self, mut writer: &'a mut impl Write) {
        encode_varint(|b| writer.write_all(b).ok(), self.query_index);
        encode_varint(|b| writer.write_all(b).ok(), self.counter);
    }

    fn max_size(&self) -> usize {
        20
    }
}

impl BucketWriter for CounterEntry {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        self.encode(bucket);
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        self.max_size()
    }
}

impl QueryPipeline {
    pub fn counters_sorting(
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

        file_counters_inputs
            .par_iter()
            .enumerate()
            .for_each(|(index, input)| {
                let file = filebuffer::FileBuffer::open(input).unwrap();

                let mut reader = Cursor::new(file.deref());
                let mut vec: Vec<CounterEntry> = Vec::new();

                while let Some(value) = CounterEntry::decode(&mut reader) {
                    vec.push(value);
                }

                drop(file);
                MemoryFs::remove_file(&input, !KEEP_FILES.load(Ordering::Relaxed));

                struct Compare;
                impl SortKey<CounterEntry> for Compare {
                    type KeyType = u64;
                    const KEY_BITS: usize = size_of::<u64>() * 8;

                    #[inline(always)]
                    fn compare(left: &CounterEntry, right: &CounterEntry) -> std::cmp::Ordering {
                        left.query_index.cmp(&right.query_index)
                    }

                    #[inline(always)]
                    fn get_shifted(value: &CounterEntry, rhs: u8) -> u8 {
                        (value.query_index >> rhs) as u8
                    }
                }

                fast_smart_radix_sort::<_, Compare, false>(&mut vec[..]);

                for x in vec.group_by(|a, b| a.query_index == b.query_index) {
                    let query_index = x[0].query_index;
                    final_counters[query_index as usize - 1]
                        .store(x.iter().map(|e| e.counter).sum(), Ordering::Relaxed);
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
