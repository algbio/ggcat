use crate::assemble_pipeline::parallel_kmers_merge::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use crate::config::BucketIndexType;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::utils::owned_drop::OwnedDrop;
use parallel_processor::buckets::bucket_writer::BucketItem;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::PathBuf;

const USED_MARKER: usize = usize::MAX >> FLAGS_COUNT;
const FLAGS_COUNT: usize = 2;
const FLAGS_SHIFT: usize = size_of::<usize>() * 8 - FLAGS_COUNT;
const COUNTER_MASK: usize = (1 << FLAGS_SHIFT) - 1;

pub struct MapEntry<CHI> {
    count_flags: usize,
    pub color_index: CHI,
}

impl<CHI> MapEntry<CHI> {
    pub fn new(color_index: CHI) -> Self {
        Self {
            count_flags: 0,
            color_index,
        }
    }

    #[inline(always)]
    pub fn incr(&mut self) {
        self.count_flags += 1;
    }

    #[inline(always)]
    pub fn set_used(&mut self) {
        self.count_flags |= USED_MARKER;
    }

    #[inline(always)]
    pub fn is_used(&self) -> bool {
        (self.count_flags & USED_MARKER) == USED_MARKER
    }

    #[inline(always)]
    pub fn get_counter(&self) -> usize {
        self.count_flags & COUNTER_MASK
    }

    #[inline(always)]
    pub fn update_flags(&mut self, flags: u8) {
        self.count_flags |= (flags as usize) << FLAGS_SHIFT;
    }

    #[inline(always)]
    pub fn get_flags(&self) -> u8 {
        (self.count_flags >> FLAGS_SHIFT) as u8
    }

    pub(crate) fn get_kmer_multiplicity(&self) -> usize {
        // If the current set has both the partial sequences endings, we should divide the counter by 2,
        // as all the kmers are counted exactly two times
        self.get_counter()
            >> ((self.get_flags() == (READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END)) as u8)
    }
}

pub struct ResultsBucket<X: SequenceExtraData> {
    pub read_index: u64,
    pub reads_writer: OwnedDrop<CompressedBinaryWriter>,
    pub temp_buffer: Vec<u8>,
    pub bucket_index: BucketIndexType,
    pub _phantom: PhantomData<X>,
}

impl<X: SequenceExtraData> ResultsBucket<X> {
    pub fn add_read(&mut self, el: X, read: &[u8]) -> u64 {
        self.temp_buffer.clear();
        CompressedReadsBucketHelper::<X, typenum::U0, false, true>::new(read, 0, 0)
            .write_to(&mut self.temp_buffer, &el);
        self.reads_writer.write_data(self.temp_buffer.as_slice());

        let read_index = self.read_index;
        self.read_index += 1;
        read_index
    }

    pub fn get_bucket_index(&self) -> BucketIndexType {
        self.bucket_index
    }
}

impl<X: SequenceExtraData> Drop for ResultsBucket<X> {
    fn drop(&mut self) {
        unsafe { self.reads_writer.take().finalize() }
    }
}

pub struct RetType {
    pub sequences: Vec<PathBuf>,
    pub hashes: Vec<PathBuf>,
}
