use crate::config::{SwapPriority, PARTIAL_VECS_CHECKPOINT_SIZE, SECOND_BUCKETS_COUNT};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint_flags, encode_varint_flags};
use crate::utils::get_memory_mode;
use crate::utils::resource_counter::ResourceCounter;
use crate::{CompressedRead, KEEP_FILES};
use byteorder::ReadBytesExt;
use crossbeam::queue::SegQueue;
use parallel_processor::buckets::bucket_writer::BucketItem;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::generic_binary_reader::{
    ChunkDecoder, GenericChunkedBinaryReader,
};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parking_lot::Condvar;
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Copy, Clone, Debug)]
pub struct ReadRef<'a, E: SequenceExtraData, FlagsCount: typenum::Unsigned> {
    pub flags: u8,
    pub read: CompressedRead<'a>,
    pub _phantom: PhantomData<(E, FlagsCount)>,
}

impl<'x, E: SequenceExtraData, FlagsCount: typenum::Unsigned> BucketItem
    for ReadRef<'x, E, FlagsCount>
{
    type ExtraData = E;
    type ReadBuffer = Vec<u8>;
    type ReadType<'a> = (ReadRef<'a, E, FlagsCount>, E);

    fn write_to(&self, bucket: &mut Vec<u8>, extra_data: &Self::ExtraData) {
        encode_varint_flags::<_, _, FlagsCount>(
            |slice| bucket.extend_from_slice(slice),
            self.read.bases_count() as u64,
            self.flags,
        );
        self.read.copy_to_buffer(bucket);
        extra_data.encode(bucket);
    }

    fn read_from<'b, S: Read>(
        mut stream: S,
        read_buffer: &'b mut Self::ReadBuffer,
    ) -> Option<Self::ReadType<'b>> {
        let (read_len, flags) = decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;

        let read_len = read_len as usize;
        let read_len_bytes = (read_len + 3) / 4;

        read_buffer.clear();
        read_buffer.reserve(read_len_bytes);
        unsafe {
            read_buffer.set_len(read_len_bytes);
        }
        stream.read_exact(read_buffer.as_mut_slice()).unwrap();

        let read = CompressedRead::<'b>::new_from_compressed(read_buffer.as_slice(), read_len);

        let extra = E::decode(&mut stream).unwrap();

        Some((
            ReadRef {
                flags,
                read,
                _phantom: PhantomData,
            },
            extra,
        ))
    }

    fn get_size(&self, extra: &Self::ExtraData) -> usize {
        extra.max_size() + 12 + (self.read.bases_count() / 4)
    }
}

// pub struct ReprocessData<E: SequenceExtraData> {
//     pub reader: FileReader,
// }

pub struct ProcessQueueItem {
    pub path: PathBuf,
    pub can_resplit: bool,
    pub buffers_counter: Arc<ResourceCounter>,
}

impl Drop for ProcessQueueItem {
    fn drop(&mut self) {
        // self.buffers_counter
        //     .deallocate(1, SECOND_BUCKETS_COUNT as u64);
    }
}

pub struct BucketProcessData<FileType: ChunkDecoder> {
    pub reader: GenericChunkedBinaryReader<FileType>,
    pub buckets: MultiThreadBuckets<LockFreeBinaryWriter>,
    process_queue: Arc<SegQueue<ProcessQueueItem>>,
    buffers_counter: Arc<ResourceCounter>,
    can_resplit: bool,
}

impl<FileType: ChunkDecoder> BucketProcessData<FileType> {
    pub fn new_blocking(
        path: impl AsRef<Path>,
        split_name: &str,
        process_queue: Arc<SegQueue<ProcessQueueItem>>,
        buffers_counter: Arc<ResourceCounter>,
        resplit_phase: bool,
    ) -> Self {
        // if resplit_phase {
        //     buffers_counter.allocate_overflow(SECOND_BUCKETS_COUNT as u64);
        // } else {
        //     buffers_counter.allocate_blocking(SECOND_BUCKETS_COUNT as u64);
        // }
        let tmp_dir = path.as_ref().parent().unwrap_or(Path::new("."));
        Self {
            reader: GenericChunkedBinaryReader::<FileType>::new(
                &path,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
            ),
            buckets: MultiThreadBuckets::new(
                SECOND_BUCKETS_COUNT,
                PathBuf::from(tmp_dir).join(split_name),
                &(
                    get_memory_mode(SwapPriority::KmersMergeBuckets),
                    PARTIAL_VECS_CHECKPOINT_SIZE,
                ),
            ),
            process_queue,
            buffers_counter,
            can_resplit: !resplit_phase,
        }
    }
}

impl<FileType: ChunkDecoder> Drop for BucketProcessData<FileType> {
    fn drop(&mut self) {
        for path in self.buckets.finalize() {
            self.process_queue.push(ProcessQueueItem {
                path,
                can_resplit: self.can_resplit,
                buffers_counter: self.buffers_counter.clone(),
            });
        }
    }
}
