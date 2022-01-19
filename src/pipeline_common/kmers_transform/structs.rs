use crate::config::{SortingHashType, SwapPriority, SECOND_BUCKETS_COUNT};
use crate::hashes::HashableSequence;
use crate::io::concurrent::intermediate_storage::{IntermediateReadsReader, SequenceExtraData};
use crate::io::varint::{decode_varint_flags, encode_varint, encode_varint_flags};
use crate::{CompressedRead, KEEP_FILES};
use crossbeam::queue::SegQueue;
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parking_lot::Condvar;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

const HASH_OFFSET: usize =
    (std::mem::size_of::<u64>() - std::mem::size_of::<SortingHashType>()) * 8;
const HASH_SIZE: usize = std::mem::size_of::<SortingHashType>();

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct ReadRef(u64);

impl ReadRef {
    pub const TMP_DATA_OFFSET: usize = 32;

    pub fn new(index: usize, hash: SortingHashType) -> Self {
        Self {
            0: ((hash as u64) << HASH_OFFSET) | (index as u64),
        }
    }

    #[inline(always)]
    pub fn get_hash(&self) -> SortingHashType {
        (self.0 >> HASH_OFFSET) as SortingHashType
    }

    #[inline(always)]
    fn get_offset(&self) -> usize {
        (self.0 & ((1 << HASH_OFFSET) - 1)) as usize
    }

    #[inline(always)]
    #[allow(non_camel_case_types)]
    pub fn pack<'a, E: SequenceExtraData, FLAGS_COUNT: typenum::Unsigned>(
        hash: SortingHashType,
        flags: u8,
        read: CompressedRead,
        extra: &E,
        memory: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        unsafe {
            memory.set_len(Self::TMP_DATA_OFFSET);
        }
        let mut backward_offset = Self::TMP_DATA_OFFSET;

        encode_varint_flags::<_, _, FLAGS_COUNT>(
            |slice| memory.extend_from_slice(slice),
            read.bases_count() as u64,
            flags,
        );
        read.copy_to_buffer(memory);

        extra.encode(memory);

        let element_size = (memory.len() - Self::TMP_DATA_OFFSET) as u64;

        encode_varint(
            |bytes| {
                let end_pos = backward_offset;
                backward_offset -= bytes.len();
                memory[backward_offset..end_pos].copy_from_slice(bytes);
            },
            element_size,
        );

        memory[(backward_offset - HASH_SIZE)..backward_offset].copy_from_slice(&hash.to_ne_bytes());
        backward_offset -= HASH_SIZE;

        &memory[backward_offset..]
    }

    #[inline(always)]
    #[allow(non_camel_case_types)]
    pub fn unpack<'a, 'b, E: SequenceExtraData, FLAGS_COUNT: typenum::Unsigned>(
        &'a self,
        memory: &'b [u8],
    ) -> (u8, CompressedRead<'b>, E) {
        let mut read_start = self.get_offset();
        let (read_len, flags) = decode_varint_flags::<_, FLAGS_COUNT>(|| {
            let x = memory[read_start];
            read_start += 1;
            Some(x)
        })
        .unwrap();

        let read_bases_start = read_start;
        let read_len = read_len as usize;
        let read_len_bytes = (read_len + 3) / 4;

        let read_slice = &memory[read_bases_start..(read_bases_start + read_len_bytes)];

        let read = CompressedRead::new_from_compressed(read_slice, read_len);

        let extra_data = unsafe {
            let extra_slice_ptr = memory.as_ptr().add(read_bases_start + read_len_bytes);
            E::decode_from_pointer(extra_slice_ptr).unwrap()
        };

        (flags, read, extra_data)
    }
}

unsafe impl Sync for ReadRef {}
unsafe impl Send for ReadRef {}

pub struct BucketProcessData<E: SequenceExtraData> {
    pub reader: IntermediateReadsReader<E>,
    pub buckets: MultiThreadBuckets<LockFreeBinaryWriter>,
    vecs_queue: Arc<SegQueue<(PathBuf, bool)>>,
    instances_count: Arc<AtomicUsize>,
    notify_condvar: Arc<Condvar>,
}

static QUEUE_IDENTIFIER: AtomicU64 = AtomicU64::new(0);

impl<E: SequenceExtraData> BucketProcessData<E> {
    pub fn new(
        path: impl AsRef<Path>,
        vecs_queue: Arc<SegQueue<(PathBuf, bool)>>,
        instances_count: Arc<AtomicUsize>,
        notify_condvar: Arc<Condvar>,
    ) -> Self {
        instances_count.fetch_add(1, Ordering::Relaxed);

        let tmp_dir = path.as_ref().parent().unwrap_or(Path::new("."));
        Self {
            reader: IntermediateReadsReader::<E>::new(&path, !KEEP_FILES.load(Ordering::Relaxed)),
            buckets: MultiThreadBuckets::new(
                SECOND_BUCKETS_COUNT,
                &(
                    PathBuf::from(tmp_dir).join(format!(
                        "vec{}",
                        QUEUE_IDENTIFIER.fetch_add(1, Ordering::Relaxed)
                    )),
                    MemoryFileMode::PreferMemory {
                        swap_priority: SwapPriority::KmersMergeBuckets,
                    },
                ),
                None,
            ),
            vecs_queue,
            instances_count,
            notify_condvar,
        }
    }
}

impl<E: SequenceExtraData> Drop for BucketProcessData<E> {
    fn drop(&mut self) {
        for path in self.buckets.finalize() {
            self.vecs_queue.push((path, true));
        }
        self.instances_count.fetch_sub(1, Ordering::Relaxed);
        self.notify_condvar.notify_all();
    }
}
