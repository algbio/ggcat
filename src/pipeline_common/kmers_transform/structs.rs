use crate::config::{SwapPriority, SECOND_BUCKETS_COUNT};
use crate::hashes::HashableSequence;
use crate::io::concurrent::intermediate_storage::{IntermediateReadsReader, SequenceExtraData};
use crate::io::varint::{decode_varint_flags, encode_varint_flags};
use crate::{CompressedRead, KEEP_FILES};
use byteorder::ReadBytesExt;
use crossbeam::queue::SegQueue;
use parallel_processor::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parking_lot::Condvar;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct ReadRef(());

impl ReadRef {
    #[inline(always)]
    #[allow(non_camel_case_types)]
    pub fn pack<'a, E: SequenceExtraData, FLAGS_COUNT: typenum::Unsigned>(
        flags: u8,
        read: CompressedRead,
        extra: &E,
        memory: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        memory.clear();

        encode_varint_flags::<_, _, FLAGS_COUNT>(
            |slice| memory.extend_from_slice(slice),
            read.bases_count() as u64,
            flags,
        );
        read.copy_to_buffer(memory);
        extra.encode(memory);

        memory.as_slice()
    }

    #[inline(always)]
    #[allow(non_camel_case_types)]
    pub fn unpack<'a, E: SequenceExtraData, R: Read, FLAGS_COUNT: typenum::Unsigned>(
        stream: &mut R,
        tmp_read_bases: &'a mut Vec<u8>,
    ) -> Option<(u8, CompressedRead<'a>, E)> {
        let (read_len, flags) = decode_varint_flags::<_, FLAGS_COUNT>(|| stream.read_u8().ok())?;

        let read_len = read_len as usize;
        let read_len_bytes = (read_len + 3) / 4;

        tmp_read_bases.clear();
        tmp_read_bases.reserve(read_len_bytes);
        unsafe {
            tmp_read_bases.set_len(read_len_bytes);
        }
        stream.read_exact(tmp_read_bases.as_mut_slice()).unwrap();

        let read = CompressedRead::new_from_compressed(tmp_read_bases.as_slice(), read_len);

        let extra_data = E::decode(stream).unwrap();

        Some((flags, read, extra_data))
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
