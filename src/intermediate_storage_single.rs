use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::hash::HashableSequence;
use crate::intermediate_storage::{IntermediateReadsWriter, SequenceExtraData};
use crate::sequences_reader::FastaSequence;
use crate::types::BucketIndexType;
use crate::utils::{cast_static, cast_static_mut, Utils};
use crate::varint::{decode_varint, encode_varint};
use crate::DEFAULT_BUFFER_SIZE;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use os_pipe::{PipeReader, PipeWriter};
use parallel_processor::multi_thread_buckets::{BucketType, MultiThreadBuckets};
use std::cell::{Cell, UnsafeCell};
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::slice::from_raw_parts;

pub struct IntermediateSequencesStorageSingleBucket<'a, T: SequenceExtraData> {
    buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    bucket_index: BucketIndexType,
    buffer: Vec<u8>,
}
impl<'a, T: SequenceExtraData> IntermediateSequencesStorageSingleBucket<'a, T> {
    const ALLOWED_LEN: usize = 65536;

    pub fn new(
        bucket_index: BucketIndexType,
        buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    ) -> Self {
        let buffer = Vec::with_capacity(parallel_processor::Utils::multiply_by(
            Self::ALLOWED_LEN,
            1.05,
        ));

        Self {
            buckets,
            bucket_index,
            buffer,
        }
    }

    pub fn get_path(&self) -> PathBuf {
        self.buckets.get_path(self.bucket_index)
    }

    fn flush_buffer(&mut self) {
        if self.buffer.len() == 0 {
            return;
        }

        self.buckets.add_data(self.bucket_index, &self.buffer);
        self.buffer.clear();
    }

    pub fn add_read(&mut self, el: T, seq: &[u8]) {
        if self.buffer.len() > 0 && self.buffer.len() + seq.len() > Self::ALLOWED_LEN {
            self.flush_buffer();
        }

        el.encode(&mut self.buffer);
        CompressedRead::from_plain_write_directly_to_buffer(seq, &mut self.buffer);
    }

    pub fn finalize(self) {}
}

impl<'a, T: SequenceExtraData> Drop for IntermediateSequencesStorageSingleBucket<'a, T> {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}
