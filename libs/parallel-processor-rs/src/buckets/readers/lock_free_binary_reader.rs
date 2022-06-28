use crate::buckets::readers::generic_binary_reader::{ChunkDecoder, GenericChunkedBinaryReader};
use crate::buckets::writers::lock_free_binary_writer::LOCK_FREE_BUCKET_MAGIC;
use crate::memory_fs::file::reader::FileReader;
use std::io::{Read, Take};

pub struct LockFreeStreamDecoder;

impl ChunkDecoder for LockFreeStreamDecoder {
    const MAGIC_HEADER: &'static [u8; 16] = LOCK_FREE_BUCKET_MAGIC;
    type ReaderType = Take<FileReader>;

    fn decode_stream(reader: FileReader, size: u64) -> Self::ReaderType {
        reader.take(size)
    }

    fn dispose_stream(stream: Self::ReaderType) -> FileReader {
        stream.into_inner()
    }
}

pub type LockFreeBinaryReader = GenericChunkedBinaryReader<LockFreeStreamDecoder>;
