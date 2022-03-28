use crate::buckets::readers::generic_binary_reader::{ChunkDecoder, GenericChunkedBinaryReader};
use crate::buckets::writers::compressed_binary_writer::COMPRESSED_BUCKET_MAGIC;
use crate::memory_fs::file::reader::FileReader;
use crate::utils::vec_reader::VecReader;

pub struct CompressedStreamDecoder;

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

impl ChunkDecoder for CompressedStreamDecoder {
    const MAGIC_HEADER: &'static [u8; 16] = COMPRESSED_BUCKET_MAGIC;
    type ReaderType = VecReader<lz4::Decoder<FileReader>>;

    fn decode_stream(reader: FileReader, _size: u64) -> Self::ReaderType {
        VecReader::new(DEFAULT_BUFFER_SIZE, lz4::Decoder::new(reader).unwrap())
    }

    fn dispose_stream(stream: Self::ReaderType) -> FileReader {
        let (file, result) = stream.into_inner().finish();
        result.unwrap();
        file
    }
}

pub type CompressedBinaryReader = GenericChunkedBinaryReader<CompressedStreamDecoder>;
