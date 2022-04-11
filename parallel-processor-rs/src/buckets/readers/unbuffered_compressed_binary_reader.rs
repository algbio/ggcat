use crate::buckets::readers::generic_binary_reader::{ChunkDecoder, GenericChunkedBinaryReader};
use crate::buckets::writers::compressed_binary_writer::COMPRESSED_BUCKET_MAGIC;
use crate::memory_fs::file::reader::FileReader;

pub struct UnbufferedCompressedStreamDecoder;

impl ChunkDecoder for UnbufferedCompressedStreamDecoder {
    const MAGIC_HEADER: &'static [u8; 16] = COMPRESSED_BUCKET_MAGIC;
    type ReaderType = lz4::Decoder<FileReader>;

    fn decode_stream(reader: FileReader, _size: u64) -> Self::ReaderType {
        lz4::Decoder::new(reader).unwrap()
    }

    fn dispose_stream(stream: Self::ReaderType) -> FileReader {
        let (file, result) = stream.finish();
        result.unwrap();
        file
    }
}

pub type UnbufferedCompressedBinaryReader =
    GenericChunkedBinaryReader<UnbufferedCompressedStreamDecoder>;
