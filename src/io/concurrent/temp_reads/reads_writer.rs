use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::concurrent::temp_reads::{
    IntermediateReadsHeader, IntermediateReadsIndex, INTERMEDIATE_READS_MAGIC,
};
use crate::utils::get_memory_mode;
use crate::CompressedRead;
use desse::Desse;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use parallel_processor::buckets::bucket_type::BucketType;
use parallel_processor::memory_fs::file::writer::FileWriter;
use replace_with::replace_with_or_abort;
use std::io::Write;
use std::marker::PhantomData;
use std::path::PathBuf;

pub struct IntermediateReadsWriter<T> {
    writer: lz4::Encoder<FileWriter>,
    chunk_size: u64,
    index: IntermediateReadsIndex,
    path: PathBuf,
    _phantom: PhantomData<T>,
}

unsafe impl<T> Sync for IntermediateReadsWriter<T> {}

impl<T: SequenceExtraData> IntermediateReadsWriter<T> {
    fn create_new_block(&mut self) {
        replace_with_or_abort(&mut self.writer, |writer| {
            let (file_buf, res) = writer.finish();
            res.unwrap();

            let checkpoint_pos = file_buf.len();
            self.index.index.push(checkpoint_pos as u64);

            lz4::EncoderBuilder::new().level(0).build(file_buf).unwrap()
        });
        self.chunk_size = 0;
    }

    #[allow(non_camel_case_types)]
    pub fn add_read<FLAGS_COUNT: typenum::Unsigned>(&mut self, el: T, seq: &[u8], flags: u8) {
        el.encode(&mut self.writer);
        CompressedRead::from_plain_write_directly_to_stream_with_flags::<_, FLAGS_COUNT>(
            seq,
            &mut self.writer,
            flags,
        );
    }
}

const MAXIMUM_CHUNK_SIZE: u64 = 1024 * 1024 * 16;

impl<T: SequenceExtraData> BucketType for IntermediateReadsWriter<T> {
    type InitType = (usize, PathBuf);
    type DataType = u8;
    const SUPPORTS_LOCK_FREE: bool = false;

    fn new((swap_priority, base_path): &(usize, PathBuf), index: usize) -> Self {
        let path = base_path.parent().unwrap().join(format!(
            "{}.{}.tmp",
            base_path.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut file = FileWriter::create(&path, get_memory_mode(*swap_priority));

        // Write empty header
        file.write_all(&IntermediateReadsHeader::default().serialize()[..])
            .unwrap();

        let first_block_pos = file.len() as u64;
        let compress_stream = lz4::EncoderBuilder::new()
            .level(3)
            .checksum(ContentChecksum::NoChecksum)
            .block_mode(BlockMode::Linked)
            .block_size(BlockSize::Max4MB)
            .build(file)
            .unwrap();

        IntermediateReadsWriter {
            writer: compress_stream,
            chunk_size: 0,
            index: IntermediateReadsIndex {
                index: vec![first_block_pos],
            },
            path,
            _phantom: PhantomData,
        }
    }

    fn write_batch_data(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).unwrap();
        self.chunk_size += bytes.len() as u64;
        if self.chunk_size > MAXIMUM_CHUNK_SIZE {
            self.create_new_block();
        }
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn finalize(mut self) {
        self.writer.flush().unwrap();
        let (mut file, res) = self.writer.finish();
        res.unwrap();

        file.flush().unwrap();
        let index_position = file.len() as u64;
        bincode::serialize_into(&mut file, &self.index).unwrap();

        file.write_at_start(
            &IntermediateReadsHeader {
                magic: INTERMEDIATE_READS_MAGIC,
                index_offset: index_position,
            }
            .serialize()[..],
        )
        .unwrap();

        file.flush().unwrap();
    }
}
