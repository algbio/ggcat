use crate::buckets::writers::{finalize_bucket_file, initialize_bucket_file, THREADS_BUSY_WRITING};
use crate::buckets::LockFreeBucket;
use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::file::flush::GlobalFlush;
use crate::memory_fs::file::internal::MemoryFileMode;
use crate::memory_fs::file::writer::FileWriter;
use crate::utils::memory_size_to_log2;
use counter_stats::counter::AtomicCounterGuardSum;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use parking_lot::Mutex;
use replace_with::replace_with_or_abort;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const COMPRESSED_BUCKET_MAGIC: &[u8; 16] = b"CPLZ4_INTR_BKT_M";

pub struct CompressedCheckpointSize(u8);
impl CompressedCheckpointSize {
    pub const fn new_from_size(size: MemoryDataSize) -> Self {
        Self(memory_size_to_log2(size))
    }
    pub const fn new_from_log2(val: u8) -> Self {
        Self(val)
    }
}

fn create_lz4_stream<W: Write>(writer: W, level: CompressionLevelInfo) -> lz4::Encoder<W> {
    let (queue_occupation, queue_size) = GlobalFlush::global_queue_occupation();

    let level = if queue_size < 2 * queue_occupation {
        level.slow_disk
    } else {
        level.fast_disk
    };

    lz4::EncoderBuilder::new()
        .level(level)
        .checksum(ContentChecksum::NoChecksum)
        .block_mode(BlockMode::Linked)
        .block_size(BlockSize::Max64KB)
        .build(writer)
        .unwrap()
}

struct CompressedBinaryWriterInternal {
    writer: lz4::Encoder<FileWriter>,
    checkpoint_max_size: u64,
    checkpoints: Vec<u64>,
    current_chunk_size: u64,
    level: CompressionLevelInfo,
}

pub struct CompressedBinaryWriter {
    inner: Mutex<CompressedBinaryWriterInternal>,
    path: PathBuf,
}
unsafe impl Send for CompressedBinaryWriter {}

impl CompressedBinaryWriterInternal {
    fn create_new_block(&mut self) {
        replace_with_or_abort(&mut self.writer, |writer| {
            let (file_buf, res) = writer.finish();
            res.unwrap();

            let checkpoint_pos = file_buf.len();
            self.checkpoints.push(checkpoint_pos as u64);

            create_lz4_stream(file_buf, self.level)
        });
        self.current_chunk_size = 0;
    }
}

impl CompressedBinaryWriter {
    pub const CHECKPOINT_SIZE_UNLIMITED: CompressedCheckpointSize =
        CompressedCheckpointSize::new_from_log2(62);
}

#[derive(Copy, Clone)]
pub struct CompressionLevelInfo {
    pub fast_disk: u32,
    pub slow_disk: u32,
}

impl LockFreeBucket for CompressedBinaryWriter {
    type InitData = (
        MemoryFileMode,
        CompressedCheckpointSize,
        CompressionLevelInfo,
    );

    fn new(
        path_prefix: &Path,
        (file_mode, checkpoint_max_size, compression_level): &(
            MemoryFileMode,
            CompressedCheckpointSize,
            CompressionLevelInfo,
        ),
        index: usize,
    ) -> Self {
        let path = path_prefix.parent().unwrap().join(format!(
            "{}.{}",
            path_prefix.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut file = FileWriter::create(&path, *file_mode);

        let first_checkpoint = initialize_bucket_file(&mut file);

        let writer = create_lz4_stream(file, *compression_level);

        Self {
            inner: Mutex::new(CompressedBinaryWriterInternal {
                writer,
                checkpoint_max_size: (1 << checkpoint_max_size.0),
                checkpoints: vec![first_checkpoint],
                current_chunk_size: 0,
                level: *compression_level,
            }),
            path,
        }
    }

    fn write_data(&self, bytes: &[u8]) {
        let stat_raii = AtomicCounterGuardSum::new(&THREADS_BUSY_WRITING, 1);
        //
        let mut inner = self.inner.lock();

        inner.writer.write_all(bytes).unwrap();
        inner.current_chunk_size += bytes.len() as u64;
        if inner.current_chunk_size > inner.checkpoint_max_size {
            inner.create_new_block();
        }

        drop(stat_raii);
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }
    fn finalize(self) {
        let mut inner = self.inner.into_inner();

        inner.writer.flush().unwrap();
        let (file, res) = inner.writer.finish();
        res.unwrap();

        finalize_bucket_file(file, COMPRESSED_BUCKET_MAGIC, inner.checkpoints);
    }
}
