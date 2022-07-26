use crate::storage::ColorsSerializerTrait;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use config::{ColorIndexType, COLORS_SINGLE_BATCH_SIZE};
use desse::{Desse, DesseSized};
use io::chunks_writer::ChunksWriter;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const STORAGE_VERSION: u64 = 1;

#[derive(Debug, Desse, DesseSized, Default)]
pub(crate) struct ColorsFileHeader {
    pub magic: [u8; 16],
    pub version: u64,
    pub index_offset: u64,
    pub colors_count: u64,
    pub subsets_count: u64,
    pub total_size: u64,
    pub total_uncompressed_size: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct ColorsIndexEntry {
    pub start_index: ColorIndexType,
    pub file_offset: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ColorsIndexMap {
    pub pairs: Vec<ColorsIndexEntry>,
    pub subsets_count: u64,
}

pub struct ColorsSerializer<SI: ColorsSerializerTrait> {
    colors_count: u64,
    serializer_impl: ManuallyDrop<SI>,
}

impl<SI: ColorsSerializerTrait> ColorsSerializer<SI> {
    pub fn new(file: impl AsRef<Path>, color_names: Vec<String>) -> Self {
        let mut colormap_file = File::create(file).unwrap();

        colormap_file
            .write_all(&ColorsFileHeader::default().serialize()[..])
            .unwrap();

        colormap_file = {
            let mut color_names_stream = lz4::EncoderBuilder::new()
                .level(4)
                .build(colormap_file)
                .unwrap();
            bincode::serialize_into(&mut color_names_stream, &color_names).unwrap();

            let (cf, res) = color_names_stream.finish();
            res.unwrap();
            cf
        };

        let file_offset = colormap_file.stream_position().unwrap();

        let color_processor = ColorsFlushProcessing {
            colormap_file: Mutex::new((
                BufWriter::new(colormap_file),
                ColorsIndexMap {
                    pairs: vec![],
                    subsets_count: 0,
                },
            )),
            offset: AtomicU64::new(file_offset),
            uncompressed_size: AtomicU64::new(0),
        };

        let colors_count = color_names.len() as u64;

        Self {
            colors_count,
            serializer_impl: ManuallyDrop::new(SI::new(
                color_processor,
                COLORS_SINGLE_BATCH_SIZE as usize,
                colors_count,
            )),
        }
    }

    pub fn serialize_colors(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        self.serializer_impl.serialize_colors(colors)
    }

    pub fn print_stats(&self) {
        self.serializer_impl.print_stats()
    }
}

fn bincode_serialize_ref<S: Write, D: Serialize>(ser: &mut S, data: &D) {
    bincode::serialize_into(ser, data).unwrap();
}

impl<SI: ColorsSerializerTrait> Drop for ColorsSerializer<SI> {
    fn drop(&mut self) {
        let subsets_count = self.serializer_impl.get_subsets_count();

        let chunks_writer =
            unsafe { std::ptr::read(self.serializer_impl.deref() as *const SI).finalize() };

        let mut colors_lock = chunks_writer.colormap_file.lock();
        let (colors_file, index_map) = colors_lock.deref_mut();
        index_map.pairs.sort();
        index_map.subsets_count = subsets_count;

        colors_file.flush().unwrap();

        let index_position = colors_file.stream_position().unwrap();

        bincode_serialize_ref(colors_file, index_map);
        colors_file.flush().unwrap();

        let total_size = colors_file.stream_position().unwrap();
        colors_file.seek(SeekFrom::Start(0)).unwrap();

        colors_file
            .write_all(
                &ColorsFileHeader {
                    magic: SI::MAGIC,
                    version: STORAGE_VERSION,
                    index_offset: index_position,
                    colors_count: self.colors_count,
                    subsets_count,
                    total_size,
                    total_uncompressed_size: chunks_writer
                        .uncompressed_size
                        .load(Ordering::Relaxed),
                }
                .serialize()[..],
            )
            .unwrap();

        colors_file.flush().unwrap();
    }
}

pub struct ColorsFlushProcessing {
    colormap_file: Mutex<(BufWriter<File>, ColorsIndexMap)>,
    offset: AtomicU64,
    uncompressed_size: AtomicU64,
}

pub struct StreamWrapper<'a> {
    serializer: &'a ColorsFlushProcessing,
    tmp_data: &'a mut (lz4::Encoder<Vec<u8>>, u64),
}
impl<'a> Write for StreamWrapper<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.serializer.flush_data(&mut self.tmp_data, buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChunksWriter for ColorsFlushProcessing {
    type ProcessingData = (lz4::Encoder<Vec<u8>>, u64);
    type TargetData = u8;
    type StreamType<'a> = StreamWrapper<'a>;

    fn start_processing(&self) -> Self::ProcessingData {
        (
            lz4::EncoderBuilder::new()
                .level(4)
                .build(Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE))
                .unwrap(),
            0,
        )
    }

    fn flush_data(&self, tmp_data: &mut Self::ProcessingData, data: &[Self::TargetData]) {
        tmp_data.0.write_all(data).unwrap();
        tmp_data.1 += data.len() as u64;
    }

    fn get_stream<'a>(&'a self, tmp_data: &'a mut Self::ProcessingData) -> Self::StreamType<'a> {
        StreamWrapper {
            serializer: self,
            tmp_data,
        }
    }

    fn end_processing(&self, tmp_data: Self::ProcessingData, start_index: ColorIndexType) {
        let mut file_lock = self.colormap_file.lock();

        self.uncompressed_size
            .fetch_add(tmp_data.1, Ordering::Relaxed);

        let (data, res) = tmp_data.0.finish();
        res.unwrap();

        let file_offset = self.offset.fetch_add(data.len() as u64, Ordering::Relaxed);

        file_lock.0.write_all(data.as_slice()).unwrap();
        file_lock.1.pairs.push(ColorsIndexEntry {
            start_index,
            file_offset,
        });
    }
}
