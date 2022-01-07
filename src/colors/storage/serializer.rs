use crate::colors::storage::ColorsSerializerImpl;
use crate::colors::ColorIndexType;
use crate::config::DEFAULT_OUTPUT_BUFFER_SIZE;
use crate::io::chunks_writer::ChunksWriter;
use bincode::Options;
use desse::{Desse, DesseSized};
use parking_lot::Mutex;
use rayon::str::SplitTerminator;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC_STRING: [u8; 16] = *b"BILOKI_COLORMAPS";
const STORAGE_VERSION: u64 = 1;

const CHECKPOINT_DISTANCE: usize = 20000;

#[derive(Debug, Desse, DesseSized, Default)]
struct ColorsFileHeader {
    magic: [u8; 16],
    version: u64,
    index_offset: u64,
    colors_count: u64,
    subsets_count: u64,
    total_size: u64,
    total_uncompressed_size: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct ColorsIndexEntry {
    pub start_index: ColorIndexType,
    pub stride: ColorIndexType,
    file_offset: u64,
}

#[derive(Serialize, Deserialize)]
struct ColorsIndexMap {
    pairs: Vec<ColorsIndexEntry>,
}

pub struct ColorsSerializer<SI: ColorsSerializerImpl> {
    colors_count: u64,
    serializer_impl: ManuallyDrop<SI>,
}

impl<SI: ColorsSerializerImpl> ColorsSerializer<SI> {
    pub fn read_color(file: impl AsRef<Path>, color_index: ColorIndexType) -> Vec<String> {
        let mut result = Vec::new();

        let mut file = File::open(file).unwrap();

        let mut header_buffer = [0; ColorsFileHeader::SIZE];
        file.read_exact(&mut header_buffer);

        let header: ColorsFileHeader = ColorsFileHeader::deserialize_from(&header_buffer);
        assert_eq!(header.magic, MAGIC_STRING);

        println!("Colors header: {:#?}", &header);

        let color_names = {
            let mut compressed_stream = lz4::Decoder::new(BufReader::new(file)).unwrap();

            let color_names: Vec<String> =
                bincode::deserialize_from(&mut compressed_stream).unwrap();
            file = compressed_stream.finish().0.into_inner();
            color_names
        };

        let mut color_entry = ColorsIndexEntry {
            start_index: 0,
            stride: 0,
            file_offset: 0,
        };

        {
            file.seek(SeekFrom::Start(header.index_offset));
            let colors_index: ColorsIndexMap = bincode::deserialize_from(&mut file).unwrap();

            for entry in colors_index.pairs.iter() {
                if entry.start_index <= color_index {
                    color_entry = *entry;
                } else {
                    break;
                }
            }
        }

        {
            assert_ne!(color_entry.file_offset, 0);
            file.seek(SeekFrom::Start(color_entry.file_offset));
            let mut compressed_stream = lz4::Decoder::new(BufReader::new(file)).unwrap();

            let colors = SI::decode_color(compressed_stream, color_entry, color_index);

            for color in colors {
                result.push(color_names[color as usize].clone());
            }
        }

        result
    }

    pub fn new(file: impl AsRef<Path>, color_names: Vec<String>) -> Self {
        let mut colormap_file = File::create(file).unwrap();

        colormap_file.write_all(&ColorsFileHeader::default().serialize()[..]);

        colormap_file = {
            let mut color_names_stream = lz4::EncoderBuilder::new()
                .level(4)
                .build(colormap_file)
                .unwrap();
            bincode::serialize_into(&mut color_names_stream, &color_names);

            let (cf, res) = color_names_stream.finish();
            res.unwrap();
            cf
        };

        let file_offset = colormap_file.stream_position().unwrap();

        let color_processor = ColorsFlushProcessing {
            colormap_file: Mutex::new((
                BufWriter::new(colormap_file),
                ColorsIndexMap { pairs: vec![] },
            )),
            offset: AtomicU64::new(file_offset),
            uncompressed_size: AtomicU64::new(0),
        };

        let colors_count = color_names.len() as u64;

        Self {
            colors_count,
            serializer_impl: ManuallyDrop::new(SI::new(
                color_processor,
                CHECKPOINT_DISTANCE,
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
    bincode::serialize_into(ser, data);
}

impl<SI: ColorsSerializerImpl> Drop for ColorsSerializer<SI> {
    fn drop(&mut self) {
        let subsets_count = self.serializer_impl.get_subsets_count();

        let chunks_writer =
            unsafe { std::ptr::read(self.serializer_impl.deref() as *const SI).finalize() };

        let mut colors_lock = chunks_writer.colormap_file.lock();
        let (colors_file, index_map) = colors_lock.deref_mut();
        index_map.pairs.sort();

        colors_file.flush();

        let index_position = colors_file.stream_position().unwrap();

        bincode_serialize_ref(colors_file, index_map);
        colors_file.flush();

        let total_size = colors_file.stream_position().unwrap();
        colors_file.seek(SeekFrom::Start(0));

        colors_file
            .write_all(
                &ColorsFileHeader {
                    magic: MAGIC_STRING,
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

        colors_file.flush();
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
        Ok((buf.len()))
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

    fn end_processing(
        &self,
        tmp_data: Self::ProcessingData,
        start_index: ColorIndexType,
        stride: ColorIndexType,
    ) {
        let mut file_lock = self.colormap_file.lock();

        self.uncompressed_size
            .fetch_add(tmp_data.1, Ordering::Relaxed);

        let (data, res) = tmp_data.0.finish();
        res.unwrap();

        let file_offset = self.offset.fetch_add(data.len() as u64, Ordering::Relaxed);

        file_lock.0.write_all(data.as_slice());
        file_lock.1.pairs.push(ColorsIndexEntry {
            start_index,
            stride,
            file_offset,
        });
    }
}
