use crate::async_slice_queue::{AsyncSliceProcessor, AsyncSliceQueue};
use crate::dummy_hasher::{DummyHasher, DummyHasherBuilder};
use crate::varint::{decode_varint, encode_varint};
use crate::KEEP_FILES;
use byteorder::ReadBytesExt;
use dashmap::DashMap;
use desse::{Desse, DesseSized};
use parking_lot::Mutex;
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use siphasher::sip128::{Hash128, Hasher128, SipHasher13};
use std::cell::UnsafeCell;
use std::cmp::max;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::{swap, transmute};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub type ColorIndexType = u32;

pub struct ColorIndexSerializer;
impl ColorIndexSerializer {
    pub fn serialize_colors(mut writer: impl Write, colors: &[ColorIndexType]) {
        encode_varint(|b| writer.write_all(b), (colors[0] as u64) + 2);

        let mut last_color = colors[0];

        let mut encode_2order = false;
        let mut encode_2order_value = 0;
        let mut encode_2order_count = 0;

        for i in 1..colors.len() {
            let current_diff = colors[i] - last_color;

            let mut flush_2ndorder = false;

            if i + 1 < colors.len() {
                let next_diff = colors[i + 1] - colors[i];

                if current_diff == next_diff {
                    if !encode_2order {
                        // Second order encoding saves space only if there are at least
                        if i + 2 < colors.len() && (colors[i + 2] - colors[i + 1] == current_diff) {
                            encode_varint(|b| writer.write_all(b), 1);
                            encode_2order_count = 2;
                            encode_2order = true;
                            encode_2order_value = current_diff;
                        }
                    } else {
                        encode_2order_count += 1;
                    }
                } else if encode_2order {
                    flush_2ndorder = true;
                }
            } else if encode_2order {
                flush_2ndorder = true;
            }

            if flush_2ndorder {
                encode_varint(|b| writer.write_all(b), encode_2order_value as u64);
                encode_varint(|b| writer.write_all(b), encode_2order_count);
                encode_2order = false;
            } else if !encode_2order {
                encode_varint(|b| writer.write_all(b), (current_diff + 1) as u64);
            }

            last_color = colors[i];
        }
        encode_varint(|b| writer.write_all(b), 0);
    }

    #[inline(always)]
    pub fn deserialize_colors_diffs(
        mut reader: impl Read,
        mut add_color: impl FnMut(ColorIndexType),
    ) {
        add_color((decode_varint(|| reader.read_u8().ok()).unwrap() - 2) as ColorIndexType);
        loop {
            let result = decode_varint(|| reader.read_u8().ok()).unwrap() as ColorIndexType;
            if result == 0 {
                break;
            } else if result == 1 {
                // 2nd order encoding
                let value = decode_varint(|| reader.read_u8().ok()).unwrap() as ColorIndexType;
                let count = decode_varint(|| reader.read_u8().ok()).unwrap() as ColorIndexType;

                for _ in 0..count {
                    add_color(value);
                }
            } else {
                add_color(result - 1);
            }
        }
    }

    pub fn deserialize_colors(mut reader: impl Read, colors: &mut Vec<ColorIndexType>) {
        colors.clear();

        Self::deserialize_colors_diffs(reader, |c| colors.push(c));

        let mut last_color = colors[0];
        for i in 1..colors.len() {
            colors[i] += last_color;
            last_color = colors[i];
        }
    }
}

const MAGIC_STRING: [u8; 16] = *b"BILOKI_COLORMAPS";
const STORAGE_VERSION: u64 = 1;

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

#[derive(Serialize, Deserialize)]
struct ColorsIndexMap {
    pairs: Vec<(ColorIndexType, u64)>,
}

pub struct ColorsStorage {
    colors_count: u64,
    async_buffer: AsyncSliceQueue<u8, ColorsFlushProcessing>,
}

const CHECKPOINT_DISTANCE: usize = 20000;

fn bincode_serialize_ref<S: Write, D: Serialize>(ser: &mut S, data: &D) {
    bincode::serialize_into(ser, data);
}

impl Drop for ColorsStorage {
    fn drop(&mut self) {
        self.async_buffer.finish();

        let mut colors_lock = self.async_buffer.async_processor.colormap_file.lock();

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
                    subsets_count: self.async_buffer.get_counter(),
                    total_size,
                    total_uncompressed_size: self
                        .async_buffer
                        .async_processor
                        .uncompressed_size
                        .load(Ordering::Relaxed),
                }
                .serialize()[..],
            )
            .unwrap();

        colors_file.flush();
    }
}

#[thread_local]
static mut TEMP_COLOR_BUFFER: Vec<u8> = Vec::new();

struct ColorsFlushProcessing {
    colormap_file: Mutex<(BufWriter<File>, ColorsIndexMap)>,
    offset: AtomicU64,
    uncompressed_size: AtomicU64,
}

impl AsyncSliceProcessor for ColorsFlushProcessing {
    type ProcessingData = (lz4::Encoder<Vec<u8>>, u64);
    type TargetData = u8;

    fn start_processing(&self) -> Self::ProcessingData {
        (
            lz4::EncoderBuilder::new()
                .level(4)
                .build(Vec::with_capacity(1024 * 1024))
                .unwrap(),
            0,
        )
    }

    fn flush_data(&self, tmp_data: &mut Self::ProcessingData, data: &[Self::TargetData]) {
        tmp_data.0.write(data).unwrap();
        tmp_data.1 += data.len() as u64;
    }

    fn end_processing(&self, tmp_data: Self::ProcessingData, start_index: ColorIndexType) {
        let mut file_lock = self.colormap_file.lock();

        self.uncompressed_size
            .fetch_add(tmp_data.1, Ordering::Relaxed);

        let (data, res) = tmp_data.0.finish();
        res.unwrap();

        let file_offset = self.offset.fetch_add(data.len() as u64, Ordering::Relaxed);

        file_lock.0.write_all(data.as_slice());
        file_lock.1.pairs.push((start_index, file_offset));
    }
}

impl ColorsStorage {
    pub fn read_color(file: impl AsRef<Path>, color_index: ColorIndexType) -> Vec<String> {
        let mut result = Vec::new();

        let mut file = File::open(file).unwrap();

        let mut header_buffer = [0; ColorsFileHeader::SIZE];
        file.read_exact(&mut header_buffer);

        let header: ColorsFileHeader = ColorsFileHeader::deserialize_from(&header_buffer);
        assert_eq!(header.magic, MAGIC_STRING);

        let color_names = {
            let mut compressed_stream = lz4::Decoder::new(BufReader::new(file)).unwrap();

            let color_names: Vec<String> =
                bincode::deserialize_from(&mut compressed_stream).unwrap();
            file = compressed_stream.finish().0.into_inner();
            color_names
        };

        let mut color_off = 0;
        let mut chunk_off = 0;
        {
            file.seek(SeekFrom::Start(header.index_offset));
            let colors_index: ColorsIndexMap = bincode::deserialize_from(&mut file).unwrap();

            for (cidx, chref) in colors_index.pairs.iter() {
                if *cidx <= color_index {
                    color_off = *cidx;
                    chunk_off = *chref;
                } else {
                    break;
                }
            }
        }

        {
            assert_ne!(chunk_off, 0);
            file.seek(SeekFrom::Start(chunk_off));
            let mut compressed_stream = lz4::Decoder::new(BufReader::new(file)).unwrap();

            for _skip in color_off..color_index {
                ColorIndexSerializer::deserialize_colors_diffs(&mut compressed_stream, |_| {});
            }

            let mut colors = Vec::new();
            ColorIndexSerializer::deserialize_colors(&mut compressed_stream, &mut colors);

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

        Self {
            colors_count: color_names.len() as u64,
            async_buffer: AsyncSliceQueue::new(
                1024 * 1024 * 8,
                rayon::current_num_threads(),
                CHECKPOINT_DISTANCE,
                color_processor,
            ),
        }
    }

    pub fn serialize_colors(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        unsafe {
            TEMP_COLOR_BUFFER.clear();
            ColorIndexSerializer::serialize_colors(&mut TEMP_COLOR_BUFFER, colors);
            self.async_buffer.add_data(TEMP_COLOR_BUFFER.as_slice()) as ColorIndexType
        }
    }

    pub fn print_stats(&self) {
        println!("Subsets count: {}", self.async_buffer.get_counter());
    }
}

#[cfg(test)]
mod tests {
    use crate::colors_storage::{ColorIndexSerializer, ColorIndexType};
    use std::io::Cursor;

    fn color_subset_encoding(colors: &[ColorIndexType]) {
        let mut buffer = Vec::new();

        ColorIndexSerializer::serialize_colors(&mut buffer, colors);

        println!("Buffer size: {}", buffer.len());
        let mut cursor = Cursor::new(buffer);

        let mut des_colors = Vec::new();

        ColorIndexSerializer::deserialize_colors(&mut cursor, &mut des_colors);
        assert_eq!(colors, des_colors.as_slice());
    }

    #[test]
    fn colors_subset_encoding_test() {
        color_subset_encoding(&[0, 1, 2, 3, 4, 5, 6, 7]);

        color_subset_encoding(&[0]);
        color_subset_encoding(&[1]);

        color_subset_encoding(&[1, 2, 5, 10, 15, 30, 45]);

        color_subset_encoding(&[1, 100, 200, 300, 400, 800]);

        color_subset_encoding(&[
            3, 6, 9, 12, 70, 71, 62, 63, 64, 88, 95, 100, 105, 110, 198, 384,
        ]);
    }
}
