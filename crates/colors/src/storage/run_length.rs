use crate::storage::ColorsSerializerTrait;
use crate::storage::serializer::COLORMAP_STORAGE_VERSION;
use crate::storage::serializer::ColorsFileHeader;
use crate::storage::serializer::ColorsFlushProcessing;
use byteorder::ReadBytesExt;
use config::COLORS_SINGLE_INDEX_DEFAULT_COLORS;
use config::ColorIndexType;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use desse::Desse;
use io::varint::{decode_varint, encode_varint};
use parking_lot::Condvar;
use parking_lot::Mutex;
use serde::Serialize;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{Read, Write};
use std::ops::DerefMut;
use utils::resize_containers::ResizableVec;

fn bincode_serialize_ref<S: Write, D: Serialize>(ser: &mut S, data: &D) {
    bincode::serialize_into(ser, data).unwrap();
}

pub struct ColorIndexSerializer;
impl ColorIndexSerializer {
    pub fn serialize_colors(writer: &mut impl Write, colors: &[ColorIndexType]) {
        encode_varint(|b| writer.write_all(b), (colors[0] as u64) + 2).unwrap();

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
                        // Second order encoding saves space only if there are at least 2 consecutive elements
                        if i + 2 < colors.len() && (colors[i + 2] - colors[i + 1] == current_diff) {
                            encode_varint(|b| writer.write_all(b), 1).unwrap();
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
                encode_varint(|b| writer.write_all(b), encode_2order_value as u64).unwrap();
                encode_varint(|b| writer.write_all(b), encode_2order_count).unwrap();
                encode_2order = false;
            } else if !encode_2order {
                encode_varint(|b| writer.write_all(b), (current_diff + 1) as u64).unwrap();
            }

            last_color = colors[i];
        }
        encode_varint(|b| writer.write_all(b), 0).unwrap();
    }

    #[inline(always)]
    pub fn deserialize_colors_diffs(
        mut reader: impl Read,
        mut add_color: impl FnMut(ColorIndexType),
    ) -> Option<()> {
        add_color((decode_varint(|| reader.read_u8().ok())? - 2) as ColorIndexType);
        loop {
            let result = decode_varint(|| reader.read_u8().ok())? as ColorIndexType;
            if result == 0 {
                break;
            } else if result == 1 {
                // 2nd order encoding
                let value = decode_varint(|| reader.read_u8().ok())? as ColorIndexType;
                let count = decode_varint(|| reader.read_u8().ok())? as ColorIndexType;

                for _ in 0..count {
                    add_color(value);
                }
            } else {
                add_color(result - 1);
            }
        }
        Some(())
    }

    pub fn deserialize_colors(reader: impl Read, colors: &mut Vec<ColorIndexType>) -> Option<()> {
        colors.clear();

        Self::deserialize_colors_diffs(reader, |c| colors.push(c))?;

        let mut last_color = colors[0];
        for i in 1..colors.len() {
            colors[i] += last_color;
            last_color = colors[i];
        }
        Some(())
    }
}

pub struct RunLengthColorsSerializer {
    writer: Mutex<(u64, ColorsFlushProcessing)>,
    condvar: Condvar,
    colors_count: u64,
}

pub struct RunLengthCheckpointTracker {
    checkpoint_distance: u64,
    chunk_written_subsets: u64,
    total_flushed_subsets: u64,
    total_checkpoints: u64,
}

pub struct RunLengthCheckpointWriter<'a> {
    checkpoint_index: u64,
    checkpoint_subset_start: u64,
    checkpoint_buffer: &'a mut ResizableVec<u8, DEFAULT_OUTPUT_BUFFER_SIZE>,
}

impl ColorsSerializerTrait for RunLengthColorsSerializer {
    const MAGIC: [u8; 16] = *b"GGCAT_CMAP_RNLEN";

    type PreSerializer = ResizableVec<u8, COLORS_SINGLE_INDEX_DEFAULT_COLORS>;
    type CheckpointTracker = RunLengthCheckpointTracker;
    type CheckpointBuffer = ResizableVec<u8, DEFAULT_OUTPUT_BUFFER_SIZE>;
    type CompressedCheckpointBuffer = ResizableVec<u8, DEFAULT_OUTPUT_BUFFER_SIZE>;
    type CheckpointWriter<'a> = RunLengthCheckpointWriter<'a>;

    fn decode_color(mut reader: impl Read, out_vec: Option<&mut Vec<u32>>) {
        match out_vec {
            None => {
                ColorIndexSerializer::deserialize_colors_diffs(&mut reader, |_| {});
            }
            Some(out_vec) => {
                ColorIndexSerializer::deserialize_colors(&mut reader, out_vec);
            }
        }
    }

    fn new(
        writer: ColorsFlushProcessing,
        checkpoint_distance: usize,
        colors_count: u64,
    ) -> (Self, Self::CheckpointTracker) {
        (
            Self {
                writer: Mutex::new((0, writer)),
                condvar: Condvar::new(),
                colors_count,
            },
            RunLengthCheckpointTracker {
                checkpoint_distance: checkpoint_distance as u64,
                chunk_written_subsets: 0,
                total_flushed_subsets: 0,
                total_checkpoints: 0,
            },
        )
    }

    #[inline(always)]
    fn preserialize_colors(pre_serializer: &mut Self::PreSerializer, colors: &[ColorIndexType]) {
        ColorIndexSerializer::serialize_colors(pre_serializer.deref_mut(), colors);
    }

    #[inline(always)]
    fn write_color_subset<'a>(
        tracker: &mut Self::CheckpointTracker,
        buffer: &'a mut Self::CheckpointBuffer,
        pre_serializer: &Self::PreSerializer,
    ) -> Option<Self::CheckpointWriter<'a>> {
        buffer.extend_from_slice(&pre_serializer);
        tracker.chunk_written_subsets += 1;
        if tracker.chunk_written_subsets == tracker.checkpoint_distance {
            let checkpoint_subset_start = tracker.total_flushed_subsets;
            let checkpoint_index = tracker.total_checkpoints;

            tracker.total_checkpoints += 1;
            tracker.total_flushed_subsets += tracker.chunk_written_subsets;
            tracker.chunk_written_subsets = 0;

            Some(RunLengthCheckpointWriter {
                checkpoint_index,
                checkpoint_subset_start,
                checkpoint_buffer: buffer,
            })
        } else {
            None
        }
    }

    fn flush_checkpoint(
        &self,
        checkpoint: Self::CheckpointWriter<'_>,
        compressed_buffer: &mut Self::CheckpointBuffer,
    ) {
        ColorsFlushProcessing::compress_chunk(&checkpoint.checkpoint_buffer, compressed_buffer);

        let mut writer = self.writer.lock();
        while writer.0 != checkpoint.checkpoint_index {
            self.condvar.wait(&mut writer);
        }

        writer.1.write_compressed_chunk(
            checkpoint.checkpoint_subset_start as ColorIndexType,
            checkpoint.checkpoint_buffer.len(),
            &compressed_buffer,
        );
        checkpoint.checkpoint_buffer.clear();
        compressed_buffer.clear();

        writer.0 += 1;
        self.condvar.notify_all();
    }

    fn final_flush_buffer(
        &self,
        tracker: &mut Self::CheckpointTracker,
        mut buffer: Self::CheckpointBuffer,
        mut compressed_buffer: Self::CheckpointBuffer,
    ) {
        if buffer.len() == 0 {
            return;
        }
        self.flush_checkpoint(
            RunLengthCheckpointWriter {
                checkpoint_index: tracker.total_checkpoints,
                checkpoint_subset_start: tracker.total_flushed_subsets,
                checkpoint_buffer: &mut buffer,
            },
            &mut compressed_buffer,
        );
    }

    fn get_subsets_count(tracker: &mut Self::CheckpointTracker) -> u64 {
        tracker.total_flushed_subsets + tracker.chunk_written_subsets
    }
    fn print_stats(tracker: &Self::CheckpointTracker) {
        ggcat_logging::info!(
            "Total color subsets: {}",
            tracker.total_flushed_subsets + tracker.chunk_written_subsets
        )
    }

    fn finalize(self, tracker: Self::CheckpointTracker) {
        let mut colormap_writer = self.writer.into_inner().1;

        let colors_file = &mut colormap_writer.colormap_file;
        let index_map = &mut colormap_writer.colormap_index;
        let subsets_count = tracker.total_flushed_subsets + tracker.chunk_written_subsets;

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
                    magic: Self::MAGIC,
                    version: COLORMAP_STORAGE_VERSION,
                    index_offset: index_position,
                    colors_count: self.colors_count,
                    subsets_count,
                    total_size,
                    total_uncompressed_size: colormap_writer.uncompressed_size,
                }
                .serialize()[..],
            )
            .unwrap();

        colors_file.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::ColorIndexSerializer;
    use config::ColorIndexType;
    use std::io::Cursor;

    fn color_subset_encoding(colors: &[ColorIndexType]) {
        let mut buffer = Vec::new();

        ColorIndexSerializer::serialize_colors(&mut buffer, colors);

        ggcat_logging::info!("Buffer size: {}", buffer.len());
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
