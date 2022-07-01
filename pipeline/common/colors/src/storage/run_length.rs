use crate::async_slice_queue::AsyncSliceQueue;
use crate::storage::serializer::ColorsFlushProcessing;
use crate::storage::ColorsSerializerTrait;
use byteorder::ReadBytesExt;
use config::ColorIndexType;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use io::varint::{decode_varint, encode_varint};
use std::io::{Read, Write};

pub struct ColorIndexSerializer;
impl ColorIndexSerializer {
    pub fn serialize_colors(mut writer: impl Write, colors: &[ColorIndexType]) {
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
                        // Second order encoding saves space only if there are at least
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

    pub fn deserialize_colors(reader: impl Read, colors: &mut Vec<ColorIndexType>) {
        colors.clear();

        Self::deserialize_colors_diffs(reader, |c| colors.push(c));

        let mut last_color = colors[0];
        for i in 1..colors.len() {
            colors[i] += last_color;
            last_color = colors[i];
        }
    }
}

pub struct RunLengthColorsSerializer {
    async_buffer: AsyncSliceQueue<u8, ColorsFlushProcessing>,
}

#[thread_local]
static mut TEMP_COLOR_BUFFER: Vec<u8> = Vec::new();

impl ColorsSerializerTrait for RunLengthColorsSerializer {
    const MAGIC: [u8; 16] = *b"GGCAT_CMAP_RNLEN";

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

    fn new(writer: ColorsFlushProcessing, checkpoint_distance: usize, _colors_count: u64) -> Self {
        Self {
            async_buffer: AsyncSliceQueue::new(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                rayon::current_num_threads(),
                checkpoint_distance,
                writer,
            ),
        }
    }

    fn serialize_colors(&self, colors: &[u32]) -> u32 {
        unsafe {
            TEMP_COLOR_BUFFER.clear();
            ColorIndexSerializer::serialize_colors(&mut TEMP_COLOR_BUFFER, colors);
            self.async_buffer.add_data(TEMP_COLOR_BUFFER.as_slice()) as ColorIndexType
        }
    }

    fn get_subsets_count(&self) -> u64 {
        self.async_buffer.get_counter()
    }

    fn print_stats(&self) {
        println!("Total color subsets: {}", self.async_buffer.get_counter())
    }

    fn finalize(self) -> ColorsFlushProcessing {
        self.async_buffer.finish()
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
