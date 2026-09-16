use crate::bucket_colors::ColorRun;
use crate::storage::ColorsSerializerTrait;
use crate::storage::serializer::COLORMAP_STORAGE_VERSION;
use crate::storage::serializer::ColorsFileHeader;
use crate::storage::serializer::ColorsFlushProcessing;
use config::COLORS_SINGLE_INDEX_DEFAULT_COLORS;
use config::ColorIndexType;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use desse::Desse;
use io::varint::{BufVarintSource, VarintSource, encode_varint};
use parking_lot::Condvar;
use parking_lot::Mutex;
use serde::Serialize;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{BufRead, Write};
use std::ops::DerefMut;
use utils::resize_containers::ResizableVec;

fn bincode_serialize_ref<S: Write, D: Serialize>(ser: &mut S, data: &D) {
    bincode::serialize_into(ser, data).unwrap();
}

pub struct ColorIndexSerializer;
impl ColorIndexSerializer {
    /// Writes one color subset in the persistent color-map format, reading the
    /// set as the runs it is held as rather than as individual colors.
    ///
    /// Seen as a whole it is a first absolute color biased by two, then the
    /// sequence of differences between successive colors, then a zero.
    /// What the encoder does with that sequence
    /// depends only on how it breaks into maximal stretches of one repeated
    /// difference: a stretch of three or more is written as the escape `1`, the
    /// difference and the count, and a shorter one as its differences biased by
    /// one. (A literal is at least two, so neither the escape nor the terminator
    /// can be mistaken for one.)
    ///
    /// Runs hand those stretches over directly, which is why nothing here has to
    /// visit a color. Inside a run every difference is one, so a run of any
    /// length is a single stretch costing one iteration instead of its whole
    /// length. Between runs there is one difference, the gap, and because runs
    /// are maximal a gap is never one -- so a stretch of ones never spans a run
    /// boundary, and consecutive gaps join a stretch only where the runs between
    /// them hold a single color.
    pub fn serialize_colors(writer: &mut impl Write, runs: &[ColorRun]) {
        debug_assert!(!runs.is_empty());
        debug_assert!(
            runs.windows(2)
                .all(|window| window[0].end() < window[1].start() as u64)
        );

        let mut emit = |value: u64| encode_varint(|b| writer.write_all(b), value).unwrap();

        // A stretch of `count` differences all equal to `difference`.
        let stretch = |emit: &mut dyn FnMut(u64), difference: u64, count: u64| {
            if count >= 3 {
                emit(1);
                emit(difference);
                emit(count);
            } else {
                for _ in 0..count {
                    emit(difference + 1);
                }
            }
        };

        emit(runs[0].start() as u64 + 2);
        if runs[0].len() > 1 {
            stretch(&mut emit, 1, runs[0].len() - 1);
        }

        let mut index = 1;
        let mut last = runs[0].last() as u64;
        while index < runs.len() {
            let gap = runs[index].start() as u64 - last;
            let mut count = 1;
            last = runs[index].last() as u64;

            // The next gap joins this stretch only when no difference separates
            // them, which is to say only when this run holds one color.
            while runs[index].len() == 1
                && index + 1 < runs.len()
                && runs[index + 1].start() as u64 - last == gap
            {
                index += 1;
                count += 1;
                last = runs[index].last() as u64;
            }
            stretch(&mut emit, gap, count);

            if runs[index].len() > 1 {
                stretch(&mut emit, 1, runs[index].len() - 1);
            }
            index += 1;
        }
        emit(0);
    }

    /// Takes a [`VarintSource`] rather than a `Read` so that a caller holding a
    /// buffered stream gets the word-at-a-time varint decode; a caller with
    /// nothing but a stream still works, one byte at a time.
    #[inline(always)]
    pub fn deserialize_colors_diffs(
        source: &mut impl VarintSource,
        mut add_color: impl FnMut(ColorIndexType),
    ) -> Option<()> {
        add_color((source.next_varint()? - 2) as ColorIndexType);
        loop {
            let result = source.next_varint()? as ColorIndexType;
            if result == 0 {
                break;
            } else if result == 1 {
                // 2nd order encoding
                let value = source.next_varint()? as ColorIndexType;
                let count = source.next_varint()? as ColorIndexType;

                for _ in 0..count {
                    add_color(value);
                }
            } else {
                add_color(result - 1);
            }
        }
        Some(())
    }

    pub fn deserialize_colors(
        source: &mut impl VarintSource,
        colors: &mut Vec<ColorIndexType>,
    ) -> Option<()> {
        colors.clear();

        Self::deserialize_colors_diffs(source, |c| colors.push(c))?;

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

    fn decode_color(reader: &mut impl BufRead, out_vec: Option<&mut Vec<u32>>) {
        let mut source = BufVarintSource::new(reader);
        match out_vec {
            None => {
                ColorIndexSerializer::deserialize_colors_diffs(&mut source, |_| {});
            }
            Some(out_vec) => {
                ColorIndexSerializer::deserialize_colors(&mut source, out_vec);
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
    fn preserialize_colors(pre_serializer: &mut Self::PreSerializer, colors: &[ColorRun]) {
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
    use crate::bucket_colors::{ColorRun, runs_from_colors};
    use config::ColorIndexType;
    use io::varint::{BufVarintSource, ReadVarintSource, SliceVarintSource, encode_varint};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::io::{BufReader, Write};

    fn runs_of(colors: &[ColorIndexType]) -> Vec<ColorRun> {
        let mut runs = Vec::new();
        runs_from_colors(colors, &mut runs);
        runs
    }

    /// The encoder exactly as it was before it was driven by runs: one pass over
    /// the colors, discovering equal differences with a two-element look-ahead.
    /// Kept only here, as the reference the run-driven encoder is held to. The
    /// color map is a persistent format, so "equivalent" is not enough -- the
    /// bytes have to match.
    fn serialize_colors_from_colors(writer: &mut impl Write, colors: &[ColorIndexType]) {
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

    /// Every source has to decode the same subset. The buffered one is checked
    /// at capacities that cut the buffer mid-varint, so its byte fallback is
    /// exercised as well as its word path.
    fn color_subset_encoding(colors: &[ColorIndexType]) {
        let mut buffer = Vec::new();

        ColorIndexSerializer::serialize_colors(&mut buffer, &runs_of(colors));

        let mut reference = Vec::new();
        serialize_colors_from_colors(&mut reference, colors);
        assert_eq!(buffer, reference, "{colors:?}");

        let mut des_colors = Vec::new();
        let mut source = SliceVarintSource::new(&buffer);
        ColorIndexSerializer::deserialize_colors(&mut source, &mut des_colors);
        assert_eq!(colors, des_colors.as_slice());
        assert_eq!(source.position(), buffer.len());

        for capacity in [1, 3, 8, 64] {
            let mut buffered = BufReader::with_capacity(capacity, buffer.as_slice());
            let mut source = BufVarintSource::new(&mut buffered);
            let mut des_colors = Vec::new();
            ColorIndexSerializer::deserialize_colors(&mut source, &mut des_colors);
            assert_eq!(colors, des_colors.as_slice(), "at capacity {capacity}");
        }

        let mut stream = buffer.as_slice();
        let mut source = ReadVarintSource::new(&mut stream);
        let mut des_colors = Vec::new();
        ColorIndexSerializer::deserialize_colors(&mut source, &mut des_colors);
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
            3, 6, 9, 12, 70, 71, 72, 73, 74, 88, 95, 100, 105, 110, 198, 384,
        ]);
    }

    /// The shapes where reading runs instead of colors could diverge: equal gaps
    /// spanning several one-color runs, runs at exactly the two and three
    /// element thresholds of the second-order escape, a run adjoining a stretch
    /// of equal gaps, and the top of the color space.
    #[test]
    fn run_driven_encoding_matches_the_reference_on_the_awkward_shapes() {
        let cases: Vec<Vec<ColorIndexType>> = vec![
            // Equal gaps across one-color runs: a second-order stretch that a
            // run-driven encoder only sees by joining consecutive gaps.
            vec![0, 5, 10, 15, 20],
            vec![0, 5, 10],
            vec![0, 5, 10, 15, 16, 17, 18],
            // A run, then equal gaps, then another run.
            vec![0, 1, 2, 3, 10, 20, 30, 40, 41, 42, 43, 44],
            // Runs at each threshold.
            vec![0, 1],
            vec![0, 1, 2],
            vec![0, 1, 2, 3],
            vec![0, 1, 2, 3, 100, 101],
            // Gaps equal to a run's interior difference must not join it.
            vec![0, 1, 2, 3, 4, 5, 6],
            // Alternating, so no stretch ever reaches three.
            vec![0, 2, 5, 7, 10, 12],
            // The top of the space. A set spanning it from zero is left out on
            // purpose: the persistent format writes differences biased by one as
            // `ColorIndexType`, so a difference of the whole space has never
            // been expressible in it. Color identifiers are handed out densely
            // from zero, so no difference can reach that far.
            vec![u32::MAX],
            vec![u32::MAX - 1, u32::MAX],
            vec![u32::MAX - 3, u32::MAX - 2, u32::MAX - 1, u32::MAX],
        ];
        for colors in cases {
            color_subset_encoding(&colors);
        }
    }

    /// Randomized sets over the same mix of consecutive and sparse colors the
    /// bucket codec is exercised with. The color map is persistent, so this is
    /// the check that the format did not move.
    #[test]
    fn run_driven_encoding_matches_the_reference_on_random_sets() {
        let mut rng = StdRng::seed_from_u64(0x0c01_0125);
        for _ in 0..5000 {
            let mut colors = vec![rng.gen_range(0..1000u32)];
            for _ in 0..rng.gen_range(0..200) {
                // A mix heavy enough in unit gaps to build long runs, and in
                // repeated gaps to build second-order stretches out of them.
                let gap = match rng.gen_range(0..10) {
                    0..=5 => 1,
                    6 | 7 => 3,
                    8 => rng.gen_range(2..10),
                    _ => rng.gen_range(2..100000),
                };
                colors.push(colors.last().unwrap() + gap);
            }
            color_subset_encoding(&colors);
        }
    }

    /// A run of any length is one iteration rather than one per color, so a
    /// million consecutive colors must encode without ever touching one.
    #[test]
    fn a_long_run_encodes_from_its_two_endpoints() {
        let mut buffer = Vec::new();
        ColorIndexSerializer::serialize_colors(&mut buffer, &[ColorRun::new(0, 1_000_000)]);

        let colors: Vec<ColorIndexType> = (0..1_000_000).collect();
        let mut reference = Vec::new();
        serialize_colors_from_colors(&mut reference, &colors);
        assert_eq!(buffer, reference);

        let mut decoded = Vec::new();
        let mut source = SliceVarintSource::new(&buffer);
        ColorIndexSerializer::deserialize_colors(&mut source, &mut decoded);
        assert_eq!(decoded, colors);
    }
}
