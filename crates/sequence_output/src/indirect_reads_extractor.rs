use std::io::Cursor;

use colors::colors_manager::{
    ColorsManager, ColorsMergeManager, color_types::PartialUnitigsColorStructure,
};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use hashes::HashableSequence;

use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::extra_data::{
        SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement, TempBuffer,
    },
    concurrent_filewriter::ConcurrentFileWriter,
    partial_unitigs_extra_data::{IndirectReadInfo, PartialUnitigExtraData, PartialUnitigMode},
};

pub struct ReadExtractWorkData<CX: ColorsManager> {
    pub file_buffer: Vec<u8>,
    pub file_color_buffer: TempBuffer<PartialUnitigsColorStructure<CX>>,
    pub output_buffer: Vec<u8>,
    pub output_extra_buffer: TempBuffer<PartialUnitigsColorStructure<CX>>,
}

impl<CX: ColorsManager> ReadExtractWorkData<CX> {
    pub fn new() -> Self {
        Self {
            file_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            file_color_buffer: Default::default(),
            output_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            output_extra_buffer: PartialUnitigsColorStructure::<CX>::new_temp_buffer(),
        }
    }
}

#[inline(always)]
pub fn indirect_read_extract_parts<
    CX: ColorsManager,
    const EXTRACT_COLORS: bool,
    const EXTRACT_SEQUENCE: bool,
>(
    file_buffer: &mut Vec<u8>,
    file_color_buffer: &mut TempBuffer<PartialUnitigsColorStructure<CX>>,
    k: usize,
    read: CompressedRead,
    extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    input_extra_buffer: &(
        TempBuffer<PartialUnitigsColorStructure<CX>>,
        Vec<IndirectReadInfo>,
    ),
    indirect_file: &ConcurrentFileWriter,
    mut parts_callback: impl for<'a> FnMut(
        CompressedRead<'a>,
        PartialUnitigsColorStructure<CX>,
        (usize, Option<usize>),
        bool,
        &TempBuffer<PartialUnitigsColorStructure<CX>>,
        bool,
    ),
) {
    match extra.mode.clone() {
        PartialUnitigMode::Inline => parts_callback(
            read,
            extra.colors,
            (0, None),
            false,
            &input_extra_buffer.0,
            true,
        ),
        PartialUnitigMode::Indirect {
            indirection_start,
            indirections_range,
        } => {
            let prefix = read.sub_slice(0..indirection_start);

            parts_callback(
                prefix,
                extra.colors,
                (0, Some(indirection_start - k + 1)),
                false,
                &input_extra_buffer.0,
                false,
            );

            for part_idx in indirections_range {
                let part = input_extra_buffer.1[part_idx];
                file_buffer.clear();
                let extra_length = part.get_extra_length();
                let sequence_length = part.get_sequence_length();
                let is_rc = part.is_rc();

                let extra_offset = if EXTRACT_COLORS { 0 } else { extra_length };

                let total_bytes = if EXTRACT_COLORS { extra_length } else { 0 }
                    + if EXTRACT_SEQUENCE {
                        sequence_length.div_ceil(4)
                    } else {
                        0
                    };
                indirect_file
                    .read_all_at(
                        file_buffer,
                        (part.get_file_offset() + extra_offset) as u64,
                        total_bytes,
                    )
                    .unwrap();

                let color = if CX::COLORS_ENABLED && EXTRACT_COLORS {
                    PartialUnitigsColorStructure::<CX>::clear_temp_buffer(file_color_buffer);
                    PartialUnitigsColorStructure::<CX>::decode_extended(
                        file_color_buffer,
                        &mut Cursor::new(&file_buffer[..extra_length]),
                        Default::default(),
                        0,
                    )
                    .unwrap()
                } else {
                    Default::default()
                };

                let part = if EXTRACT_SEQUENCE {
                    CompressedRead::from_compressed_reads(
                        &file_buffer[(extra_length - extra_offset)..],
                        0,
                        sequence_length,
                    )
                } else {
                    CompressedRead::new_offset(&[], 0, 0)
                };

                parts_callback(part, color, (0, None), is_rc, &file_color_buffer, false);
            }

            let suffix = read.sub_slice(indirection_start..read.get_length());
            parts_callback(
                suffix,
                extra.colors,
                (indirection_start - k + 1, None),
                false,
                &input_extra_buffer.0,
                true,
            );
        }
    }
}

pub fn indirect_read_extract_all<'a, 'b, CX: ColorsManager>(
    workdata: &'a mut ReadExtractWorkData<CX>,
    k: usize,
    read: CompressedRead<'a>,
    extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    input_extra_buffer: &'a (
        TempBuffer<PartialUnitigsColorStructure<CX>>,
        Vec<IndirectReadInfo>,
    ),
    indirect_file: &ConcurrentFileWriter,
) -> (
    CompressedRead<'a>,
    PartialUnitigsColorStructure<CX>,
    &'a TempBuffer<PartialUnitigsColorStructure<CX>>,
) {
    match extra.mode.clone() {
        PartialUnitigMode::Inline => (read, extra.colors.clone(), &input_extra_buffer.0),
        PartialUnitigMode::Indirect { .. } => {
            workdata.output_buffer.clear();
            PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                &mut workdata.output_extra_buffer,
            );

            let mut bases_count = 0;

            indirect_read_extract_parts::<CX, true, true>(
                &mut workdata.file_buffer,
                &mut workdata.file_color_buffer,
                k,
                read,
                extra,
                input_extra_buffer,
                indirect_file,
                |read, color, color_range, is_rc, color_buffer, _| {
                    read.copy_to_buffer_with_offset(
                        &mut workdata.output_buffer,
                        bases_count % 4,
                        is_rc,
                    );
                    bases_count += read.bases_count();
                    if CX::COLORS_ENABLED {
                        if is_rc {
                            CX::ColorsMergeManagerType::join_structures::<true>(
                                &mut workdata.output_extra_buffer,
                                &color,
                                color_buffer,
                                color_range.0,
                                color_range.1,
                            );
                        } else {
                            CX::ColorsMergeManagerType::join_structures::<false>(
                                &mut workdata.output_extra_buffer,
                                &color,
                                color_buffer,
                                color_range.0,
                                color_range.1,
                            );
                        }
                    }
                },
            );

            let colors = CX::ColorsMergeManagerType::get_color_from_fully_joined_buffer(
                &mut workdata.output_extra_buffer,
            );

            (
                CompressedRead::from_compressed_reads(&workdata.output_buffer[..], 0, bases_count),
                colors,
                &workdata.output_extra_buffer,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Cursor,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use colors::{
        bundles::multifile_building::ColorBundleMultifileBuilding,
        colors_manager::color_types::PartialUnitigsColorStructure,
    };
    #[cfg(feature = "support_kmer_counters")]
    use io::partial_unitigs_extra_data::SequenceAbundance;
    use io::{
        compressed_read::CompressedReadIndipendent,
        concurrent::temp_reads::extra_data::{
            SequenceExtraData, SequenceExtraDataTempBufferManagement,
        },
        concurrent_filewriter::ConcurrentFileWriter,
        ident_writer::IdentSequenceWriter,
        partial_unitigs_extra_data::{PartialUnitigExtraData, PartialUnitigMode},
        varint::encode_varint,
    };

    use crate::sequences_joiner::IndirectSequencesJoiner;

    use super::{ReadExtractWorkData, indirect_read_extract_all};

    type TestColors = PartialUnitigsColorStructure<ColorBundleMultifileBuilding>;
    type TestExtra = PartialUnitigExtraData<TestColors>;
    type TestExtraBuffer = <TestExtra as SequenceExtraDataTempBufferManagement>::TempBuffer;

    fn new_temp_path(prefix: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{}_{}.bin", std::process::id(), now))
    }

    fn build_inline_extra(runs: &[(u32, usize)]) -> (TestExtra, TestExtraBuffer) {
        let mut encoded = Vec::new();
        encode_varint(
            |bytes| Some(encoded.extend_from_slice(bytes)),
            runs.len() as u64,
        )
        .unwrap();
        for (color, count) in runs {
            encode_varint(
                |bytes| Some(encoded.extend_from_slice(bytes)),
                *color as u64,
            )
            .unwrap();
            encode_varint(
                |bytes| Some(encoded.extend_from_slice(bytes)),
                *count as u64,
            )
            .unwrap();
        }

        let mut extra_buffer = TestExtra::new_temp_buffer();
        let colors = TestColors::decode_extended(&mut extra_buffer.0, &mut Cursor::new(encoded))
            .expect("valid encoded test colors");

        (
            TestExtra {
                colors,
                mode: PartialUnitigMode::Inline,
                #[cfg(feature = "support_kmer_counters")]
                counters: SequenceAbundance {
                    first: 1,
                    sum: runs.iter().map(|(_, count)| *count as u64).sum(),
                    last: 1,
                },
            },
            extra_buffer,
        )
    }

    fn color_runs_to_ident(
        colors: &TestColors,
        buffer: &<TestColors as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> String {
        let mut output = Vec::new();
        let mut partial = None;
        colors.write_as_ident(&mut partial, (0, None), false, &mut output, buffer);
        TestColors::flush_partial_as_ident(partial, &mut output);
        String::from_utf8(output).unwrap()
    }

    fn append_colored_sequence(
        joiner: &mut IndirectSequencesJoiner<ColorBundleMultifileBuilding>,
        sequence: &str,
        runs: &[(u32, usize)],
        overlap: usize,
    ) {
        let mut storage = Vec::new();
        let read = CompressedReadIndipendent::from_plain(sequence.as_bytes(), &mut storage);
        let (extra, extra_buffer) = build_inline_extra(runs);
        joiner.append_sequence(
            read.as_reference(&storage),
            &extra,
            overlap,
            false,
            &extra_buffer,
            None,
        );
    }

    fn extract_joined_color_ident(
        k: usize,
        writer: &ConcurrentFileWriter,
        joiner: &IndirectSequencesJoiner<ColorBundleMultifileBuilding>,
    ) -> String {
        let joined = joiner.get_sequence();
        let mut workdata = ReadExtractWorkData::<ColorBundleMultifileBuilding>::new();
        let (_, extracted_colors, extracted_buffer) = indirect_read_extract_all(
            &mut workdata,
            k,
            joined.sequence,
            &joined.extra,
            joined.extra_buffer,
            writer,
        );

        color_runs_to_ident(&extracted_colors, extracted_buffer)
    }

    #[test]
    fn extract_all_preserves_colors_for_indirected_join() {
        let k = 5;
        let left_len = 2300;
        let right_len = 2305;

        let left_seq = "A".repeat(left_len);
        let right_seq = "A".repeat(k) + &"C".repeat(right_len - k);

        let left_runs = [(0x1, left_len - k + 1)];
        let right_runs = [(0x2, right_len - k + 1)];

        let temp_path = new_temp_path("indirect_extract_colors");
        let writer = ConcurrentFileWriter::create(&temp_path).unwrap();
        let mut joiner =
            IndirectSequencesJoiner::<ColorBundleMultifileBuilding>::new(k, writer.clone());

        let mut left_storage = Vec::new();
        let left_read =
            CompressedReadIndipendent::from_plain(left_seq.as_bytes(), &mut left_storage);
        let (left_extra, left_extra_buffer) = build_inline_extra(&left_runs);
        joiner.append_sequence(
            left_read.as_reference(&left_storage),
            &left_extra,
            0,
            false,
            &left_extra_buffer,
            None,
        );

        let mut right_storage = Vec::new();
        let right_read =
            CompressedReadIndipendent::from_plain(right_seq.as_bytes(), &mut right_storage);
        let (right_extra, right_extra_buffer) = build_inline_extra(&right_runs);
        joiner.append_sequence(
            right_read.as_reference(&right_storage),
            &right_extra,
            k,
            false,
            &right_extra_buffer,
            None,
        );

        let joined = joiner.get_sequence();
        assert!(matches!(
            joined.extra.mode,
            PartialUnitigMode::Indirect { .. }
        ));

        let mut workdata = ReadExtractWorkData::<ColorBundleMultifileBuilding>::new();
        let (_, extracted_colors, extracted_buffer) = indirect_read_extract_all(
            &mut workdata,
            k,
            joined.sequence,
            &joined.extra,
            joined.extra_buffer,
            &writer,
        );

        let expected_ident = format!(" C:1:{} C:2:{}", left_len - k + 1, right_len - k,);
        assert_eq!(
            color_runs_to_ident(&extracted_colors, extracted_buffer),
            expected_ident
        );

        fs::remove_file(&temp_path).unwrap();
    }

    #[test]
    fn append_into_existing_indirection_preserves_colors() {
        let k = 5;
        let left_len = 2300;
        let middle_len = 2305;
        let right_len = 2207;

        let temp_path = new_temp_path("indirect_join_append_colors");
        let writer = ConcurrentFileWriter::create(&temp_path).unwrap();
        let mut joiner =
            IndirectSequencesJoiner::<ColorBundleMultifileBuilding>::new(k, writer.clone());

        append_colored_sequence(
            &mut joiner,
            &"A".repeat(left_len),
            &[(0x1, left_len - k + 1)],
            0,
        );
        append_colored_sequence(
            &mut joiner,
            &("A".repeat(k) + &"C".repeat(middle_len - k)),
            &[(0x2, middle_len - k + 1)],
            k,
        );
        assert!(matches!(
            joiner.get_sequence().extra.mode,
            PartialUnitigMode::Indirect { .. }
        ));

        append_colored_sequence(
            &mut joiner,
            &("C".repeat(k) + &"G".repeat(right_len - k)),
            &[(0x3, right_len - k + 1)],
            k,
        );

        assert_eq!(
            extract_joined_color_ident(k, &writer, &joiner),
            format!(
                " C:1:{} C:2:{} C:3:{}",
                left_len - k + 1,
                middle_len - k,
                right_len - k,
            )
        );

        fs::remove_file(&temp_path).unwrap();
    }

    #[test]
    fn merge_two_indirected_unitigs_preserves_colors() {
        let k = 5;
        let first_len = 2300;
        let second_len = 2305;
        let third_len = 2310;
        let fourth_len = 2315;

        let temp_path = new_temp_path("indirect_join_merge_colors");
        let writer = ConcurrentFileWriter::create(&temp_path).unwrap();

        let mut left_joiner =
            IndirectSequencesJoiner::<ColorBundleMultifileBuilding>::new(k, writer.clone());
        append_colored_sequence(
            &mut left_joiner,
            &"A".repeat(first_len),
            &[(0x1, first_len - k + 1)],
            0,
        );
        append_colored_sequence(
            &mut left_joiner,
            &("A".repeat(k) + &"C".repeat(second_len - k)),
            &[(0x2, second_len - k + 1)],
            k,
        );
        let left_joined = left_joiner.get_sequence();
        assert!(matches!(
            left_joined.extra.mode,
            PartialUnitigMode::Indirect { .. }
        ));

        let mut right_joiner =
            IndirectSequencesJoiner::<ColorBundleMultifileBuilding>::new(k, writer.clone());
        append_colored_sequence(
            &mut right_joiner,
            &"C".repeat(third_len),
            &[(0x3, third_len - k + 1)],
            0,
        );
        append_colored_sequence(
            &mut right_joiner,
            &("C".repeat(k) + &"G".repeat(fourth_len - k)),
            &[(0x4, fourth_len - k + 1)],
            k,
        );
        let right_joined = right_joiner.get_sequence();
        assert!(matches!(
            right_joined.extra.mode,
            PartialUnitigMode::Indirect { .. }
        ));

        let mut merged_joiner =
            IndirectSequencesJoiner::<ColorBundleMultifileBuilding>::new(k, writer.clone());
        merged_joiner.append_sequence(
            left_joined.sequence,
            &left_joined.extra,
            0,
            false,
            left_joined.extra_buffer,
            None,
        );
        merged_joiner.append_sequence(
            right_joined.sequence,
            &right_joined.extra,
            k,
            false,
            right_joined.extra_buffer,
            None,
        );

        assert_eq!(
            extract_joined_color_ident(k, &writer, &merged_joiner),
            format!(
                " C:1:{} C:2:{} C:3:{} C:4:{}",
                first_len - k + 1,
                second_len - k,
                third_len - k,
                fourth_len - k,
            )
        );

        fs::remove_file(&temp_path).unwrap();
    }
}
