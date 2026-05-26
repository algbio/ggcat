use std::io::Cursor;

use colors::colors_manager::{
    ColorsManager, ColorsMergeManager,
    color_types::{PartialUnitigsColorStructure, TempUnitigColorStructure},
};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use hashes::HashableSequence;

use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression,
    concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
    concurrent_filewriter::ConcurrentFileWriter,
    partial_unitigs_extra_data::{IndirectReadInfo, PartialUnitigExtraData, PartialUnitigMode},
};

pub struct ReadExtractWorkData<CX: ColorsManager> {
    pub file_buffer: Vec<u8>,
    pub file_color_buffer:
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    output_buffer: Vec<u8>,
    output_extra_buffer:
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    color_data: TempUnitigColorStructure<CX>,
}

impl<CX: ColorsManager> ReadExtractWorkData<CX> {
    pub fn new() -> Self {
        Self {
            file_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            file_color_buffer: Default::default(),
            output_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            output_extra_buffer: PartialUnitigsColorStructure::<CX>::new_temp_buffer(),
            color_data: CX::ColorsMergeManagerType::alloc_unitig_color_structure(),
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
    file_color_buffer: &mut <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    k: usize,
    read: CompressedRead,
    extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    input_extra_buffer: &(
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        Vec<IndirectReadInfo>,
    ),
    indirect_file: &ConcurrentFileWriter,
    mut parts_callback: impl for<'a> FnMut(
        CompressedRead<'a>,
        PartialUnitigsColorStructure<CX>,
        (usize, Option<usize>),
        bool,
        &<PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ),
) {
    match extra.mode.clone() {
        PartialUnitigMode::Inline => {
            parts_callback(read, extra.colors, (0, None), false, &input_extra_buffer.0)
        }
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

                parts_callback(part, color, (0, None), is_rc, &file_color_buffer);
            }

            let suffix = read.sub_slice(indirection_start..read.get_length());
            parts_callback(
                suffix,
                extra.colors,
                (indirection_start - k + 1, None),
                false,
                &input_extra_buffer.0,
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
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        Vec<IndirectReadInfo>,
    ),
    indirect_file: &ConcurrentFileWriter,
) -> (
    CompressedRead<'a>,
    PartialUnitigsColorStructure<CX>,
    &'a <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
) {
    match extra.mode.clone() {
        PartialUnitigMode::Inline => (read, extra.colors.clone(), &input_extra_buffer.0),
        PartialUnitigMode::Indirect { .. } => {
            workdata.output_buffer.clear();
            CX::ColorsMergeManagerType::reset_unitig_color_structure(&mut workdata.color_data);

            let mut bases_count = 0;

            indirect_read_extract_parts::<CX, true, true>(
                &mut workdata.file_buffer,
                &mut workdata.file_color_buffer,
                k,
                read,
                extra,
                input_extra_buffer,
                indirect_file,
                |read, color, color_range, is_rc, color_buffer| {
                    read.copy_to_buffer_with_offset(
                        &mut workdata.output_buffer,
                        bases_count % 4,
                        is_rc,
                    );
                    bases_count += read.bases_count();
                    if CX::COLORS_ENABLED {
                        if is_rc {
                            CX::ColorsMergeManagerType::join_structures::<true>(
                                &mut workdata.color_data,
                                &color,
                                color_buffer,
                                color_range.0,
                                color_range.1,
                            );
                        } else {
                            CX::ColorsMergeManagerType::join_structures::<false>(
                                &mut workdata.color_data,
                                &color,
                                color_buffer,
                                color_range.0,
                                color_range.1,
                            );
                        }
                    }
                },
            );

            PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                &mut workdata.output_extra_buffer,
            );
            CX::ColorsMergeManagerType::encode_part_unitigs_colors(
                &mut workdata.color_data,
                &mut workdata.output_extra_buffer,
            );

            (
                CompressedRead::from_compressed_reads(&workdata.output_buffer[..], 0, bases_count),
                extra.colors.clone(),
                &workdata.output_extra_buffer,
            )
        }
    }
}
