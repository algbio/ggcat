use std::io::Cursor;

use colors::colors_manager::{
    ColorsManager, ColorsMergeManager,
    color_types::{PartialUnitigsColorStructure, TempUnitigColorStructure},
};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression,
    concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
    concurrent_filewriter::ConcurrentFileWriter,
};
use structs::partial_unitigs_extra_data::{
    IndirectReadInfo, PartialUnitigExtraData, PartialUnitigMode,
};

pub struct ReadExtractWorkData<CX: ColorsManager> {
    file_buffer: Vec<u8>,
    file_color_buffer:
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    output_buffer: Vec<u8>,
    color_data: TempUnitigColorStructure<CX>,
}

impl<CX: ColorsManager> ReadExtractWorkData<CX> {
    pub fn new() -> Self {
        Self {
            file_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            file_color_buffer: Default::default(),
            output_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            color_data: CX::ColorsMergeManagerType::alloc_unitig_color_structure(),
        }
    }
}

pub fn indirect_read_extract_all<'a, CX: ColorsManager>(
    workdata: &'a mut ReadExtractWorkData<CX>,
    k: usize,
    read: CompressedRead<'a>,
    extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    input_extra_buffer: &(
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        Vec<IndirectReadInfo>,
    ),
    output_extra_buffer: &mut <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    indirect_file: &ConcurrentFileWriter,
) -> (CompressedRead<'a>, PartialUnitigsColorStructure<CX>) {
    match extra.mode.clone() {
        PartialUnitigMode::Inline => (read, extra.colors.clone()),
        PartialUnitigMode::Indirect {
            indirection_start,
            indirections_range,
        } => {
            workdata.file_buffer.clear();
            workdata.output_buffer.clear();

            CX::ColorsMergeManagerType::reset_unitig_color_structure(&mut workdata.color_data);

            let mut bases_count = 0;
            read.sub_slice(0..indirection_start)
                .copy_to_buffer(&mut workdata.output_buffer);
            CX::ColorsMergeManagerType::join_structures::<false>(
                &mut workdata.color_data,
                &extra.colors,
                &input_extra_buffer.0,
                0,
                Some(indirection_start - k + 1),
            );

            bases_count += indirection_start;
            for part_idx in indirections_range {
                let part = input_extra_buffer.1[part_idx];
                workdata.file_buffer.clear();
                let extra_length = part.get_extra_length();
                let sequence_length = part.get_sequence_length();
                let is_rc = part.is_rc();
                let total_bytes = extra_length + sequence_length.div_ceil(4);
                indirect_file
                    .read_all_at(
                        &mut workdata.file_buffer,
                        part.get_file_offset() as u64,
                        total_bytes,
                    )
                    .unwrap();

                if CX::COLORS_ENABLED {
                    PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                        &mut workdata.file_color_buffer,
                    );
                    let color = PartialUnitigsColorStructure::<CX>::decode_extended(
                        &mut workdata.file_color_buffer,
                        &mut Cursor::new(&workdata.file_buffer[..extra_length]),
                        Default::default(),
                        0,
                    )
                    .unwrap();
                    CX::ColorsMergeManagerType::join_structures::<false>(
                        &mut workdata.color_data,
                        &color,
                        &workdata.file_color_buffer,
                        0,
                        None,
                    );
                }

                let part = CompressedRead::from_compressed_reads(
                    &workdata.file_buffer[extra_length..],
                    0,
                    sequence_length,
                );
                part.copy_to_buffer_with_offset(
                    &mut workdata.output_buffer,
                    bases_count % 4,
                    is_rc,
                );
                bases_count += part.get_length();
            }
            read.sub_slice(indirection_start..read.get_length())
                .copy_to_buffer_with_offset(&mut workdata.output_buffer, bases_count % 4, false);
            CX::ColorsMergeManagerType::join_structures::<false>(
                &mut workdata.color_data,
                &extra.colors,
                &input_extra_buffer.0,
                indirection_start - k + 1,
                None,
            );

            PartialUnitigsColorStructure::<CX>::clear_temp_buffer(output_extra_buffer);
            CX::ColorsMergeManagerType::encode_part_unitigs_colors(
                &mut workdata.color_data,
                output_extra_buffer,
            );

            bases_count += read.get_length() - indirection_start;

            (
                CompressedRead::from_compressed_reads(&workdata.output_buffer[..], 0, bases_count),
                extra.colors.clone(),
            )
        }
    }
}
