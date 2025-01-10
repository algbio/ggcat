// use byteorder::ReadBytesExt;
// use colors::colors_manager::{color_types::PartialUnitigsColorStructure, ColorsManager};
// use io::{
//     concurrent::temp_reads::extra_data::{
//         SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
//     },
//     varint::{decode_varint, encode_varint, VARINT_MAX_SIZE},
// };

// #[derive(Clone, Debug)]
// pub struct ExtraCompactedData<CX: ColorsManager> {
//     pub multiplicity: u64,
//     pub colors: PartialUnitigsColorStructure<CX>,
// }

// impl<CX: ColorsManager> SequenceExtraDataTempBufferManagement for ExtraCompactedData<CX> {
//     type TempBuffer =
//         <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer;

//     fn new_temp_buffer() -> Self::TempBuffer {
//         <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::new_temp_buffer(
//         )
//     }

//     fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
//         <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::clear_temp_buffer(buffer);
//     }

//     fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
//         <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::copy_temp_buffer(dest, src);
//     }

//     fn copy_extra_from(
//         mut extra: Self,
//         src: &Self::TempBuffer,
//         dst: &mut Self::TempBuffer,
//     ) -> Self {
//         extra.colors = <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::copy_extra_from(extra.colors, src, dst);
//         extra
//     }
// }

// impl<CX: ColorsManager> SequenceExtraDataConsecutiveCompression for ExtraCompactedData<CX> {
//     type LastData =
//         <PartialUnitigsColorStructure<CX> as SequenceExtraDataConsecutiveCompression>::LastData;

//     fn decode_extended(
//         buffer: &mut Self::TempBuffer,
//         reader: &mut impl std::io::Read,
//         last_data: Self::LastData,
//     ) -> Option<Self> {
//         let multiplicity = decode_varint(|| reader.read_u8().ok())?;
//         let colors =
//             PartialUnitigsColorStructure::<CX>::decode_extended(buffer, reader, last_data)?;
//         Some(Self {
//             multiplicity,
//             colors,
//         })
//     }

//     fn encode_extended(
//         &self,
//         buffer: &Self::TempBuffer,
//         writer: &mut impl std::io::Write,
//         last_data: Self::LastData,
//     ) {
//         encode_varint(|b| writer.write_all(b), self.multiplicity).unwrap();
//         self.colors.encode_extended(buffer, writer, last_data);
//     }

//     fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
//         self.colors.obtain_last_data(last_data)
//     }

//     fn max_size(&self) -> usize {
//         self.colors.max_size() + VARINT_MAX_SIZE
//     }
// }
