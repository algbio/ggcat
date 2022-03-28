// use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
// use crate::utils::compressed_read::CompressedRead;
// use parking_lot::Mutex;
//
// pub struct SingleIntermediateReadsThreadWriter<'a, T: SequenceExtraData> {
//     bucket: &'a Mutex<IntermediateReadsWriter<T>>,
//     buffer: Vec<u8>,
// }
//
// impl<'a, T: SequenceExtraData> SingleIntermediateReadsThreadWriter<'a, T> {
//     const ALLOWED_LEN: usize = 65536;
//
//     pub fn new(bucket: &'a Mutex<IntermediateReadsWriter<T>>) -> Self {
//         let buffer = Vec::with_capacity(parallel_processor::Utils::multiply_by(
//             Self::ALLOWED_LEN,
//             1.05,
//         ));
//
//         Self { bucket, buffer }
//     }
//
//     fn flush_buffer(&mut self) {
//         self.bucket.lock().write_batch_data(&self.buffer);
//         self.buffer.clear();
//     }
//
//     #[allow(non_camel_case_types)]
//     pub fn add_read<FLAGS_COUNT: typenum::Unsigned>(&mut self, el: T, seq: &[u8], flags: u8) {
//         if self.buffer.len() > 0 && self.buffer.len() + seq.len() > Self::ALLOWED_LEN {
//             self.flush_buffer();
//         }
//
//         el.encode(&mut self.buffer);
//         CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FLAGS_COUNT>(
//             seq,
//             &mut self.buffer,
//             flags,
//         );
//     }
//
//     pub fn finalize(self) {}
// }
//
// impl<'a, T: SequenceExtraData> Drop for SingleIntermediateReadsThreadWriter<'a, T> {
//     fn drop(&mut self) {
//         if self.buffer.len() > 0 {
//             self.flush_buffer();
//         }
//     }
// }
