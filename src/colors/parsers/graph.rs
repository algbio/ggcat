//
//
// #[derive(Clone, Debug, Eq, PartialEq)]
// pub enum MinBkSingleColor {
//     SingleColor(ColorIndexType),
//     // FIXME: Manage allocations in a better way
//     MultipleColors(Vec<(usize, ColorIndexType)>),
// }
//
// impl Default for MinBkSingleColor {
//     fn default() -> Self {
//         Self::SingleColor(0)
//     }
// }
//
// pub struct MinBkSingleColorIterator<'a> {
//     reference: &'a MinBkSingleColor,
//     vec_pos: usize,
//     iter_idx: usize,
// }
//
// impl<'a> Iterator for MinBkSingleColorIterator<'a> {
//     type Item = ColorIndexType;
//
//     #[inline(always)]
//     fn next(&mut self) -> Option<Self::Item> {
//         Some(match self.reference {
//             MinBkSingleColor::SingleColor(color) => *color,
//             MinBkSingleColor::MultipleColors(colors) => {
//                 while (self.vec_pos + 1) < colors.len()
//                     && colors[self.vec_pos + 1].0 <= self.iter_idx
//                 {
//                     self.vec_pos += 1;
//                 }
//                 self.iter_idx += 1;
//
//                 colors[self.vec_pos].1
//             }
//         })
//     }
// }
//
// #[inline(always)]
// fn decode_minbk_color(get_byte_fn: impl Fn() -> Option<u8>) -> Option<MinBkSingleColor> {
//     let (x, flag) = decode_varint_flags::<_, typenum::consts::U1>(&get_byte_fn)?;
//     Some(if flag == 0 {
//         MinBkSingleColor::SingleColor(x as ColorIndexType)
//     } else {
//         let mut tmp_vec = Vec::with_capacity(x as usize);
//         for _ in 0..x {
//             let position = decode_varint(&get_byte_fn)? as usize;
//             let color = decode_varint(&get_byte_fn)? as ColorIndexType;
//             tmp_vec.push((position, color));
//         }
//         MinBkSingleColor::MultipleColors(tmp_vec)
//     })
// }
//
// impl SequenceExtraData for MinBkSingleColor {
//     fn decode_from_slice(slice: &[u8]) -> Option<Self> {
//         let mut index = 0;
//         decode_minbk_color(|| {
//             let data = slice[index];
//             index += 1;
//             Some(data)
//         })
//     }
//
//     unsafe fn decode_from_pointer(mut ptr: *const u8) -> Option<Self> {
//         decode_minbk_color(|| {
//             let data = *ptr;
//             ptr = ptr.add(1);
//             Some(data)
//         })
//     }
//
//     fn decode<'a>(reader: &'a mut impl Read) -> Option<Self> {
//         decode_minbk_color(|| reader.read_u8().ok())
//     }
//
//     fn encode<'a>(&self, writer: &'a mut impl Write) {
//         match self {
//             MinBkSingleColor::SingleColor(file_index) => {
//                 encode_varint_flags::<_, _, typenum::consts::U1>(
//                     |b| writer.write_all(b),
//                     *file_index as u64,
//                     0,
//                 )
//                     .unwrap();
//             }
//             MinBkSingleColor::MultipleColors(colors_list) => {
//                 encode_varint_flags::<_, _, typenum::consts::U1>(
//                     |b| writer.write_all(b),
//                     colors_list.len() as u64,
//                     0,
//                 )
//                     .unwrap();
//
//                 for (pos, color) in colors_list.iter() {
//                     encode_varint(|b| writer.write_all(b), *pos as u64).unwrap();
//                     encode_varint(|b| writer.write_all(b), *color as u64).unwrap();
//                 }
//             }
//         }
//     }
//
//     #[inline(always)]
//     fn max_size(&self) -> usize {
//         9
//     }
// }
//
// fn parse_colors(ident: &[u8]) -> Vec<(usize, ColorIndexType)> {
//     let mut colors = vec![];
//
//     for col_pos in ident.find_iter(b"C:") {
//         let (color_index, next_pos) = ColorIndexType::from_radix_16(&ident[col_pos..]);
//         let position_index = usize::from_radix_10(&ident[(col_pos + next_pos)..]).0;
//         colors.push((position_index, color_index));
//     }
//
//     colors
// }
//
// impl MinimizerBucketingSeqColorData for MinBkSingleColor {
//     type KmerColor = ColorIndexType;
//     type KmerColorIterator<'a> = MinBkSingleColorIterator<'a>;
//
//     fn create(sequence_info: SingleSequenceInfo) -> Self {
//         match sequence_info {
//             SingleSequenceInfo::SingleReference { file_index } => {
//                 Self::SingleColor(file_index as ColorIndexType)
//             }
//             SingleSequenceInfo::MultiReferenceGraph { sequence_ident } => {
//                 Self::MultipleColors(parse_colors(sequence_ident))
//             }
//         }
//     }
//
//     fn get_iterator<'a>(&'a self) -> Self::KmerColorIterator<'a> {
//         MinBkSingleColorIterator {
//             reference: self,
//             vec_pos: 0,
//             iter_idx: 0,
//         }
//     }
// }
