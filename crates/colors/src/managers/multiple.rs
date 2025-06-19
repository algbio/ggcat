use crate::DefaultColorsSerializer;
use crate::colors_manager::ColorsMergeManager;
use crate::colors_memmap_writer::ColorsMemMapWriter;
use atoi::{FromRadix10, FromRadix16};
use bstr::ByteSlice;
use byteorder::ReadBytesExt;
use config::{ColorCounterType, ColorIndexType, DEFAULT_OUTPUT_BUFFER_SIZE};
use hashbrown::HashMap;
use hashes::ExtendableHashTraitType;
use hashes::{HashFunction, HashFunctionFactory};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use itertools::Itertools;
use nightly_quirks::slice_partition_dedup::SlicePartitionDedup;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::Path;
use std::slice::from_raw_parts;
use structs::map_entry::MapEntry;
use utils::inline_vec::{AllocatorU32, InlineVec};
use utils::resize_containers::ResizableVec;

struct ColorEntry {
    tracking_counter_or_color: u64,
    colors: InlineVec<ColorIndexType, 2>,
}

impl ColorEntry {
    const REACHED_MULTIPLICITY_FLAG_SHIFT: usize = size_of::<u64>() * 8 - 1;
    const REACHED_MULTIPLICITY_FLAG: u64 = 1 << Self::REACHED_MULTIPLICITY_FLAG_SHIFT;

    fn reached_multiplicity(&self) -> bool {
        self.tracking_counter_or_color & Self::REACHED_MULTIPLICITY_FLAG != 0
            && self.tracking_counter_or_color & !Self::REACHED_MULTIPLICITY_FLAG > 0
    }

    fn get_counter(&self) -> u64 {
        self.tracking_counter_or_color & !Self::REACHED_MULTIPLICITY_FLAG
    }
}

pub struct MultipleColorsManager {
    colors_buffer: AllocatorU32,
    colors_list: ResizableVec<ColorEntry, DEFAULT_OUTPUT_BUFFER_SIZE>,
    last_color_index: usize,
    last_switch_color_index: usize,
}
pub type HashMapTempColorIndex = usize;

#[inline]
fn get_entry_color(
    manager: &MultipleColorsManager,
    entry: &MapEntry<HashMapTempColorIndex>,
) -> ColorIndexType {
    manager.colors_list[entry.color_index].tracking_counter_or_color as ColorIndexType
}

impl ColorsMergeManager for MultipleColorsManager {
    type SingleKmerColorDataType = ColorIndexType;
    type GlobalColorsTableWriter = ColorsMemMapWriter<DefaultColorsSerializer>;
    type GlobalColorsTableReader = ();

    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: &[String],
        threads_count: usize,
        print_stats: bool,
    ) -> anyhow::Result<Self::GlobalColorsTableWriter> {
        ColorsMemMapWriter::new(path, color_names, threads_count, print_stats)
    }

    fn open_colors_table(_path: impl AsRef<Path>) -> anyhow::Result<Self::GlobalColorsTableReader> {
        Ok(())
    }

    type ColorsBufferTempStructure = Self;

    fn allocate_temp_buffer_structure(_temp_dir: &Path) -> Self::ColorsBufferTempStructure {
        Self {
            colors_buffer: AllocatorU32::new(DEFAULT_OUTPUT_BUFFER_SIZE),
            colors_list: ResizableVec::new(),
            last_color_index: 0,
            last_switch_color_index: 0,
        }
    }

    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure) {
        data.colors_buffer.reset();
        data.colors_list.clear();
    }

    fn add_temp_buffer_structure_el<MH: HashFunctionFactory>(
        data: &mut Self::ColorsBufferTempStructure,
        kmer_color: &[ColorIndexType],
        _el: (usize, MH::HashTypeUnextendable),
        entry: &mut MapEntry<Self::HashMapTempColorIndex>,
        same_color: bool,
        reached_threshold: bool,
    ) {
        if same_color && data.last_switch_color_index == entry.color_index {
            // Shortcut to assign the same color to adjacent kmers that share the same value
            if data.last_switch_color_index != usize::MAX {
                let old_colors = &mut data.colors_list[data.last_switch_color_index];
                old_colors.tracking_counter_or_color -= 1;
                if entry.color_index != data.last_color_index && old_colors.get_counter() == 0 {
                    data.colors_buffer.free_vec(&mut old_colors.colors);
                }
            }
            entry.color_index = data.last_color_index;
            data.colors_list[data.last_color_index].tracking_counter_or_color += 1;
        } else {
            data.last_switch_color_index = entry.color_index;
            if entry.color_index == usize::MAX {
                let mut new_colors = data.colors_buffer.new_vec(kmer_color.len());
                let new_slice = data.colors_buffer.slice_vec_mut(&mut new_colors);
                new_slice.copy_from_slice(kmer_color);
                entry.color_index = data.colors_list.len();
                data.colors_list.push(ColorEntry {
                    tracking_counter_or_color: 1,
                    colors: new_colors,
                });
            } else {
                let old_colors = &mut data.colors_list[entry.color_index];

                if old_colors.get_counter() == 1 {
                    // It is the last reference, take ownership and extend the vector
                    data.colors_buffer
                        .extend_vec(&mut old_colors.colors, kmer_color);
                } else {
                    // Reduce tracking counter and optionally free the vector
                    old_colors.tracking_counter_or_color -= 1;

                    // Allocate a new buffer to store the colors
                    let mut new_colors = data
                        .colors_buffer
                        .new_vec(old_colors.colors.len() + kmer_color.len());

                    let old_colors_slice =
                        unsafe { data.colors_buffer.slice_vec_static(&old_colors.colors) };
                    let new_slice = data.colors_buffer.slice_vec_mut(&mut new_colors);
                    new_slice[..old_colors.colors.len()].copy_from_slice(old_colors_slice);
                    new_slice[old_colors.colors.len()..].copy_from_slice(kmer_color);

                    // Add the new color
                    entry.color_index = data.colors_list.len();
                    data.colors_list.push(ColorEntry {
                        tracking_counter_or_color: 1,
                        colors: new_colors,
                    });
                }
            };

            let color = &data.colors_list[entry.color_index];
            if color.reached_multiplicity() {
                assert!(color.colors.len() > 0);
            }

            // Set the reached threshold flag
            data.last_color_index = entry.color_index;
        }

        // Set the reached multiplicity flag
        data.colors_list[entry.color_index].tracking_counter_or_color |=
            (reached_threshold as u64) << ColorEntry::REACHED_MULTIPLICITY_FLAG_SHIFT;
    }

    type HashMapTempColorIndex = HashMapTempColorIndex;

    fn new_color_index() -> Self::HashMapTempColorIndex {
        usize::MAX
    }

    fn process_colors<MH: HashFunctionFactory>(
        global_colors_table: &Self::GlobalColorsTableWriter,
        data: &mut Self::ColorsBufferTempStructure,
    ) {
        let mut last_partition = &[][..];
        let mut last_color = 0;

        for color_entry in data.colors_list.iter_mut() {
            if !color_entry.reached_multiplicity() {
                continue;
            }

            // Assign the final color
            let colors_range = data.colors_buffer.slice_vec_mut(&mut color_entry.colors);

            if colors_range.len() == 0 {
                println!(
                    "Colors range: {} with counter: {}",
                    colors_range.len(),
                    color_entry.tracking_counter_or_color
                );
            }

            colors_range.sort_unstable();

            // Get the final colors count
            let colors_count = colors_range.nq_partition_dedup().0.len();

            let unique_colors = &colors_range[..colors_count];

            // Assign the subset color index to the current kmer
            if unique_colors != last_partition {
                last_color = global_colors_table.get_id(unique_colors);
                last_partition =
                    unsafe { from_raw_parts(unique_colors.as_ptr(), unique_colors.len()) };
            }

            color_entry.tracking_counter_or_color = last_color as u64;
        }
        data.colors_buffer.reset();
    }

    type PartialUnitigsColorStructure = UnitigColorData;
    type TempUnitigColorStructure = DefaultUnitigsTempColorData;

    fn alloc_unitig_color_structure() -> Self::TempUnitigColorStructure {
        DefaultUnitigsTempColorData {
            colors: VecDeque::new(),
        }
    }

    fn reset_unitig_color_structure(ts: &mut Self::TempUnitigColorStructure) {
        ts.colors.clear();
    }

    #[inline]
    fn extend_forward(
        data: &Self::ColorsBufferTempStructure,
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        let kmer_color = get_entry_color(data, entry);

        if let Some(back_ts) = ts.colors.back_mut() {
            if back_ts.color == kmer_color {
                back_ts.counter += 1;
                return;
            }
        }

        ts.colors.push_back(KmerSerializedColor {
            color: kmer_color,
            counter: 1,
        });
    }

    #[inline]
    fn extend_backward(
        data: &Self::ColorsBufferTempStructure,
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        let kmer_color = get_entry_color(data, entry);

        if let Some(front_ts) = ts.colors.front_mut() {
            if front_ts.color == kmer_color {
                front_ts.counter += 1;
                return;
            }
        }

        ts.colors.push_front(KmerSerializedColor {
            color: kmer_color,
            counter: 1,
        });
    }

    fn join_structures<const REVERSE: bool>(
        dest: &mut Self::TempUnitigColorStructure,
        src: &Self::PartialUnitigsColorStructure,
        src_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        mut skip: ColorCounterType,
        count: Option<usize>,
    ) {
        let get_index = |i| {
            if REVERSE {
                src.slice.end - i - 1
            } else {
                src.slice.start + i
            }
        };

        let len = src.slice.end - src.slice.start;

        let colors_slice = &src_buffer.colors.as_slice();

        let mut count = count.unwrap_or(usize::MAX);

        for i in 0..len {
            let color = colors_slice[get_index(i)];

            if color.counter <= skip {
                skip -= color.counter;
            } else {
                let left = (color.counter - skip).min(count as ColorCounterType);

                // Reached the needed count
                if count == 0 {
                    break;
                }

                count -= left as usize;
                skip = 0;
                if dest
                    .colors
                    .back()
                    .map(|x| x.color == color.color)
                    .unwrap_or(false)
                {
                    dest.colors.back_mut().unwrap().counter += left;
                } else {
                    dest.colors.push_back(KmerSerializedColor {
                        color: color.color,
                        counter: left,
                    });
                }
            }
        }
    }

    fn pop_base(target: &mut Self::TempUnitigColorStructure) {
        if let Some(last) = target.colors.back_mut() {
            last.counter -= 1;
            if last.counter == 0 {
                target.colors.pop_back();
            }
        }
    }

    fn encode_part_unitigs_colors(
        ts: &mut Self::TempUnitigColorStructure,
        colors_buffer: &mut <Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> Self::PartialUnitigsColorStructure {
        colors_buffer.colors.clear();
        colors_buffer.colors.extend(ts.colors.iter());

        UnitigColorData {
            slice: 0..colors_buffer.colors.len(),
        }
    }

    fn debug_tucs(str: &Self::TempUnitigColorStructure, seq: &[u8]) {
        let sum: usize = str
            .colors
            .iter()
            .map(|x| x.counter)
            .sum::<ColorCounterType>()
            + 30;
        if sum != seq.len() {
            ggcat_logging::info!("Temp values: {} {}", sum as usize, seq.len());
            ggcat_logging::info!("Dbg: {:?}", str.colors);
            assert_eq!(sum as usize, seq.len());
        }
    }

    fn debug_colors<MH: HashFunctionFactory>(
        data: &Self::ColorsBufferTempStructure,
        color: &Self::PartialUnitigsColorStructure,
        colors_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        seq: &[u8],
        hmap: &HashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
    ) {
        let mut storage = vec![];
        let rind = CompressedReadIndipendent::from_plain(seq, &mut storage);
        let read = rind.as_reference(&storage);

        let hashes = MH::new(read, 31);

        let subslice = &colors_buffer.colors[color.slice.clone()];

        for (hash, color) in hashes.iter().zip(
            subslice
                .iter()
                .map(|x| (0..x.counter).into_iter().map(|_| x.color))
                .flatten(),
        ) {
            let entry = hmap.get(&hash.to_unextendable()).unwrap();
            let kmer_color = get_entry_color(data, entry);
            if kmer_color != color {
                let hashes = MH::new(read, 31);
                ggcat_logging::error!(
                    "Error: {:?}",
                    hashes
                        .iter()
                        .map(|h| {
                            let entry = hmap.get(&h.to_unextendable()).unwrap();
                            let kmer_color = get_entry_color(data, entry);
                            kmer_color
                        })
                        .zip(
                            subslice
                                .iter()
                                .map(|x| (0..x.counter).into_iter().map(|_| x.color))
                                .flatten()
                        )
                        .collect::<Vec<_>>()
                );

                assert_eq!(kmer_color, color);
            }
        }
    }
}

#[derive(Debug)]
pub struct DefaultUnitigsTempColorData {
    colors: VecDeque<KmerSerializedColor>,
}

#[derive(Debug, Default)]
pub struct UnitigsSerializerTempBuffer {
    pub(crate) colors: Vec<KmerSerializedColor>,
}

#[derive(Clone, Debug)]
pub struct UnitigColorData {
    pub(crate) slice: Range<usize>,
}

impl Default for UnitigColorData {
    fn default() -> Self {
        Self { slice: 0..0 }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct KmerSerializedColor {
    pub(crate) color: ColorIndexType,
    pub(crate) counter: ColorCounterType,
}

impl SequenceExtraDataTempBufferManagement for UnitigColorData {
    type TempBuffer = UnitigsSerializerTempBuffer;

    fn new_temp_buffer() -> UnitigsSerializerTempBuffer {
        UnitigsSerializerTempBuffer { colors: Vec::new() }
    }

    fn clear_temp_buffer(buffer: &mut UnitigsSerializerTempBuffer) {
        buffer.colors.clear();
    }

    fn copy_temp_buffer(dest: &mut UnitigsSerializerTempBuffer, src: &UnitigsSerializerTempBuffer) {
        dest.colors.clear();
        dest.colors.extend_from_slice(&src.colors);
    }

    fn copy_extra_from(
        extra: Self,
        src: &UnitigsSerializerTempBuffer,
        dst: &mut UnitigsSerializerTempBuffer,
    ) -> Self {
        let start = dst.colors.len();
        dst.colors.extend(&src.colors[extra.slice]);
        Self {
            slice: start..dst.colors.len(),
        }
    }
}

impl SequenceExtraData for UnitigColorData {
    fn decode_extended(buffer: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        let start = buffer.colors.len();

        let colors_count = decode_varint(|| reader.read_u8().ok())?;

        for _ in 0..colors_count {
            buffer.colors.push(KmerSerializedColor {
                color: decode_varint(|| reader.read_u8().ok())? as ColorIndexType,
                counter: decode_varint(|| reader.read_u8().ok())? as ColorCounterType,
            });
        }
        Some(Self {
            slice: start..buffer.colors.len(),
        })
    }

    fn encode_extended(&self, buffer: &Self::TempBuffer, writer: &mut impl Write) {
        let colors_count = self.slice.end - self.slice.start;
        encode_varint(|b| writer.write_all(b), colors_count as u64).unwrap();

        for i in self.slice.clone() {
            let el = buffer.colors[i];
            encode_varint(|b| writer.write_all(b), el.color as u64).unwrap();
            encode_varint(|b| writer.write_all(b), el.counter as u64).unwrap();
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        (2 * (self.slice.end - self.slice.start) + 1) * VARINT_MAX_SIZE
    }
}

impl IdentSequenceWriter for UnitigColorData {
    fn write_as_ident(&self, stream: &mut impl Write, extra_buffer: &Self::TempBuffer) {
        for i in self.slice.clone() {
            write!(
                stream,
                " C:{:x}:{}",
                extra_buffer.colors[i].color, extra_buffer.colors[i].counter
            )
            .unwrap();
        }
    }

    #[allow(unused_variables)]
    fn write_as_gfa<const VERSION: u32>(
        &self,
        _k: u64,
        _index: u64,
        _length: u64,
        stream: &mut impl Write,
        extra_buffer: &Self::TempBuffer,
    ) {
        if self.slice.len() > 0 {
            write!(stream, "CS",).unwrap();
        }

        for i in self.slice.clone() {
            write!(
                stream,
                ":{:x}:{}",
                extra_buffer.colors[i].color, extra_buffer.colors[i].counter
            )
            .unwrap();
        }
    }

    #[allow(unused_variables)]
    fn parse_as_ident<'a>(ident: &[u8], colors_buffer: &mut Self::TempBuffer) -> Option<Self> {
        let mut colors_count = 0;
        for col_pos in ident.find_iter(b"C:") {
            let (color_index, next_pos) = ColorIndexType::from_radix_16(&ident[(col_pos + 2)..]);

            let kmers_count = ColorCounterType::from_radix_10(&ident[(col_pos + next_pos + 3)..]).0;
            colors_buffer.colors.push(KmerSerializedColor {
                color: color_index,
                counter: kmers_count,
            });
            colors_count += kmers_count
        }
        if colors_count == 0 {
            ggcat_logging::error!("Error: 0 colors for {:?}", std::str::from_utf8(ident));
        }

        Some(UnitigColorData {
            slice: 0..colors_count,
        })
    }

    #[allow(unused_variables)]
    fn parse_as_gfa<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        let mut colors_count = 0;
        if let Some(mut col_pos) = ident.find(b"CA:") {
            col_pos += 3;

            for (col_string, col_len) in ident[col_pos..].split(|c| *c == b':').tuples() {
                let color_index = ColorIndexType::from_radix_16(col_string).0;
                let kmers_count = ColorCounterType::from_radix_10(col_len).0;
                extra_buffer.colors.push(KmerSerializedColor {
                    color: color_index,
                    counter: kmers_count,
                });
                colors_count += kmers_count
            }
        }
        if colors_count == 0 {
            ggcat_logging::error!("Error: 0 colors for {:?}", std::str::from_utf8(ident));
        }

        Some(UnitigColorData {
            slice: 0..colors_count,
        })
    }
}
