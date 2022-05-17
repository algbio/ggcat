use crate::assemble_pipeline::parallel_kmers_merge::structs::MapEntry;
use crate::colors::colors_manager::{
    ColorsManager, ColorsMergeManager, MinimizerBucketingSeqColorData,
};
use crate::colors::colors_memmap::ColorsMemMap;
use crate::colors::storage::roaring::RoaringColorsSerializer;
use crate::colors::ColorIndexType;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint, encode_varint};
use byteorder::ReadBytesExt;
use hashbrown::HashMap;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::Path;
use std::slice::from_raw_parts;

#[derive(Copy, Clone)]
pub struct DefaultColorsManager;

impl ColorsManager for DefaultColorsManager {
    const COLORS_ENABLED: bool = true;
    type GlobalColorsTable = ColorsMemMap<RoaringColorsSerializer>;

    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: Vec<String>,
    ) -> Self::GlobalColorsTable {
        ColorsMemMap::new(path, color_names)
    }

    fn print_color_stats(global_colors_table: &Self::GlobalColorsTable) {
        global_colors_table.print_stats();
    }

    type MinimizerBucketingSeqColorDataType = MinBkSingleColor;
    type ColorsMergeManagerType<H: HashFunctionFactory> = DefaultColorsMergeManager<H>;
}

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub struct MinBkSingleColor(ColorIndexType);

impl SequenceExtraData for MinBkSingleColor {
    fn decode_from_slice(slice: &[u8]) -> Option<Self> {
        let mut index = 0;
        decode_varint(|| {
            let data = slice[index];
            index += 1;
            Some(data)
        })
        .map(|x| Self(x as ColorIndexType))
    }

    unsafe fn decode_from_pointer(mut ptr: *const u8) -> Option<Self> {
        decode_varint(|| {
            let data = *ptr;
            ptr = ptr.add(1);
            Some(data)
        })
        .map(|x| Self(x as ColorIndexType))
    }

    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self> {
        decode_varint(|| reader.read_u8().ok()).map(|x| Self(x as ColorIndexType))
    }

    fn encode<'a>(&self, writer: &'a mut impl Write) {
        encode_varint(|b| writer.write_all(b), self.0 as u64).unwrap();
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        9
    }
}

impl MinimizerBucketingSeqColorData for MinBkSingleColor {
    fn create(file_index: u64) -> Self {
        Self(file_index as ColorIndexType)
    }
}

pub struct DefaultColorsMergeManager<H: HashFunctionFactory> {
    flags: Vec<(usize, usize, MinBkSingleColor)>,
    kmers: Vec<H::HashTypeUnextendable>,
    temp_colors: Vec<ColorIndexType>,
}

pub union DefaultHashMapTempColorIndex {
    color_index: ColorIndexType,
    temp_index: (u32 /*Vector index*/, u32 /*Remaining*/),
}

#[derive(Debug)]
pub struct DefaultUnitigsTempColorData {
    colors: VecDeque<(ColorIndexType, u64)>,
}

#[derive(Clone, Debug)]
pub struct UnitigColorDataSerializer {
    data: &'static DefaultUnitigsTempColorData,
    slice: (usize, usize),
}

// FIXME: Workaround for allocations!
#[thread_local]
static mut DESERIALIZED_TEMP_COLOR_DATA: Option<DefaultUnitigsTempColorData> = None;

impl SequenceExtraData for UnitigColorDataSerializer {
    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self> {
        let temp_data = unsafe {
            if DESERIALIZED_TEMP_COLOR_DATA.is_none() {
                DESERIALIZED_TEMP_COLOR_DATA = Some(DefaultUnitigsTempColorData {
                    colors: VecDeque::new(),
                });
            }

            DESERIALIZED_TEMP_COLOR_DATA.as_mut().unwrap()
        };

        let start = temp_data.colors.len();

        let colors_count = decode_varint(|| reader.read_u8().ok())?;

        for _ in 0..colors_count {
            temp_data.colors.push_back((
                decode_varint(|| reader.read_u8().ok())? as ColorIndexType,
                decode_varint(|| reader.read_u8().ok())?,
            ));
        }
        Some(Self {
            data: temp_data,
            slice: (start, temp_data.colors.len()),
        })
    }

    fn encode<'a>(&self, writer: &'a mut impl Write) {
        let colors_count = self.slice.1 - self.slice.0;
        encode_varint(|b| writer.write_all(b), colors_count as u64).unwrap();

        for i in self.slice.0..self.slice.1 {
            let el = self.data.colors[i];
            encode_varint(|b| writer.write_all(b), el.0 as u64).unwrap();
            encode_varint(|b| writer.write_all(b), el.1).unwrap();
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        (2 * (self.slice.1 - self.slice.0) + 1) * 9
    }
}

impl<H: HashFunctionFactory> ColorsMergeManager<H, DefaultColorsManager>
    for DefaultColorsMergeManager<H>
{
    type ColorsBufferTempStructure = Self;

    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure {
        Self {
            flags: vec![],
            kmers: vec![],
            temp_colors: vec![],
        }
    }

    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure) {
        data.flags.clear();
        data.kmers.clear();
        data.temp_colors.clear()
    }

    fn add_temp_buffer_structure_el(
        data: &mut Self::ColorsBufferTempStructure,
        flags: &MinBkSingleColor,
        el: (usize, H::HashTypeUnextendable),
    ) {
        if data.flags.last().map(|l| &l.2) != Some(flags) {
            if let Some(last) = data.flags.last_mut() {
                last.1 = data.kmers.len();
            }
            data.flags.push((data.kmers.len(), 0, flags.clone()));
        }
        data.kmers.push(el.1);
    }

    type HashMapTempColorIndex = DefaultHashMapTempColorIndex;

    fn new_color_index() -> Self::HashMapTempColorIndex {
        DefaultHashMapTempColorIndex { temp_index: (0, 0) }
    }

    fn process_colors(
        global_colors_table: &ColorsMemMap<RoaringColorsSerializer>,
        data: &mut Self::ColorsBufferTempStructure,
        map: &mut HashMap<H::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
        min_multiplicity: usize,
    ) {
        let vec_len = data.kmers.len();
        data.flags.last_mut().iter_mut().for_each(|l| l.1 = vec_len);

        let mut last_partition = (0, 0);
        let mut last_color = 0;

        for (start, end, color) in &data.flags {
            for kmer_hash in &data.kmers[*start..*end] {
                let entry = map.get_mut(kmer_hash).unwrap();
                let entry_count = entry.get_counter();

                if entry_count < min_multiplicity {
                    continue;
                }

                unsafe {
                    if entry.color_index.temp_index.1 == 0 {
                        entry.color_index.temp_index.0 = data.temp_colors.len() as u32;
                        entry.color_index.temp_index.1 = entry_count as u32;
                        if data.temp_colors.capacity() < data.temp_colors.len() + entry_count {
                            data.temp_colors.reserve(entry_count);
                        }
                        data.temp_colors
                            .set_len(data.temp_colors.len() + entry_count);
                    }

                    data.temp_colors[entry.color_index.temp_index.0 as usize] = color.0;
                    entry.color_index.temp_index.0 += 1;
                    entry.color_index.temp_index.1 -= 1;

                    // All colors were added, let's assign the final color
                    if entry.color_index.temp_index.1 == 0 {
                        let slice_start = entry.color_index.temp_index.0 as usize - entry_count;
                        let slice_end = entry.color_index.temp_index.0 as usize;

                        let slice = &mut data.temp_colors[slice_start..slice_end];
                        slice.sort_unstable();
                        // Assign the subset color index to the current kmer

                        let unique_colors = slice.partition_dedup().0;

                        // println!("Unique colors: {:?}", unique_colors);

                        let partition = from_raw_parts(unique_colors.as_ptr(), unique_colors.len());
                        if partition == &data.temp_colors[last_partition.0..last_partition.1] {
                            entry.color_index.color_index = last_color;
                        } else {
                            entry.color_index.color_index = global_colors_table.get_id(partition);
                            last_color = entry.color_index.color_index;
                            last_partition = (slice_start, slice_end);
                        }
                    }
                }
            }
        }
    }

    type PartialUnitigsColorStructure = UnitigColorDataSerializer;
    type TempUnitigColorStructure = DefaultUnitigsTempColorData;

    fn alloc_unitig_color_structure() -> Self::TempUnitigColorStructure {
        DefaultUnitigsTempColorData {
            colors: VecDeque::new(),
        }
    }

    fn reset_unitig_color_structure(ts: &mut Self::TempUnitigColorStructure) {
        ts.colors.clear();
    }

    fn extend_forward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        unsafe {
            if let Some(back_ts) = ts.colors.back_mut() {
                if back_ts.0 == entry.color_index.color_index {
                    back_ts.1 += 1;
                    return;
                }
            }
            ts.colors.push_back((entry.color_index.color_index, 1));
        }
    }

    fn extend_backward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        unsafe {
            if let Some(front_ts) = ts.colors.front_mut() {
                if front_ts.0 == entry.color_index.color_index {
                    front_ts.1 += 1;
                    return;
                }
            }
            ts.colors.push_front((entry.color_index.color_index, 1));
        }
    }

    fn join_structures<const REVERSE: bool>(
        dest: &mut Self::TempUnitigColorStructure,
        src: &Self::PartialUnitigsColorStructure,
        mut skip: u64,
    ) {
        let get_index = |i| {
            if REVERSE {
                src.slice.0 + i
            } else {
                src.slice.1 - i - 1
            }
        };

        let len = src.slice.1 - src.slice.0;

        for i in 0..len {
            let color = src.data.colors[get_index(i)];

            if color.1 <= skip {
                skip -= color.1;
            } else {
                let left = color.1 - skip;
                skip = 0;
                if dest.colors.back().map(|x| x.0 == color.0).unwrap_or(false) {
                    dest.colors.back_mut().unwrap().1 += left;
                } else {
                    dest.colors.push_back((color.0, left));
                }
            }
        }
    }

    fn pop_base(target: &mut Self::TempUnitigColorStructure) {
        if let Some(last) = target.colors.back_mut() {
            last.1 -= 1;
            if last.1 == 0 {
                target.colors.pop_back();
            }
        }
    }

    fn clear_deserialized_unitigs_colors() {
        if let Some(data) = unsafe { DESERIALIZED_TEMP_COLOR_DATA.as_mut() } {
            data.colors.clear();
        }
    }

    fn encode_part_unitigs_colors(
        ts: &mut Self::TempUnitigColorStructure,
    ) -> Self::PartialUnitigsColorStructure {
        UnitigColorDataSerializer {
            data: unsafe { &mut *(ts as *mut _) },
            slice: (0, ts.colors.len()),
        }
    }

    fn print_color_data(data: &Self::PartialUnitigsColorStructure, buffer: &mut impl Write) {
        for i in data.slice.0..data.slice.1 {
            write!(
                buffer,
                " C:{}:{}",
                data.data.colors[i].0, data.data.colors[i].1
            )
            .unwrap();
        }
    }

    fn debug_tucs(str: &Self::TempUnitigColorStructure, seq: &[u8]) {
        let sum: u64 = str.colors.iter().map(|x| x.1).sum::<u64>() + 62;
        if sum as usize != seq.len() {
            println!("Temp values: {} {}", sum as usize, seq.len());
            println!("Dbg: {:?}", str.colors);
            assert_eq!(sum as usize, seq.len());
        }
    }
}
