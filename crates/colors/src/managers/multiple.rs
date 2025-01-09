use crate::colors_manager::ColorsMergeManager;
use crate::colors_memmap_writer::ColorsMemMapWriter;
use crate::DefaultColorsSerializer;
use atoi::{FromRadix10, FromRadix16};
use bstr::ByteSlice;
use byteorder::ReadBytesExt;
use config::{
    get_compression_level_info, get_memory_mode, ColorCounterType, ColorIndexType, MinimizerType,
    SwapPriority, PARTIAL_VECS_CHECKPOINT_SIZE, READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END,
};
use hashbrown::HashMap;
use hashes::default::MNHFactory;
use hashes::ExtendableHashTraitType;
use hashes::{HashFunction, HashFunctionFactory, HashableSequence, MinimizerHashFunctionFactory};
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::varint::{
    decode_varint, decode_varint_flags, encode_varint, encode_varint_flags, VARINT_MAX_SIZE,
};
use itertools::Itertools;
use nightly_quirks::slice_partition_dedup::SlicePartitionDedup;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::memory_fs::RemoveFileMode;
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use structs::map_entry::{MapEntry, COUNTER_BITS};

const COLOR_SEQUENCES_SUBBUKETS: usize = 32;

struct SequencesStorage {
    buffer: Vec<u8>,
    file: Option<CompressedBinaryWriter>,
}

struct SequencesStorageStream<'a> {
    buffer: Cursor<&'a [u8]>,
    reader: Option<CompressedBinaryReader>,
}

impl<'a> Read for SequencesStorageStream<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(ref mut reader) = self.reader {
            let amount = reader.get_single_stream().read(buf)?;
            if amount > 0 {
                return Ok(amount);
            } else {
                self.reader.take();
            }
        }
        self.buffer.read(buf)
    }
}

impl SequencesStorage {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(READS_BUFFERS_MAX_CAPACITY),
            file: None,
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        assert!(self.file.is_none());
    }

    pub fn get_stream(&mut self) -> SequencesStorageStream {
        let reader = self.file.take().map(|f| {
            let path = f.get_path();
            f.finalize();
            CompressedBinaryReader::new(path, RemoveFileMode::Remove { remove_fs: true }, None)
        });
        SequencesStorageStream {
            buffer: Cursor::new(&self.buffer),
            reader,
        }
    }

    pub fn flush(&mut self, temp_dir: &PathBuf) {
        if self.buffer.len() >= READS_BUFFERS_MAX_CAPACITY {
            if self.file.is_none() {
                static COLOR_STORAGE_INDEX: AtomicUsize = AtomicUsize::new(0);
                self.file = Some(CompressedBinaryWriter::new(
                    temp_dir.join("color-storage-temp").as_path(),
                    &(
                        get_memory_mode(SwapPriority::KmersMergeTempColors),
                        PARTIAL_VECS_CHECKPOINT_SIZE,
                        get_compression_level_info(),
                    ),
                    COLOR_STORAGE_INDEX.fetch_add(1, Ordering::Relaxed),
                    &(),
                ))
            }

            self.file.as_ref().unwrap().write_data(&self.buffer);
            self.buffer.clear();
        }
    }
}

pub struct MultipleColorsManager {
    last_color: ColorIndexType,
    sequences: Vec<SequencesStorage>,
    kmers_count: usize,
    sequences_count: usize,
    temp_colors_buffer: Vec<ColorIndexType>,
    temp_dir: PathBuf,
}

const VISITED_BIT: usize = 1 << (COUNTER_BITS - 1);
const TEMP_BUFFER_START_SIZE: usize = 1024 * 64;
const READS_BUFFERS_MAX_CAPACITY: usize = 1024 * 32;

#[cfg(feature = "support_kmer_counters")]
type HashMapTempColorIndex = usize;

#[cfg(not(feature = "support_kmer_counters"))]
type HashMapTempColorIndex = ();

#[inline]
fn get_entry_color(entry: &MapEntry<HashMapTempColorIndex>) -> ColorIndexType {
    (match () {
        #[cfg(not(feature = "support_kmer_counters"))]
        () => entry.get_counter() & !VISITED_BIT,
        #[cfg(feature = "support_kmer_counters")]
        () => entry.color_index & !VISITED_BIT,
    }) as ColorIndexType
}

impl ColorsMergeManager for MultipleColorsManager {
    type SingleKmerColorDataType = ColorIndexType;
    type GlobalColorsTableWriter = ColorsMemMapWriter<DefaultColorsSerializer>;
    type GlobalColorsTableReader = ();

    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: &[String],
    ) -> anyhow::Result<Self::GlobalColorsTableWriter> {
        ColorsMemMapWriter::new(path, color_names)
    }

    fn open_colors_table(_path: impl AsRef<Path>) -> anyhow::Result<Self::GlobalColorsTableReader> {
        Ok(())
    }

    fn print_color_stats(global_colors_table: &Self::GlobalColorsTableWriter) {
        global_colors_table.print_stats();
    }

    type ColorsBufferTempStructure = Self;

    fn allocate_temp_buffer_structure(temp_dir: &Path) -> Self::ColorsBufferTempStructure {
        Self {
            last_color: 0,
            sequences: (0..COLOR_SEQUENCES_SUBBUKETS)
                .map(|_| SequencesStorage::new())
                .collect(),
            kmers_count: 0,
            sequences_count: 0,
            temp_colors_buffer: vec![],
            temp_dir: temp_dir.to_path_buf(),
        }
    }

    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure) {
        data.sequences.iter_mut().for_each(|s| s.clear());
        data.temp_colors_buffer.clear();
        data.temp_colors_buffer.shrink_to(TEMP_BUFFER_START_SIZE);
        data.kmers_count = 0;
        data.sequences_count = 0;
    }

    fn add_temp_buffer_structure_el<MH: HashFunctionFactory>(
        data: &mut Self::ColorsBufferTempStructure,
        kmer_color: &ColorIndexType,
        _el: (usize, MH::HashTypeUnextendable),
        _entry: &mut MapEntry<Self::HashMapTempColorIndex>,
    ) {
        data.last_color = *kmer_color;
    }

    #[inline(always)]
    fn add_temp_buffer_sequence(
        data: &mut Self::ColorsBufferTempStructure,
        sequence: CompressedRead,
        k: usize,
        m: usize,
        flags: u8,
    ) {
        let decr_val =
            ((sequence.bases_count() == k) && (flags & READ_FLAG_INCL_END) == 0) as usize;
        let hashes = MNHFactory::new(sequence.sub_slice((1 - decr_val)..(k - decr_val)), m);

        let minimizer = hashes
            .iter()
            .map(|m| MNHFactory::get_full_minimizer(m.to_unextendable()))
            .min()
            .unwrap();

        const MINIMIZER_SHIFT: usize =
            size_of::<MinimizerType>() * 8 - (2 * COLOR_SEQUENCES_SUBBUKETS.ilog2() as usize);
        let bucket = (minimizer >> MINIMIZER_SHIFT) as usize % COLOR_SEQUENCES_SUBBUKETS;

        data.sequences[bucket]
            .buffer
            .extend_from_slice(&data.last_color.to_ne_bytes());

        let kmer_length_dist_flag = if sequence.bases_count() > k {
            0
        } else {
            decr_val as u8
        };

        encode_varint_flags::<_, _, typenum::U1>(
            |b| data.sequences[bucket].buffer.extend_from_slice(b),
            sequence.bases_count() as u64,
            kmer_length_dist_flag,
        );
        sequence.copy_to_buffer(&mut data.sequences[bucket].buffer);
        data.sequences[bucket].flush(&data.temp_dir)
    }

    #[cfg(feature = "support_kmer_counters")]
    type HashMapTempColorIndex = usize;

    #[cfg(not(feature = "support_kmer_counters"))]
    type HashMapTempColorIndex = ();

    fn new_color_index() -> Self::HashMapTempColorIndex {
        Default::default()
    }

    fn process_colors<MH: HashFunctionFactory>(
        global_colors_table: &Self::GlobalColorsTableWriter,
        data: &mut Self::ColorsBufferTempStructure,
        map: &mut FxHashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
        k: usize,
        min_multiplicity: usize,
    ) {
        for buffer in data.sequences.iter_mut() {
            data.temp_colors_buffer.clear();

            let mut stream = buffer.get_stream();

            let mut color_buf = [0; size_of::<ColorIndexType>()];
            let mut read_buf = vec![];

            let mut last_partition = 0..0;
            let mut last_color = 0;

            loop {
                // TODO: Distinguish between error and no more data
                if stream.read_exact(&mut color_buf).is_err() {
                    break;
                }

                let color = ColorIndexType::from_ne_bytes(color_buf);

                let (read_length, only_extra_ending) =
                    decode_varint_flags::<_, typenum::U1>(|| stream.read_u8().ok()).unwrap();
                let only_extra_ending = if only_extra_ending != 0 { true } else { false };

                let read_bytes_count = (read_length as usize + 3) / 4;

                read_buf.clear();
                read_buf.reserve(read_bytes_count);
                unsafe { read_buf.set_len(read_bytes_count) };
                stream.read_exact(&mut read_buf[..]).unwrap();

                let read = CompressedRead::new_from_compressed(&read_buf[..], read_length as usize);

                let hashes = MH::new(read, k);

                data.temp_colors_buffer
                    .reserve(data.kmers_count + data.sequences_count);

                let mut is_first = !only_extra_ending;

                for kmer_hash in hashes.iter() {
                    let entry = map.get_mut(&kmer_hash.to_unextendable()).unwrap();

                    let is_first = {
                        let tmp = is_first;
                        is_first = false;
                        tmp
                    };

                    if entry.get_kmer_multiplicity() < min_multiplicity {
                        continue;
                    }

                    let mut entry_count = entry.get_counter();

                    let missing_temp_color = match () {
                        #[cfg(not(feature = "support_kmer_counters"))]
                        () => entry_count & VISITED_BIT == 0,
                        #[cfg(feature = "support_kmer_counters")]
                        () => (entry.color_index & VISITED_BIT) == 0,
                    };

                    const BIDIRECTIONAL_FLAGS: u8 = READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END;

                    if entry.get_flags() & BIDIRECTIONAL_FLAGS == BIDIRECTIONAL_FLAGS {
                        if kmer_hash.is_forward() ^ is_first {
                            continue;
                        } else if missing_temp_color {
                            entry_count /= 2;
                        }
                    }

                    if missing_temp_color {
                        let colors_count = entry_count;
                        let start_temp_color_index = data.temp_colors_buffer.len();

                        match () {
                            #[cfg(not(feature = "support_kmer_counters"))]
                            () => {
                                entry_count = VISITED_BIT | start_temp_color_index;
                                entry.set_counter_after_check(entry_count);
                            }
                            #[cfg(feature = "support_kmer_counters")]
                            () => {
                                entry.color_index = VISITED_BIT | start_temp_color_index;
                            }
                        };

                        data.temp_colors_buffer
                            .resize(data.temp_colors_buffer.len() + colors_count + 1, 0);

                        data.temp_colors_buffer[start_temp_color_index] = 1;
                    }

                    let position = match () {
                        #[cfg(not(feature = "support_kmer_counters"))]
                        () => entry_count & !VISITED_BIT,
                        #[cfg(feature = "support_kmer_counters")]
                        () => entry.color_index & !VISITED_BIT,
                    };

                    let col_count = data.temp_colors_buffer[position] as usize;
                    data.temp_colors_buffer[position] += 1;

                    assert_eq!(data.temp_colors_buffer[position + col_count], 0);
                    data.temp_colors_buffer[position + col_count] = color;

                    let has_all_colors = (position + col_count + 1)
                        == data.temp_colors_buffer.len()
                        || data.temp_colors_buffer[position + col_count + 1] != 0;

                    // All colors were added, let's assign the final color
                    if has_all_colors {
                        let colors_range = &mut data.temp_colors_buffer
                            [(position + 1)..(position + col_count + 1)];

                        colors_range.sort_unstable();

                        // Get the new partition indexes, start to dedup last element
                        let new_partition = (position + 1)
                            ..(position + 1 + colors_range.nq_partition_dedup().0.len());

                        let unique_colors = &data.temp_colors_buffer[new_partition.clone()];

                        // Assign the subset color index to the current kmer
                        if unique_colors != &data.temp_colors_buffer[last_partition.clone()] {
                            last_color = global_colors_table.get_id(unique_colors);
                            last_partition = new_partition;
                        }

                        match () {
                            #[cfg(not(feature = "support_kmer_counters"))]
                            () => {
                                entry.set_counter_after_check(VISITED_BIT | (last_color as usize));
                            }
                            #[cfg(feature = "support_kmer_counters")]
                            () => {
                                entry.color_index = VISITED_BIT | (last_color as usize);
                            }
                        };
                    }
                }
            }
        }
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

    fn extend_forward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        let kmer_color = get_entry_color(entry);

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

    fn extend_backward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        let kmer_color = get_entry_color(entry);

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
            let kmer_color = get_entry_color(entry);
            if kmer_color != color {
                let hashes = MH::new(read, 31);
                ggcat_logging::error!(
                    "Error: {:?}",
                    hashes
                        .iter()
                        .map(|h| {
                            let entry = hmap.get(&h.to_unextendable()).unwrap();
                            let kmer_color = get_entry_color(entry);
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

#[derive(Debug)]
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
    fn write_as_gfa(
        &self,
        _k: u64,
        _index: u64,
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
