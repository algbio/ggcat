use crate::colors_manager::ColorsMergeManager;
use crate::colors_memmap_writer::ColorsMemMapWriter;
use crate::DefaultColorsSerializer;
use byteorder::ReadBytesExt;
use config::{
    get_compression_level_info, get_memory_mode, ColorIndexType, MinimizerType, SwapPriority,
    PARTIAL_VECS_CHECKPOINT_SIZE, READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END,
};
use hashbrown::HashMap;
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
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::memory_fs::RemoveFileMode;
use std::collections::VecDeque;
use std::io::{Cursor, Read, Write};
use std::marker::PhantomData;
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
                ))
            }

            self.file.as_ref().unwrap().write_data(&self.buffer);
            self.buffer.clear();
        }
    }
}

pub struct MultipleColorsManager<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory> {
    last_color: ColorIndexType,
    sequences: Vec<SequencesStorage>,
    kmers_count: usize,
    sequences_count: usize,
    temp_colors_buffer: Vec<ColorIndexType>,
    temp_dir: PathBuf,
    _phantom: PhantomData<(H, MH)>,
}

const VISITED_BIT: usize = 1 << (COUNTER_BITS - 1);
const TEMP_BUFFER_START_SIZE: usize = 1024 * 64;
const READS_BUFFERS_MAX_CAPACITY: usize = 1024 * 32;

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory> ColorsMergeManager<H, MH>
    for MultipleColorsManager<H, MH>
{
    type SingleKmerColorDataType = ColorIndexType;
    type GlobalColorsTableWriter = ColorsMemMapWriter<DefaultColorsSerializer>;
    type GlobalColorsTableReader = ();

    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: Vec<String>,
    ) -> Self::GlobalColorsTableWriter {
        ColorsMemMapWriter::new(path, color_names)
    }

    fn open_colors_table(_path: impl AsRef<Path>) -> Self::GlobalColorsTableReader {
        ()
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
            _phantom: PhantomData,
        }
    }

    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure) {
        data.sequences.iter_mut().for_each(|s| s.clear());
        data.temp_colors_buffer.clear();
        data.temp_colors_buffer.shrink_to(TEMP_BUFFER_START_SIZE);
        data.kmers_count = 0;
        data.sequences_count = 0;
    }

    fn add_temp_buffer_structure_el(
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
        let hashes = H::new(sequence.sub_slice((1 - decr_val)..(k - decr_val)), m);

        let minimizer = hashes
            .iter()
            .map(|m| H::get_full_minimizer(m.to_unextendable()))
            .min()
            .unwrap();

        const MINIMIZER_SHIFT: usize =
            size_of::<MinimizerType>() * 8 - (2 * COLOR_SEQUENCES_SUBBUKETS.ilog2() as usize);
        let bucket = (minimizer >> MINIMIZER_SHIFT) as usize % COLOR_SEQUENCES_SUBBUKETS;

        data.sequences[bucket]
            .buffer
            .extend_from_slice(&data.last_color.to_ne_bytes());

        let klen_dist_flag = if sequence.bases_count() > k {
            0
        } else {
            decr_val as u8
        };

        encode_varint_flags::<_, _, typenum::U1>(
            |b| data.sequences[bucket].buffer.extend_from_slice(b),
            sequence.bases_count() as u64,
            klen_dist_flag,
        );
        sequence.copy_to_buffer(&mut data.sequences[bucket].buffer);
        data.sequences[bucket].flush(&data.temp_dir)
    }

    type HashMapTempColorIndex = ();

    fn new_color_index() -> Self::HashMapTempColorIndex {
        ()
    }

    fn process_colors(
        global_colors_table: &Self::GlobalColorsTableWriter,
        data: &mut Self::ColorsBufferTempStructure,
        map: &mut HashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
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

                    const BIDIRECTIONAL_FLAGS: u8 = READ_FLAG_INCL_BEGIN | READ_FLAG_INCL_END;

                    if entry.get_flags() & BIDIRECTIONAL_FLAGS == BIDIRECTIONAL_FLAGS {
                        if kmer_hash.is_forward() ^ is_first {
                            continue;
                        } else if entry_count & VISITED_BIT == 0 {
                            entry_count /= 2;
                        }
                    }

                    if entry_count & VISITED_BIT == 0 {
                        let colors_count = entry_count;
                        let start_temp_color_index = data.temp_colors_buffer.len();

                        entry_count = VISITED_BIT | start_temp_color_index;
                        entry.set_counter_after_check(entry_count);

                        data.temp_colors_buffer
                            .resize(data.temp_colors_buffer.len() + colors_count + 1, 0);

                        data.temp_colors_buffer[start_temp_color_index] = 1;
                    }

                    let position = entry_count & !VISITED_BIT;

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
                        let new_partition =
                            (position + 1)..(position + 1 + colors_range.partition_dedup().0.len());

                        let unique_colors = &data.temp_colors_buffer[new_partition.clone()];

                        // Assign the subset color index to the current kmer
                        if unique_colors != &data.temp_colors_buffer[last_partition.clone()] {
                            last_color = global_colors_table.get_id(unique_colors);
                            last_partition = new_partition;
                        }

                        entry.set_counter_after_check(VISITED_BIT | (last_color as usize));
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
        let kmer_color = (entry.get_counter() & !VISITED_BIT) as ColorIndexType;

        if let Some(back_ts) = ts.colors.back_mut() && back_ts.0 == kmer_color {
            back_ts.1 += 1;
        } else {
            ts.colors.push_back((kmer_color, 1));
        }
    }

    fn extend_backward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        let kmer_color = (entry.get_counter() & !VISITED_BIT) as ColorIndexType;

        if let Some(front_ts) = ts.colors.front_mut()
            && front_ts.0 == kmer_color {
            front_ts.1 += 1;
        } else {
            ts.colors.push_front((kmer_color, 1));
        }
    }

    fn join_structures<const REVERSE: bool>(
        dest: &mut Self::TempUnitigColorStructure,
        src: &Self::PartialUnitigsColorStructure,
        src_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
        mut skip: u64,
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

        for i in 0..len {
            let color = colors_slice[get_index(i)];

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

    fn encode_part_unitigs_colors(
        ts: &mut Self::TempUnitigColorStructure,
        colors_buffer: &mut <Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
    ) -> Self::PartialUnitigsColorStructure {
        colors_buffer.colors.clear();
        colors_buffer.colors.extend(ts.colors.iter());

        UnitigColorDataSerializer {
            slice: 0..colors_buffer.colors.len(),
        }
    }

    fn debug_tucs(str: &Self::TempUnitigColorStructure, seq: &[u8]) {
        let sum: u64 = str.colors.iter().map(|x| x.1).sum::<u64>() + 30;
        if sum as usize != seq.len() {
            println!("Temp values: {} {}", sum as usize, seq.len());
            println!("Dbg: {:?}", str.colors);
            assert_eq!(sum as usize, seq.len());
        }
    }

    fn debug_colors(
        color: &Self::PartialUnitigsColorStructure,
        colors_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
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
                .map(|x| (0..x.1).into_iter().map(|_| x.0))
                .flatten(),
        ) {
            let entry = hmap.get(&hash.to_unextendable()).unwrap();
            let kmer_color = (entry.get_counter() & !VISITED_BIT) as ColorIndexType;
            if kmer_color != color {
                let hashes = MH::new(read, 31);
                println!(
                    "Err: {:?}",
                    hashes
                        .iter()
                        .map(|h| {
                            let entry = hmap.get(&h.to_unextendable()).unwrap();
                            let kmer_color = (entry.get_counter() & !VISITED_BIT) as ColorIndexType;
                            kmer_color
                        })
                        .zip(
                            subslice
                                .iter()
                                .map(|x| (0..x.1).into_iter().map(|_| x.0))
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
    colors: VecDeque<(ColorIndexType, u64)>,
}

#[derive(Debug)]
pub struct UnitigsSerializerTempBuffer {
    colors: Vec<(ColorIndexType, u64)>,
}

#[derive(Clone, Debug)]
pub struct UnitigColorDataSerializer {
    slice: Range<usize>,
}

impl SequenceExtraDataTempBufferManagement<UnitigsSerializerTempBuffer>
    for UnitigColorDataSerializer
{
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

impl SequenceExtraData for UnitigColorDataSerializer {
    type TempBuffer = UnitigsSerializerTempBuffer;

    fn decode_extended(buffer: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        let start = buffer.colors.len();

        let colors_count = decode_varint(|| reader.read_u8().ok())?;

        for _ in 0..colors_count {
            buffer.colors.push((
                decode_varint(|| reader.read_u8().ok())? as ColorIndexType,
                decode_varint(|| reader.read_u8().ok())?,
            ));
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
            encode_varint(|b| writer.write_all(b), el.0 as u64).unwrap();
            encode_varint(|b| writer.write_all(b), el.1).unwrap();
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        (2 * (self.slice.end - self.slice.start) + 1) * VARINT_MAX_SIZE
    }
}

impl IdentSequenceWriter for UnitigColorDataSerializer {
    fn write_as_ident(&self, stream: &mut impl Write, extra_buffer: &Self::TempBuffer) {
        for i in self.slice.clone() {
            write!(
                stream,
                " C:{:x}:{}",
                extra_buffer.colors[i].0, extra_buffer.colors[i].1
            )
            .unwrap();
        }
    }

    #[allow(unused_variables)]
    fn write_as_gfa(&self, stream: &mut impl Write, extra_buffer: &Self::TempBuffer) {
        todo!()
    }

    #[allow(unused_variables)]
    fn parse_as_ident<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        todo!()
    }

    #[allow(unused_variables)]
    fn parse_as_gfa<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        todo!()
    }
}
