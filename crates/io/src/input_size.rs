//! Estimate the entire decoded input stream, never its FASTA/FASTQ fraction.
//! Only compression metadata is read: payloads are skipped with seeks.
use anyhow::Context;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

// Missing sizes (notably bzip2 and streaming encoders) need a heuristic.
const EXPANSION_RATIO: u64 = 4;
const METADATA_BUDGET: usize = 8 * 1024 * 1024;
const READ_BUDGET: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Compression {
    Gzip,
    Xz,
    Lz4,
    Zstd,
    Bzip2,
}
impl Compression {
    fn of(path: &Path) -> Option<Self> {
        match path.extension()?.to_str()? {
            "gz" | "tgz" => Some(Self::Gzip),
            "xz" | "txz" => Some(Self::Xz),
            "lz4" => Some(Self::Lz4),
            "zst" | "zstd" => Some(Self::Zstd),
            "bz2" | "tbz2" => Some(Self::Bzip2),
            _ => None,
        }
    }
}
const FORMAT_SAMPLE_LIMIT: u64 = 128 * 1024 * 1024;

struct InputFile {
    index: usize,
    path: std::path::PathBuf,
    stored: u64,
}

/// Estimate each compression group once, then reuse the per-input results.
pub fn estimate_blocks(
    blocks: &[crate::sequences_stream::general::GeneralSequenceBlockData],
) -> anyhow::Result<Vec<u64>> {
    use crate::sequences_stream::general::GeneralSequenceBlockData;
    let mut sizes = vec![0; blocks.len()];
    let mut groups = std::collections::BTreeMap::<Compression, Vec<InputFile>>::new();
    for (index, block) in blocks.iter().enumerate() {
        let path = match block {
            GeneralSequenceBlockData::FASTA((path, _)) => {
                crate::sequences_stream::tar::split_archive_member(path)
                    .map(|(path, _)| path)
                    .unwrap_or_else(|| path.clone())
            }
            GeneralSequenceBlockData::TAR(block) => block.path.clone(),
            _ => {
                sizes[index] = block.estimated_bases_count()?;
                continue;
            }
        };
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("Cannot read input metadata: {}", path.display()))?;
        let stored = metadata.len();
        match Compression::of(&path) {
            Some(format) if metadata.is_file() => {
                groups.entry(format).or_default().push(InputFile {
                    index,
                    path,
                    stored,
                })
            }
            Some(_) => sizes[index] = stored.saturating_mul(EXPANSION_RATIO),
            None => sizes[index] = stored,
        }
    }
    for (format, mut files) in groups {
        if files.len() == 1 {
            let file = &files[0];
            let mut budget = METADATA_BUDGET;
            sizes[file.index] = inspect_file(&file.path, file.stored, format, &mut budget)?;
            continue;
        }
        let ratio = sample_group(&mut files, |file, budget| {
            inspect_file(&file.path, file.stored, format, budget)
        })?;
        for file in &files {
            sizes[file.index] = ratio.apply(file.stored);
        }
        ggcat_logging::info!(
            "Input size estimation {:?}: sampled {}/{} files, {} compressed bytes represented, {} metadata bytes read, ratio {:.3}",
            format,
            ratio.files,
            files.len(),
            ratio.stored,
            ratio.metadata_bytes,
            if ratio.stored == 0 {
                EXPANSION_RATIO as f64
            } else {
                ratio.decoded as f64 / ratio.stored as f64
            }
        );
    }
    Ok(sizes)
}

struct SampleRatio {
    stored: u64,
    decoded: u128,
    files: usize,
    metadata_bytes: usize,
}
impl SampleRatio {
    fn apply(&self, size: u64) -> u64 {
        if self.stored == 0 {
            return size.saturating_mul(EXPANSION_RATIO);
        }
        ((size as u128)
            .saturating_mul(self.decoded)
            .div_ceil(self.stored as u128))
        .min(u64::MAX as u128) as u64
    }
}

fn sample_group(
    files: &mut [InputFile],
    mut inspect: impl FnMut(&InputFile, &mut usize) -> anyhow::Result<u64>,
) -> anyhow::Result<SampleRatio> {
    use std::hash::{Hash, Hasher};
    // Stable pseudo-random order avoids always selecting the first input batch.
    files.sort_by_cached_key(|file| {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        file.path.hash(&mut hash);
        hash.finish()
    });
    let total = files
        .iter()
        .fold(0u64, |total, file| total.saturating_add(file.stored));
    let budget = total.div_ceil(10).min(FORMAT_SAMPLE_LIMIT);
    let mut metadata_left = budget as usize;
    let mut ratio = SampleRatio {
        stored: 0,
        decoded: 0,
        files: 0,
        metadata_bytes: 0,
    };
    for file in files {
        let weight = file.stored.min(budget - ratio.stored);
        if weight == 0 {
            continue;
        }
        let decoded = inspect(file, &mut metadata_left)?;
        // Metadata gives the whole-file ratio. Weight the final sampled file only
        // by the remaining quota: sampling never needs to read its payload.
        ratio.decoded += decoded as u128 * weight as u128 / file.stored as u128;
        ratio.stored += weight;
        ratio.files += 1;
        if ratio.stored == budget {
            break;
        }
    }
    ratio.metadata_bytes = budget as usize - metadata_left;
    Ok(ratio)
}

pub fn estimate(path: &Path) -> anyhow::Result<u64> {
    let metadata = std::fs::metadata(path)?;
    let length = metadata.len();
    let Some(format) = Compression::of(path) else {
        return Ok(length);
    };
    if !metadata.is_file() {
        return Ok(length.saturating_mul(EXPANSION_RATIO));
    }
    let mut budget = METADATA_BUDGET;
    inspect_file(path, length, format, &mut budget)
}

fn inspect_file(
    path: &Path,
    length: u64,
    format: Compression,
    budget: &mut usize,
) -> anyhow::Result<u64> {
    let fallback = length.saturating_mul(EXPANSION_RATIO);
    if format == Compression::Bzip2 || *budget == 0 {
        return Ok(fallback);
    }
    let file =
        File::open(path).with_context(|| format!("Cannot inspect input: {}", path.display()))?;
    let mut reader = MetadataReader::new(file, length);
    let allowance = (*budget).min(METADATA_BUDGET);
    reader.bytes_left = allowance;
    let result = match format {
        Compression::Gzip => gzip_size(&mut reader),
        Compression::Xz => xz_size(&mut reader),
        Compression::Lz4 => framed_size(&mut reader, true),
        Compression::Zstd => framed_size(&mut reader, false),
        Compression::Bzip2 => unreachable!(),
    };
    *budget -= allowance - reader.bytes_left;
    Ok(result.unwrap_or(fallback))
}

struct MetadataReader<R> {
    source: R,
    length: u64,
    bytes_left: usize,
    reads_left: usize,
}
impl<R: Read + Seek> MetadataReader<R> {
    fn new(source: R, length: u64) -> Self {
        Self {
            source,
            length,
            bytes_left: METADATA_BUDGET,
            reads_left: READ_BUDGET,
        }
    }
    fn read(&mut self, offset: u64, length: usize) -> Option<Vec<u8>> {
        if offset.checked_add(length as u64)? > self.length {
            return None;
        }
        self.bytes_left = self.bytes_left.checked_sub(length)?;
        self.reads_left = self.reads_left.checked_sub(1)?;
        self.source.seek(SeekFrom::Start(offset)).ok()?;
        let mut bytes = vec![0; length];
        self.source.read_exact(&mut bytes).ok()?;
        Some(bytes)
    }
    fn take(&mut self, offset: &mut u64, length: usize) -> Option<Vec<u8>> {
        let bytes = self.read(*offset, length)?;
        *offset = offset.checked_add(length as u64)?;
        Some(bytes)
    }
    fn skip(&self, offset: &mut u64, length: u64) -> Option<()> {
        *offset = offset.checked_add(length)?;
        (*offset <= self.length).then_some(())
    }
}
fn le(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .enumerate()
        .fold(0, |value, (i, b)| value | ((*b as u64) << (8 * i)))
}

// RFC 1952: ISIZE is the last member's decoded length modulo 2^32.
fn gzip_size<R: Read + Seek>(reader: &mut MetadataReader<R>) -> Option<u64> {
    if reader.length < 18 {
        return None;
    }
    let header = reader.read(0, 10)?;
    if header[..3] != [0x1f, 0x8b, 8] || header[3] & 0xe0 != 0 {
        return None;
    }
    let size = le(&reader.read(reader.length - 4, 4)?);
    Some(unwrap_gzip_size(reader.length, size))
}
fn unwrap_gzip_size(stored: u64, size: u64) -> u64 {
    const MODULUS: u64 = 1 << 32;
    // Permit header/trailer and incompressible DEFLATE overhead without inventing
    // a 4 GiB wrap for small/empty inputs. Invisible wraps cannot be recovered.
    let lower_bound = stored.saturating_sub(32 + stored / 1000);
    if stored < MODULUS && size >= lower_bound {
        return size;
    }
    // An implausibly small final member may instead indicate concatenated gzip.
    // Do not turn a small multi-member file into a multi-gigabyte estimate.
    if stored.saturating_mul(EXPANSION_RATIO) < MODULUS / 2 {
        return size.max(stored.saturating_mul(EXPANSION_RATIO));
    }
    let minimum_wraps = lower_bound.saturating_sub(size).div_ceil(MODULUS);
    let expected = stored.saturating_mul(EXPANSION_RATIO);
    let wraps = expected.saturating_sub(size).saturating_add(MODULUS / 2) / MODULUS;
    size.saturating_add(wraps.max(minimum_wraps).saturating_mul(MODULUS))
}

// XZ specification sections 2 and 4: walk backwards through compression indexes,
// summing every block's 63-bit uncompressed size; never inspect block contents.
fn xz_size<R: Read + Seek>(reader: &mut MetadataReader<R>) -> Option<u64> {
    let mut end = reader.length;
    if end % 4 != 0 {
        return None;
    }
    let mut total = 0u64;
    let mut streams = 0;
    while end > 0 {
        // Stream padding consists of groups of four zero bytes.
        while reader.read(end.checked_sub(4)?, 4)? == [0; 4] {
            end = end.checked_sub(4)?;
            if end == 0 {
                return None;
            }
        }
        let footer = reader.read(end.checked_sub(12)?, 12)?;
        if &footer[10..] != b"YZ"
            || footer[8] != 0
            || footer[9] & 0xf0 != 0
            || crc32fast::hash(&footer[4..10]) as u64 != le(&footer[..4])
        {
            return None;
        }
        let index_size = (le(&footer[4..8]) + 1).checked_mul(4)?;
        let index_start = end.checked_sub(12)?.checked_sub(index_size)?;
        let index = reader.read(index_start, index_size.try_into().ok()?)?;
        if index.len() < 8 || index[0] != 0 {
            return None;
        }
        let data_end = index.len() - 4;
        if crc32fast::hash(&index[..data_end]) as u64 != le(&index[data_end..]) {
            return None;
        }
        let mut pos = 1;
        let records = vli(&index[..data_end], &mut pos)?;
        if records > (data_end - pos) as u64 / 2 {
            return None;
        }
        let mut blocks_size = 0u64;
        for _ in 0..records {
            let unpadded = vli(&index[..data_end], &mut pos)?;
            if unpadded < 5 {
                return None;
            }
            blocks_size = blocks_size.checked_add(unpadded.checked_add(3)? & !3)?;
            total = total.checked_add(vli(&index[..data_end], &mut pos)?)?;
        }
        if data_end - pos > 3 || index[pos..data_end].iter().any(|b| *b != 0) {
            return None;
        }
        let start = index_start.checked_sub(blocks_size)?.checked_sub(12)?;
        let header = reader.read(start, 12)?;
        if &header[..6] != b"\xfd7zXZ\0"
            || header[6..8] != footer[8..10]
            || crc32fast::hash(&header[6..8]) as u64 != le(&header[8..])
        {
            return None;
        }
        end = start;
        streams += 1;
    }
    (streams > 0).then_some(total)
}
fn vli(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0;
    for i in 0..9 {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        if i > 0 && byte == 0 {
            return None;
        }
        value |= ((byte & 127) as u64) << (7 * i);
        if byte & 128 == 0 {
            return Some(value);
        }
    }
    None
}

// LZ4 and Zstandard frame headers optionally carry full content sizes. Block
// headers let us seek to following frames even when sizes were not recorded.
fn framed_size<R: Read + Seek>(reader: &mut MetadataReader<R>, lz4: bool) -> Option<u64> {
    let mut offset = 0;
    let mut total = 0u64;
    while offset < reader.length {
        let magic = le(&reader.take(&mut offset, 4)?);
        if (0x184d2a50..=0x184d2a5f).contains(&magic) {
            let size = le(&reader.take(&mut offset, 4)?);
            reader.skip(&mut offset, size)?;
            continue;
        }
        let size = if lz4 && magic == 0x184d2204 {
            lz4_size(reader, &mut offset)?
        } else if !lz4 && magic == 0xfd2fb528 {
            zstd_size(reader, &mut offset)?
        } else {
            return None;
        };
        total = total.checked_add(size)?;
    }
    Some(total)
}
fn lz4_size<R: Read + Seek>(reader: &mut MetadataReader<R>, offset: &mut u64) -> Option<u64> {
    let header = reader.take(offset, 2)?;
    let flags = header[0];
    let block_id = (header[1] >> 4) & 7;
    if flags & 0xc2 != 0x40 || header[1] & 0x8f != 0 || !(4..=7).contains(&block_id) {
        return None;
    }
    let content = if flags & 8 != 0 {
        Some(le(&reader.take(offset, 8)?))
    } else {
        None
    };
    if flags & 1 != 0 {
        reader.skip(offset, 4)?;
    } // Dictionary ID
    reader.skip(offset, 1)?; // Header checksum; estimation is not decompression validation.
    let mut estimate = 0u64;
    loop {
        let block = le(&reader.take(offset, 4)?) as u32;
        if block == 0 {
            break;
        }
        let size = (block & 0x7fffffff) as u64;
        if size > (1 << (8 + 2 * block_id)) {
            return None;
        }
        let decoded = if block & 0x80000000 != 0 {
            size
        } else {
            size.checked_mul(EXPANSION_RATIO)?
        };
        estimate = estimate.checked_add(decoded)?;
        reader.skip(offset, size)?;
        if flags & 16 != 0 {
            reader.skip(offset, 4)?;
        }
    }
    if flags & 4 != 0 {
        reader.skip(offset, 4)?;
    }
    Some(content.unwrap_or(estimate))
}
fn zstd_size<R: Read + Seek>(reader: &mut MetadataReader<R>, offset: &mut u64) -> Option<u64> {
    let flags = reader.take(offset, 1)?[0];
    if flags & 0x18 != 0 {
        return None;
    }
    let single = flags & 32 != 0;
    if !single {
        reader.skip(offset, 1)?;
    } // Window descriptor
    reader.skip(offset, [0, 1, 2, 4][(flags & 3) as usize])?;
    let size_bytes = match flags >> 6 {
        0 => usize::from(single),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let content = if size_bytes == 0 {
        None
    } else {
        Some(le(&reader.take(offset, size_bytes)?) + if size_bytes == 2 { 256 } else { 0 })
    };
    let mut estimate = 0u64;
    loop {
        let block = le(&reader.take(offset, 3)?);
        let size = block >> 3;
        if size > 128 * 1024 {
            return None;
        }
        let (stored, decoded) = match (block >> 1) & 3 {
            0 => (size, size),
            1 => (1, size),
            2 => (size, size.checked_mul(EXPANSION_RATIO)?),
            _ => return None,
        };
        estimate = estimate.checked_add(decoded)?;
        reader.skip(offset, stored)?;
        if block & 1 != 0 {
            break;
        }
    }
    if flags & 4 != 0 {
        reader.skip(offset, 4)?;
    }
    Some(content.unwrap_or(estimate))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    fn inspect(bytes: &[u8], format: &str) -> Option<u64> {
        let mut reader = MetadataReader::new(Cursor::new(bytes), bytes.len() as u64);
        match format {
            "xz" => xz_size(&mut reader),
            "gz" => gzip_size(&mut reader),
            "lz4" => framed_size(&mut reader, true),
            "zst" => framed_size(&mut reader, false),
            _ => unreachable!(),
        }
    }
    fn compress(bytes: &[u8], format: &str, record_size: bool) -> Vec<u8> {
        match format {
            "xz" => {
                let mut encoder = liblzma::write::XzEncoder::new(Vec::new(), 1);
                encoder.write_all(bytes).unwrap();
                encoder.finish().unwrap()
            }
            "gz" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                encoder.write_all(bytes).unwrap();
                encoder.finish().unwrap()
            }
            "lz4" => {
                let mut builder = lz4::EncoderBuilder::new();
                if record_size {
                    builder.content_size(bytes.len() as u64);
                }
                let mut encoder = builder.build(Vec::new()).unwrap();
                encoder.write_all(bytes).unwrap();
                encoder.finish().0
            }
            "zst" => {
                let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
                if record_size {
                    encoder
                        .set_pledged_src_size(Some(bytes.len() as u64))
                        .unwrap();
                }
                encoder.write_all(bytes).unwrap();
                encoder.finish().unwrap()
            }
            _ => unreachable!(),
        }
    }

    fn files(count: usize, stored: u64) -> Vec<InputFile> {
        (0..count)
            .map(|index| InputFile {
                index,
                path: format!("input-{index}.gz").into(),
                stored,
            })
            .collect()
    }

    #[test]
    fn group_sampling_uses_ten_percent_or_128_mib() {
        for (count, stored, expected_quota) in [(10, 100, 100), (2, 1 << 30, FORMAT_SAMPLE_LIMIT)] {
            let mut inputs = files(count, stored);
            let mut inspected = 0;
            let ratio = sample_group(&mut inputs, |file, metadata_left| {
                inspected += 1;
                *metadata_left -= 14;
                Ok(file.stored * 8)
            })
            .unwrap();
            assert_eq!(ratio.stored, expected_quota);
            assert_eq!(ratio.files, 1);
            assert_eq!(inspected, 1);
            assert_eq!(ratio.metadata_bytes, 14);
            assert_eq!(
                ratio.apply(stored * count as u64),
                stored * count as u64 * 8
            );
        }
    }

    #[test]
    fn group_ratio_aggregates_samples_and_preserves_input_order_independence() {
        let mut inputs = files(20, 5);
        let mut observed = Vec::new();
        let ratio = sample_group(&mut inputs, |file, _| {
            observed.push(file.index);
            Ok(if observed.len() == 1 { 5 } else { 50 })
        })
        .unwrap();
        assert_eq!(ratio.stored, 10);
        assert_eq!(ratio.decoded, 55);
        assert_eq!(ratio.apply(100), 550);
        let mut reversed = files(20, 5);
        reversed.reverse();
        let mut again = Vec::new();
        sample_group(&mut reversed, |file, _| {
            again.push(file.index);
            Ok(5)
        })
        .unwrap();
        assert_eq!(observed, again);
        let ratio = sample_group(&mut files(10, 0), |_, _| panic!("empty input sampled")).unwrap();
        assert_eq!(ratio.apply(0), 0);
    }

    #[test]
    fn compression_groups_include_aliases() {
        for (left, right) in [
            ("fa.gz", "input.tgz"),
            ("fa.xz", "input.txz"),
            ("fa.bz2", "input.tbz2"),
            ("fa.zst", "tar.zstd"),
        ] {
            assert_eq!(
                Compression::of(Path::new(left)),
                Compression::of(Path::new(right))
            );
        }
        assert_ne!(
            Compression::of(Path::new("a.gz")),
            Compression::of(Path::new("a.xz"))
        );
        assert_eq!(Compression::of(Path::new("a.tar")), None);
    }

    #[test]
    fn grouped_blocks_apply_separate_ratios_to_full_input_sizes() {
        use crate::sequences_stream::general::GeneralSequenceBlockData;
        use crate::sequences_stream::tar::TarSequenceBlock;
        let directory =
            std::env::temp_dir().join(format!("ggcat-group-size-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let mut blocks = Vec::new();
        let mut expected = Vec::new();
        for (format, suffix, size) in [
            ("gz", "gz", 30_000),
            ("xz", "xz", 60_000),
            ("lz4", "lz4", 80_000),
            ("zst", "zst", 100_000),
        ] {
            let bytes = compress(&vec![b'A'; size], format, true);
            for index in 0..10 {
                let path = directory.join(format!("input-{index}.tar.{suffix}"));
                std::fs::write(&path, &bytes).unwrap();
                blocks.push(if index % 2 == 0 {
                    GeneralSequenceBlockData::TAR(TarSequenceBlock::new(path, None))
                } else {
                    GeneralSequenceBlockData::FASTA((path, None))
                });
                expected.push(size as u64);
            }
        }
        let plain = directory.join("plain.fa");
        std::fs::write(&plain, b">read\nACGT\n").unwrap();
        blocks.push(GeneralSequenceBlockData::FASTA((plain, None)));
        expected.push(11);
        assert_eq!(estimate_blocks(&blocks).unwrap(), expected);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn full_sizes_from_real_compressors() {
        for size in [0, 1, 256, 300_000] {
            let contents = vec![b'A'; size];
            for format in ["xz", "gz", "lz4", "zst"] {
                let bytes = compress(&contents, format, true);
                assert_eq!(
                    inspect(&bytes, format),
                    Some(size as u64),
                    "{format}, {size}"
                );
            }
        }
    }
    #[test]
    fn concatenated_streams_and_padding() {
        for format in ["xz", "lz4", "zst"] {
            let mut bytes = compress(&vec![b'A'; 20_000], format, true);
            if format == "xz" {
                bytes.extend([0; 16]);
            } else {
                // Shared LZ4/Zstandard skippable-frame format.
                bytes.extend(0x184d2a50u32.to_le_bytes());
                bytes.extend(7u32.to_le_bytes());
                bytes.extend(b"ignored");
            }
            bytes.extend(compress(&vec![b'C'; 40_000], format, true));
            if format == "xz" {
                bytes.extend([0; 8]);
            }
            assert_eq!(inspect(&bytes, format), Some(60_000), "{format}");
        }
    }
    #[test]
    fn missing_frame_sizes_use_block_estimates() {
        for format in ["lz4", "zst"] {
            let bytes = compress(&vec![b'A'; 300_000], format, false);
            let estimate = inspect(&bytes, format).unwrap();
            assert!(estimate > 0 && estimate <= 300_000, "{format}: {estimate}");
        }
        // A zstd RLE block stores just one byte, but declares the decoded size.
        let mut bytes = 0xfd2fb528u32.to_le_bytes().to_vec();
        bytes.extend([0, 0]); // no content size, window descriptor
        bytes.extend(&((100u32 << 3) | 3).to_le_bytes()[..3]);
        bytes.push(b'A');
        assert_eq!(inspect(&bytes, "zst"), Some(100));
    }

    // A sparse, guarded file model: any read into a compressed payload panics.
    struct MetadataOnly {
        length: u64,
        offset: u64,
        regions: Vec<(u64, Vec<u8>)>,
        bytes_read: usize,
    }
    impl Read for MetadataOnly {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let (start, bytes) = self
                .regions
                .iter()
                .find(|(start, bytes)| {
                    self.offset >= *start && self.offset < start + bytes.len() as u64
                })
                .expect("estimation attempted to read payload");
            let index = (self.offset - start) as usize;
            let count = buffer.len().min(bytes.len() - index);
            buffer[..count].copy_from_slice(&bytes[index..index + count]);
            self.offset += count as u64;
            self.bytes_read += count;
            Ok(count)
        }
    }
    impl Seek for MetadataOnly {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            let SeekFrom::Start(offset) = pos else {
                panic!("unexpected seek");
            };
            assert!(offset <= self.length);
            self.offset = offset;
            Ok(offset)
        }
    }
    fn encode_vli(mut value: u64, bytes: &mut Vec<u8>) {
        while value >= 128 {
            bytes.push(value as u8 | 128);
            value >>= 7;
        }
        bytes.push(value as u8);
    }
    #[test]
    fn xz_multiple_blocks_above_4gib_without_reading_payloads() {
        let header = compress(b"", "xz", true)[..12].to_vec();
        let unpadded = [1024u64, 2048];
        let sizes = [(1u64 << 32) + 19, (1u64 << 33) + 7];
        let mut index = vec![0, 2];
        for (stored, decoded) in unpadded.into_iter().zip(sizes) {
            encode_vli(stored, &mut index);
            encode_vli(decoded, &mut index);
        }
        while index.len() % 4 != 0 {
            index.push(0);
        }
        index.extend(crc32fast::hash(&index).to_le_bytes());
        let mut footer = ((index.len() as u32 / 4) - 1).to_le_bytes().to_vec();
        footer.extend(&header[6..8]);
        let mut tail = crc32fast::hash(&footer).to_le_bytes().to_vec();
        tail.extend(footer);
        tail.extend(b"YZ");
        let index_start = 12 + unpadded.iter().sum::<u64>();
        let length = index_start + index.len() as u64 + 12;
        let source = MetadataOnly {
            length,
            offset: 0,
            bytes_read: 0,
            regions: vec![(0, header), (index_start, index), (length - 12, tail)],
        };
        let mut reader = MetadataReader::new(source, length);
        assert_eq!(xz_size(&mut reader), Some(sizes.iter().sum()));
        assert!(reader.source.bytes_read < 100);
    }
    #[test]
    fn gzip_wrap_estimation_reads_only_header_and_isize() {
        let length = 3u64 << 30;
        let source = MetadataOnly {
            length,
            offset: 0,
            bytes_read: 0,
            regions: vec![
                (0, vec![31, 139, 8, 0, 0, 0, 0, 0, 0, 0]),
                (length - 4, 123u32.to_le_bytes().to_vec()),
            ],
        };
        let mut reader = MetadataReader::new(source, length);
        let size = gzip_size(&mut reader).unwrap();
        assert!(size >= length);
        assert_eq!(size % (1 << 32), 123);
        assert_eq!(reader.source.bytes_read, 14);
        assert_eq!(unwrap_gzip_size(20, 0), 0);
        assert_eq!(unwrap_gzip_size(1024, 1000), 1000);
        let mut concatenated = compress(&vec![b'A'; 10_000], "gz", true);
        concatenated.extend(compress(b"", "gz", true));
        assert_eq!(
            inspect(&concatenated, "gz"),
            Some(concatenated.len() as u64 * EXPANSION_RATIO)
        );
    }
    #[test]
    fn framed_payloads_are_only_seeked_over() {
        let length = 1_000_030;
        let mut header = 0x184d2204u32.to_le_bytes().to_vec();
        header.extend([0x68, 0x70]);
        header.extend((1u64 << 34).to_le_bytes());
        header.push(0);
        header.extend(1_000_000u32.to_le_bytes());
        let source = MetadataOnly {
            length: length - 7,
            offset: 0,
            bytes_read: 0,
            regions: vec![(0, header), (1_000_019, vec![0; 4])],
        };
        let mut reader = MetadataReader::new(source, length - 7);
        assert_eq!(framed_size(&mut reader, true), Some(1 << 34));
        assert!(reader.source.bytes_read < 30);

        let mut header = 0xfd2fb528u32.to_le_bytes().to_vec();
        header.push(0xa0); // single segment, four-byte content size
        header.extend(131_072u32.to_le_bytes());
        header.extend(&(65_536u32 << 3).to_le_bytes()[..3]);
        let second = ((65_536u32 << 3) | 1).to_le_bytes()[..3].to_vec();
        let length = 131_087;
        let source = MetadataOnly {
            length,
            offset: 0,
            bytes_read: 0,
            regions: vec![(0, header), (65_548, second)],
        };
        let mut reader = MetadataReader::new(source, length);
        assert_eq!(framed_size(&mut reader, false), Some(131_072));
        assert_eq!(reader.source.bytes_read, 15);
    }
    #[test]
    fn malformed_and_excessive_metadata_fall_back() {
        for format in ["xz", "gz", "lz4", "zst"] {
            assert!(inspect(b"not compressed", format).is_none());
            let mut bytes = compress(&vec![b'A'; 1000], format, true);
            if format == "gz" {
                bytes.truncate(5);
            } else {
                bytes.truncate(bytes.len() - 3);
            }
            assert!(inspect(&bytes, format).is_none(), "{format}");
        }
        let bytes = compress(b"data", "xz", true);
        let mut reader = MetadataReader::new(Cursor::new(&bytes), bytes.len() as u64);
        reader.bytes_left = 10;
        assert!(xz_size(&mut reader).is_none());
        let bytes = compress(b"data", "lz4", true);
        let mut reader = MetadataReader::new(Cursor::new(&bytes), bytes.len() as u64);
        reader.reads_left = 1;
        assert!(framed_size(&mut reader, true).is_none());
    }
    #[test]
    fn file_estimates_include_all_tar_bytes_and_aliases() {
        let directory =
            std::env::temp_dir().join(format!("ggcat-input-size-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let mut builder = tar::Builder::new(Vec::new());
        let contents = vec![b'x'; 30_000];
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "ignored.txt", contents.as_slice())
            .unwrap();
        let tar = builder.into_inner().unwrap();
        for (suffix, format) in [
            ("tar", ""),
            ("txz", "xz"),
            ("tgz", "gz"),
            ("tar.lz4", "lz4"),
            ("tar.zstd", "zst"),
            ("fa.xz", "xz"),
        ] {
            let bytes = if format.is_empty() {
                tar.clone()
            } else {
                compress(&tar, format, true)
            };
            let path = directory.join(format!("input.{suffix}"));
            std::fs::write(&path, bytes).unwrap();
            assert_eq!(estimate(&path).unwrap(), tar.len() as u64, "{suffix}");
        }
        for suffix in ["bz2", "tbz2", "xz", "gz", "lz4", "zst"] {
            let path = directory.join(format!("invalid.{suffix}"));
            std::fs::write(&path, b"invalid").unwrap();
            assert_eq!(estimate(&path).unwrap(), 28);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
