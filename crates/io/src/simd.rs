//! Sequences read with the SIMD parser of `simd-accel`, from files and archives as
//! [`SequencesReader`](crate::sequences_reader::SequencesReader) reads them.

use crate::binary_reader::{
    BinaryEvent, CHUNK_BLOCK, CopybacksSettings, GenericBinaryReader, OffsetBase, is_archive,
};
use simd_accel::copyback_tracker::{CopybackTracker, LzCopy, TrackerStats};
use simd_accel::masks::MaskKernel;
use simd_accel::sequence_parser::BLOCK_SIZE;
use std::path::Path;
use utils::dna_sequence::DnaSequencesFileType;

/// What the reader hands out, from `simd-accel`: its sequences turn into io's `CompressedRead`
/// with `.into()`.
pub use simd_accel::sequence_buffer::{SequenceBuffer, SequenceDescription};
/// How each source's parser is set up; also the parser's own defaults.
pub use simd_accel::sequence_parser::{
    DEFAULT_BASES_CAPACITY, DEFAULT_IDENT_CAPACITY, LzMode, ParserSettings, SequenceParser,
};

/// How much the parser gets at once: a whole number of its blocks, as it requires of every
/// slice but an input's last, and many of them, so that the kernel it runs costs a branch per
/// chunk rather than per block.
const CHUNK_SIZE: usize = 64 * 1024;
const _: () = assert!(CHUNK_SIZE.is_multiple_of(BLOCK_SIZE));
// The reader cuts its chunks at whole blocks of its own, which must be whole parser blocks.
const _: () = assert!(CHUNK_BLOCK.is_multiple_of(BLOCK_SIZE));

/// Whether the SIMD reader follows the LZ copies of the file at `path`: a FASTA file, by its
/// name. The others get no copybacks, see [`CopybacksSettings::accepts_file`].
pub fn is_fasta(path: &Path) -> bool {
    path.to_str().and_then(DnaSequencesFileType::from_name) == Some(DnaSequencesFileType::FASTA)
}

pub struct SimdSequencesReader {
    /// Hands the input out in chunks of at most [`CHUNK_SIZE`] bytes, each a whole number of
    /// blocks, only the last one of each file shorter.
    reader: GenericBinaryReader,
    /// The kernel every parser runs, chosen once for this CPU.
    kernel: MaskKernel,
    /// The copybacks of an input whose [`LzMode`] uses its LZ copies.
    copybacks_settings: CopybacksSettings,
    /// The sources of the LZ copies of the stream being read, with copybacks.
    tracker: CopybackTracker,
}

/// The copybacks the pipeline reads its inputs with, when its [`LzMode`] uses them: those of
/// the FASTA files ([`is_fasta`]), of the hard threshold and compression ratio `config` sets.
/// Its `min_len` is a placeholder: each input gets its own, from its
/// [`ParserSettings::copybacks_min_len`].
pub fn pipeline_copybacks_settings() -> CopybacksSettings {
    CopybacksSettings {
        min_len: 1,
        hard_min_len: config::COPYBACKS_HARD_MIN_LEN,
        min_compression_ratio: config::COPYBACKS_MIN_COMPRESSION_RATIO,
        accepts_file: is_fasta,
    }
}

impl SimdSequencesReader {
    /// A reader of the sequences of its inputs, which uses their LZ copies as each input's
    /// [`ParserSettings::lz_mode`] says, with [`pipeline_copybacks_settings`]. With
    /// [`LzMode::SkipOnly`] it follows the sources of the copies of their FASTA files as it
    /// parses them (see [`CopybackTracker`]); any other file's copybacks are only dropped as the
    /// reader gets past them, so that their queue does not grow.
    pub fn new() -> Self {
        Self::with_copybacks(pipeline_copybacks_settings())
    }

    /// As [`Self::new`], with `copybacks` when the mode uses them. The reader follows the copies
    /// of FASTA files only, so their [`CopybacksSettings::accepts_file`] is ignored: it is
    /// [`is_fasta`].
    pub fn with_copybacks(copybacks: CopybacksSettings) -> Self {
        Self {
            reader: GenericBinaryReader::new(Some(CHUNK_SIZE), None),
            kernel: MaskKernel::detect(),
            copybacks_settings: CopybacksSettings {
                accepts_file: is_fasta,
                ..copybacks
            },
            tracker: CopybackTracker::new(),
        }
    }

    /// The kernel the parsers run.
    pub fn kernel(&self) -> MaskKernel {
        self.kernel
    }

    /// What happened to the LZ copies followed so far, across the inputs read.
    pub fn tracker_stats(&self) -> TrackerStats {
        self.tracker.stats()
    }

    /// As [`SequencesReader::process_file_extended`], with the bases packed and handed to
    /// `func` a [`SequenceBuffer`] at a time, by value: `func` gives back a buffer for the
    /// parser to fill later, a new one or one it is done with. The source gets a parser of
    /// `settings`, which every file, or every member of an archive, is fed to a chunk at a time,
    /// after a reset to its format. Each file's last buffer is handed over at its end; a
    /// copyback splits the sequences too long for a buffer, as the line reader's does. The
    /// sequences hold only ACGT bases, split at the other bytes, and those shorter than
    /// `min_seqlen` are left out. Returns how many records the source held, as
    /// [`SequenceParser::records`] counts them: the record indices, if asked for, go on across
    /// the members of an archive.
    ///
    /// [`SequencesReader::process_file_extended`]: crate::sequences_reader::SequencesReader::process_file_extended
    pub fn process_file_extended<F: FnMut(SequenceBuffer) -> SequenceBuffer>(
        &mut self,
        source: impl AsRef<Path>,
        settings: &ParserSettings,
        func: F,
        remove_file: bool,
    ) -> u64 {
        self.process_file_with_names(source, settings, |_| {}, func, remove_file)
    }

    /// As [`Self::process_file_extended`], calling `on_file` with the name of each file it reads
    /// sequences from as it starts, before any buffer of its sequences: the source itself, or
    /// each member of the archive it is that holds FASTA or FASTQ, named `archive:member` as
    /// [`BinaryEvent::FilePath`] names it. Each buffer `func` gets holds the sequences of the
    /// file named last only.
    pub fn process_file_with_names<F: FnMut(SequenceBuffer) -> SequenceBuffer>(
        &mut self,
        source: impl AsRef<Path>,
        settings: &ParserSettings,
        mut on_file: impl FnMut(&str),
        mut func: F,
        remove_file: bool,
    ) -> u64 {
        let source = source.as_ref();
        self.reader.set_copybacks(match settings.lz_mode {
            LzMode::Off => None,
            LzMode::SkipOnly => Some(CopybacksSettings {
                min_len: settings.copybacks_min_len,
                ..self.copybacks_settings
            }),
            LzMode::SkipAndCount => unimplemented!("LZ copies with their multiplicities"),
        });
        let mut parser = settings.parser(self.kernel);
        // Whether the file being read is a sequence file the parser reads, not one skipped.
        let mut parsing = false;
        // Whether it is a FASTA file with copybacks, whose copies are followed.
        let mut tracking = false;
        let tracker = &mut self.tracker;
        tracker.start_stream(settings.copy_margin);

        let result = self.reader.read_file(source, |event| match event {
            BinaryEvent::FilePath { name, base } => {
                // The previous file is finished before the next one starts.
                if parsing {
                    parser.finish(&mut func);
                }
                if tracking {
                    tracker.end_file();
                }
                if base == OffsetBase::New {
                    tracker.start_stream(settings.copy_margin);
                }
                let name = name.unwrap_or_default();
                let format = DnaSequencesFileType::from_name(name);
                // Only FASTA files get copybacks, see `with_copybacks`.
                tracking = base != OffsetBase::None;
                debug_assert!(!tracking || format == Some(DnaSequencesFileType::FASTA));
                parsing = match format {
                    Some(format @ (DnaSequencesFileType::FASTA | DnaSequencesFileType::FASTQ)) => {
                        parser.reset(format);
                        on_file(name);
                        true
                    }
                    Some(DnaSequencesFileType::GFA) => todo!(),
                    // Archives can hold other files too, such as their documentation.
                    None if is_archive(source) => {
                        ggcat_logging::info!("Skipping {name}: unsupported sequence format");
                        false
                    }
                    None => panic!("Cannot recognize file type of '{}'", source.display()),
                };
            }
            BinaryEvent::Chunk(chunk) => {
                if parsing {
                    parser.parse_data(chunk, &mut func);
                }
            }
            BinaryEvent::ChunkWithCopybacks {
                chunk,
                offset,
                copybacks,
            } => {
                // The copies found since the last chunk, all of those copying its bytes: the
                // tracker takes them out of the queue, and merges those continuing its last one.
                for copyback in copybacks.drain(..) {
                    debug_assert!(copyback.len <= copyback.offset);
                    debug_assert!(copyback.len >= settings.copybacks_min_len);
                    tracker.add_copy(LzCopy {
                        destination: copyback.destination,
                        offset: copyback.offset,
                        len: copyback.len,
                    });
                }
                debug_assert!(tracking);
                parser.parse_data_tracked(chunk, offset, tracker, &mut func);
                tracker.retire_before(offset + chunk.len() as u64);
            }
        });
        if parsing {
            parser.finish(&mut func);
        }
        if tracking {
            tracker.end_file();
        }
        // The copies landing past the last file, in the end of an archive, go with the stream.
        tracker.retire_before(u64::MAX);
        if let Err(error) = result {
            ggcat_logging::error!(
                "WARNING: Error while reading file {}: {error:#}",
                source.display()
            );
        }

        if remove_file {
            std::fs::remove_file(source).unwrap();
        }
        parser.records()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn settings(lz_mode: LzMode) -> ParserSettings {
        ParserSettings {
            keep_ident: true,
            copyback_size: None,
            min_seqlen: 1,
            record_indices: false,
            bases_capacity: DEFAULT_BASES_CAPACITY,
            ident_capacity: DEFAULT_IDENT_CAPACITY,
            lz_mode,
            copybacks_min_len: 16,
            copy_margin: 0,
        }
    }

    fn copybacks() -> CopybacksSettings {
        CopybacksSettings {
            min_len: 16,
            hard_min_len: 8,
            min_compression_ratio: 0.0,
            accepts_file: is_fasta,
        }
    }

    /// `count` near-identical genomes of `len` bases, wrapped at 80 columns: long copies a
    /// genome apart, across records and lines.
    fn genomes(seed: u64, count: usize, len: usize) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let base: Vec<u8> = (0..len).map(|_| b"ACGT"[next(4) as usize]).collect();
        let mut fasta = Vec::new();
        for genome in 0..count {
            fasta.extend_from_slice(format!(">genome_{seed}_{genome}\n").as_bytes());
            let sequence: Vec<u8> = (base.iter())
                .map(|&base| {
                    if next(500) == 0 {
                        b"ACGTN"[next(5) as usize]
                    } else {
                        base
                    }
                })
                .collect();
            for line in sequence.chunks(80) {
                fasta.extend_from_slice(line);
                fasta.push(b'\n');
            }
        }
        fasta
    }

    /// `genomes` with long runs of `A` and of `N` in their sequences, which LZ matches
    /// overlapping their own output repeat: lz_copyback drops them, as their offset is shorter
    /// than its threshold.
    fn genomes_with_runs(seed: u64, count: usize, len: usize) -> Vec<u8> {
        let mut fasta = Vec::new();
        for (index, line) in genomes(seed, count, len)
            .split_inclusive(|&byte| byte == b'\n')
            .enumerate()
        {
            fasta.extend_from_slice(line);
            if index % 50 == 25 && !line.starts_with(b">") {
                let run = if index % 100 == 25 { b'A' } else { b'N' };
                fasta.extend(std::iter::repeat_n(run, 3000 + 7 * index));
                fasta.push(b'\n');
            }
        }
        fasta
    }

    /// FASTQ reads cut out of `fasta`'s sequence lines.
    fn reads(fasta: &[u8]) -> Vec<u8> {
        let mut fastq = Vec::new();
        for (index, line) in fasta.split(|&byte| byte == b'\n').enumerate() {
            if line.len() == 80 {
                fastq.extend_from_slice(format!("@read_{index}\n").as_bytes());
                fastq.extend_from_slice(line);
                fastq.extend_from_slice(b"\n+\n");
                fastq.extend_from_slice(&[b'I'; 80]);
                fastq.push(b'\n');
            }
        }
        fastq
    }

    /// The bytes of FASTA sequence lines, which the base index counts.
    fn fasta_bases(fasta: &[u8]) -> u64 {
        (fasta.split(|&byte| byte == b'\n'))
            .filter(|line| !line.starts_with(b">"))
            .map(|line| line.len() as u64)
            .sum()
    }

    fn xz(data: &[u8]) -> Vec<u8> {
        let mut encoder = liblzma::write::XzEncoder::new(Vec::new(), 6);
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn tar(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            builder.append_data(&mut header, name, *data).unwrap();
        }
        builder.into_inner().unwrap()
    }

    /// Every buffer `reader` hands out for `path`, in LZ mode `lz_mode`: its sequences, headers
    /// and packed bases.
    fn buffers(
        reader: &mut SimdSequencesReader,
        path: &Path,
        lz_mode: LzMode,
    ) -> Vec<(Vec<SequenceDescription>, Vec<u8>, Vec<u8>)> {
        let mut buffers = Vec::new();
        reader.process_file_extended(
            path,
            &settings(lz_mode),
            |buffer| {
                buffers.push((
                    buffer.descriptions().to_vec(),
                    buffer.ident().to_vec(),
                    buffer.bases().to_vec(),
                ));
                buffer
            },
            false,
        );
        buffers
    }

    fn directory(test: &str) -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-simd-copybacks-{test}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    /// With copybacks the sequences are the same, the sources of the FASTA copies are followed
    /// to their end, never late, those of the FASTQ files not at all, and no copyback is left in
    /// the queue at the end; the base index runs on across the FASTA members of one stream.
    #[test]
    fn copies_are_followed_in_fasta_only_and_the_queue_is_emptied() {
        let directory = directory("follow");
        let (a, c) = (genomes(1, 6, 20_000), genomes(2, 4, 30_000));
        let b = reads(&genomes(3, 3, 20_000));
        let d = genomes_with_runs(4, 4, 20_000);
        let inputs = [
            ("a.fa.xz", xz(&a), fasta_bases(&a), true),
            ("a.fa.gz", gzip(&a), fasta_bases(&a), true),
            ("d.fa.xz", xz(&d), fasta_bases(&d), true),
            ("b.fq.xz", xz(&b), 0, false),
            (
                "abc.tar.xz",
                xz(&tar(&[("a.fa", &a), ("b.fq", &b), ("c.fa", &c)])),
                fasta_bases(&a) + fasta_bases(&c),
                true,
            ),
        ];
        for (name, encoded, bases, has_fasta) in inputs {
            let path = directory.join(name);
            std::fs::write(&path, encoded).unwrap();
            let mut plain_reader = SimdSequencesReader::new();
            let plain = buffers(&mut plain_reader, &path, LzMode::Off);
            assert_eq!(
                plain_reader.tracker_stats().received,
                0,
                "{name}: copybacks with LZ off"
            );
            let mut reader = SimdSequencesReader::with_copybacks(copybacks());
            assert!(
                buffers(&mut reader, &path, LzMode::SkipOnly) == plain,
                "{name}: the sequences differ"
            );
            let stats = reader.tracker_stats();
            // A FASTQ file of its own records none.
            assert_eq!(stats.received > 0, has_fasta, "{name}: {stats:?}");
            assert_eq!(stats.dropped_late, 0, "{name}");
            // Every open copy closes, the margin being 0; every copy received ends closed or
            // dropped.
            assert_eq!(stats.opened, stats.closed, "{name}");
            assert_eq!(
                stats.received,
                stats.closed + stats.dropped_too_short + stats.dropped_late,
                "{name}: {stats:?}"
            );
            assert_eq!(stats.opened > 0, has_fasta, "{name}: {stats:?}");
            assert_eq!(reader.tracker.live_copies(), 0, "{name}: copybacks left");
            assert_eq!(
                reader.tracker.base_index(),
                bases,
                "{name}: the bases counted"
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
