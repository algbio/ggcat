//! Sequences read with the SIMD parser of `simd-accel`, from files and archives as
//! [`SequencesReader`](crate::sequences_reader::SequencesReader) reads them.

use crate::binary_reader::{
    BinaryEvent, CHUNK_BLOCK, CopybacksSettings, GenericBinaryReader, OffsetBase, is_archive,
};
#[cfg(debug_assertions)]
use simd_accel::copyback_tracker::TrackedCopy;
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
    /// The copies [`DebugFollowingCheck`] compared, across inputs.
    #[cfg(debug_assertions)]
    debug_compared_copies: u64,
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

/// Whether the parser of `settings` tells the copyback tracker where each header line is, so
/// that the skips keep the headers and record indices exact: when it keeps either, or counts
/// k-mers. Otherwise nothing reads them, and the skips spare that work.
fn needs_header_marks(settings: &ParserSettings) -> bool {
    settings.keep_ident || settings.record_indices || settings.lz_mode == LzMode::SkipAndCount
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
            #[cfg(debug_assertions)]
            debug_compared_copies: 0,
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

    /// How many copies the debug check of the skips compared, across the inputs read: see
    /// [`DebugFollowingCheck`].
    #[cfg(debug_assertions)]
    pub fn debug_compared_copies(&self) -> u64 {
        self.debug_compared_copies
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
    /// With [`LzMode::SkipOnly`], the parser skips the copies of the FASTA files it can. If
    /// `settings` keep the headers or the record indices, the skips keep them exact, header
    /// marks telling the parser the header lines it skips (see [`needs_header_marks`]): each
    /// sequence after a skip has the header and index of its record, and the count of records
    /// is exact. Otherwise the sequences after a skip have no header, and the count of records
    /// is not exact.
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
        let header_marks = needs_header_marks(settings);
        tracker.start_stream(settings.copy_margin, header_marks);
        #[cfg(debug_assertions)]
        let mut check = (settings.lz_mode == LzMode::SkipOnly)
            .then(|| DebugFollowingCheck::new(settings, self.kernel, tracker));

        let result = self.reader.read_file(source, |event| match event {
            BinaryEvent::FilePath {
                name,
                base,
                end_offset,
            } => {
                // The previous file is finished before the next one starts.
                if parsing {
                    parser.finish(&mut func);
                }
                if tracking {
                    tracker.end_file();
                    #[cfg(debug_assertions)]
                    if let Some(check) = &mut check {
                        check.end_file();
                    }
                }
                if base == OffsetBase::New {
                    tracker.start_stream(settings.copy_margin, header_marks);
                    #[cfg(debug_assertions)]
                    if let Some(check) = &mut check {
                        check
                            .tracker
                            .start_stream(settings.copy_margin, header_marks);
                    }
                }
                let name = name.unwrap_or_default();
                let format = DnaSequencesFileType::from_name(name);
                // Only FASTA files get copybacks, see `with_copybacks`.
                tracking = base != OffsetBase::None;
                debug_assert!(!tracking || format == Some(DnaSequencesFileType::FASTA));
                if tracking {
                    tracker.start_file(end_offset);
                    #[cfg(debug_assertions)]
                    if let Some(check) = &mut check {
                        check.parser.reset(DnaSequencesFileType::FASTA);
                        check.tracker.start_file(end_offset);
                    }
                }
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
                    let copy = LzCopy {
                        destination: copyback.destination,
                        offset: copyback.offset,
                        len: copyback.len,
                    };
                    tracker.add_copy(copy);
                    #[cfg(debug_assertions)]
                    if let Some(check) = &mut check {
                        check.tracker.add_copy(copy);
                    }
                }
                debug_assert!(tracking);
                parser.parse_data_tracked(chunk, offset, tracker, &mut func);
                #[cfg(debug_assertions)]
                if let Some(check) = &mut check {
                    check.chunk(chunk, offset, tracker);
                }
                tracker.retire_before(offset + chunk.len() as u64);
            }
        });
        if parsing {
            parser.finish(&mut func);
        }
        if tracking {
            tracker.end_file();
            #[cfg(debug_assertions)]
            if let Some(check) = &mut check {
                check.end_file();
            }
        }
        #[cfg(debug_assertions)]
        if let Some(mut check) = check {
            check.assert_same_progress(tracker);
            check.assert_same_records_before(tracker, u64::MAX);
            self.debug_compared_copies += check.compared;
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

/// With debug assertions, the check of the skips of [`SimdSequencesReader`]: a parser of its
/// own and a tracker that only follows the copies, fed the same files, copies and chunks as the
/// reader's, which skips them. Skipping must change none of the tracker's records: after each
/// chunk, the two base indices and the following side of the stats are the same, and so are
/// the copies about to be retired, their cuts and base indices. Debug builds parse twice.
#[cfg(debug_assertions)]
struct DebugFollowingCheck {
    parser: SequenceParser,
    tracker: CopybackTracker,
    /// The stats of the reader's tracker when the input started: they run on across inputs.
    stats_before: TrackerStats,
    /// How many copies were compared.
    compared: u64,
}

#[cfg(debug_assertions)]
impl DebugFollowingCheck {
    fn new(settings: &ParserSettings, kernel: MaskKernel, skipping: &CopybackTracker) -> Self {
        let mut tracker = CopybackTracker::following_only();
        tracker.start_stream(settings.copy_margin, needs_header_marks(settings));
        Self {
            parser: settings.parser(kernel),
            tracker,
            stats_before: skipping.stats(),
            compared: 0,
        }
    }

    fn end_file(&mut self) {
        self.parser.finish(|buffer| buffer);
        self.tracker.end_file();
    }

    /// Parses the chunk at stream offset `offset` the reader's parser just parsed, and compares
    /// the trackers, then retires the copies passed, as the reader does.
    fn chunk(&mut self, chunk: &[u8], offset: u64, skipping: &CopybackTracker) {
        (self.parser).parse_data_tracked(chunk, offset, &mut self.tracker, |buffer| buffer);
        let end = offset + chunk.len() as u64;
        self.assert_same_progress(skipping);
        self.assert_same_records_before(skipping, end);
        self.tracker.retire_before(end);
    }

    /// Compares the base indices, and the stats skipping must not change.
    fn assert_same_progress(&self, skipping: &CopybackTracker) {
        debug_assert_eq!(skipping.base_index(), self.tracker.base_index());
        let (a, before, b) = (skipping.stats(), self.stats_before, self.tracker.stats());
        debug_assert_eq!(
            [
                a.received - before.received,
                a.opened - before.opened,
                a.closed - before.closed,
                a.dropped_too_short - before.dropped_too_short,
                a.dropped_late - before.dropped_late,
                a.skippable_bases - before.skippable_bases,
            ],
            [
                b.received,
                b.opened,
                b.closed,
                b.dropped_too_short,
                b.dropped_late,
                b.skippable_bases,
            ],
            "the stats of the copies followed differ from those of the copies skipped"
        );
    }

    /// Compares the copies of both trackers whose destination starts before `position`, those
    /// retired next.
    fn assert_same_records_before(&mut self, skipping: &CopybackTracker, position: u64) {
        let passed = |tracked: &&TrackedCopy| tracked.copy.destination < position;
        let mut ours = self.tracker.copies().take_while(passed);
        let mut theirs = skipping.copies().take_while(passed);
        loop {
            match (ours.next(), theirs.next()) {
                (None, None) => break,
                (followed, skipped) => {
                    debug_assert_eq!(skipped, followed, "a copy differs once skipped");
                    self.compared += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::io::Write;

    /// The settings of the reader with k-mers of 31 bases, which keep no headers, so no header
    /// marks.
    fn settings(lz_mode: LzMode) -> ParserSettings {
        ParserSettings {
            keep_ident: false,
            copyback_size: None,
            min_seqlen: 1,
            record_indices: false,
            bases_capacity: DEFAULT_BASES_CAPACITY,
            ident_capacity: DEFAULT_IDENT_CAPACITY,
            lz_mode,
            copybacks_min_len: 16,
            copy_margin: lz_mode.copy_margin(31, 15),
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

    /// The k-mers of `k` bases of the sequences of `buffers`, each as its bases' codes.
    fn kmers(
        buffers: &[(Vec<SequenceDescription>, Vec<u8>, Vec<u8>)],
        k: usize,
    ) -> HashSet<Vec<u8>> {
        let mut kmers = HashSet::new();
        for (descriptions, _, bases) in buffers {
            for description in descriptions {
                let start = description.bases_start;
                let codes: Vec<u8> = (start..start + description.bases_len)
                    .map(|base| (bases[base / 4] >> (2 * (base % 4))) & 3)
                    .collect();
                kmers.extend(codes.windows(k).map(<[u8]>::to_vec));
            }
        }
        kmers
    }

    fn directory(test: &str) -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-simd-copybacks-{test}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    /// With copybacks the 31-mers are the same, the copies of the FASTA files are followed and
    /// skipped, never late, those of the FASTQ files not at all, and no copyback is left in the
    /// queue at the end; the base index runs on across the FASTA members of one stream, the
    /// bases skipped counted. With debug assertions, the reader's check compares the copies
    /// skipped with the copies followed.
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
            let skipped = buffers(&mut reader, &path, LzMode::SkipOnly);
            assert!(
                kmers(&skipped, 31) == kmers(&plain, 31),
                "{name}: the 31-mers differ"
            );
            let stats = reader.tracker_stats();
            // A FASTQ file of its own records none.
            assert_eq!(stats.received > 0, has_fasta, "{name}: {stats:?}");
            assert_eq!(stats.dropped_late, 0, "{name}");
            // Every copy received ends closed or dropped.
            assert_eq!(
                stats.received,
                stats.closed + stats.dropped_too_short + stats.dropped_late,
                "{name}: {stats:?}"
            );
            assert_eq!(stats.opened > 0, has_fasta, "{name}: {stats:?}");
            assert_eq!(stats.skips > 0, has_fasta, "{name}: {stats:?}");
            #[cfg(debug_assertions)]
            assert_eq!(
                reader.debug_compared_copies() > 0,
                has_fasta,
                "{name}: the copies compared"
            );
            assert_eq!(reader.tracker.live_copies(), 0, "{name}: copybacks left");
            assert_eq!(
                reader.tracker.base_index(),
                bases,
                "{name}: the bases counted"
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// Records of one line each, two of them repeated in turn, with a base changed here and
    /// there: the LZ copies run across their headers, and break inside their sequences.
    fn repeated_records(seed: u64, count: usize, len: usize) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let bases: Vec<Vec<u8>> = (0..2)
            .map(|_| (0..len).map(|_| b"ACGT"[next(4) as usize]).collect())
            .collect();
        let mut fasta = Vec::new();
        for record in 0..count {
            fasta.extend_from_slice(format!(">kind_{}\n", record % 2).as_bytes());
            let mut sequence = bases[record % 2].clone();
            if next(3) == 0 {
                sequence[next(len as u64) as usize] = b'N';
            }
            fasta.extend_from_slice(&sequence);
            fasta.push(b'\n');
        }
        fasta
    }

    /// A sequence as the reader hands it over: its header, its record's index, and its bases'
    /// codes.
    type Sequence = (Vec<u8>, u64, Vec<u8>);

    /// Every sequence `reader` hands out for `path` in LZ mode `lz_mode`, with the headers and
    /// record indices kept, and the records counted.
    fn records(
        reader: &mut SimdSequencesReader,
        path: &Path,
        lz_mode: LzMode,
    ) -> (Vec<Sequence>, u64) {
        let settings = ParserSettings {
            keep_ident: true,
            record_indices: true,
            ..settings(lz_mode)
        };
        let mut sequences = Vec::new();
        let count = reader.process_file_extended(
            path,
            &settings,
            |buffer| {
                let (ident, bases) = (buffer.ident(), buffer.bases());
                for (description, &record) in
                    buffer.descriptions().iter().zip(buffer.record_indices())
                {
                    let start = description.bases_start;
                    let codes = (start..start + description.bases_len)
                        .map(|base| (bases[base / 4] >> (2 * (base % 4))) & 3)
                        .collect();
                    let header = &ident[description.ident_start..][..description.ident_len];
                    sequences.push((header.to_vec(), record, codes));
                }
                buffer
            },
            false,
        );
        (sequences, count)
    }

    /// With the headers and record indices kept, the skips keep them exact: every sequence of a
    /// skipping read has the header and the index of its record in a full read, and its
    /// 31-mers are that record's; the records counted are the same. The copies of repeated
    /// records run across their headers, which the skips pass.
    #[test]
    fn skips_keep_headers_and_record_indices() {
        let directory = directory("headers");
        let (a, c) = (genomes(1, 6, 20_000), genomes(2, 4, 30_000));
        let e = repeated_records(5, 60, 3_000);
        let inputs = [
            ("a.fa.xz", xz(&a), false),
            ("e.fa.xz", xz(&e), true),
            (
                "ace.tar.xz",
                xz(&tar(&[("a.fa", &a), ("c.fa", &c), ("e.fa", &e)])),
                false,
            ),
        ];
        // Whether the input holds records on one line only, where a skip holding more bytes
        // than bases passed a header.
        for (name, encoded, one_line) in inputs {
            let path = directory.join(name);
            std::fs::write(&path, encoded).unwrap();
            let (full, full_records) = records(&mut SimdSequencesReader::new(), &path, LzMode::Off);
            let mut reader = SimdSequencesReader::with_copybacks(copybacks());
            let (skipped, skipped_records) = records(&mut reader, &path, LzMode::SkipOnly);
            assert_eq!(skipped_records, full_records, "{name}: the records counted");
            // Each record of the full read: its header, and its 31-mers.
            let mut by_record: HashMap<u64, (Vec<u8>, HashSet<Vec<u8>>)> = HashMap::new();
            for (header, record, codes) in &full {
                let entry =
                    (by_record.entry(*record)).or_insert_with(|| (header.clone(), HashSet::new()));
                assert_eq!(&entry.0, header, "{name}: record {record} has two headers");
                entry.1.extend(codes.windows(31).map(<[u8]>::to_vec));
            }
            for (index, (header, record, codes)) in skipped.iter().enumerate() {
                let at = format!("{name}: sequence {index}, record {record}");
                let (full_header, kmers) = (by_record.get(record))
                    .unwrap_or_else(|| panic!("{at}: no such record in the full read"));
                assert_eq!(header, full_header, "{at}: the header");
                assert!(
                    codes.windows(31).all(|kmer| kmers.contains(kmer)),
                    "{at}: a 31-mer of another record"
                );
            }
            let stats = reader.tracker_stats();
            assert!(stats.skips > 0, "{name}: {stats:?}");
            if one_line {
                assert!(
                    stats.skipped_bytes > stats.skipped_bases,
                    "{name}: {stats:?}"
                );
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
