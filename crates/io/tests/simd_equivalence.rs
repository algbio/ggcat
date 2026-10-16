//! Compares the SIMD sequences reader with the line-based one: on synthetic inputs of every
//! shape, always; on real inputs, timing both against decoding alone, when asked.
//!
//! The real inputs come from `GGCAT_SIMD_COMPARE_INPUTS`, paths separated by whitespace; without
//! it that test does nothing. `GGCAT_SIMD_COMPARE_THREADS` (1 by default) is how many inputs are
//! read at once. Run it in release mode, with its output:
//!
//! ```text
//! GGCAT_SIMD_COMPARE_INPUTS="$(cat inputs.txt)" GGCAT_SIMD_COMPARE_THREADS=8 \
//!     cargo test --release -p ggcat_io --test simd_equivalence -- --nocapture
//! ```

use ggcat_io::binary_reader::{BinaryEvent, GenericBinaryReader};
use ggcat_io::compressed_read::CompressedRead;
use ggcat_io::sequences_reader::SequencesReader;
use ggcat_io::simd::{
    DEFAULT_BASES_CAPACITY, DEFAULT_IDENT_CAPACITY, ParserSettings, SimdSequencesReader,
};
use std::hash::Hasher;
use std::hint::black_box;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// One stretch of A, C, G and T a reader found, with hashes in place of the bytes: the header of
/// its record, its length and its bases. Both readers are reduced to these, as the SIMD one can
/// hold nothing but those four bases.
#[derive(Debug, PartialEq, Eq)]
struct Run {
    header: u64,
    length: usize,
    bases: u64,
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    hasher.write(bytes);
    hasher.finish()
}

/// The runs of the line reader: its sequences cut at every `N`, which is what its normalization
/// turns any other byte into.
fn line_runs(path: &Path) -> Vec<Run> {
    let mut runs = Vec::new();
    SequencesReader::new().process_file_extended(
        path,
        |sequence| {
            let (header, bases) = (hash(sequence.ident_data), sequence.seq);
            let mut start = 0;
            for end in memchr::memchr_iter(b'N', bases).chain([bases.len()]) {
                if end > start {
                    runs.push(Run {
                        header,
                        length: end - start,
                        bases: hash(&bases[start..end]),
                    });
                }
                start = end + 1;
            }
        },
        None,
        true,
        false,
    );
    runs
}

/// Each packed byte as its four letters.
const LETTERS: [[u8; 4]; 256] = {
    let mut table = [[0; 4]; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut base = 0;
        while base < 4 {
            table[byte][base] = b"ACTG"[(byte >> (2 * base)) & 3];
            base += 1;
        }
        byte += 1;
    }
    table
};

/// The bases of `read` as letters, into `letters`.
fn unpack(read: &CompressedRead, letters: &mut Vec<u8>) {
    letters.clear();
    for &byte in read.get_packed_slice() {
        letters.extend_from_slice(&LETTERS[byte as usize]);
    }
    let start = read.start as usize;
    letters.copy_within(start..start + read.get_length(), 0);
    letters.truncate(read.get_length());
}

/// As the line reader reads: headers kept, no copyback, and every run kept, however short.
const SETTINGS: ParserSettings = ParserSettings {
    keep_ident: true,
    copyback_size: None,
    min_seqlen: 0,
    record_indices: false,
    bases_capacity: DEFAULT_BASES_CAPACITY,
    ident_capacity: DEFAULT_IDENT_CAPACITY,
};

/// The runs of the SIMD reader, which hands them out as they are.
fn simd_runs(path: &Path) -> Vec<Run> {
    let mut runs = Vec::new();
    let mut letters = Vec::new();
    SimdSequencesReader::new().process_file_extended(
        path,
        &SETTINGS,
        |buffer| {
            for sequence in buffer.sequences() {
                unpack(&sequence.seq.into(), &mut letters);
                runs.push(Run {
                    header: hash(sequence.ident_data),
                    length: letters.len(),
                    bases: hash(&letters),
                });
            }
            buffer
        },
        false,
    );
    runs
}

/// What one pass over an input cost, and what it found.
struct Pass {
    time: Duration,
    /// Decoded bytes for decoding alone, sequences for a reader.
    items: u64,
    /// Bases handed out, none for decoding alone.
    bases: u64,
}

/// Decoding alone, which both readers do first.
fn decode(path: &Path) -> Pass {
    let start = Instant::now();
    let mut bytes = 0;
    GenericBinaryReader::new(None)
        .read_file(path, |event| {
            if let BinaryEvent::Chunk(chunk) = black_box(event) {
                bytes += chunk.len() as u64;
            }
        })
        .unwrap();
    Pass {
        time: start.elapsed(),
        items: bytes,
        bases: 0,
    }
}

fn read_lines(path: &Path) -> Pass {
    let start = Instant::now();
    let (mut sequences, mut bases) = (0, 0);
    SequencesReader::new().process_file_extended(
        path,
        |sequence| {
            sequences += 1;
            bases += black_box(sequence.seq).len() as u64;
        },
        None,
        true,
        false,
    );
    Pass {
        time: start.elapsed(),
        items: sequences,
        bases,
    }
}

fn read_simd(path: &Path) -> Pass {
    let start = Instant::now();
    let (mut sequences, mut bases) = (0, 0);
    SimdSequencesReader::new().process_file_extended(
        path,
        &SETTINGS,
        |buffer| {
            for sequence in buffer.sequences() {
                sequences += 1;
                let read: CompressedRead = black_box(sequence.seq).into();
                bases += read.get_length() as u64;
            }
            buffer
        },
        false,
    );
    Pass {
        time: start.elapsed(),
        items: sequences,
        bases,
    }
}

/// Bytes per second, in GB/s.
fn speed(bytes: u64, time: Duration) -> f64 {
    bytes as f64 / time.as_secs_f64() / 1e9
}

/// `pass` on every input, `threads` inputs at a time, the largest first so that the last ones
/// to start are the shortest. Returns the result of each input, in input order, and the wall
/// time of the whole.
fn run_all<T: Send>(
    inputs: &[&Path],
    threads: usize,
    pass: impl Fn(&Path) -> T + Sync,
) -> (Vec<T>, Duration) {
    let mut order: Vec<usize> = (0..inputs.len()).collect();
    order.sort_by_key(|&index| {
        std::cmp::Reverse(std::fs::metadata(inputs[index]).map_or(0, |metadata| metadata.len()))
    });
    let next = AtomicUsize::new(0);
    let results = Mutex::new((0..inputs.len()).map(|_| None).collect::<Vec<_>>());

    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                while let Some(&index) = order.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let result = pass(inputs[index]);
                    results.lock().unwrap()[index] = Some(result);
                }
            });
        }
    });
    let wall = start.elapsed();
    let results = results.into_inner().unwrap();
    (results.into_iter().map(Option::unwrap).collect(), wall)
}

/// The first run the two readers disagree on in `path`, described, if any.
fn compare(path: &Path) -> Option<String> {
    let (expected, found) = (line_runs(path), simd_runs(path));
    let index = (0..expected.len().max(found.len()))
        .find(|&index| expected.get(index) != found.get(index))?;
    Some(format!(
        "{}: {} runs from the line reader, {} from the SIMD one; first difference at run \
         {index}: {:?} against {:?}",
        path.display(),
        expected.len(),
        found.len(),
        expected.get(index),
        found.get(index),
    ))
}

#[test]
fn simd_reader_matches_the_line_reader() {
    let Ok(inputs) = std::env::var("GGCAT_SIMD_COMPARE_INPUTS") else {
        eprintln!("GGCAT_SIMD_COMPARE_INPUTS is not set: nothing to compare");
        return;
    };
    let inputs: Vec<&Path> = inputs.split_whitespace().map(Path::new).collect();
    let threads = std::env::var("GGCAT_SIMD_COMPARE_THREADS").map_or(1, |threads| {
        threads
            .parse()
            .expect("GGCAT_SIMD_COMPARE_THREADS is not a number")
    });
    assert!(threads > 0, "GGCAT_SIMD_COMPARE_THREADS must be at least 1");

    // One pass over every input after the other, so that with several threads each input is
    // read under the load of the others reading the same way.
    let (decoding, decoding_wall) = run_all(&inputs, threads, decode);
    let (lines, lines_wall) = run_all(&inputs, threads, read_lines);
    let (simd, simd_wall) = run_all(&inputs, threads, read_simd);

    println!(
        "{threads} thread(s), SIMD kernel {:?}; per input, the time it took on its thread:",
        SimdSequencesReader::new().kernel()
    );
    println!(
        "{:<48} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12} {:>10} {:>12}",
        "input",
        "GB",
        "decode s",
        "lines s",
        "simd s",
        "lines seq",
        "lines bases",
        "simd seq",
        "simd bases"
    );
    for (index, path) in inputs.iter().enumerate() {
        let (decoding, lines, simd) = (&decoding[index], &lines[index], &simd[index]);
        println!(
            "{:<48} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>10} {:>12} {:>10} {:>12}",
            path.file_name().unwrap().to_string_lossy(),
            decoding.items as f64 / 1e9,
            decoding.time.as_secs_f64(),
            lines.time.as_secs_f64(),
            simd.time.as_secs_f64(),
            lines.items,
            lines.bases,
            simd.items,
            simd.bases,
        );
    }

    let bytes: u64 = decoding.iter().map(|pass| pass.items).sum();
    println!("total: {:.2} GB decoded", bytes as f64 / 1e9);
    for (name, passes, wall) in [
        ("decode", &decoding, decoding_wall),
        ("lines", &lines, lines_wall),
        ("simd", &simd, simd_wall),
    ] {
        let summed: Duration = passes.iter().map(|pass| pass.time).sum();
        println!(
            "  {name:<7} wall {:>7.2} s  {:>6.2} GB/s  {:>5.2}x decoding  (inputs summed {:>7.2} s, \
             {:>5.2} GB/s per thread)",
            wall.as_secs_f64(),
            speed(bytes, wall),
            wall.as_secs_f64() / decoding_wall.as_secs_f64(),
            summed.as_secs_f64(),
            speed(bytes, summed),
        );
    }

    // Untimed: what each reader found, run by run.
    let (differences, _) = run_all(&inputs, threads, compare);
    let differences: Vec<String> = differences.into_iter().flatten().collect();
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

/// Xorshift, for the synthetic inputs: the same seed makes the same files.
struct Random(u64);

impl Random {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number in `0..bound`.
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    /// True with a probability of `per_mille` / 1000.
    fn chance(&mut self, per_mille: u64) -> bool {
        self.next() % 1000 < per_mille
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// How a synthetic input's lines look, and how big it is.
struct Synthetic {
    /// Per mille of the sequences' bytes that are not ACGT.
    other: u64,
    /// Per mille of the line endings that are `\r\n`.
    crlf: u64,
    /// Whether a `\r` can be a byte of a line, not only end one. The line reader splits a line
    /// in two where its chunks end, and drops a `\r` ending the first part as a line ending:
    /// only in a plain file of one chunk is such a `\r` a byte of the line wherever it is.
    lone_carriage_returns: bool,
    /// Bytes to write at least.
    size: usize,
}

impl Synthetic {
    fn end_line(&self, random: &mut Random, input: &mut Vec<u8>) {
        if random.chance(self.crlf) {
            input.push(b'\r');
        }
        input.push(b'\n');
    }

    /// The bytes of a header or a comment past its first.
    fn text(&self, random: &mut Random) -> Vec<u8> {
        let longest = if random.chance(100) { 300 } else { 40 };
        let length = random.below(longest);
        let bytes: &[u8] = if self.lone_carriage_returns {
            b"abcxyz0123456789 _-|:.\t>;@+\r\x80\xff"
        } else {
            b"abcxyz0123456789 _-|:.\t>;@+\x80\xff"
        };
        (0..length).map(|_| random.pick(bytes)).collect()
    }

    /// A sequence: mostly within a block or a few, sometimes over many.
    fn bases(&self, random: &mut Random) -> Vec<u8> {
        let length = match random.below(20) {
            0..=9 => random.below(81),
            10..=15 => random.below(300),
            16..=18 => 300 + random.below(2000),
            _ => 2000 + random.below(20000),
        };
        let other: &[u8] = if self.lone_carriage_returns {
            b"NnNnRYKMSWBDHVrykmswbdhvUuX-*. \t\r>;@+\x00\x80\xc1\xe1\xff"
        } else {
            b"NnNnRYKMSWBDHVrykmswbdhvUuX-*. \t>;@+\x00\x80\xc1\xe1\xff"
        };
        (0..length)
            .map(|_| {
                if random.chance(self.other) {
                    random.pick(other)
                } else {
                    random.pick(b"ACGTacgt")
                }
            })
            .collect()
    }

    /// Sometimes takes the last line feed of `input` away, or its whole line ending.
    fn end_input(&self, random: &mut Random, input: &mut Vec<u8>) {
        let strip = random.below(10);
        if strip < 3 && input.last() == Some(&b'\n') {
            input.pop();
        }
        if strip == 2 && input.last() == Some(&b'\r') {
            input.pop();
        }
    }

    /// FASTA: records on one line, wrapped at one width or at random, or with no bases; lines
    /// before any header; comments and empty lines anywhere.
    fn fasta(&self, random: &mut Random) -> Vec<u8> {
        let mut input = Vec::new();
        let mut first = !random.chance(150);
        while input.len() < self.size {
            if !first {
                input.push(b'>');
                input.extend(self.text(random));
                self.end_line(random, &mut input);
            }
            first = false;
            let bases = self.bases(random);
            let (mode, width) = (random.below(3), 1 + random.below(120));
            let mut rest = &bases[..];
            loop {
                match random.below(100) {
                    0..=2 => {
                        input.push(b';');
                        input.extend(self.text(random));
                        self.end_line(random, &mut input);
                    }
                    3..=4 => self.end_line(random, &mut input),
                    _ => {}
                }
                if rest.is_empty() {
                    break;
                }
                let length = match mode {
                    0 => rest.len(),
                    1 => width,
                    _ => 1 + random.below(200),
                };
                let (line, next) = rest.split_at(length.min(rest.len()));
                input.extend_from_slice(line);
                self.end_line(random, &mut input);
                rest = next;
            }
        }
        self.end_input(random, &mut input);
        input
    }

    /// FASTQ: sequences of every length, with no bases too; separators with or without the
    /// header again; quality bytes that start lines; sometimes cut anywhere in its last record.
    fn fastq(&self, random: &mut Random) -> Vec<u8> {
        let mut input = Vec::new();
        let mut last_record = 0;
        while input.len() < self.size {
            last_record = input.len();
            let header = self.text(random);
            input.push(b'@');
            input.extend_from_slice(&header);
            self.end_line(random, &mut input);
            let bases = self.bases(random);
            input.extend_from_slice(&bases);
            self.end_line(random, &mut input);
            input.push(b'+');
            if random.chance(300) {
                input.extend_from_slice(&header);
            }
            self.end_line(random, &mut input);
            input.extend((0..bases.len()).map(|_| random.pick(b"!#+5?@ABCDEFGHIJ~")));
            self.end_line(random, &mut input);
        }
        if input.len() > last_record && random.chance(300) {
            input.truncate(last_record + random.below(input.len() - last_record));
        } else {
            self.end_input(random, &mut input);
        }
        input
    }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// A tar archive of `members`, as (path, data).
fn tar(members: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (path, data) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        builder.append_data(&mut header, path, &data[..]).unwrap();
    }
    builder.into_inner().unwrap()
}

/// FASTA and FASTQ files of every shape, of a few bytes to a few chunks of the SIMD reader, plain,
/// gzipped and in tar archives along with files of other kinds, read by both readers.
/// `GGCAT_SIMD_SYNTHETIC_CASES` sets how many (100 by default).
#[test]
fn simd_reader_matches_the_line_reader_on_synthetic_inputs() {
    let cases: u64 = std::env::var("GGCAT_SIMD_SYNTHETIC_CASES").map_or(100, |cases| {
        cases
            .parse()
            .expect("GGCAT_SIMD_SYNTHETIC_CASES is not a number")
    });
    let directory = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("simd-synthetic-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let mut differences = Vec::new();
    for seed in 0..cases {
        let mut random = Random::new(seed + 1);
        let kind = random.below(4);
        let mut synthetic = Synthetic {
            other: random.pick(&[0, 0, 1, 10, 100, 700]),
            crlf: random.pick(&[0, 0, 1000, 1000, 500]),
            lone_carriage_returns: kind == 0,
            size: random.pick(&[1, 1000, 30_000, 70_000, 200_000, 300_000]),
        };
        let fastq = random.chance(500);
        let sequences = |synthetic: &Synthetic, random: &mut Random, fastq: bool| {
            let name = if fastq { "reads.fq" } else { "contigs.fa" };
            let data = if fastq {
                synthetic.fastq(random)
            } else {
                synthetic.fasta(random)
            };
            (name, data)
        };
        let (path, data) = match kind {
            // Plain, of one chunk for the line reader.
            0 | 1 => {
                let (name, data) = sequences(&synthetic, &mut random, fastq);
                (directory.join(format!("{seed}-{name}")), data)
            }
            2 => {
                let (name, data) = sequences(&synthetic, &mut random, fastq);
                (directory.join(format!("{seed}-{name}.gz")), gzip(&data))
            }
            // Members of both formats, each read from its own start to its end.
            _ => {
                synthetic.size /= 4;
                let mut members = Vec::new();
                for index in 0..1 + random.below(5) {
                    let fastq = random.chance(500);
                    let (name, data) = sequences(&synthetic, &mut random, fastq);
                    let data = if random.chance(300) {
                        gzip(&data)
                    } else {
                        data
                    };
                    let suffix = if data.starts_with(&[0x1f, 0x8b]) {
                        ".gz"
                    } else {
                        ""
                    };
                    members.push((format!("dir/{index}-{name}{suffix}"), data));
                    if random.chance(300) {
                        members.push((format!("{index}-readme.txt"), b"not sequences\n".to_vec()));
                    }
                }
                (directory.join(format!("{seed}-archive.tar")), tar(&members))
            }
        };
        std::fs::write(&path, &data).unwrap();
        differences.extend(compare(&path));
    }
    std::fs::remove_dir_all(&directory).unwrap();
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}
