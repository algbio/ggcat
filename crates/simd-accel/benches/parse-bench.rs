//! Parsing synthetic FASTA and FASTQ held in memory, as the dev branch's bench of its FASTA
//! parser does, for both of this crate's formats: the bases handed out per second, on one
//! thread and on 16 at once, each thread parsing the whole input. The inputs are the same as
//! in the dev branch's copy of this bench: k = 61, headers not kept, records too short for a
//! k-mer dropped.

use criterion::*;
use ggcat_simd_accel::sequence_buffer::SequenceBuffer;
use ggcat_simd_accel::sequence_parser::SequenceParser;
use rayon::prelude::*;
use utils::dna_sequence::DnaSequencesFileType;

const BRANCH: &str = "current";
const K: usize = 61;
/// What io's reader hands the parser at a time.
const CHUNK_BYTES: usize = 64 * 1024;

/// `input` through a parser set as for k-mers of [`K`]; the bases of the sequences it hands out.
fn parse(input: &[u8], fastq: bool) -> u64 {
    let mut parser = SequenceParser::new(false, Some(K - 1), K);
    parser.reset(if fastq {
        DnaSequencesFileType::FASTQ
    } else {
        DnaSequencesFileType::FASTA
    });
    let mut kept = 0u64;
    let mut count = |buffer: SequenceBuffer| {
        kept += buffer
            .descriptions()
            .iter()
            .map(|description| description.bases_len as u64)
            .sum::<u64>();
        buffer
    };
    for chunk in input.chunks(CHUNK_BYTES) {
        parser.parse_data(chunk, &mut count);
    }
    parser.finish(&mut count);
    kept
}

/// Xorshift, so that both branches parse the same bytes.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn base(&mut self) -> u8 {
        b"ACTG"[(self.next() & 3) as usize]
    }
}

/// FASTA as the dev branch's `generate_fasta`: `sequence_bases` bases in records of 11000 to
/// 28000 bases, in lines of 80, with an `N` every 4093 bases.
fn generate_fasta(sequence_bases: usize, seed: u64) -> Vec<u8> {
    const LINE_BASES: usize = 80;
    let mut random = Random(seed | 1);
    let mut fasta = Vec::with_capacity(sequence_bases + sequence_bases / LINE_BASES + 4096);
    let mut remaining = sequence_bases;
    let mut record = 0usize;
    while remaining != 0 {
        let record_len = remaining.min(11_000 + record.wrapping_mul(7_919) % 17_000);
        fasta.extend_from_slice(format!(">synthetic-{record}\n").as_bytes());
        for position in 0..record_len {
            let base = if position % 4_093 == record % 4_093 {
                b'N'
            } else {
                random.base()
            };
            fasta.push(base);
            if (position + 1) % LINE_BASES == 0 {
                fasta.push(b'\n');
            }
        }
        if record_len % LINE_BASES != 0 {
            fasta.push(b'\n');
        }
        remaining -= record_len;
        record += 1;
    }
    fasta
}

/// The same number of bases as FASTQ reads of 150, the last one shorter, an `N` in about one
/// read in 27 as in [`generate_fasta`].
fn generate_fastq(sequence_bases: usize, seed: u64) -> Vec<u8> {
    const READ_BASES: usize = 150;
    let mut random = Random(seed | 1);
    let mut fastq = Vec::with_capacity(2 * sequence_bases + sequence_bases / READ_BASES * 32);
    let mut remaining = sequence_bases;
    let mut read = 0usize;
    while remaining != 0 {
        let read_len = remaining.min(READ_BASES);
        fastq.extend_from_slice(format!("@synthetic-{read}\n").as_bytes());
        for position in 0..read_len {
            let base = if position == read % 4_093 {
                b'N'
            } else {
                random.base()
            };
            fastq.push(base);
        }
        fastq.extend_from_slice(b"\n+\n");
        fastq.extend(std::iter::repeat_n(b'I', read_len));
        fastq.push(b'\n');
        remaining -= read_len;
        read += 1;
    }
    fastq
}

const SEQUENCE_BASES: usize = 1024 * 1024 - 8 * 1024;
const THREADS: usize = 16;

/// Parsing alone, the inputs in memory: `input` once on one thread, then on each of
/// [`THREADS`] threads at once, in bases per second.
fn bench_input(c: &mut Criterion, name: &str, input: &[u8], fastq: bool) {
    let bases = SEQUENCE_BASES as u64;
    let mut group = c.benchmark_group(format!("{BRANCH}-parse-{name}"));
    group.throughput(Throughput::Elements(bases));
    group.bench_function("1-thread", |b| {
        b.iter(|| black_box(parse(black_box(input), fastq)))
    });
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(THREADS)
        .build()
        .unwrap();
    group.throughput(Throughput::Elements(bases * THREADS as u64));
    group.bench_function(format!("{THREADS}-threads"), |b| {
        b.iter(|| {
            let input = black_box(input);
            let checksum = pool.install(|| {
                (0..THREADS)
                    .into_par_iter()
                    .map(|worker| parse(input, fastq) ^ worker as u64)
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });
    group.finish();
}

pub fn parse_benchmark(c: &mut Criterion) {
    let fasta = generate_fasta(SEQUENCE_BASES, 0xFA57_A51D);
    let fastq = generate_fastq(SEQUENCE_BASES, 0xFA57_A51D);
    println!(
        "{BRANCH}: FASTA {} bytes, FASTQ {} bytes, {SEQUENCE_BASES} bases each; kept {} and {}",
        fasta.len(),
        fastq.len(),
        parse(&fasta, false),
        parse(&fastq, true)
    );
    bench_input(c, "fasta", &fasta, false);
    bench_input(c, "fastq", &fastq, true);
}

criterion_group!(benches, parse_benchmark);
criterion_main!(benches);
