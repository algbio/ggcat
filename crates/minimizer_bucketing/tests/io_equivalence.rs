//! The byte-oriented input path must deliver exactly the sequences the
//! record-oriented reader delivers, for every input format.

use ggcat_minimizer_bucketing::simd_batch::{
    LaneBatch, RECORD_CONTINUED, RECORD_CONTINUES, RecordExtra,
};
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_sink::SequencesSink;
use io::sequences_splitter::split_and_compress_sequences;
use io::sequences_stream::general::{GeneralSequenceBlockData, GeneralSequencesStream};
use io::sequences_stream::{GenericSequencesStream, SequenceInfo};
use simd_accel::batch::VecBatchSink;
use simd_accel::fasta_lexer::FastaSimdLexer;
use simd_accel::hashing::SIMD_LANES;
use simd_accel::packer::LanePacker;
use std::io::Write;
use std::path::{Path, PathBuf};

const K: usize = 11;
const BASES: [u8; 4] = [b'A', b'C', b'T', b'G'];

/// A record with the pieces of valid DNA it was reduced to.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    read_index: u64,
    /// Only filled when identifiers were asked for: the record reader copies
    /// them unconditionally for FASTQ, the parser only when they are wanted.
    ident: Vec<u8>,
    fragments: Vec<String>,
}

// ---------------------------------------------------------------- new path

enum Active {
    Idle,
    Fasta,
    Fastq,
}

struct TestSink {
    packer: LanePacker<RecordExtra>,
    fasta: FastaSimdLexer<RecordExtra>,
    fastq: io::fastq_lexer::FastqLexer,
    batches: VecBatchSink<RecordExtra>,
    active: Active,
    extra: RecordExtra,
    copy_ident: bool,
}

impl TestSink {
    fn new(bases_per_lane: usize, ignored_length: usize, copy_ident: bool) -> Self {
        Self {
            packer: LanePacker::new(K, bases_per_lane, ignored_length, copy_ident, 1 << 20)
                .unwrap(),
            fasta: FastaSimdLexer::new(),
            fastq: io::fastq_lexer::FastqLexer::new(copy_ident),
            batches: VecBatchSink::new(bases_per_lane),
            active: Active::Idle,
            extra: (SequenceInfo { color: None }, DnaSequencesFileType::FASTA),
            copy_ident,
        }
    }

    fn finish(mut self) -> Vec<LaneBatch<RecordExtra>> {
        self.packer.finish(&mut self.batches);
        for batch in &self.batches.batches {
            batch.debug_check(K);
        }
        self.batches.batches
    }
}

impl SequencesSink for TestSink {
    fn wants_ident(&self) -> bool {
        self.copy_ident
    }

    fn begin_stream(&mut self, format: DnaSequencesFileType, info: SequenceInfo) {
        self.extra = (info, format);
        match format {
            DnaSequencesFileType::FASTA => {
                self.active = Active::Fasta;
                self.fasta.begin_stream(self.extra);
            }
            DnaSequencesFileType::FASTQ => {
                self.active = Active::Fastq;
                self.fastq.begin_stream();
            }
            _ => unimplemented!(),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        match self.active {
            Active::Fasta => self.fasta.push(&mut self.packer, &mut self.batches, bytes),
            Active::Fastq => {
                let (packer, batches, extra) = (&mut self.packer, &mut self.batches, self.extra);
                self.fastq.push(bytes, |ident, sequence| {
                    packer.push_record(batches, extra, ident, sequence, true)
                });
            }
            Active::Idle => {}
        }
    }

    fn end_stream(&mut self) {
        match self.active {
            Active::Fasta => self.fasta.end_stream(&mut self.packer, &mut self.batches),
            Active::Fastq => {
                let (packer, batches, extra) = (&mut self.packer, &mut self.batches, self.extra);
                self.fastq.end_stream(|ident, sequence| {
                    packer.push_record(batches, extra, ident, sequence, true)
                });
            }
            Active::Idle => {}
        }
        self.active = Active::Idle;
    }

    fn push_record(&mut self, sequence: DnaSequence<&[u8]>, info: SequenceInfo) {
        self.packer.push_record(
            &mut self.batches,
            (info, sequence.format),
            sequence.ident_data,
            sequence.seq,
            true,
        );
    }
}

/// Rebuilds the records, joining the pieces a long record was cut into.
fn rebuild(batches: &[LaneBatch<RecordExtra>]) -> Vec<Record> {
    #[derive(Clone)]
    struct Piece {
        source_start: u64,
        bases: String,
    }
    let mut records: Vec<(Record, Vec<Piece>)> = Vec::new();
    for batch in batches {
        let mut mapping = vec![usize::MAX; batch.records.len()];
        for (index, record) in batch.records.iter().enumerate() {
            let continued = record.flags & RECORD_CONTINUED != 0;
            let previous = records
                .iter()
                .rposition(|(existing, _)| existing.read_index == record.read_index);
            match previous {
                Some(position) if continued => mapping[index] = position,
                _ => {
                    mapping[index] = records.len();
                    records.push((
                        Record {
                            read_index: record.read_index,
                            ident: batch.header(index).to_vec(),
                            fragments: Vec::new(),
                        },
                        Vec::new(),
                    ));
                }
            }
            // A record marked as continuing must own bases in this batch.
            if record.flags & RECORD_CONTINUES != 0 {
                assert!(
                    batch.lanes.iter().any(|lane| lane
                        .iter()
                        .any(|fragment| fragment.record_idx == index as u32)),
                    "a continuing record has no bases in its batch"
                );
            }
        }
        for lane in 0..SIMD_LANES {
            for pair in batch.lanes[lane].windows(2) {
                let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
                let bases: String = (start..end)
                    .map(|position| BASES[batch.base(lane, position) as usize] as char)
                    .collect();
                records[mapping[pair[0].record_idx as usize]].1.push(Piece {
                    source_start: pair[0].source_start,
                    bases,
                });
            }
        }
    }

    records
        .into_iter()
        .map(|(mut record, mut pieces)| {
            // Lanes are filled in order, so the pieces are already in order.
            pieces.sort_by_key(|piece| piece.source_start);
            record.fragments = pieces.into_iter().map(|piece| piece.bases).collect();
            record
        })
        .collect()
}

/// Joins the pieces that overlap by `k - 1`, undoing the lane splitting.
fn join_lane_pieces(batches: &[LaneBatch<RecordExtra>]) -> Vec<Record> {
    let mut records = rebuild(batches);
    for record in &mut records {
        let mut joined: Vec<String> = Vec::new();
        for fragment in record.fragments.drain(..) {
            match joined.last_mut() {
                Some(previous)
                    if previous.len() >= K - 1
                        && fragment.len() >= K - 1
                        && previous[previous.len() - (K - 1)..] == fragment[..K - 1] =>
                {
                    previous.push_str(&fragment[K - 1..]);
                }
                _ => joined.push(fragment),
            }
        }
        record.fragments = joined;
    }
    records
}

// ------------------------------------------------------------ legacy path

fn legacy(
    block: &GeneralSequenceBlockData,
    ignored_length: usize,
    copy_ident: bool,
) -> Vec<Record> {
    let mut stream = GeneralSequencesStream::new();
    let mut records = Vec::new();
    let mut buffer = Vec::new();
    let mut read_index = 0u64;
    stream.read_block(block, copy_ident, None, |sequence, _info| {
        if sequence.seq.len() < ignored_length {
            return;
        }
        let mut fragments = Vec::new();
        split_and_compress_sequences(&mut buffer, K, &sequence, |read, _range| {
            fragments.push(read.to_string())
        });
        records.push(Record {
            read_index,
            ident: if copy_ident {
                sequence.ident_data.to_vec()
            } else {
                Vec::new()
            },
            fragments,
        });
        read_index += 1;
    });
    records
}

fn parsed(
    block: &GeneralSequenceBlockData,
    ignored_length: usize,
    copy_ident: bool,
    bases_per_lane: usize,
) -> Vec<Record> {
    let mut stream = GeneralSequencesStream::new();
    let mut sink = TestSink::new(bases_per_lane, ignored_length, copy_ident);
    stream.read_block_into(block, &mut sink).unwrap();
    join_lane_pieces(&sink.finish())
}

// --------------------------------------------------------------- fixtures

struct Fixture(PathBuf);

impl Fixture {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ggcat-io-equivalence-{}-{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_file(&path);
        Self(path)
    }

    fn write(self, contents: &[u8]) -> Self {
        std::fs::write(&self.0, contents).unwrap();
        self
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn fasta_contents() -> Vec<u8> {
    let mut contents = Vec::new();
    // No ';' comment line here: the record reader answers one by dropping the
    // rest of the file, which is not a behaviour worth reproducing.
    contents.extend_from_slice(b">first record\r\nacgtACGTacgtACG\r\nTTTTNNNNACGTACGTACGT\r\n");
    contents.extend_from_slice(b">short\nACGT\n");
    contents.extend_from_slice(b">empty\n");
    contents.extend_from_slice(b">ambiguous\nACGTACGTACGTRACGTACGTACGTACGT\n");
    contents.extend_from_slice(b">long\n");
    for index in 0..40 {
        let line: Vec<u8> = (0..80).map(|i| b"ACGTTGCA"[(i + index) % 8]).collect();
        contents.extend_from_slice(&line);
        contents.push(b'\n');
    }
    contents.extend_from_slice(b">last no newline\nACGTACGTACGTACGT");
    contents
}

fn fastq_contents() -> Vec<u8> {
    let mut contents = Vec::new();
    for index in 0..6 {
        let length = 10 + index * 17;
        let sequence: Vec<u8> = (0..length)
            .map(|position| {
                if position == length / 2 && index % 2 == 0 {
                    b'N'
                } else {
                    b"ACGT"[(position + index) % 4]
                }
            })
            .collect();
        contents.extend_from_slice(format!("@read{index} description\n").as_bytes());
        contents.extend_from_slice(&sequence);
        contents.extend_from_slice(b"\n+\n");
        contents.extend_from_slice(&vec![b'I'; sequence.len()]);
        contents.push(b'\n');
    }
    contents
}

fn check(block: GeneralSequenceBlockData, name: &str) {
    for ignored_length in [0usize, K] {
        for copy_ident in [false, true] {
            for bases_per_lane in [64usize, 512, 4096] {
                let expected = legacy(&block, ignored_length, copy_ident);
                let actual = parsed(&block, ignored_length, copy_ident, bases_per_lane);
                assert_eq!(
                    expected, actual,
                    "{name}, ignored {ignored_length}, ident {copy_ident}, lane {bases_per_lane}"
                );
                assert!(!expected.is_empty());
            }
        }
    }
}

#[test]
fn plain_fasta_matches_the_record_reader() {
    let fixture = Fixture::new("plain.fa").write(&fasta_contents());
    check(
        GeneralSequenceBlockData::FASTA((fixture.path().to_owned(), Some(3))),
        "fasta",
    );
}

#[test]
fn compressed_fasta_matches_the_record_reader() {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&fasta_contents()).unwrap();
    let fixture = Fixture::new("plain.fa.gz").write(&encoder.finish().unwrap());
    check(
        GeneralSequenceBlockData::FASTA((fixture.path().to_owned(), Some(1))),
        "fasta.gz",
    );
}

#[test]
fn fastq_matches_the_record_reader() {
    let fixture = Fixture::new("reads.fq").write(&fastq_contents());
    check(
        GeneralSequenceBlockData::FASTA((fixture.path().to_owned(), None)),
        "fastq",
    );
}

#[test]
fn archive_members_match_the_record_reader() {
    let fixture = Fixture::new("inputs.tar");
    {
        let file = std::fs::File::create(fixture.path()).unwrap();
        let mut archive = tar::Builder::new(file);
        for (name, contents) in [
            ("a.fa", fasta_contents()),
            ("b.fq", fastq_contents()),
            ("c.fa", fasta_contents()),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            archive
                .append_data(&mut header, name, &contents[..])
                .unwrap();
        }
        archive.finish().unwrap();
    }
    check(
        GeneralSequenceBlockData::FASTA((fixture.path().to_owned(), Some(0))),
        "tar",
    );
}
