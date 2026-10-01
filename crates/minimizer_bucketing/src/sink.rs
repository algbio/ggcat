//! Parses input streams straight into the batches the bucketing threads read.

use config::SIMD_BATCH_MAX_HEADER_BYTES;
use ggcat_logging::stats;
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_sink::{LzCopy, SequencesSink};
use io::sequences_stream::SequenceInfo;
use parallel_processor::execution_manager::executor::AddressProducer;
use parallel_processor::execution_manager::packet::{Packet, PacketsPool};
use parking_lot::RwLock;
use simd_accel::batch::BatchSink;
use simd_accel::fasta_copyback::{CopySpan, CopybackTracker, min_range_bases};
use simd_accel::fasta_lexer::FastaSimdLexer;
use simd_accel::packer::LanePacker;
use simd_accel::stats::CopybackStats;

use crate::simd_batch::{RecordExtra, SequencesLaneBatch, SimdSequencesBatch};

/// Holds the batch being filled and sends it on when it is full.
struct BatchPackets<'a, F: Clone + Sync + Send + Default + 'static> {
    pool: &'a PacketsPool<SimdSequencesBatch<F>>,
    address: &'a RwLock<Option<AddressProducer<SimdSequencesBatch<F>>>>,
    packet: Option<Packet<SimdSequencesBatch<F>>>,
    stream_info: F,
    stream_index: Option<u16>,
}

impl<'a, F: Clone + Sync + Send + Default + 'static> BatchPackets<'a, F> {
    fn packet_mut(&mut self) -> &mut SimdSequencesBatch<F> {
        if self.packet.is_none() {
            let packet = self.pool.alloc_packet();
            self.packet = Some(packet);
        }
        self.packet.as_mut().unwrap()
    }

    fn stream_count(&self) -> usize {
        self.packet
            .as_ref()
            .map_or(0, |packet| packet.stream_infos.len())
    }
}

impl<F: Clone + Sync + Send + Default + 'static> BatchSink<RecordExtra> for BatchPackets<'_, F> {
    fn current(&mut self) -> &mut SequencesLaneBatch {
        &mut self.packet_mut().batch
    }

    fn emit(&mut self, _is_last: bool) {
        let Some(packet) = self.packet.take() else {
            return;
        };
        self.stream_index = None;
        stats!(
            let stat_id = ggcat_logging::generate_stat_id!();
            let packet = &mut packet;
            packet.stats_block_id = stat_id;
            let chunk_index = {
                let counter = &mut ggcat_logging::get_stat!(stats.input_counter);
                *counter += 1;
                *counter
            };
            let sequences_count = packet.batch.records.len();
            let sequences_size = packet.batch.lane_fill.iter().map(|fill| *fill as usize).sum();
            let send_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
        );
        self.address
            .read()
            .as_ref()
            .expect("the bucketing threads are not listening")
            .send_packet(packet);
        stats!(
            stats.assembler.input_chunks.push(ggcat_logging::stats::InputChunkStats {
                id: stat_id,
                index: chunk_index,
                sequences_count,
                sequences_size,
                start_time: send_time.into(),
                end_time: send_time.into(),
                finished_send_time: ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into(),
                is_last: _is_last,
            });
        );
    }

    fn stream_index(&mut self) -> u16 {
        if let Some(index) = self.stream_index {
            return index;
        }
        let info = self.stream_info.clone();
        let packet = self.packet_mut();
        let index = packet.stream_infos.len() as u16;
        packet.stream_infos.push(info);
        self.stream_index = Some(index);
        index
    }
}

enum ActiveLexer {
    Idle,
    Fasta,
    Fastq,
}

pub struct BucketingSink<'a, F: Clone + Sync + Send + Default + 'static> {
    packer: LanePacker<RecordExtra>,
    fasta: FastaSimdLexer<RecordExtra>,
    fastq: io::fastq_lexer::FastqLexer,
    active: ActiveLexer,
    extra: RecordExtra,
    packets: BatchPackets<'a, F>,
    /// The LZ copyback extension, present only when it is switched on.
    copyback: Option<Box<CopybackTracker>>,
    /// `Below this value a copy cannot yield a hole, so it is not worth reporting.
    copyback_min_len: u64,
    k: usize,
    m: usize,
}

impl<'a, F: Clone + Sync + Send + Default + 'static> BucketingSink<'a, F> {
    pub fn new(
        k: usize,
        m: usize,
        bases_per_lane: usize,
        ignored_length: usize,
        copy_ident: bool,
        pool: &'a PacketsPool<SimdSequencesBatch<F>>,
        address: &'a RwLock<Option<AddressProducer<SimdSequencesBatch<F>>>>,
    ) -> Self {
        Self {
            packer: LanePacker::new(
                k,
                bases_per_lane,
                ignored_length,
                copy_ident,
                SIMD_BATCH_MAX_HEADER_BYTES,
            )
            .expect("invalid sequence packing parameters"),
            fasta: FastaSimdLexer::new(),
            fastq: io::fastq_lexer::FastqLexer::new(copy_ident),
            active: ActiveLexer::Idle,
            extra: (SequenceInfo { color: None }, DnaSequencesFileType::FASTA),
            packets: BatchPackets {
                pool,
                address,
                packet: None,
                stream_info: F::default(),
                stream_index: None,
            },
            k,
            m,
            copyback_min_len: min_range_bases(k, m),
            copyback: io::raw_reader::LZ_COPYBACK_ENABLED.then(|| {
                Box::new(
                    CopybackTracker::new(io::raw_reader::LZ_COPYBACK_SKIP, k, m)
                )
            }),
        }
    }

    /// What the extension saw, for the caller to aggregate across reader threads.
    pub fn copyback_stats(&self) -> Option<CopybackStats> {
        self.copyback.as_ref().map(|t| t.stats)
    }

    /// Starts one input block: its records are numbered from zero again.
    pub fn begin_block(&mut self, stream_info: F) {
        // One batch can only refer to so many input blocks.
        if self.packets.stream_count() + 1 >= u16::MAX as usize {
            self.packer.finish(&mut self.packets);
        }
        self.packets.stream_info = stream_info;
        self.packets.stream_index = None;
        self.packer.reset_read_index();
    }

    /// Sends the partially filled batch, after the last input block.
    pub fn finish(&mut self) {
        self.packer.finish(&mut self.packets);
    }
}

impl<F: Clone + Sync + Send + Default + 'static> SequencesSink for BucketingSink<'_, F> {
    fn wants_ident(&self) -> bool {
        self.packer.wants_ident()
    }

    fn wants_copies(&self) -> bool {
        self.copyback.is_some()
    }

    fn copyback_min_len(&self) -> u64 {
        self.copyback_min_len
    }

    fn begin_tracked_input(&mut self, path: &std::path::Path, retain: u64) {
        let Some(tracker) = self.copyback.as_mut() else {
            return;
        };
        tracker.begin_input(retain);
    }

    fn end_tracked_input(&mut self) {
        // The reader lets go of its copy before the last packet leaves, so the manager
        // is always finalized by a bucketing thread.
        if self.packer.has_content() {
            self.packer.finish(&mut self.packets);
        } else {
            let packet = self.packets.packet_mut();
            self.packets.emit(true);
        }
    }

    fn mark_opaque(&mut self, at: u64, len: u64) {
        if let Some(tracker) = self.copyback.as_mut() {
            tracker.mark_opaque(at, len);
        }
    }

    fn push_bytes_tracked(
        &mut self,
        base: u64,
        bytes: &[u8],
        forward: &[LzCopy],
        backward: &[LzCopy],
    ) {
        let Some(tracker) = self.copyback.as_mut() else {
            self.lex_and_pack(bytes);
            return;
        };
        // Only the FASTA lexer hands out absolute base coordinates; the bases of any
        // other stream have none, and its bytes can be no copy's source.
        if !matches!(self.active, ActiveLexer::Fasta) {
            tracker.mark_opaque(base, bytes.len() as u64);
            self.lex_and_pack(bytes);
            return;
        }
        tracker.push_span(
            &mut self.fasta,
            &mut self.packer,
            &mut self.packets,
            base,
            bytes,
            forward.iter().map(|c| (c.src(), c.len())),
            backward.iter().map(|c| CopySpan {
                src: c.src(),
                dst: c.dst(),
                len: c.len(),
            }),
        );
    }

    fn begin_stream(&mut self, format: DnaSequencesFileType, info: SequenceInfo) {
        self.extra = (info, format);
        match format {
            DnaSequencesFileType::FASTA => {
                self.active = ActiveLexer::Fasta;
                self.fasta.begin_stream(self.extra);
            }
            DnaSequencesFileType::FASTQ => {
                self.active = ActiveLexer::Fastq;
                self.fastq.begin_stream();
            }
            DnaSequencesFileType::GFA | DnaSequencesFileType::BINARY => {
                unimplemented!("unsupported input format")
            }
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        self.lex_and_pack(bytes);
    }

    fn end_stream(&mut self) {
        match self.active {
            ActiveLexer::Fasta => self.fasta.end_stream(&mut self.packer, &mut self.packets),
            ActiveLexer::Fastq => {
                let (packer, packets, extra) = (&mut self.packer, &mut self.packets, self.extra);
                self.fastq.end_stream(|ident, sequence| {
                    packer.push_record(packets, extra, ident, sequence, true)
                });
            }
            ActiveLexer::Idle => {}
        }
        self.active = ActiveLexer::Idle;
    }

    fn push_record(&mut self, sequence: DnaSequence<&[u8]>, info: SequenceInfo) {
        self.packer.push_record(
            &mut self.packets,
            (info, sequence.format),
            sequence.ident_data,
            sequence.seq,
            true,
        );
    }
}

impl<F: Clone + Sync + Send + Default + 'static> BucketingSink<'_, F> {
    fn lex_and_pack(&mut self, bytes: &[u8]) {
        match self.active {
            ActiveLexer::Fasta => self.fasta.push(&mut self.packer, &mut self.packets, bytes),
            ActiveLexer::Fastq => {
                let (packer, packets, extra) = (&mut self.packer, &mut self.packets, self.extra);
                self.fastq.push(bytes, |ident, sequence| {
                    // A record read whole is kept even when empty, matching
                    // what the line-based reader hands over.
                    packer.push_record(packets, extra, ident, sequence, true)
                });
            }
            ActiveLexer::Idle => {}
        }
    }
}
