use crate::assemble_pipeline::current_kmers_merge::KmersFlags;
use crate::colors::colors_manager::ColorsManager;
use crate::io::concurrent::intermediate_storage::IntermediateReadsWriter;
use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parallel_processor::threadpools_chain::{ObjectsPoolManager, ThreadChainObject};
use std::intrinsics::unlikely;
use std::mem::swap;
use std::path::PathBuf;

pub struct MinimizerBucketingQueueData {
    data: Vec<u8>,
    pub sequences: Vec<(usize, usize, usize, usize)>,
    pub file_index: u64,
}

impl MinimizerBucketingQueueData {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            sequences: Vec::with_capacity(capacity / 512),
            file_index: 0,
        }
    }

    pub fn push_sequences(&mut self, seq: FastaSequence) -> bool {
        let qual_len = seq.qual.map(|q| q.len()).unwrap_or(0);
        let ident_len = seq.ident.len();
        let seq_len = seq.seq.len();

        let tot_len = qual_len + ident_len + seq_len;

        if self.data.len() != 0 && (self.data.capacity() - self.data.len()) < tot_len {
            return false;
        }

        let mut start = self.data.len();
        self.data.extend_from_slice(seq.ident);
        self.data.extend_from_slice(seq.seq);
        if let Some(qual) = seq.qual {
            self.data.extend_from_slice(qual);
        }

        self.sequences.push((start, ident_len, seq_len, qual_len));

        true
    }

    pub fn iter_sequences(&self) -> impl Iterator<Item = FastaSequence> {
        self.sequences
            .iter()
            .map(move |&(start, id_len, seq_len, qual_len)| {
                let mut start = start;

                let ident = &self.data[start..start + id_len];
                start += id_len;

                let seq = &self.data[start..start + seq_len];
                start += seq_len;

                let qual = match qual_len {
                    0 => None,
                    _ => Some(&self.data[start..start + qual_len]),
                };

                FastaSequence { ident, seq, qual }
            })
    }
}

pub struct MinimizerBucketingExecutionContext<CX: ColorsManager, ExtraData> {
    pub k: usize,
    pub m: usize,
    pub buckets_count: usize,
    pub buckets: MultiThreadBuckets<
        IntermediateReadsWriter<KmersFlags<CX::MinimizerBucketingSeqColorDataType>>,
    >,
    pub extra: ExtraData,
}

impl ThreadChainObject for MinimizerBucketingQueueData {
    type InitData = usize;

    fn initialize(params: &Self::InitData) -> Self {
        Self::new(*params)
    }

    fn reset(&mut self) {
        self.data.clear();
        self.sequences.clear();
    }
}

pub fn minb_reader<CX: ColorsManager, ExtraData>(
    context: &MinimizerBucketingExecutionContext<CX, ExtraData>,
    manager: ObjectsPoolManager<MinimizerBucketingQueueData, (PathBuf, u64)>,
) {
    while let Some((input, file_index)) = manager.recv_obj() {
        let mut data = manager.allocate();
        data.file_index = file_index;

        SequencesReader::process_file_extended(
            input,
            |x| {
                if x.seq.len() < context.k {
                    return;
                }

                if unsafe { unlikely(!data.push_sequences(x)) } {
                    let mut tmp_data = manager.allocate();
                    data.file_index = file_index;

                    swap(&mut data, &mut tmp_data);
                    manager.send(tmp_data);

                    if !data.push_sequences(x) {
                        panic!("Out of memory!");
                    }
                }
            },
            false,
        );
        if data.sequences.len() > 0 {
            manager.send(data);
        }
    }
}
