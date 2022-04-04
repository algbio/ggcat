use crate::io::sequences_reader::FastaSequence;
use parallel_processor::mem_tracker::tracked_vec::TrackedVec;
use parallel_processor::threadpools_chain::ThreadChainObject;

pub struct MinimizerBucketingQueueData<F: Clone + Sync + Send + Default> {
    pub data: TrackedVec<u8>,
    pub sequences: TrackedVec<(usize, usize, usize, usize)>,
    pub file_info: F,
    pub start_read_index: u64,
}

impl<F: Clone + Sync + Send + Default> MinimizerBucketingQueueData<F> {
    pub fn new(capacity: usize, file_info: F) -> Self {
        Self {
            data: TrackedVec::with_capacity(capacity),
            sequences: TrackedVec::with_capacity(capacity / 512),
            file_info,
            start_read_index: 0,
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

        if (self.data.capacity() - self.data.len()) < tot_len {
            println!("Requesting size increase for length: {}!", tot_len);
        }

        let capacity = self.data.capacity();

        let start = self.data.len();
        self.data.extend_from_slice(seq.ident);
        self.data.extend_from_slice(seq.seq);
        if let Some(qual) = seq.qual {
            self.data.extend_from_slice(qual);
        }

        self.sequences.push((start, ident_len, seq_len, qual_len));

        if self.data.len() != 0 {
            assert_eq!(self.data.capacity(), capacity);
        }
        
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

impl<F: Clone + Sync + Send + Default> ThreadChainObject for MinimizerBucketingQueueData<F> {
    type InitData = usize;

    fn initialize(params: &Self::InitData) -> Self {
        Self::new(*params, F::default())
    }

    fn reset(&mut self) {
        self.data.clear();
        self.sequences.clear();
    }
}
