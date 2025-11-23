use ggcat_logging::stats::StatId;
use io::sequences_reader::{DnaSequence, DnaSequencesFileType};
use io::sequences_stream::SequenceInfo;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::PacketTrait;
use std::mem::size_of;

type SequencesType = (usize, usize, usize, DnaSequencesFileType, SequenceInfo);

pub struct MinimizerBucketingQueueData<F: Clone + Sync + Send + Default + 'static> {
    data: Vec<u8>,
    pub sequences: Vec<SequencesType>,
    pub stream_info: F,
    pub start_read_index: u64,
    pub stats_block_id: StatId,
}

impl<F: Clone + Sync + Send + Default + 'static> MinimizerBucketingQueueData<F> {
    pub fn new(capacity: usize, stream_info: F) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            sequences: Vec::with_capacity(capacity / 512),
            stream_info,
            start_read_index: 0,
            stats_block_id: StatId::default(),
        }
    }

    #[allow(dead_code)]
    pub fn get_total_size(&self) -> usize {
        self.data.len()
    }

    pub fn push_sequences(&mut self, seq: DnaSequence, seq_info: SequenceInfo) -> bool {
        let ident_len = seq.ident_data.len();
        let seq_len = seq.seq.len();

        let tot_len = ident_len + seq_len;

        if self.data.len() != 0 && (self.data.capacity() - self.data.len()) < tot_len {
            return false;
        }

        let start = self.data.len();
        self.data.extend_from_slice(seq.ident_data);
        self.data.extend_from_slice(seq.seq);

        self.sequences
            .push((start, ident_len, seq_len, seq.format, seq_info));

        true
    }

    pub fn iter_sequences(&self) -> impl Iterator<Item = (DnaSequence<'_>, SequenceInfo)> {
        self.sequences
            .iter()
            .map(move |&(start, id_len, seq_len, format, seq_info)| {
                let mut start = start;

                let ident_data = &self.data[start..start + id_len];
                start += id_len;

                let seq = &self.data[start..start + seq_len];

                (
                    DnaSequence {
                        ident_data,
                        seq,
                        format,
                    },
                    seq_info,
                )
            })
    }
}

impl<F: Clone + Sync + Send + Default + 'static> PoolObjectTrait
    for MinimizerBucketingQueueData<F>
{
    type InitData = usize;

    fn allocate_new(init_data: &Self::InitData) -> Self {
        Self::new(*init_data, F::default())
    }

    fn reset(&mut self) {
        self.data.clear();
        self.sequences.clear();
    }
}

impl<F: Clone + Sync + Send + Default + 'static> PacketTrait for MinimizerBucketingQueueData<F> {
    fn get_size(&self) -> usize {
        self.data.len() + (self.sequences.len() * size_of::<SequencesType>())
    }
}
