use crate::io::reads_writer::ReadsWriter;
use crate::io::sequences_reader::FastaSequence;
use crate::utils::vec_slice::VecSlice;
use parking_lot::Mutex;

pub struct FastaWriterConcurrentBuffer<'a> {
    target: &'a Mutex<ReadsWriter>,
    sequences: Vec<(VecSlice<u8>, VecSlice<u8>, Option<VecSlice<u8>>)>,
    ident_buf: Vec<u8>,
    seq_buf: Vec<u8>,
    qual_buf: Vec<u8>,
}

impl<'a> FastaWriterConcurrentBuffer<'a> {
    pub fn new(target: &'a Mutex<ReadsWriter>, max_size: usize) -> Self {
        Self {
            target,
            sequences: Vec::with_capacity(max_size / 128),
            ident_buf: Vec::with_capacity(max_size),
            seq_buf: Vec::with_capacity(max_size),
            qual_buf: Vec::new(),
        }
    }

    fn flush(&mut self) -> usize {
        let mut buffer = self.target.lock();

        let first_read_index = buffer.get_reads_count();

        for (ident, seq, qual) in self.sequences.iter() {
            buffer.add_read(FastaSequence {
                ident: ident.get_slice(&self.ident_buf),
                seq: seq.get_slice(&self.seq_buf),
                qual: qual.as_ref().map(|qual| qual.get_slice(&self.qual_buf)),
            })
        }
        drop(buffer);
        self.sequences.clear();
        self.ident_buf.clear();
        self.seq_buf.clear();
        self.qual_buf.clear();

        first_read_index
    }

    #[inline(always)]
    fn will_overflow(vec: &Vec<u8>, len: usize) -> bool {
        vec.len() > 0 && (vec.len() + len > vec.capacity())
    }

    pub fn add_read(&mut self, read: FastaSequence) -> Option<usize> {
        let mut result = None;

        if Self::will_overflow(&self.ident_buf, read.ident.len())
            || Self::will_overflow(&self.seq_buf, read.seq.len())
            || match read.qual {
                None => false,
                Some(qual) => Self::will_overflow(&self.qual_buf, qual.len()),
            }
        {
            result = Some(self.flush());
        }
        let qual = read
            .qual
            .map(|qual| VecSlice::new_extend(&mut self.qual_buf, qual));

        self.sequences.push((
            VecSlice::new_extend(&mut self.ident_buf, read.ident),
            VecSlice::new_extend(&mut self.seq_buf, read.seq),
            qual,
        ));

        result
    }

    pub fn finalize(mut self) -> usize {
        self.flush()
    }
}

impl Drop for FastaWriterConcurrentBuffer<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}
