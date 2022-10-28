use crate::concurrent::structured_sequences::{
    IdentSequenceWriter, StructuredSequenceBackend, StructuredSequenceWriter,
};
use utils::vec_slice::VecSlice;

pub struct FastaWriterConcurrentBuffer<
    'a,
    ColorInfo: IdentSequenceWriter,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
> {
    target: &'a StructuredSequenceWriter<ColorInfo, LinksInfo, Backend>,
    sequences: Vec<(VecSlice<u8>, ColorInfo, LinksInfo)>,
    seq_buf: Vec<u8>,
    extra_buffers: (ColorInfo::TempBuffer, LinksInfo::TempBuffer),
    temp_buffer: Backend::SequenceTempBuffer,
    current_index: Option<u64>,
    auto_flush: bool,
}

impl<
        'a,
        ColorInfo: IdentSequenceWriter,
        LinksInfo: IdentSequenceWriter,
        Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
    > FastaWriterConcurrentBuffer<'a, ColorInfo, LinksInfo, Backend>
{
    pub fn new(
        target: &'a StructuredSequenceWriter<ColorInfo, LinksInfo, Backend>,
        max_size: usize,
        auto_flush: bool,
    ) -> Self {
        Self {
            target,
            sequences: Vec::with_capacity(max_size / 128),
            seq_buf: Vec::with_capacity(max_size),
            extra_buffers: (ColorInfo::new_temp_buffer(), LinksInfo::new_temp_buffer()),
            temp_buffer: Backend::alloc_temp_buffer(),
            current_index: None,
            auto_flush,
        }
    }

    pub fn flush(&mut self) -> u64 {
        if self.sequences.len() == 0 {
            return 0;
        }

        let first_read_index = self.target.write_sequences(
            &mut self.temp_buffer,
            self.current_index.map(|c| c - self.sequences.len() as u64),
            self.sequences
                .drain(..)
                .map(|(slice, col, link)| (slice.get_slice(&self.seq_buf), col, link)),
            &self.extra_buffers,
        );

        ColorInfo::clear_temp_buffer(&mut self.extra_buffers.0);
        LinksInfo::clear_temp_buffer(&mut self.extra_buffers.1);
        self.seq_buf.clear();

        first_read_index
    }

    #[inline(always)]
    fn will_overflow(vec: &Vec<u8>, len: usize) -> bool {
        vec.len() > 0 && (vec.len() + len > vec.capacity())
    }

    pub fn add_read(
        &mut self,
        sequence: &[u8],
        sequence_index: Option<u64>,
        color: ColorInfo,
        color_extra_buffer: &ColorInfo::TempBuffer,
        links: LinksInfo,
        links_extra_buffer: &LinksInfo::TempBuffer,
    ) -> Option<u64> {
        let mut result = None;

        if let Some(sequence_index) = sequence_index
            && Some(sequence_index) != self.current_index {
            result = Some(self.flush());
            self.current_index = Some(sequence_index);
        }
        else if self.auto_flush && Self::will_overflow(&self.seq_buf, sequence.len()) {
            result = Some(self.flush());
        }

        let color =
            ColorInfo::copy_extra_from(color, &color_extra_buffer, &mut self.extra_buffers.0);
        let links =
            LinksInfo::copy_extra_from(links, &links_extra_buffer, &mut self.extra_buffers.1);

        self.sequences.push((
            VecSlice::new_extend(&mut self.seq_buf, sequence),
            color,
            links,
        ));

        if let Some(current_index) = &mut self.current_index {
            *current_index += 1;
        }

        result
    }

    pub fn finalize(mut self) -> u64 {
        self.flush()
    }
}

impl<
        'a,
        ColorInfo: IdentSequenceWriter,
        LinksInfo: IdentSequenceWriter,
        Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
    > Drop for FastaWriterConcurrentBuffer<'a, ColorInfo, LinksInfo, Backend>
{
    fn drop(&mut self) {
        self.flush();
    }
}
