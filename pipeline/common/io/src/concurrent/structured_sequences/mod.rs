use parallel_processor::buckets::bucket_writer::BucketItem;
use parking_lot::{Condvar, Mutex};
use std::fmt::Write;
use std::marker::PhantomData;
use std::path::Path;

pub mod fasta;

trait IndentSequenceWriter: BucketItem {
    fn write_as_ident(
        stream: impl Write,
        extra_data: &Self::ExtraData,
        extra_read_buffer: &Self::ExtraDataBuffer,
    ) -> bool;
    fn write_as_gfa(
        stream: impl Write,
        extra_data: &Self::ExtraData,
        extra_read_buffer: &Self::ExtraDataBuffer,
    ) -> bool;

    fn parse_as_ident<'a>(
        ident: &[u8],
        read_buffer: &'a mut Self::ReadBuffer,
        extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>>;

    fn parse_as_gfa<'a>(
        ident: &[u8],
        read_buffer: &'a mut Self::ReadBuffer,
        extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>>;
}

trait StructuredSequenceBackend<ColorInfo: IndentSequenceWriter, LinksInfo: IndentSequenceWriter> {
    type SequenceTempBuffer;

    fn alloc_temp_buffer() -> Self::SequenceTempBuffer;

    fn write_sequence(
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],

        color_info: &ColorInfo,
        color_extra_data: &ColorInfo::ExtraData,
        color_extra_read_buffer: &ColorInfo::ExtraDataBuffer,

        links_info: &LinksInfo,
        links_extra_data: &LinksInfo::ExtraData,
        links_extra_read_buffer: &LinksInfo::ExtraDataBuffer,
    );

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer);

    fn new(output_file: impl AsRef<Path>) -> Self;
}

struct StructuredSequenceWriter<
    ColorInfo: IndentSequenceWriter,
    LinksInfo: IndentSequenceWriter,
    Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
> {
    current_index: Mutex<(u64, u64)>,
    backend: Mutex<Backend>,
    index_condvar: Condvar,
    _phantom: PhantomData<(ColorInfo, LinksInfo, Backend)>,
}

impl<
        ColorInfo: IndentSequenceWriter,
        LinksInfo: IndentSequenceWriter,
        Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
    > StructuredSequenceWriter<ColorInfo, LinksInfo, Backend>
{
    fn new(output_file: impl AsRef<Path>) -> Self {
        Self {
            current_index: Mutex::new((0, 0)),
            backend: Mutex::new(Backend::new(output_file)),
            index_condvar: Condvar::new(),
            _phantom: PhantomData,
        }
    }

    fn write_sequences<'a>(
        &self,
        buffer: &mut Backend::SequenceTempBuffer,
        first_index: Option<u64>,
        sequences: impl ExactSizeIterator<
            Item = (
                &'a [u8],
                ColorInfo,
                ColorInfo::ExtraData,
                LinksInfo,
                LinksInfo::ExtraData,
            ),
        >,
        color_extra_read_buffer: &ColorInfo::ExtraDataBuffer,
        links_extra_read_buffer: &LinksInfo::ExtraDataBuffer,
    ) {
        let sequences_count = sequences.len() as u64;

        // Preallocate the sequences indexes (depending on the first index)
        let start_sequence_index = match first_index {
            Some(first_index) => first_index,
            None => {
                let mut index_lock = self.current_index.lock();
                let start_index = index_lock.0;
                index_lock.0 += sequences_count;
                start_index
            }
        };

        let mut current_index = start_sequence_index;
        // Write the sequences to a temporary buffer
        for (sequence, color_info, color_extra, links_info, links_extra) in sequences {
            Backend::write_sequence(
                buffer,
                current_index,
                sequence,
                &color_info,
                &color_extra,
                color_extra_read_buffer,
                &links_info,
                &links_extra,
                links_extra_read_buffer,
            );
            current_index += 1;
        }

        loop {
            // If we are the first ones that need to write, flush the buffer to file
            let mut index_lock = self.current_index.lock();

            if index_lock.1 == start_sequence_index {
                self.backend.lock().flush_temp_buffer(buffer);
                index_lock.1 += sequences_count;
                self.index_condvar.notify_all();
                break;
            } else {
                self.index_condvar.wait(&mut index_lock);
            }
        }
    }
}
