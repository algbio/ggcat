use super::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;
use dynamic_dispatch::dynamic_dispatch;
use parking_lot::{Condvar, Mutex};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub mod binary;
pub mod concurrent;
pub mod fasta;
pub mod gfa;
pub mod stream_finish;

pub trait IdentSequenceWriter: SequenceExtraDataConsecutiveCompression + Sized {
    fn write_as_ident(&self, stream: &mut impl Write, extra_buffer: &Self::TempBuffer);
    fn write_as_gfa(
        &self,
        k: u64,
        index: u64,
        stream: &mut impl Write,
        extra_buffer: &Self::TempBuffer,
    );

    fn parse_as_ident<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self>;

    fn parse_as_gfa<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self>;
}

impl IdentSequenceWriter for () {
    fn write_as_ident(&self, _stream: &mut impl Write, _extra_buffer: &Self::TempBuffer) {}

    fn write_as_gfa(
        &self,
        _k: u64,
        _index: u64,
        _stream: &mut impl Write,
        _extra_buffer: &Self::TempBuffer,
    ) {
    }

    fn parse_as_ident<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        Some(())
    }

    fn parse_as_gfa<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        Some(())
    }
}

#[derive(Clone, Debug, Default)]
#[cfg(feature = "support_kmer_counters")]
pub struct SequenceAbundance {
    pub first: u64,
    pub sum: u64,
    pub last: u64,
}

#[cfg(feature = "support_kmer_counters")]
pub type SequenceAbundanceType = SequenceAbundance;

#[cfg(not(feature = "support_kmer_counters"))]
pub type SequenceAbundanceType = ();

pub trait StructuredSequenceBackendInit: Sync + Send + Sized {
    fn new_compressed_gzip(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_compressed_lz4(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_plain(_path: impl AsRef<Path>) -> Self {
        unimplemented!()
    }
}

#[dynamic_dispatch]
pub trait StructuredSequenceBackendWrapper: 'static {
    type Backend<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter>:
         StructuredSequenceBackendInit +
         StructuredSequenceBackend<ColorInfo, LinksInfo>;
}

pub trait StructuredSequenceBackend<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter>:
    Sync + Send
{
    type SequenceTempBuffer;

    fn alloc_temp_buffer() -> Self::SequenceTempBuffer;

    fn write_sequence(
        k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],

        color_info: ColorInfo,
        links_info: LinksInfo,
        extra_buffers: &(ColorInfo::TempBuffer, LinksInfo::TempBuffer),

        #[cfg(feature = "support_kmer_counters")] abundance: SequenceAbundance,
    );

    fn get_path(&self) -> PathBuf;

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer);

    fn finalize(self);
}

pub struct StructuredSequenceWriter<
    ColorInfo: IdentSequenceWriter,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
> {
    current_index: Mutex<(u64, u64)>,
    k: usize,
    backend: Mutex<Backend>,
    index_condvar: Condvar,
    _phantom: PhantomData<(ColorInfo, LinksInfo, Backend)>,
}

impl<
        ColorInfo: IdentSequenceWriter,
        LinksInfo: IdentSequenceWriter,
        Backend: StructuredSequenceBackend<ColorInfo, LinksInfo>,
    > StructuredSequenceWriter<ColorInfo, LinksInfo, Backend>
{
    pub fn new(backend: Backend, k: usize) -> Self {
        Self {
            current_index: Mutex::new((0, 0)),
            k,
            backend: Mutex::new(backend),
            index_condvar: Condvar::new(),
            _phantom: PhantomData,
        }
    }

    fn write_sequences<'a>(
        &self,
        buffer: &mut Backend::SequenceTempBuffer,
        first_index: Option<u64>,
        sequences: impl ExactSizeIterator<
            Item = (&'a [u8], ColorInfo, LinksInfo, SequenceAbundanceType),
        >,
        extra_buffers: &(ColorInfo::TempBuffer, LinksInfo::TempBuffer),
    ) -> u64 {
        let sequences_count = sequences.len() as u64;
        assert!(sequences_count > 0);

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
        for (sequence, color_info, links_info, _abundance) in sequences {
            Backend::write_sequence(
                self.k,
                buffer,
                current_index,
                sequence,
                color_info,
                links_info,
                extra_buffers,
                #[cfg(feature = "support_kmer_counters")]
                _abundance,
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

        start_sequence_index
    }

    pub fn get_path(&self) -> PathBuf {
        self.backend.lock().get_path()
    }

    pub fn finalize(self) {
        self.backend.into_inner().finalize();
    }
}
