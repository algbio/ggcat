use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use dynamic_dispatch::dynamic_dispatch;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{SequenceExtraData, TempBuffer};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::ident_writer::IdentSequenceWriter;
use io::partial_unitigs_extra_data::{PartialUnitigExtraData, SequenceAbundanceType};
use parking_lot::{Condvar, Mutex};

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::indirect_reads_extractor::ReadExtractWorkData;

pub mod binary;
pub mod concurrent;
pub mod fasta;
pub mod gfa;
pub mod stream_finish;

pub fn new_sequence_abundance(_multiplicity: usize, _kmers: usize) -> SequenceAbundanceType {
    match () {
        #[cfg(feature = "support_kmer_counters")]
        () => SequenceAbundanceType {
            first: _multiplicity as u64,
            sum: (_multiplicity * _kmers) as u64,
            last: _multiplicity as u64,
        },
        #[cfg(not(feature = "support_kmer_counters"))]
        () => {}
    }
}

pub trait StructuredSequenceBackendInit: Sync + Send + Sized {
    fn new_compressed_gzip(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_compressed_lz4(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_compressed_zstd(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_compressed_bz2(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_compressed_xz(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_plain(_path: impl AsRef<Path>) -> Self {
        unimplemented!()
    }
}

#[dynamic_dispatch]
pub trait StructuredSequenceBackendWrapper: 'static + Sync + Send {
    type Backend<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData>:
         StructuredSequenceBackendInit +
         StructuredSequenceBackend<CX, LinksInfo>;
}

pub trait StructuredSequenceBackend<CX: ColorsManager, LinksInfo: IdentSequenceWriter>:
    Sync + Send
{
    type SequenceTempBuffer;

    fn alloc_temp_buffer(k: usize) -> Self::SequenceTempBuffer;

    fn write_sequence(
        extract_workdata: &mut ReadExtractWorkData<CX>,
        k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: CompressedRead,
        extra_info: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        links_info: LinksInfo,
        extra_buffers: &(
            TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
            LinksInfo::TempBuffer,
        ),
        indirect_file: Option<&ConcurrentFileWriter>,
        flush_callback: impl FnMut(&mut Self::SequenceTempBuffer),
    );

    fn get_path(&self) -> PathBuf;

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer);

    fn finalize(self);
}

pub struct StructuredSequenceWriter<
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<CX, LinksInfo>,
> {
    current_index: Mutex<(u64, u64)>,
    k: usize,
    backend: Mutex<Backend>,
    index_condvar: Condvar,
    _phantom: PhantomData<(CX, LinksInfo, Backend)>,
}

impl<
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<CX, LinksInfo>,
> StructuredSequenceWriter<CX, LinksInfo, Backend>
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
            Item = (
                CompressedRead<'a>,
                PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
                LinksInfo,
            ),
        >,
        extra_buffers: &(
            TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
            LinksInfo::TempBuffer,
        ),
        indirect_file: Option<&ConcurrentFileWriter>,
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

        let mut extract_workdata = ReadExtractWorkData::new();

        let mut flush_lock = None;

        let mut flush_function = |buffer: &mut Backend::SequenceTempBuffer| {
            if flush_lock.is_none() {
                flush_lock = Some(loop {
                    // If we are the first ones that need to write, flush the buffer to file
                    let mut index_lock = self.current_index.lock();

                    if index_lock.1 == start_sequence_index {
                        break index_lock;
                    } else {
                        self.index_condvar.wait(&mut index_lock);
                    }
                });
            }
            self.backend.lock().flush_temp_buffer(buffer);
        };

        let mut current_index = start_sequence_index;
        // Write the sequences to a temporary buffer
        for (sequence, extra_info, links_info) in sequences {
            Backend::write_sequence(
                &mut extract_workdata,
                self.k,
                buffer,
                current_index,
                sequence,
                extra_info,
                links_info,
                extra_buffers,
                indirect_file,
                &mut flush_function,
            );
            current_index += 1;
        }

        flush_function(buffer);

        let mut index_lock = flush_lock.unwrap();
        index_lock.1 += sequences_count;
        self.index_condvar.notify_all();

        start_sequence_index
    }

    pub fn get_path(&self) -> PathBuf {
        self.backend.lock().get_path()
    }

    pub fn finalize(self) {
        self.backend.into_inner().finalize();
    }
}
