use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use dynamic_dispatch::dynamic_dispatch;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{SequenceExtraData, TempBuffer};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::ident_writer::IdentSequenceWriter;
use io::partial_unitigs_extra_data::{PartialUnitigExtraData, SequenceAbundanceType};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cell::RefCell;
use std::io::Write;
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

    /// Whether the backend can write a sequence with [`DEFERRED_SEQUENCE_INDEX`], leaving its index out,
    /// so that the index is assigned only when the sequence is written to the output.
    const SUPPORTS_DEFERRED_INDEX: bool = false;

    /// The current size of the temp buffer, used to record where each deferred index sequence starts
    fn temp_buffer_size(_buffer: &Self::SequenceTempBuffer) -> usize {
        unimplemented!()
    }

    /// Flushes a temp buffer holding sequences written with [`DEFERRED_SEQUENCE_INDEX`],
    /// giving consecutive indexes starting from `first_index` to the sequences starting at `sequences_starts`
    fn flush_temp_buffer_with_indexes(
        &mut self,
        _buffer: &mut Self::SequenceTempBuffer,
        _sequences_starts: &[usize],
        _first_index: u64,
    ) {
        unimplemented!()
    }

    fn finalize(self);
}

/// Placeholder index for sequences whose index is assigned when they are written to the output
pub const DEFERRED_SEQUENCE_INDEX: u64 = u64::MAX;

/// Writes a buffer holding sequences written with [`DEFERRED_SEQUENCE_INDEX`], inserting consecutive indexes
/// starting from `first_index`, each `index_offset` bytes after the start of its sequence in `sequences_starts`
fn write_with_deferred_indexes(
    writer: &mut impl Write,
    buffer: &[u8],
    sequences_starts: &[usize],
    first_index: u64,
    index_offset: usize,
) {
    let mut written = 0;
    for (sequence_index, &start) in (first_index..).zip(sequences_starts) {
        let index_position = start + index_offset;
        writer.write_all(&buffer[written..index_position]).unwrap();
        write!(writer, "{}", sequence_index).unwrap();
        written = index_position;
    }
    writer.write_all(&buffer[written..]).unwrap();
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

        if first_index.is_none() && Backend::SUPPORTS_DEFERRED_INDEX {
            return self.write_sequences_deferred_index(
                buffer,
                sequences,
                extra_buffers,
                indirect_file,
            );
        }

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

    /// Writes the sequences assigning their indexes only when they are written to the output,
    /// so that the file is in index order without waiting for the other threads to write their sequences.
    /// The sequences are written to the temp buffer without holding any lock, and then flushed under an exclusive lock.
    /// If the temp buffer grows too large (with long indirect sequences) the lock is taken early
    /// and held until all the sequences are written, keeping the memory usage bounded.
    fn write_sequences_deferred_index<'a>(
        &self,
        buffer: &mut Backend::SequenceTempBuffer,
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
        struct FlushState<'a, Backend> {
            sequences_starts: Vec<usize>,
            locks: Option<(MutexGuard<'a, (u64, u64)>, MutexGuard<'a, Backend>)>,
            first_index: Option<u64>,
        }

        let state = RefCell::new(FlushState {
            sequences_starts: Vec::with_capacity(sequences.len()),
            locks: None,
            first_index: None,
        });

        // Writes the buffered sequences, taking the lock if not already held
        let flush = |buffer: &mut Backend::SequenceTempBuffer| {
            let mut state = state.borrow_mut();
            let FlushState {
                sequences_starts,
                locks,
                first_index,
            } = &mut *state;
            let (index_lock, backend) =
                locks.get_or_insert_with(|| (self.current_index.lock(), self.backend.lock()));
            let start_index = index_lock.0;
            index_lock.0 += sequences_starts.len() as u64;
            index_lock.1 = index_lock.0;
            backend.flush_temp_buffer_with_indexes(buffer, sequences_starts, start_index);
            sequences_starts.clear();
            first_index.get_or_insert(start_index);
        };

        let mut extract_workdata = ReadExtractWorkData::new();
        for (sequence, extra_info, links_info) in sequences {
            state
                .borrow_mut()
                .sequences_starts
                .push(Backend::temp_buffer_size(buffer));
            Backend::write_sequence(
                &mut extract_workdata,
                self.k,
                buffer,
                DEFERRED_SEQUENCE_INDEX,
                sequence,
                extra_info,
                links_info,
                extra_buffers,
                indirect_file,
                // Called only when the buffer is too large, the lock is then kept until the end
                |buffer| flush(buffer),
            );
        }

        flush(buffer);

        let state = state.into_inner();
        drop(state.locks);
        // Wake any writer with preassigned indexes waiting for its turn
        self.index_condvar.notify_all();
        state.first_index.unwrap()
    }

    pub fn get_path(&self) -> PathBuf {
        self.backend.lock().get_path()
    }

    pub fn finalize(self) {
        self.backend.into_inner().finalize();
    }
}
