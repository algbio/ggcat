use config::MultiplicityCounterType;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use parallel_processor::buckets::ExtraBucketData;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::PacketTrait;
use std::mem::size_of;

pub struct DeserializedReadIndependent<E> {
    pub read: CompressedReadIndipendent,
    pub extra: E,
    pub multiplicity: MultiplicityCounterType,
    pub flags: u8,
    pub minimizer_pos: u16,
    pub is_window_duplicate: bool,
}

pub struct ReadsVector<E> {
    reads: Vec<CompressedReadIndipendent>,
    extra_data: Vec<E>,
    flags: Vec<u8>,
    multiplicities: Vec<MultiplicityCounterType>,
    minimizer_pos: Vec<u16>,
    window_duplicates_indices: Vec<usize>,
    pub minimizer_size: usize,
    pub total_multiplicity: u64,
    pub extra_bucket_data: Option<ExtraBucketData>,
}

impl<E> ReadsVector<E> {
    pub fn push<const WITH_MULTIPLICITY: bool>(&mut self, read: DeserializedReadIndependent<E>) {
        if self.reads.len() == self.reads.capacity() {
            self.reads.reserve(1);
            self.extra_data.reserve(1);
            self.flags.reserve(1);
            self.minimizer_pos.reserve(1);
            if WITH_MULTIPLICITY {
                self.multiplicities.reserve(1);
            }
        }

        // Unsafe to avoid multiple checks
        unsafe {
            let index = self.reads.len();
            self.reads.set_len(index + 1);
            *self.reads.get_unchecked_mut(index) = read.read;

            // Increment all the lengths when debug assertions are enabled, else it crashes
            if cfg!(debug_assertions) {
                self.extra_data.set_len(index + 1);
                self.flags.set_len(index + 1);
                self.minimizer_pos.set_len(index + 1);
            }

            // Extra data and flags
            *self.extra_data.get_unchecked_mut(index) = read.extra;
            *self.flags.get_unchecked_mut(index) = read.flags;
            *self.minimizer_pos.get_unchecked_mut(index) = read.minimizer_pos;

            if read.is_window_duplicate {
                self.window_duplicates_indices.push(index);
            }

            if WITH_MULTIPLICITY {
                self.multiplicities.set_len(index + 1);
                *self.multiplicities.get_unchecked_mut(index) = read.multiplicity;
            }
        }

        self.total_multiplicity += read.multiplicity as u64;

        // Sanity checks
        if cfg!(debug_assertions) {
            if self.multiplicities.len() > 0 {
                debug_assert_eq!(self.multiplicities.len(), self.reads.len());
            }
        }
    }

    pub fn get_total_multiplicity(&self) -> u64 {
        self.total_multiplicity
    }

    pub fn len(&self) -> usize {
        self.reads.len()
    }

    pub fn capacity(&self) -> usize {
        self.reads.capacity()
    }

    fn clear(&mut self) {
        self.reads.clear();
        self.multiplicities.clear();

        unsafe {
            // Allow dropping if needed
            self.extra_data.set_len(self.reads.len());
            self.flags.set_len(self.reads.len());
        }

        self.extra_data.clear();
        self.flags.clear();
        self.minimizer_pos.clear();
        self.window_duplicates_indices.clear();
        self.total_multiplicity = 0;
        self.extra_bucket_data = None;
    }

    pub fn iter(&self) -> ReadsVectorIterator<E> {
        // Sanity checks
        if self.multiplicities.len() > 0 {
            assert_eq!(self.multiplicities.len(), self.reads.len());
        }

        ReadsVectorIterator {
            reads: self,
            index: 0,
            minimizer_index: 0,
        }
    }
}

pub struct ReadsVectorIterator<'a, E> {
    reads: &'a ReadsVector<E>,
    index: usize,
    minimizer_index: usize,
}

impl<'a, E: Copy> Iterator for ReadsVectorIterator<'a, E> {
    type Item = DeserializedReadIndependent<E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.reads.len() {
            let index = self.index;
            self.index += 1;
            Some(unsafe {
                DeserializedReadIndependent {
                    read: *self.reads.reads.get_unchecked(index),
                    extra: *self.reads.extra_data.get_unchecked(index),
                    multiplicity: if self.reads.multiplicities.len() > 0 {
                        *self.reads.multiplicities.get_unchecked(index)
                    } else {
                        1
                    },
                    flags: *self.reads.flags.get_unchecked(index),
                    minimizer_pos: *self.reads.minimizer_pos.get_unchecked(index),
                    is_window_duplicate: if self.minimizer_index
                        < self.reads.window_duplicates_indices.len()
                        && *self
                            .reads
                            .window_duplicates_indices
                            .get_unchecked(self.minimizer_index)
                            == index
                    {
                        self.minimizer_index += 1;
                        true
                    } else {
                        false
                    },
                }
            })
        } else {
            None
        }
    }
}

pub struct ReadsBuffer<E: SequenceExtraDataTempBufferManagement + 'static> {
    pub reads: ReadsVector<E>,
    pub sub_bucket: usize,
    pub extra_buffer: E::TempBuffer,
    pub reads_buffer: Vec<u8>,
}

impl<E: SequenceExtraDataTempBufferManagement + 'static> ReadsBuffer<E> {
    pub fn is_full(&self) -> bool {
        self.reads.len() == self.reads.capacity()
    }
}

impl<E: SequenceExtraDataTempBufferManagement + 'static> PoolObjectTrait for ReadsBuffer<E> {
    type InitData = usize;

    fn allocate_new(init_data: &Self::InitData) -> Self {
        Self {
            reads: ReadsVector {
                reads: Vec::with_capacity(*init_data),
                extra_data: Vec::with_capacity(*init_data),
                flags: Vec::with_capacity(*init_data),
                multiplicities: Vec::with_capacity(*init_data),
                minimizer_pos: Vec::with_capacity(*init_data),
                window_duplicates_indices: vec![],
                minimizer_size: 0,
                total_multiplicity: 0,
                extra_bucket_data: None,
            },
            sub_bucket: 0,
            extra_buffer: E::new_temp_buffer(),
            reads_buffer: vec![],
        }
    }

    fn reset(&mut self) {
        self.reads.clear();
        self.reads_buffer.clear();
    }
}

impl<E: SequenceExtraDataTempBufferManagement + 'static> PacketTrait for ReadsBuffer<E> {
    fn get_size(&self) -> usize {
        self.reads.len() * size_of::<(u8, E, CompressedReadIndipendent)>() + self.reads_buffer.len()
    }
}
