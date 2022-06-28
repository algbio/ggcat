use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::utils::compressed_read::CompressedReadIndipendent;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::PacketTrait;
use std::mem::size_of;

pub struct ReadsBuffer<E: SequenceExtraData + 'static> {
    pub reads: Vec<(u8, E, CompressedReadIndipendent)>,
    pub extra_buffer: E::TempBuffer,
    pub reads_buffer: Vec<u8>,
}

impl<E: SequenceExtraData + 'static> ReadsBuffer<E> {
    pub fn is_full(&self) -> bool {
        self.reads.len() == self.reads.capacity()
    }
}

impl<E: SequenceExtraData + 'static> PoolObjectTrait for ReadsBuffer<E> {
    type InitData = usize;

    fn allocate_new(init_data: &Self::InitData) -> Self {
        Self {
            reads: Vec::with_capacity(*init_data),
            extra_buffer: E::new_temp_buffer(),
            reads_buffer: vec![],
        }
    }

    fn reset(&mut self) {
        self.reads.clear();
        self.reads_buffer.clear();
    }
}

impl<E: SequenceExtraData + 'static> PacketTrait for ReadsBuffer<E> {
    fn get_size(&self) -> usize {
        self.reads.len() * size_of::<(u8, E, CompressedReadIndipendent)>() + self.reads_buffer.len()
    }
}
