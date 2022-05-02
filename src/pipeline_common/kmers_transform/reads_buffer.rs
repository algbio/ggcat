use crate::utils::compressed_read::CompressedReadIndipendent;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::PacketTrait;

pub struct ReadsBuffer<E: Send + Sync + 'static> {
    pub reads: Vec<(u8, E, CompressedReadIndipendent)>,
    pub reads_buffer: Vec<u8>,
}

impl<E: Send + Sync + 'static> ReadsBuffer<E> {
    pub fn is_full(&self) -> bool {
        self.reads.len() == self.reads.capacity()
    }
}

impl<E: Send + Sync + 'static> PoolObjectTrait for ReadsBuffer<E> {
    type InitData = usize;

    fn allocate_new(init_data: &Self::InitData) -> Self {
        Self {
            reads: Vec::with_capacity(*init_data),
            reads_buffer: vec![],
        }
    }

    fn reset(&mut self) {
        self.reads.clear();
        self.reads_buffer.clear();
    }
}

impl<E: Send + Sync + 'static> PacketTrait for ReadsBuffer<E> {}
