use crate::memory_data_size::MemoryDataSize;

pub mod panic_on_drop;
pub mod replace_with_async;
pub mod scoped_thread_local;
pub(crate) mod vec_reader;

pub const fn memory_size_to_log2(size: MemoryDataSize) -> u8 {
    ((size.octets as u64) * 2 - 1).ilog2() as u8
}
