use crate::buckets::bucket_writer::BucketItem;

pub mod async_binary_reader;
pub mod compressed_binary_reader;
pub mod generic_binary_reader;
pub mod lock_free_binary_reader;
pub mod unbuffered_compressed_binary_reader;

pub trait BucketReader {
    fn decode_all_bucket_items<E: BucketItem, F: for<'a> FnMut(E::ReadType<'a>)>(
        self,
        buffer: E::ReadBuffer,
        func: F,
    );
}
