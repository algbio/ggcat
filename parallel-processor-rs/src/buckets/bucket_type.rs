use std::path::PathBuf;

pub trait BucketType: Send {
    type InitType: ?Sized;
    type DataType;

    const SUPPORTS_LOCK_FREE: bool;

    fn new(init_data: &Self::InitType, index: usize) -> Self;
    fn write_batch_data(&mut self, data: &[Self::DataType]);
    fn write_batch_data_lock_free(&self, _data: &[Self::DataType]) {}
    fn get_path(&self) -> PathBuf;
    fn finalize(self);
}
