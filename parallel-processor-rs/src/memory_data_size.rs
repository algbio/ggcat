pub use measurements::data::Data as MemoryDataSize;
pub use measurements::Measurement;

pub trait MemoryDataSizeExt {
    fn as_bytes(&self) -> usize;
    fn from_bytes(bytes: usize) -> Self;
    fn max(self, other: Self) -> Self;
}

impl MemoryDataSizeExt for MemoryDataSize {
    fn as_bytes(&self) -> usize {
        self.as_base_units() as usize
    }
    fn from_bytes(bytes: usize) -> Self {
        Self::from_base_units(bytes as f64)
    }
    fn max(self, other: Self) -> Self {
        Self::from_base_units(self.as_base_units().max(other.as_base_units()))
    }
}
