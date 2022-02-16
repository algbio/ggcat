use std::io::Write;

pub trait BucketWriter<DataType = u8> {
    type ExtraData;
    fn write_to(&self, bucket: &mut Vec<DataType>, extra_data: &Self::ExtraData);
    fn get_size(&self) -> usize;
}

impl<T: Copy> BucketWriter<T> for T {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<T>, _extra_data: &Self::ExtraData) {
        bucket.push(*self);
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        1
    }
}

impl<const SIZE: usize> BucketWriter for [u8; SIZE] {
    type ExtraData = ();
    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        bucket.write(self).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl BucketWriter for [u8] {
    type ExtraData = ();
    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        bucket.write(self).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        self.len()
    }
}
