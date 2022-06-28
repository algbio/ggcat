use std::io::{Read, Write};

pub trait BucketItem {
    type ExtraData;
    type ReadBuffer;
    type ExtraDataBuffer;
    type ReadType<'a>;

    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        extra_read_buffer: &Self::ExtraDataBuffer,
    );
    fn read_from<'a, S: Read>(
        stream: S,
        read_buffer: &'a mut Self::ReadBuffer,
        extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>>;
    fn get_size(&self, extra: &Self::ExtraData) -> usize;
}

impl<const SIZE: usize> BucketItem for [u8; SIZE] {
    type ExtraData = ();
    type ExtraDataBuffer = ();
    type ReadBuffer = Self;
    type ReadType<'a> = &'a Self;

    #[inline(always)]
    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _: &Self::ExtraDataBuffer,
    ) {
        bucket.write(self).unwrap();
    }

    fn read_from<'a, S: Read>(
        mut stream: S,
        read_buffer: &'a mut Self::ReadBuffer,
        _: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>> {
        stream.read_exact(read_buffer).ok()?;
        Some(read_buffer)
    }

    #[inline(always)]
    fn get_size(&self, _: &()) -> usize {
        self.len()
    }
}

impl BucketItem for [u8] {
    type ExtraData = ();
    type ExtraDataBuffer = ();
    type ReadBuffer = ();
    type ReadType<'a> = ();

    #[inline(always)]
    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _: &Self::ExtraDataBuffer,
    ) {
        bucket.write(self).unwrap();
    }

    fn read_from<'a, S: Read>(
        _stream: S,
        _read_buffer: &'a mut Self::ReadBuffer,
        _: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>> {
        unimplemented!("Cannot read slices of unknown size!")
    }

    #[inline(always)]
    fn get_size(&self, _: &()) -> usize {
        self.len()
    }
}
