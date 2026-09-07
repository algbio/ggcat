use streaming_libdeflate_rs::{
    LibdeflateDecompressResult, LibdeflateError, LibdeflateGzipFileDecompressor,
};

pub struct GzipDecompressorWrapper<'a> {
    decompressor: &'a mut LibdeflateGzipFileDecompressor,
    pos: usize,
    offset: usize,
    // first_byte: u8,
    is_finished: bool,
}

impl<'a> GzipDecompressorWrapper<'a> {
    pub fn new(
        decompressor: &'a mut LibdeflateGzipFileDecompressor,
    ) -> Result<Self, LibdeflateError> {
        let is_finished = matches!(
            decompressor.decompress()?,
            LibdeflateDecompressResult::EndOfStream
        );

        // let first_byte = if is_finished {
        //     0
        // } else {
        //     decompressor.pending_output().get(0).copied().unwrap_or(0)
        // };

        Ok(Self {
            decompressor,
            pos: 0,
            offset: 0,
            // first_byte,
            is_finished,
        })
    }
}

impl<'a> Iterator for GzipDecompressorWrapper<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_finished {
            None
        } else {
            let mut data = self.decompressor.pending_output();

            if self.pos + 64 > data.len() {
                let consumed_bytes = self.pos.min(data.len());
                self.decompressor.consume_output(consumed_bytes);
                self.offset += consumed_bytes;

                match self
                    .decompressor
                    .decompress()
                    .expect("Error while reading compressed data")
                {
                    LibdeflateDecompressResult::MoreData => {
                        self.pos = 0;
                    }
                    LibdeflateDecompressResult::EndOfStream => {
                        self.is_finished = true;
                        return None;
                    }
                }
                data = self.decompressor.pending_output();
            }

            let pos = self.pos;
            self.pos += 64;

            unsafe {
                Some(std::slice::from_raw_parts(
                    data.as_ptr().add(pos),
                    data.len() - pos,
                ))
            }
        }
    }
}
