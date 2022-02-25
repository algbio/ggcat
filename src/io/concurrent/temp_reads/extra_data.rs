use crate::KEEP_FILES;
use core::fmt::Debug;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

struct PointerDecoder {
    ptr: *const u8,
}

impl Read for PointerDecoder {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), buf.len());
            self.ptr = self.ptr.add(buf.len());
        }
        Ok(buf.len())
    }

    #[inline(always)]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), buf.len());
            self.ptr = self.ptr.add(buf.len());
        }
        Ok(())
    }
}

pub trait SequenceExtraData: Sized + Send + Debug {
    fn decode_from_slice(slice: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(slice);
        Self::decode(&mut cursor)
    }

    unsafe fn decode_from_pointer(ptr: *const u8) -> Option<Self> {
        let mut stream = PointerDecoder { ptr };
        Self::decode(&mut stream)
    }

    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self>;
    fn encode<'a>(&self, writer: &'a mut impl Write);

    fn max_size(&self) -> usize;

    fn load_data_to_vec(file: impl AsRef<Path>, remove: bool) -> Vec<Self> {
        let mut vec = Vec::new();

        let mut reader = FileReader::open(&file).unwrap();

        while let Some(value) = Self::decode(&mut reader) {
            vec.push(value);
        }

        drop(reader);
        if remove {
            MemoryFs::remove_file(
                &file,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
            )
            .unwrap();
        }

        vec
    }
}

impl SequenceExtraData for () {
    #[inline(always)]
    fn decode<'a>(_reader: &'a mut impl Read) -> Option<Self> {
        Some(())
    }

    #[inline(always)]
    fn encode<'a>(&self, _writer: &'a mut impl Write) {}

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
}
