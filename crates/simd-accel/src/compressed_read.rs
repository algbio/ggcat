//! The packed reads the parser hands out.
//!
//! Laid out exactly as io's `CompressedRead`, which io builds from one with `.into()`: this
//! crate cannot name that type, as io depends on it.

use std::fmt::{Debug, Formatter, Write};
use std::marker::PhantomData;
use utils::Utils;

/// `size` bases packed four per byte, two bits each (A=0, C=1, T=2, G=3), base `i` of a byte in
/// its bits `2i` and `2i + 1`. The read starts at base `start` of the byte `data` points to.
#[derive(Copy, Clone)]
pub struct SimdCompressedRead<'a> {
    pub size: usize,
    pub start: u8,
    pub data: *const u8,
    _phantom: PhantomData<&'a ()>,
}

// SAFETY: a read only ever reads the bytes it borrows for `'a`, like a `&'a [u8]` would.
unsafe impl Sync for SimdCompressedRead<'_> {}
unsafe impl Send for SimdCompressedRead<'_> {}

impl<'a> SimdCompressedRead<'a> {
    /// The `size` bases of `data` from base `start` on.
    pub fn new(data: &'a [u8], start: u8, size: usize) -> Self {
        assert!(
            start as usize + size <= data.len() * 4,
            "Compressed read overflow!"
        );
        Self {
            size,
            start,
            data: data.as_ptr(),
            _phantom: PhantomData,
        }
    }
}

/// The bases as letters, as io's `CompressedRead` prints them.
impl Debug for SimdCompressedRead<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for index in self.start as usize..self.start as usize + self.size {
            // SAFETY: `new` checked that every base is inside the borrowed bytes.
            let code = (unsafe { *self.data.add(index / 4) } >> ((index % 4) * 2)) & 3;
            f.write_char(Utils::decompress_base(code) as char)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Packs `codes` four per byte.
    fn packed(codes: &[u8]) -> Vec<u8> {
        codes
            .chunks(4)
            .map(|chunk| {
                (chunk.iter().enumerate()).fold(0, |byte, (index, code)| byte | code << (2 * index))
            })
            .collect()
    }

    #[test]
    fn prints_its_bases() {
        // T G C A G T A C
        let data = packed(&[2, 3, 1, 0, 3, 2, 0, 1]);
        assert_eq!(
            format!("{:?}", SimdCompressedRead::new(&data, 0, 8)),
            "TGCAGTAC"
        );
        assert_eq!(
            format!("{:?}", SimdCompressedRead::new(&data, 1, 6)),
            "GCAGTA"
        );
        assert_eq!(format!("{:?}", SimdCompressedRead::new(&data, 3, 0)), "");
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn rejects_bases_past_the_data() {
        let data = packed(&[0; 8]);
        SimdCompressedRead::new(&data, 1, 8);
    }
}
