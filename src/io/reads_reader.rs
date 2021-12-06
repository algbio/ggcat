use std::cell::UnsafeCell;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

pub struct ReadsReader {
    reader: UnsafeCell<Box<dyn Read>>,
}

unsafe impl Send for ReadsReader {}
unsafe impl Sync for ReadsReader {}

trait UnsafeCellGetMutable {
    type Output;
    #[allow(clippy::mut_from_ref)]
    fn uget(&self) -> &mut Self::Output;
}

impl<T> UnsafeCellGetMutable for UnsafeCell<T> {
    type Output = T;

    fn uget(&self) -> &mut Self::Output {
        unsafe { &mut *self.get() }
    }
}

struct RefThreadWrapper<'a, T: ?Sized>(&'a mut T);
unsafe impl<'a, T: ?Sized> Sync for RefThreadWrapper<'a, T> {}
unsafe impl<'a, T: ?Sized> Send for RefThreadWrapper<'a, T> {}

impl ReadsReader {
    pub fn from_file(name: String) -> ReadsReader {
        let is_compressed = name.ends_with(".lz4");
        let file = File::open(name).unwrap();

        let reader: Box<dyn Read> = if is_compressed {
            let decoder = lz4::Decoder::new(file).unwrap();
            Box::new(decoder)
        } else {
            Box::new(file)
        };
        ReadsReader {
            reader: UnsafeCell::new(reader),
        }
    }

    pub fn for_each<F: FnMut(&[u8])>(&self, mut func: F) {
        let mut reader_cell = self.reader.uget();
        let reader = BufReader::new(reader_cell);
        for line in reader.lines() {
            func(line.unwrap().as_bytes());
        }
    }
}
