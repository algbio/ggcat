use crate::compressed_read::CompressedRead;
use crate::gzip_fasta_reader::FastaSequence;
use crate::utils::{cast_static, cast_static_mut, Utils};
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use nthash::NtHashIterator;
use os_pipe::{PipeReader, PipeWriter};
use std::cell::{Cell, UnsafeCell};
use std::cmp::{max, min};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Read, Write};
use std::ops::DerefMut;
use std::path::Path;
use std::process::{ChildStdin, Command, Stdio};
use std::slice::from_raw_parts;

pub struct IntermediateReadsWriter {
    writer: BufWriter<lz4::Encoder<BufWriter<File>>>,
}

pub struct IntermediateReadsReader {
    reader: lz4::Decoder<BufReader<File>>,
}

#[inline(always)]
fn encode_varint(writer: &mut impl Write, mut value: u32) {
    loop {
        let rem = ((value > 127) as u8) << 7;
        writer.write_u8(((value as u8) & 0b1111111) | rem);
        value >>= 7;
        if value == 0 {
            break;
        }
    }
}

fn decode_varint(reader: &mut VecReader<impl Read>) -> u32 {
    let mut result = 0;
    let mut offset = 0u32;
    loop {
        let mut value = reader.read_byte();
        let next = (value & 0b10000000) != 0;
        result |= ((value & 0b1111111) as u32) << offset;
        if !next {
            break;
        }
        offset += 7;
    }
    result
}

impl IntermediateReadsWriter {
    pub fn add_acgt_read(&mut self, read: &[u8]) {
        encode_varint(&mut self.writer, read.len() as u32);
        for chunk in read.chunks(16) {
            let mut value = 0;
            for aa in chunk.iter().rev() {
                value = (value << 2) | (((*aa >> 1) & 0x3) as u32);
            }
            // values[idx / 4] = (values[idx / 4] << 2) | ((*aa >> 1) & 0x3);

            self.writer
                .write(&value.to_le_bytes()[..(chunk.len() + 3) / 4]);
        }
    }

    pub fn finalize(mut self) {
        self.writer.flush();
        self.writer
            .into_inner()
            .unwrap_or_else(|_| panic!("Cannot unwrap!"))
            .finish()
            .0
            .flush()
            .unwrap();
    }
}

struct VecReader<'a, R: Read> {
    vec: Vec<u8>,
    fill: usize,
    pos: usize,
    reader: &'a mut R,
    stream_ended: bool,
}

impl<'a, R: Read> VecReader<'a, R> {
    fn new(capacity: usize, reader: &'a mut R) -> VecReader<'a, R> {
        let mut vec = vec![];
        vec.resize(capacity, 0);
        VecReader {
            vec,
            fill: 0,
            pos: 0,
            reader,
            stream_ended: false,
        }
    }

    fn update_buffer(&mut self) {
        self.fill = match self.reader.read(&mut self.vec[..]) {
            Ok(fill) => fill,
            Err(_) => 0,
        };
        self.stream_ended = self.fill == 0;
        self.pos = 0;
    }

    fn read_byte(&mut self) -> u8 {
        if self.fill == self.pos {
            self.update_buffer();

            if self.fill == self.pos {
                return 0;
            }
        }
        let value = unsafe { *self.vec.get_unchecked(self.pos) };

        self.pos += 1;
        return value;
    }

    fn read_bytes(&mut self, slice: &mut [u8]) -> usize {
        let mut offset = 0;

        while offset < slice.len() {
            if self.fill == self.pos {
                self.update_buffer();

                if self.fill == self.pos {
                    return offset;
                }
            }

            let amount = min(slice.len() - offset, self.fill - self.pos);

            unsafe {
                std::ptr::copy(
                    self.vec.as_ptr().add(self.pos),
                    slice.as_mut_ptr().add(offset),
                    amount,
                );
            }

            self.pos += amount;
            offset += amount;
        }
        offset
    }
}

impl IntermediateReadsReader {
    pub fn for_each(&mut self, mut lambda: impl FnMut(CompressedRead)) {
        let mut vec_reader = VecReader::new(1024 * 1024, &mut self.reader);

        // const LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];
        let mut read = vec![];

        loop {
            let size = decode_varint(&mut vec_reader) as usize;

            if size == 0 && vec_reader.stream_ended {
                break;
            }
            let bytes = ((size + 3) / 4);
            read.resize(max(read.len(), bytes), 0);

            vec_reader.read_bytes(&mut read[..bytes]);

            lambda(CompressedRead::new(&read[..bytes], size));

            // read.resize(max(read.len(), size + 32), 0);
            //
            // let bytes = ((size + 15) / 16) * 4;
            // let mut pos = 0;
            //
            // //             let mut remlen = len;
            // //
            // //             while remlen > 0 {
            // //                 let mut value = self.reader.read_u32::<LittleEndian>().unwrap();
            // //                 let enclen = min(16, remlen);
            // // `                // for b in (0..enclen).rev() {
            // //                 //     data[b] = LETTERS[(value & 0x3) as usize];
            // //                 //     value >>= 2;
            // //                 // }
            // //                 // read.extend_from_slice(&data[0..enclen]);
            // //                 remlen -= enclen;
            // //             }
            //
            // for _ in 0..bytes {
            //     let byte = vec_reader.read_byte();
            //     read[pos + 0] = LETTERS[((byte >> 0) & 0x3) as usize];
            //     read[pos + 1] = LETTERS[((byte >> 2) & 0x3) as usize];
            //     read[pos + 2] = LETTERS[((byte >> 4) & 0x3) as usize];
            //     read[pos + 3] = LETTERS[((byte >> 6) & 0x3) as usize];
            //     pos += 4;
            // }
            //
            // lambda(&read[0..size]);
        }

        //
        //
        //         let mut count = 0;
        //         let mut tot_len = 0;
        //         while let Some(len) = decode_varint(&mut self.reader) {
        //             let len = len as usize;
        //             read.clear();
        //             let size = (len + 15) / 16;
        //             let mut data: [u8; 16] = [0; 16];
        //
        //             count += 1;
        //             tot_len += len;
        //             if count % 100000 == 0 || size > 1000 {
        //                 eprintln!("Count {} size {} len {}", count, len, tot_len);
        //             }
        //

        //             // lambda(read.as_slice())
        //         }
    }
}

pub struct IntermediateStorage {}

impl IntermediateStorage {
    pub fn new_writer(name: String) -> IntermediateReadsWriter {
        let file = name + ".freeze.lz4";

        let mut compress_stream = lz4::EncoderBuilder::new()
            .level(0)
            .checksum(ContentChecksum::NoChecksum)
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Default)
            .build(BufWriter::with_capacity(
                1024 * 1024 * 4,
                File::create(file).unwrap(),
            ))
            .unwrap();

        IntermediateReadsWriter {
            writer: BufWriter::with_capacity(1024 * 1024 * 4, compress_stream),
        }
    }

    pub fn new_reader(name: String) -> IntermediateReadsReader {
        IntermediateReadsReader {
            reader: lz4::Decoder::new(BufReader::with_capacity(
                1024 * 1024 * 4,
                File::open(name).unwrap(),
            ))
            .unwrap(),
        }
    }

    // pub fn from_file(name: String) -> IntermediateStorage {
    //     let is_compressed = name.ends_with(".lz4");
    //     let file = File::open(name).unwrap();
    //
    //     let reader: Box<dyn Read> = if is_compressed {
    //         let decoder = lz4::Decoder::new(file).unwrap();
    //         Box::new(decoder)
    //     } else {
    //         Box::new(file)
    //     };
    //     IntermediateStorage {
    //         reader: UnsafeCell::new(reader),
    //     }
    // }

    // pub fn for_each<F: FnMut(&[u8])>(&self, mut func: F) {
    //     let mut reader_cell = self.reader.uget();
    //     let reader = BufReader::new(reader_cell);
    //     for line in reader.lines() {
    //         func(line.unwrap().as_bytes());
    //     }
    // }
}

// #[cfg(feature = "test")]
mod tests {
    use crate::intermediate_storage::{
        decode_varint, encode_varint, IntermediateStorage, VecReader,
    };
    use byteorder::WriteBytesExt;
    use rand::RngCore;
    use std::io::Cursor;
    use std::iter::FromIterator;
    use std::panic::resume_unwind;

    #[test]
    fn varints() {
        let mut result: Vec<u8> = vec![];

        for i in 0..100000 {
            result.clear();
            encode_varint(&mut result, i);
            let mut cursor = Cursor::new(&result);
            let mut vecreader = VecReader::new(4096, &mut cursor);
            assert_eq!(i, decode_varint(&mut vecreader));
        }
    }

    #[test]
    fn encoding() {
        let mut sequences: Vec<String> = Vec::new();
        let mut rng = rand::thread_rng();

        const LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

        let mut buffer = [0; 1024];
        for i in 0..100000 {
            let size = (rng.next_u32() % 512) as usize + 1;
            rng.fill_bytes(&mut buffer[..size]);
            sequences.push(String::from_iter(
                buffer[..size]
                    .iter()
                    .map(|x| LETTERS[*x as usize % 4] as char),
            ));
        }

        let mut writer = IntermediateStorage::new_writer("/tmp/test-encoding".to_string());
        for read in sequences.iter() {
            writer.add_acgt_read(read.as_bytes());
        }
        writer.finalize();

        let mut reader =
            IntermediateStorage::new_reader("/tmp/test-encoding.freeze.lz4".to_string());

        let mut index = 0;
        reader.for_each(|x| {
            let val = x.to_string();
            // println!("SQ: {} / {}", val, sequences[index]);
            if val != sequences[index].as_str() {
                println!("R: {}", val);
                println!("E: {}", sequences[index]);
                panic!("AA {}", index);
            }
            index += 1;
        });
    }
}
