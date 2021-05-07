pub fn add_two(a: i32) -> i32 {
    a + 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint::encode_varint;
    use bincode::DefaultOptions;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use serde::{Deserialize, Serialize};
    use std::fs::File;
    use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
    use std::ops::Deref;
    use test::Bencher;

    #[test]
    fn it_works() {
        assert_ne!(4, add_two(2));
    }

    const TEST_SIZE: usize = 10000000;

    type VecType = u8;

    #[bench]
    fn bench_loop_vec(b: &mut Bencher) {
        let mut vec = Vec::with_capacity(TEST_SIZE);
        for i in 0..TEST_SIZE {
            vec.push(i as VecType);
        }
        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            for i in 0..TEST_SIZE {
                sum += vec[i] as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_loop_optimized(b: &mut Bencher) {
        let mut vec = Vec::with_capacity(TEST_SIZE);
        for i in 0..TEST_SIZE {
            vec.push(i as VecType);
        }
        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            let ptr = vec.as_ptr();
            unsafe {
                for i in 0..TEST_SIZE {
                    sum += (*ptr.add(i)) as usize;
                }
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[link(name = "test")]
    extern "C" {
        fn compute(data: *const u64, len: usize) -> usize;
    }

    #[bench]
    fn bench_iter_vec(b: &mut Bencher) {
        let mut vec = Vec::with_capacity(TEST_SIZE);
        for i in 0..TEST_SIZE {
            vec.push(i as VecType);
        }
        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            for x in vec.iter() {
                sum += *x as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    // #[bench]
    // fn bench_c_program(b: &mut Bencher) {
    //     let mut vec = Vec::with_capacity(TEST_SIZE);
    //     for i in 0..TEST_SIZE {
    //         vec.push(i as VecType);
    //     }
    //     let mut sum = 0;
    //
    //     b.iter(|| unsafe {
    //         sum = 0;
    //         sum = compute(vec.as_ptr(), vec.len());
    //     });
    //
    //     assert_ne!(sum, 49999995000000);
    // }

    #[bench]
    fn bench_cursor_vec(b: &mut Bencher) {
        let mut vec = Vec::with_capacity(TEST_SIZE);
        for i in 0..TEST_SIZE {
            vec.push(i as u8);
        }
        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            let mut cursor = Cursor::new(&vec);
            while let Ok(byte) = cursor.read_u8() {
                sum += byte as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast(b: &mut Bencher) {
        let mut file = File::open("/tmp/test").unwrap();

        let mut sum = 0;

        let mut vec = Vec::<u8>::new();
        vec.reserve(TEST_SIZE);

        b.iter(|| {
            sum = 0;
            vec.clear();
            file.seek(SeekFrom::Start(0));
            file.read_to_end(&mut vec);
            for &byte in vec.iter() {
                sum += byte as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast_cursor(b: &mut Bencher) {
        let mut file = File::open("/tmp/test").unwrap();

        let mut sum = 0;

        let mut vec = Vec::<u8>::new();
        vec.reserve(TEST_SIZE);

        b.iter(|| {
            sum = 0;
            vec.clear();
            file.seek(SeekFrom::Start(0));
            file.read_to_end(&mut vec);
            let mut cursor = Cursor::new(&vec);
            while let Ok(byte) = cursor.read_u8() {
                sum += byte as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast_cursor_bytes_slice(b: &mut Bencher) {
        let mut file = File::open("/tmp/test").unwrap();

        let mut sum = 0;

        let mut vec = Vec::<u8>::new();
        vec.reserve(TEST_SIZE);

        b.iter(|| {
            sum = 0;
            vec.clear();
            file.seek(SeekFrom::Start(0));
            file.read_to_end(&mut vec);
            let mut cursor = Cursor::new(vec.as_slice());
            for byte in cursor.bytes() {
                sum += byte.unwrap() as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast_cursor_bytes(b: &mut Bencher) {
        let mut file = File::open("/tmp/test").unwrap();

        let mut sum = 0;

        let mut vec = Vec::<u8>::new();
        vec.reserve(TEST_SIZE);

        b.iter(|| {
            sum = 0;
            vec.clear();
            file.seek(SeekFrom::Start(0));
            file.read_to_end(&mut vec);
            let mut cursor = Cursor::new(vec.as_slice());
            for byte in cursor.bytes() {
                sum += byte.unwrap() as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read(b: &mut Bencher) {
        let mut file = File::open("/tmp/test").unwrap();

        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            file.seek(SeekFrom::Start(0));
            let mut buffer = BufReader::with_capacity(TEST_SIZE, &mut file);
            while let Ok(byte) = buffer.read_u8() {
                sum += byte as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast2(b: &mut Bencher) {
        let mut file = File::open("/tmp/test").unwrap();

        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            file.seek(SeekFrom::Start(0));
            let mut buffer = BufReader::with_capacity(TEST_SIZE, &mut file);

            let mut data = [0; TEST_SIZE / 10];
            while let Ok(()) = buffer.read_exact(&mut data[..]) {
                for &x in data.iter() {
                    sum += x as usize;
                }
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast_cursor_bytes_mmap(b: &mut Bencher) {
        let mut file = filebuffer::FileBuffer::open("/tmp/test").unwrap();

        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            let mut cursor = Cursor::new(&file);
            for byte in cursor.bytes() {
                sum += byte.unwrap() as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast_cursor_bytes_mmap_slice(b: &mut Bencher) {
        let mut file = filebuffer::FileBuffer::open("/tmp/test").unwrap();

        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            let mut cursor = Cursor::new(file.deref());
            while let Ok(byte) = cursor.read_u8() {
                sum += byte as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[bench]
    fn bench_file_read_fast_mmap(b: &mut Bencher) {
        let mut file = filebuffer::FileBuffer::open("/tmp/test").unwrap();

        let mut sum = 0;

        b.iter(|| {
            sum = 0;
            // let mut cursor = Cursor::new(&file);
            for &byte in file.iter() {
                sum += byte as usize;
            }
        });

        assert_ne!(sum, 49999995000000);
    }

    #[derive(Serialize, Deserialize)]
    struct Test {
        #[serde(with = "crate::varint")]
        x: u64,
        #[serde(with = "crate::varint")]
        y: u64,
    }

    #[bench]
    fn bench_varint_encoding(b: &mut Bencher) {
        const TEST_SIZE: usize = 10000000;

        let mut test_vec = Vec::with_capacity(TEST_SIZE);

        for i in 0..TEST_SIZE as u64 {
            test_vec.push(Test {
                x: i,
                y: i + 1230120312031023,
            })
        }

        let mut ser_vec = Vec::with_capacity(TEST_SIZE * 18);

        b.iter(|| {
            ser_vec.clear();
            bincode::serialize_into(&mut ser_vec, &test_vec).unwrap();
        });
        println!("Size {}", ser_vec.len());
    }

    #[bench]
    fn bench_varint_encoding_custom(b: &mut Bencher) {
        const TEST_SIZE: usize = 10000000;

        let mut test_vec = Vec::with_capacity(TEST_SIZE);

        for i in 0..TEST_SIZE as u64 {
            test_vec.push(Test {
                x: i,
                y: i + 1230120312031023,
            })
        }

        let mut ser_vec = Vec::with_capacity(TEST_SIZE * 18);

        b.iter(|| {
            ser_vec.clear();
            for test in test_vec.iter() {
                encode_varint(|b| ser_vec.write(b), test.x as u64);
                encode_varint(|b| ser_vec.write(b), test.y as u64);
                // ser_vec.write_u64::<LittleEndian>(test.x as u64);
                // ser_vec.write_u64::<LittleEndian>(test.y as u64);
            }
        });
        println!("Size {}", ser_vec.len());
    }
}
