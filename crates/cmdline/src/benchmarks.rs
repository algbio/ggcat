#[cfg(test)]
mod tests {
    use byteorder::ReadBytesExt;
    use hashes::fw_nthash::ForwardNtHashIterator;
    use hashes::HashFunction;
    use io::compressed_read::CompressedRead;
    use std::io::Cursor;
    use test::Bencher;
    use utils::Utils;

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

    #[test]
    fn test_nthash() {
        // TGGATGGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATGGG ==> TATGTATATATATATATATATATATATATATATATATATATATATATATATATATATATGTGT
        // let str0 = b"GNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNG";
        // let str1 = b"TNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNT";

        // let str0 = b"GATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATTT";
        // let str1 = b"TATATATATATATATATATATATATATATATATATATATATATATATATATATATATATTG";

        // let str0 = b"NGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNN";
        // let str1 = b"NTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNN";

        let str0 = b"TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let str1 = b"TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT";

        let hash = ForwardNtHashIterator::new(&str0[..], 15).unwrap();
        println!("{:?}", hash.iter().collect::<Vec<_>>());
        let hash1 = ForwardNtHashIterator::new(&str1[..], 15).unwrap();
        println!("{:?}", hash1.iter().collect::<Vec<_>>())
    }

    #[test]
    fn test_seqhash() {
        // TGGATGGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATGGG ==> TATGTATATATATATATATATATATATATATATATATATATATATATATATATATATATGTGT
        // let str0 = b"GNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNG";
        // let str1 = b"TNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNT";

        // let str0 = b"GATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATAGATTT";
        // let str1 = b"TATATATATATATATATATATATATATATATATATATATATATATATATATATATATATTG";

        // let str0 = b"NGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNNGNNN";
        // let str1 = b"NTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNNTNNN";

        // let str0 = b"TTTCTTTTTTTTTTTTTTTAATTTTGAGACAA";
        // let str1 = b"TTTCTTTTTTTTTTTTTTTAATTTTGCCCCAATTTCTTTTTTTTTTTTTTTAATTTTGAGACAA";
        //
        // let hash = SeqHashIterator::new(&str0[..], 32).unwrap();
        // println!("{:?}", hash.iter().collect::<Vec<_>>());
        // let hash1 = SeqHashIterator::new(&str1[..], 32).unwrap();
        // println!("{:?}", hash1.iter().collect::<Vec<_>>());

        let a: Vec<_> = (&b"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC"[..])
            .iter()
            .map(|x| Utils::compress_base(*x))
            .collect();
        let _b: Vec<_> = (&b"CACCCACCCATTCACCTATCCATCCATCCAACCGTCCATCTGTTCATTC"[..])
            .iter()
            .map(|x| Utils::compress_base(*x))
            .collect();

        let ha: Vec<_> = ForwardNtHashIterator::new(&a[..], 15)
            .unwrap()
            .iter()
            .collect();
        // let hb: Vec<_> = SeqHashIterator::new(&b[..], 31).unwrap().iter().collect();

        // let hc = SeqHashFactory::manual_roll_forward(ha, 32, a[0], *b.last().unwrap());

        println!("X {:?}", ha);
        // println!("Y {:?}", hb);
        // println!("{:b}", hc);
    }

    #[test]
    fn test_comprread() {
        let vec = vec![0x23, 0x47, 0xFA, 0x7D, 0x59, 0xFF, 0xA1, 0x84];

        let read1 = CompressedRead::from_compressed_reads(&vec[..], 0, 32).sub_slice(1..32);
        let read12 = CompressedRead::from_compressed_reads(&vec[..], 1, 31).sub_slice(0..31);
        println!("{}", read1.to_string());
        println!("{}", read12.to_string());
    }
}
