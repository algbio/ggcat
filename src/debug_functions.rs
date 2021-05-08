use nthash::NtHashIterator;

fn assert_reads(read: &[u8], bucket: u64) {
    // Test ***************************
    let K: usize = 32;

    if read.len() == 33 {
        let hashes = NtHashIterator::new(&read[0..K], M).unwrap();
        let minimizer = hashes.iter().min_by_key(|read| *read >> 32).unwrap();

        let hashes1 = NtHashIterator::new(&read[1..K + 1], M).unwrap();
        let minimizer1 = hashes1.iter().min_by_key(|read| *read >> 32).unwrap();

        assert!(minimizer % 512 == bucket || minimizer1 % 512 == bucket);
        println!("{} / {}", minimizer, minimizer1);
    }

    if read.len() < 34 {
        return;
    }

    let x = &read[1..read.len() - 1];

    const M: usize = 12;

    let hashes = NtHashIterator::new(&x[0..K], M).unwrap();
    let minimizer = hashes.iter().min_by_key(|x| *x >> 32).unwrap();

    assert!(minimizer % 512 == bucket);

    if x.len() > K {
        let hashes2 = NtHashIterator::new(&x[..], M).unwrap();
        let minimizer2 = hashes2.iter().min_by_key(|x| *x >> 32).unwrap();

        if minimizer != minimizer2 {
            let vec: Vec<_> = NtHashIterator::new(&x[..], M)
                .unwrap()
                .iter()
                .map(|x| x >> 32)
                .collect();

            println!("Kmers {}", std::str::from_utf8(x).unwrap());
            println!("Hashes {:?}", vec);
            panic!("AA {} {}", minimizer, minimizer2);
        }
    }
    // Test ***************************
}
