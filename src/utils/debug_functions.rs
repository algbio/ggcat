use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::types::BucketIndexType;

fn assert_reads<H: HashFunctionFactory>(read: &[u8], bucket: BucketIndexType) {
    // Test ***************************
    let K: usize = 32;

    if read.len() == 33 {
        let hashes = H::new(&read[0..K], M);
        let minimizer = hashes
            .iter()
            .min_by_key(|read| H::get_minimizer(read.to_unextendable()))
            .unwrap();

        let hashes1 = H::new(&read[1..K + 1], M);
        let minimizer1 = hashes1
            .iter()
            .min_by_key(|read| H::get_minimizer(read.to_unextendable()))
            .unwrap();

        assert!(
            H::get_bucket(minimizer.to_unextendable()) % 512 == bucket
                || H::get_bucket(minimizer1.to_unextendable()) % 512 == bucket
        );
        println!(
            "{} / {}",
            minimizer.to_unextendable(),
            minimizer1.to_unextendable()
        );
    }

    if read.len() < 34 {
        return;
    }

    let x = &read[1..read.len() - 1];

    const M: usize = 12;

    let hashes = H::new(&x[0..K], M);
    let minimizer = hashes
        .iter()
        .min_by_key(|x| H::get_minimizer(x.to_unextendable()))
        .unwrap();

    assert_eq!(H::get_bucket(minimizer.to_unextendable()) % 512, bucket);

    if x.len() > K {
        let hashes2 = H::new(&x[..], M);
        let minimizer2 = hashes2
            .iter()
            .min_by_key(|x| H::get_minimizer(x.to_unextendable()))
            .unwrap();

        if minimizer.to_unextendable() != minimizer2.to_unextendable() {
            let vec: Vec<_> = H::new(&x[..], M)
                .iter()
                .map(|x| H::get_minimizer(x.to_unextendable()))
                .collect();

            println!("Kmers {}", std::str::from_utf8(x).unwrap());
            println!("Hashes {:?}", vec);
            panic!(
                "AA {} {}",
                minimizer.to_unextendable(),
                minimizer2.to_unextendable()
            );
        }
    }
    // Test ***************************
}
