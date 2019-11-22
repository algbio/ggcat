#[macro_use]
extern crate quickcheck;

use quickcheck::{Arbitrary, Gen};
use rand::Rng;

use nthash::{nthash, NtHashIterator};

#[test]
fn oracle_cmp() {
    assert_eq!(nthash(b"TGCAG", 5), vec![0x0baf_a672_8fc6_dabf]);
    assert_eq!(nthash(b"ACGTC", 5), vec![0x4802_02d5_4e8e_becd]);
    assert_eq!(
        nthash(b"ACGTCGTCAGTCGATGCAGT", 5),
        vec![
            0x4802_02d5_4e8e_becd,
            0xa997_bdc6_28b4_c98e,
            0x8c6d_7ab2_0911_b216,
            0x5ddc_b093_90aa_feef,
            0x25ff_3ac4_bc92_382f,
            0x9bda_9a5c_3560_3946,
            0x82d4_49e5_b371_0ccd,
            0x1e92_6ce7_780a_b812,
            0x2f6e_d7b2_2647_3a86,
            0xd186_5edf_eb55_b037,
            0x38b5_7494_189a_8afe,
            0x1b23_5fc5_ecac_f386,
            0x1eab_5d82_920f_da13,
            0x02c8_d157_4673_bdcd,
            0x0baf_a672_8fc6_dabf,
            0x14a3_3bb9_2827_7bed,
        ]
    );
    assert_eq!(
        nthash(b"ACGTCGANNGTA", 5),
        vec![
            0x4802_02d5_4e8e_becd,
            0xa997_bdc6_28b4_c98e,
            0xd186_5edf_eb55_b037,
            0xe015_9f5a_89f5_9b7b,
            0xe640_9a0f_689e_64e4,
            0x7a05_4a39_df66_1723,
            0x6d74_fee7_0283_5974,
            0xb744_44dd_9a94_cbf3,
        ]
    );
}

#[test]
fn iter_cmp() {
    let ksize = 5;
    for s in &vec!["TGCAG", "ACGTC", "ACGTCGTCAGTCGATGCAGT", "ACGTCGANNGTA"] {
        let seq = s.as_bytes();
        let iter = NtHashIterator::new(seq, ksize).unwrap();
        println!("{:?}", s);
        assert_eq!(nthash(seq, ksize), iter.collect::<Vec<u64>>());
    }
}

#[test]
fn out_of_range_ksize_wont_panic() {
    let ksize: usize = 10;
    let sequences = "TGCAG";
    let err = NtHashIterator::new(&sequences.as_bytes(), ksize).unwrap_err();
    assert_eq!(
        err.to_string(),
        "K size 10 is out of range for the given sequence size 5"
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
#[ignore]
fn big_ksize_wont_panic() {
    let ksize: usize = (u64::from(u32::max_value()) + 1) as usize;
    let repetitions: usize = ((f64::from(u32::max_value()) + 1.0) / 5.0).ceil() as usize;
    let sequences = "TGCAG".repeat(repetitions);
    let err = NtHashIterator::new(&sequences.as_bytes(), ksize).unwrap_err();
    assert_eq!(
        err.to_string(),
        "K size 4294967296 cannot exceed the size of a u32 4294967295"
    );
}

#[derive(Clone, Debug)]
struct Seq(String);

impl Arbitrary for Seq {
    fn arbitrary<G: Gen>(g: &mut G) -> Seq {
        let choices = ['A', 'C', 'G', 'T', 'N'];
        let size = {
            let s = g.size();
            g.gen_range(0, s)
        };
        let mut s = String::with_capacity(size);
        for _ in 0..size {
            s.push(*g.choose(&choices).expect("Not a valid nucleotide"));
        }
        Seq { 0: s }
    }
}

quickcheck! {
  fn oracle_quickcheck(s: Seq) -> bool {
     let seq = s.0.as_bytes();
     (1..(seq.len())).all(|ksize| {
       let iter = NtHashIterator::new(seq, ksize).unwrap();
       nthash(seq, ksize) == iter.collect::<Vec<u64>>()
     })
  }
}
