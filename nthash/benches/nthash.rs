#[macro_use]
extern crate criterion;

use criterion::{Bencher, Criterion, Fun};
use rand::distributions::{Distribution, Uniform};

use nthash::{nthash, NtHashIterator};

fn nthash_bench(c: &mut Criterion) {
    let range = Uniform::from(0..4);
    let mut rng = rand::thread_rng();
    let seq = (0..10000)
        .map(|_| match range.sample(&mut rng) {
            0 => 'A',
            1 => 'C',
            2 => 'G',
            3 => 'T',
            _ => 'N',
        })
        .collect::<String>();

    let nthash_it = Fun::new("nthash_iterator", |b: &mut Bencher, i: &String| {
        b.iter(|| {
            let iter = NtHashIterator::new(i.as_bytes(), 5).unwrap();
            //  iter.for_each(drop);
            let _res = iter.collect::<Vec<u64>>();
        })
    });

    let nthash_simple = Fun::new("nthash_simple", |b: &mut Bencher, i: &String| {
        b.iter(|| {
            nthash(i.as_bytes(), 5);
        })
    });

    let functions = vec![nthash_it, nthash_simple];
    c.bench_functions("nthash", functions, seq);
}

criterion_group!(benches, nthash_bench);
criterion_main!(benches);
