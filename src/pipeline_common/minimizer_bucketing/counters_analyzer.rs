use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;

#[derive(Serialize, Deserialize)]
pub struct BucketCounter {
    count: u64,
    is_outlier: bool,
}

#[derive(Serialize, Deserialize)]
pub struct CountersAnalyzer {
    counters: Vec<Vec<BucketCounter>>,
    median: u64,
}

impl CountersAnalyzer {
    pub fn new(counters: Vec<Vec<AtomicU64>>) -> Self {
        let mut sorted_counters: Vec<(u64, usize, usize)> = Vec::new();

        let mut counters: Vec<Vec<BucketCounter>> = counters
            .into_iter()
            .enumerate()
            .map(|(bucket, vec)| {
                vec.into_iter()
                    .enumerate()
                    .map(|(second_bucket, mut a)| {
                        let count = *a.get_mut();
                        if count != 0 {
                            sorted_counters.push((count, bucket, second_bucket));
                        }
                        BucketCounter {
                            count,
                            is_outlier: false,
                        }
                    })
                    .collect()
            })
            .collect();
        sorted_counters.sort_unstable_by(|a, b| b.cmp(a));

        let median = if sorted_counters.len() > 0 {
            sorted_counters[sorted_counters.len() / 2].0
        } else {
            0
        };

        for (count, bucket, second_bucket) in sorted_counters {
            if count > median * 50 {
                counters[bucket][second_bucket].is_outlier = true;
            }
        }

        Self { counters, median }
    }

    pub fn print_debug(&self) {
        println!("************** BUCKETS DEBUG: **************");
        for (i, cnt_bucket) in self.counters.iter().enumerate() {
            let mut buffer = String::new();
            for cnt_sub_bucket in cnt_bucket.iter() {
                buffer.push_str(&format!(
                    "{}{} ",
                    cnt_sub_bucket.count,
                    if cnt_sub_bucket.is_outlier { "*" } else { "" },
                ));
            }
            println!("{} SIZES: {}", i, buffer);
        }
        println!("Sub-bucket median: {}", self.median);
    }
}
