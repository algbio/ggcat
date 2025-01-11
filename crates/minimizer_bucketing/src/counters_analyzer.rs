use config::BucketIndexType;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BucketCounter {
    pub count: u64,
}

#[derive(Serialize, Deserialize)]
pub struct CountersAnalyzer {
    counters: Vec<Vec<BucketCounter>>,
    compaction_offsets: Vec<i64>,
    median: u64,
}

impl CountersAnalyzer {
    pub fn new(counters: Vec<Vec<AtomicU64>>, offsets: Vec<AtomicI64>) -> Self {
        let mut sorted_counters: Vec<(u64, usize, usize)> = Vec::new();

        let counters: Vec<Vec<BucketCounter>> = counters
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
                        BucketCounter { count }
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

        let compaction_offsets = offsets.into_iter().map(AtomicI64::into_inner).collect();

        Self {
            counters,
            median,
            compaction_offsets,
        }
    }

    pub fn get_counters_for_bucket(&self, bucket: BucketIndexType) -> &Vec<BucketCounter> {
        &self.counters[bucket as usize]
    }

    pub fn get_compaction_offset(&self, bucket: BucketIndexType) -> i64 {
        self.compaction_offsets[bucket as usize]
    }

    pub fn print_debug(&self) {
        ggcat_logging::info!("************** BUCKETS DEBUG: **************");
        // for (i, cnt_bucket) in self.counters.iter().enumerate() {
        //     let mut buffer = String::new();
        //     for cnt_sub_bucket in cnt_bucket.iter() {
        //         buffer.push_str(&format!(
        //             "{}{} ",
        //             cnt_sub_bucket.count,
        //             if cnt_sub_bucket.is_outlier { "*" } else { "" },
        //         ));
        //     }
        //     ggcat_logging::info!("{} SIZES: {}", i, buffer);
        // }
        ggcat_logging::info!("Sub-bucket median: {}", self.median);
        ggcat_logging::info!(
            "Sub-bucket maximum: {}",
            self.counters
                .iter()
                .map(|x| x.iter().map(|c| c.count).max().unwrap_or(0))
                .max()
                .unwrap_or(0)
        );
    }

    pub fn load_from_file(path: impl AsRef<Path>, remove: bool) -> Self {
        let file = BufReader::new(File::open(&path).unwrap());
        let rval: CountersAnalyzer = bincode::deserialize_from(file).unwrap();

        // rval.counters.iter_mut().enumerate().for_each(|(bn, x)| {
        //     x.iter_mut().enumerate().for_each(|(sbn, y)| {
        //         if y.is_outlier {
        //             ggcat_logging::info!("Found outlier: vec{}.{}", bn, sbn);
        //             // y.is_outlier = false
        //         }
        //     })
        // });

        // rval.print_debug();

        if remove {
            let _ = std::fs::remove_file(path);
        }
        rval
    }

    pub fn serialize_to_file(&self, path: impl AsRef<Path>) {
        let file = BufWriter::new(
            File::create(path.as_ref())
                .expect(&format!("Cannot open file {}", path.as_ref().display())),
        );
        bincode::serialize_into(file, self).unwrap();
    }
}
