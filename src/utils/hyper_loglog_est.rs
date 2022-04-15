use std::cmp::max;

#[derive(Copy, Clone)]
pub struct HyperLogLogEstimator<const BUCKETS_COUNT: usize> {
    mso: [u8; BUCKETS_COUNT],
    counter: u64,
}

impl<const BUCKETS_COUNT: usize> HyperLogLogEstimator<BUCKETS_COUNT> {
    pub const NEW: Self = Self::new();

    pub const fn new() -> Self {
        Self {
            mso: [0; BUCKETS_COUNT],
            counter: 0,
        }
    }

    #[inline(always)]
    pub fn add_element(&mut self, el: u64) {
        self.mso[(el as usize) % BUCKETS_COUNT] = max(
            self.mso[(el as usize) % BUCKETS_COUNT],
            el.leading_zeros() as u8,
        );
        self.counter += 1;
    }

    pub fn estimate_batch_elements(
        elements: &Vec<Vec<Self>>,
        correction_factor: f64,
        sample_ratio: f64,
    ) -> Vec<(u64, u64)> {
        let count = elements[0].len();
        let mut result = vec![(0, 0); count];

        for b in 0..count {
            let mut hll_gmean: f64 = 0.0;
            let el_count: u64 = elements.iter().map(|v| v[b].counter).sum();
            for hll_bucket in 0..BUCKETS_COUNT {
                let mut bucket_val = 0;

                for v in elements {
                    let hll = &v[b];
                    bucket_val = max(bucket_val, hll.mso[hll_bucket]);
                }
                bucket_val += 1;
                let bucket_mean_val = 2u128.pow(bucket_val as u32);
                hll_gmean += 1.0 / (bucket_mean_val as f64);
                println!("BVAL: {} / {}", bucket_val, bucket_mean_val);
            }

            let size_est = (BUCKETS_COUNT as f64) / hll_gmean * correction_factor;

            println!(
                "Temp size est for bucket{}: {} // {}",
                b, size_est, hll_gmean
            );

            // The avg number of times an element is repeated in the dataset
            let avg_repetition = (el_count as f64 / size_est).max(1.0);

            // The probability that an element is sampled in the reduced dataset
            let prob_sampling = 1.0 - ((1.0 - sample_ratio).powf(avg_repetition));

            // The total size estimation is given by the size of the sampled part multiplied by
            // the probability that a single element is sampled
            let total_size_est = size_est / prob_sampling;

            let tot_elements_est = (el_count as f64 / sample_ratio) as u64;

            result[b] = (total_size_est as u64, tot_elements_est);
        }
        result
    }
}
