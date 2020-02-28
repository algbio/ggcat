use crate::rolling_kmers_iterator::RollingKmerImpl;

pub struct RollingQualityCheck {
    scores_index: [u32; 256],
    prob_log: u32
}

impl RollingQualityCheck {
    pub fn new() -> RollingQualityCheck {
        let mut scores_index: [u32; 256] = [0; 256];

        let min_score = (b'!' as usize);

        for i in min_score..256 {
            let qual_idx = i - min_score;
            let err_prob = (10.0 as f64).powf(-(qual_idx as f64) / 10.0);
            let corr_prob = 1.0 - err_prob;
            let logval = (-corr_prob.log10() * 1048576.0) as u32;
            scores_index[i] = logval;
        }

        RollingQualityCheck {
            scores_index,
            prob_log: 0
        }
    }
}

impl RollingKmerImpl<u32> for RollingQualityCheck {

    #[inline(always)]
    fn clear(&mut self, ksize: usize) {
        self.prob_log = 0
    }

    #[inline(always)]
    fn init(&mut self, index: usize, base: u8) {
        //1.0 - (10.0 as f64).powf(-(0.1 * ((*qb as f64) - 33.0)));
        self.prob_log += unsafe { *self.scores_index.get_unchecked(base as usize) };
    }

    #[inline(always)]
    fn iter(&mut self, index: usize, out_base: u8, in_base: u8) -> u32 {
        self.prob_log += unsafe { *self.scores_index.get_unchecked(in_base as usize) };
        let result = self.prob_log;
        self.prob_log -= unsafe { *self.scores_index.get_unchecked(out_base as usize) };
        result
    }
}