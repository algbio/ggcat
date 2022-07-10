use crate::read_spans;
use std::cmp::max;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

#[derive(StructOpt)]
pub struct SummaryArgs {
    file: PathBuf,
}

pub fn summary(args: SummaryArgs) {
    let SummaryArgs { file } = args;

    let mut spans = read_spans(file);

    spans.sort_by_key(|s| s.meta.clone());

    let formatter = human_format::Formatter::new();

    for span in spans.as_slice().group_by(|a, b| a.meta == b.meta) {
        let meta = &span[0].meta;

        let mut ratios_cap = Vec::new();
        let mut ratios_real = Vec::new();

        let (
            elapsed_time,
            user_time,
            enter_count,
            counters,
            max_memory,
            max_own_memory,
            curr_memory,
            curr_items,
        ) = span.iter().fold(
            (
                Duration::ZERO,
                Duration::ZERO,
                0,
                vec![0; span[0].counters.len()],
                0,
                0,
                0,
                0,
            ),
            |mut accum, el| {
                accum.0 += el.executon_time;
                accum.1 += el.user_time;
                accum.2 += el.enter_count;
                for i in 0..el.counters.len() {
                    accum.3[i] += el.counters[i];
                }

                accum.4 = max(accum.4, el.max_memory);
                accum.5 = max(accum.5, el.max_own_memory);

                accum.6 = el.current_tot_memory;
                accum.7 = el.current_tot_items;

                if el.parameters.len() > 0 {
                    const L2_SIZE: usize = 1024 * 1024 * 4;
                    if true {
                        // el.parameters[1].1 > 458752 {
                        // (L2_SIZE / 32 / 2) as i128 {
                        // println!(
                        //     "CAP: {} / SZ: {}",
                        //     el.parameters[0].1, el.parameters[1].1
                        // );
                        ratios_cap.push(el.parameters[0].1 as u64); // / el.parameters[1].1);
                        ratios_real.push(el.parameters[1].1 as u64); // / el.parameters[1].1);
                    }
                }

                accum
            },
        );

        let map_ratio = if ratios_cap.len() > 0 {
            let mxsz = ratios_cap.iter().max().unwrap();
            let mc: f64 = ratios_cap.iter().sum::<u64>() as f64 / ratios_cap.len() as f64;
            let mr: f64 = ratios_real.iter().sum::<u64>() as f64 / ratios_real.len() as f64;
            format!(
                "MAP Ratio: {:.0} / {:.0} MAX: {}",
                mr,
                mc,
                formatter.format(*mxsz as f64)
            )
        } else {
            String::new()
        };

        println!(
            "Span [{}{}] {}\n\t\t=> Count: {}/{} Time: {:.2?} User time: {:.2?} | Counters: {:?} Ratios: {:?} {}\n\t\t MMEM: {} MOWN: {}\n\t\t CMEM: {} CITEMS: {}",
            meta.file.clone().unwrap_or(String::new()),
            meta.line
                .as_ref()
                .map(|l| format!(":{}", l))
                .unwrap_or(String::new()),
            &meta.name,
            span.len(),
            enter_count,
            elapsed_time,
            user_time,
            counters.iter().map(|x| formatter.format(*x as f64)).collect::<Vec<_>>(),
            counters.iter().map(|x| formatter.format(*x as f64 / user_time.as_secs_f64())).collect::<Vec<_>>(),
            map_ratio,
            formatter.format(max_memory as f64),
            formatter.format(max_own_memory as f64),
            formatter.format(curr_memory as f64),
            formatter.format(curr_items as f64),
        )
    }
}
