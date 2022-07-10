use crate::read_spans;
use plotters::backend::SVGBackend;
use plotters::prelude::*;
use plotters::style::full_palette::{LIGHTBLUE, LIGHTGREEN, ORANGE};
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

#[derive(StructOpt)]
pub struct HistogramArgs {
    file: PathBuf,
    #[structopt(short, long)]
    step_size: f64,

    #[structopt(short, long)]
    output: PathBuf,
}

fn draw_chart(
    mut points: Vec<(String, Vec<(f32, f32)>)>,
    last_point: f32,
    path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let root_area = SVGBackend::new(&path, (2048, 768)).into_drawing_area();

    root_area.fill(&RGBColor(100, 100, 100))?;

    let root_area = root_area.titled("Image Title", ("sans-serif", 60))?;

    let (upper, lower) = root_area.split_vertically(512);

    let mut cc = ChartBuilder::on(&upper)
        .margin(5)
        .set_all_label_area_size(50)
        .caption("Time usage", ("sans-serif", 40))
        .build_cartesian_2d(0.0f32..last_point, 0.0f32..32.0)?;

    cc.configure_mesh()
        .x_labels(20)
        .y_labels(10)
        .disable_mesh()
        .x_label_formatter(&|v| format!("{:.1}", v))
        .y_label_formatter(&|v| format!("{:.1}", v))
        .draw()?;

    let colors = [
        &WHITE,
        &RED,
        &GREEN,
        &BLUE,
        &ORANGE,
        &YELLOW,
        &BLACK,
        &LIGHTBLUE,
        &LIGHTGREEN,
    ];

    let mut sum = vec![];
    for i in 0..(points[0].1.len()) {
        let tot = points.iter().map(|p| p.1[i].1).sum();
        sum.push((points[0].1[i].0, tot));
    }

    points.insert(0, ("Sum".to_string(), sum));

    for (idx, (name, line)) in points.into_iter().enumerate() {
        cc.draw_series(LineSeries::new(line, &colors[idx % colors.len()]))?
            .label(name)
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], colors[idx % colors.len()])
            });
    }

    cc.configure_series_labels().border_style(&BLACK).draw()?;

    // To avoid the IO failure being ignored silently, we manually call the present function
    root_area.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
    println!("Result has been saved to {}", path.display());
    Ok(())
}

pub fn histogram(args: HistogramArgs) {
    let mut spans = read_spans(args.file);

    spans.sort_by_key(|s| s.start_time);
    let mut span_index = 0;

    let mut active_spans = Vec::new();

    // let charts = Vec::new();

    let mut time = Duration::ZERO;
    let step = Duration::from_secs_f64(args.step_size);

    let mut current_times = HashMap::new();

    let mut chart_points = HashMap::new();
    for span in &spans {
        chart_points.entry(span.meta.clone()).or_insert(Vec::new());
    }

    while span_index < spans.len() || active_spans.len() > 0 {
        let last_time = time;
        time += step;
        println!("Time: {:?}", time);

        while span_index < spans.len() && spans[span_index].start_time <= time {
            active_spans.push(span_index);
            span_index += 1;
        }

        active_spans.retain(|a| spans[*a].end_time > last_time);

        current_times.clear();

        for span in active_spans.iter() {
            let span = &spans[*span];
            let entry = current_times.entry(span.meta.clone()).or_insert(0.0);

            assert!(span.start_time < span.end_time);
            assert!(last_time < time);

            let min_time = max(last_time, span.start_time);
            let max_time = min(time, span.end_time);

            if min_time > max_time {
                println!(
                    "{:?} - {:?} // {:?} - {:?}",
                    last_time, time, span.start_time, span.end_time
                );
            }

            let ratio = (max_time - min_time).as_secs_f64() / span.executon_time.as_secs_f64();
            *entry += span.user_time.as_secs_f64() * ratio;
        }

        for (meta, chart) in chart_points.iter_mut() {
            match current_times.get(meta) {
                None => chart.push((time.as_secs_f32(), 0.0)),
                Some(wtime) => chart.push((time.as_secs_f32(), *wtime as f32)),
            }
        }
    }

    draw_chart(
        {
            let mut points: Vec<_> = chart_points
                .into_iter()
                .map(|(data, points)| (data.name, points))
                .collect();
            points.sort_by_key(|k| k.0.clone());
            points
        },
        time.as_secs_f32(),
        args.output,
    );
}
