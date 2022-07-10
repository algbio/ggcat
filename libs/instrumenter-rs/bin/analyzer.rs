#![feature(slice_group_by)]

mod histogram;
mod summary;

use crate::histogram::HistogramArgs;
use crate::summary::{summary, SummaryArgs};
use histogram::histogram;
use instrumenter::instr_span::InstrSpan;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;
use structopt::StructOpt;

#[derive(StructOpt)]
enum Args {
    Summary(SummaryArgs),
    Histogram(HistogramArgs),
}

fn read_spans(file: impl AsRef<Path>) -> Vec<InstrSpan> {
    let mut spans: Vec<InstrSpan> = vec![];
    let file = BufReader::new(File::open(file).unwrap());
    for line in file.lines() {
        spans.push(serde_json::from_str(&line.unwrap()).unwrap());
    }
    spans
}

pub fn main() {
    let args: Args = Args::from_args();

    match args {
        Args::Summary(args) => {
            summary(args);
        }
        Args::Histogram(args) => {
            histogram(args);
        }
    }
}
