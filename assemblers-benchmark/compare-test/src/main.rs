use rayon::prelude::*;
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::io::{BufRead, Write};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
enum CliCommands {
    Uniform(CanonicalArgs),
    
}

#[derive(StructOpt, Debug)]
struct CanonicalArgs {
    input: PathBuf,
    output: Option<PathBuf>,
    #[structopt(long, short)]
    k: usize,
    // /// Only print stats
    // #[structopt(long, short)]
    // count: usize,
}

fn read_lines<P>(filename: P) -> io::Result<Box<dyn Iterator<Item = io::Result<String>>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename.as_ref())?;

    if filename.as_ref().extension() == Some(OsStr::new("lz4")) {
        Ok(Box::new(
            io::BufReader::new(lz4::Decoder::new(file).unwrap()).lines(),
        ))
    } else {
        Ok(Box::new(io::BufReader::new(file).lines()))
    }
}

fn write_lines<'a, P>(filename: P, lines: impl Iterator<Item = &'a str>)
where
    P: AsRef<Path>,
{
    let file = File::create(filename).unwrap();
    let mut buffer = io::BufWriter::new(file);

    for line in lines {
        buffer.write_all(line.as_bytes()).unwrap();
        buffer.write_all(b"\n").unwrap();
    }
}

fn rcb(base: u8) -> u8 {
    match base {
        b'A' => b'T',
        b'C' => b'G',
        b'G' => b'C',
        b'T' => b'A',
        _ => panic!("Unknown base {}!", base as char),
    }
}

fn reverse_complement(s: &mut [u8]) {
    s.reverse();
    s.iter_mut().for_each(|x| *x = rcb(*x));
}

fn process_string(el: &mut [u8]) {
    let a = el.iter();
    let b = el.iter().rev();

    let should_swap = a
        .zip(b)
        .filter(|(a, b)| **a != rcb(**b))
        .next()
        .map(|(a, b)| *a > rcb(*b))
        .unwrap_or(false);

    if should_swap {
        reverse_complement(el);
    }
}

fn main() {
    let args: CanonicalArgs = CanonicalArgs::from_args();

    let total_kmers = AtomicU64::new(0);

    let mut sequences: Vec<_> = read_lines(&args.input)
        .unwrap()
        .map(|l| l.unwrap())
        .filter(|l| !l.starts_with(">"))
        .collect();

    sequences.par_iter_mut().for_each(|el: &mut String| {
        let str_bytes = el.as_bytes();

        total_kmers.fetch_add((str_bytes.len() - args.k) as u64, Ordering::Relaxed);

        // Circular normalization
        if &str_bytes[..(args.k - 1)] == &str_bytes[(str_bytes.len() - (args.k - 1))..] {
            let mut canonical = el.clone();
            process_string(unsafe { canonical.as_bytes_mut() });

            let mut deque = VecDeque::from_iter(str_bytes.iter().map(|x| *x));

            if deque.len() < args.k {
                println!("Sequence: {} has length less than k, aborting!", el);
                exit(1);
            }

            for _ in 0..str_bytes.len() {
                // Roll the sequence by 1 left
                let ins_el = deque[deque.len() - args.k];
                deque.push_front(ins_el);
                deque.pop_back();

                let mut candidate = std::str::from_utf8(deque.make_contiguous())
                    .unwrap()
                    .to_string();

                process_string(unsafe { candidate.as_bytes_mut() });
                canonical = canonical.min(candidate);
            }
            *el = canonical;
        }
        process_string(unsafe { el.as_bytes_mut() });
    });

    sequences.par_sort();

    let out_file = args.output.unwrap_or(args.input);

    sequences.push(String::new());
    write_lines(out_file, sequences.iter().map(|s| s.as_str()));

    println!(
        "Written {} sequences with {} kmers!",
        sequences.len(),
        total_kmers.load(Ordering::Relaxed)
    );
}
