use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use structopt::StructOpt;
use libdeflate_rs::*;
use libdeflate_rs::decompress_gzip::libdeflate_gzip_decompress;
use libdeflate_rs::streams::deflate_chunked_buffer_input::DeflateChunkedBufferInput;
use libdeflate_rs::streams::deflate_chunked_buffer_output::DeflateChunkedBufferOutput;
use libdeflate_rs::streams::deflate_filebuffer_input::DeflateFileBufferInput;
use libdeflate_rs::streams::deflate_membuffer_output::DeflateMemBufferOutput;

#[derive(StructOpt)]
struct GzipParams {
    input: PathBuf,
    #[structopt(short)]
    simulate: bool
}

fn main() {
    let params: GzipParams = GzipParams::from_args();

    let mut read_file = File::open(&params.input).unwrap();

    let mut in_stream = DeflateChunkedBufferInput::new(|f| {
        read_file.read(f).unwrap_or(0)
    }, 1024 * 512);


    let mut out_stream = if params.simulate {
        DeflateChunkedBufferOutput::new(move |data| { Ok(()) }, 1024 * 512)
    } else {
        let mut write_file = File::create(&params.input.with_extension("")).unwrap();
        DeflateChunkedBufferOutput::new(move |data| {
            write_file.write_all(data).map_err(|_| ())
        }, 1024 * 512)
    };

    let mut decompressor = libdeflate_alloc_decompressor();

    libdeflate_gzip_decompress(&mut decompressor, &mut in_stream, &mut out_stream).unwrap();
}