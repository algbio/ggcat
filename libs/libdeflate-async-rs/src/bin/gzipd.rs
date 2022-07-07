#![deny(unused_must_use)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]

use libdeflate_async_rs::decompress_gzip::libdeflate_gzip_decompress;
use libdeflate_async_rs::streams::deflate_async_chunked_buffer_input::DeflateAsyncChunkedBufferInput;
use libdeflate_async_rs::streams::deflate_async_chunked_buffer_output::{
    DeflateAsyncChunkedBufferOutput, OutputFnHelper,
};
use libdeflate_async_rs::{decompress_file_buffered, libdeflate_alloc_decompressor};
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Output;
use std::sync::{Arc, Mutex, RwLock};
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;

#[derive(StructOpt)]
struct GzipParams {
    input: PathBuf,
    #[structopt(short)]
    simulate: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let params: GzipParams = GzipParams::from_args();

    let read_file = tokio::fs::File::open(&params.input).await.unwrap();

    let mut decompressor = libdeflate_alloc_decompressor();
    if params.simulate {
        let mut in_stream = DeflateAsyncChunkedBufferInput::new(read_file, 1024 * 512);

        #[inline(always)]
        async fn do_nothing(_: &mut (), _: &[u8]) -> Result<(), ()> {
            Ok(())
        }

        let mut out_stream = DeflateAsyncChunkedBufferOutput::new(do_nothing, (), 1024 * 512);
        libdeflate_gzip_decompress(&mut decompressor, &mut in_stream, &mut out_stream)
            .await
            .unwrap();
    } else {
        let mut write_file = std::fs::File::create(&params.input.with_extension(""))
            // .await
            .unwrap();

        #[inline(always)]
        async fn write_to_file<'a>(file: &'a mut std::fs::File, data: &'a [u8]) -> Result<(), ()> {
            // /*async*/ {
            file.write(data).unwrap();
            Ok(())
            // }
        }

        decompress_file_buffered(read_file, write_to_file, write_file, 1024 * 512)
            .await
            .unwrap();
    }
}
