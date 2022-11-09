use bstr::ByteSlice;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use streaming_libdeflate_rs::decompress_file_buffered;
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, SumMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub struct LinesReader {
    buffer: Vec<u8>,
}

static COUNTER_THREADS_BUSY_READING: AtomicCounter<SumMode> =
    declare_counter_i64!("line_reading_threads", SumMode, false);

static COUNTER_THREADS_PROCESSING_READS: AtomicCounter<SumMode> =
    declare_counter_i64!("line_processing_threads", SumMode, false);

static COUNTER_THREADS_READ_BYTES: AtomicCounter<SumMode> =
    declare_counter_i64!("line_read_bytes", SumMode, false);
static COUNTER_THREADS_READ_BYTES_AVG: AtomicCounter<AvgMode> =
    declare_avg_counter_i64!("line_read_bytes_avg", false);

impl LinesReader {
    pub(crate) fn new() -> Self {
        Self {
            buffer: vec![0; DEFAULT_OUTPUT_BUFFER_SIZE],
        }
    }

    #[inline(always)]
    fn read_stream_buffered(
        &mut self,
        mut stream: impl Read,
        mut callback: impl FnMut(&[u8]),
    ) -> Result<(), ()> {
        COUNTER_THREADS_BUSY_READING.inc();

        while let Ok(count) = stream.read(self.buffer.as_mut_slice()) {
            COUNTER_THREADS_READ_BYTES.inc_by(count as i64);
            COUNTER_THREADS_READ_BYTES_AVG.add_value(count as i64);
            COUNTER_THREADS_BUSY_READING.sub(1);
            if count == 0 {
                COUNTER_THREADS_PROCESSING_READS.inc();
                callback(&[]);
                COUNTER_THREADS_PROCESSING_READS.sub(1);
                return Ok(());
            }
            COUNTER_THREADS_PROCESSING_READS.inc();
            callback(&self.buffer[0..count]);
            COUNTER_THREADS_PROCESSING_READS.sub(1);
            COUNTER_THREADS_BUSY_READING.inc();
        }
        Err(())
    }

    fn read_binary_file(
        &mut self,
        path: impl AsRef<Path>,
        mut callback: impl FnMut(&[u8]),
        remove: bool,
    ) {
        if path.as_ref().extension().filter(|x| *x == "gz").is_some() {
            if let Err(_err) = decompress_file_buffered(
                &path,
                |data| {
                    callback(data);
                    Ok(())
                },
                DEFAULT_OUTPUT_BUFFER_SIZE,
            ) {
                println!(
                    "WARNING: Error while reading file {}",
                    path.as_ref().display()
                );
            }
            callback(&[]);
        } else if path.as_ref().extension().filter(|x| *x == "lz4").is_some() {
            let file = lz4::Decoder::new(
                File::open(&path).expect(&format!("Cannot open file {}", path.as_ref().display())),
            )
            .unwrap();
            self.read_stream_buffered(file, callback)
                .unwrap_or_else(|_| {
                    println!(
                        "WARNING: Error while reading file {}",
                        path.as_ref().display()
                    );
                });
        } else {
            let file =
                File::open(&path).expect(&format!("Cannot open file {}", path.as_ref().display()));
            self.read_stream_buffered(file, callback)
                .unwrap_or_else(|_| {
                    println!(
                        "WARNING: Error while reading file {}",
                        path.as_ref().display()
                    );
                });
        }

        if remove {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[inline]
    fn split_line<'a, 'b>(buffer: &'b mut &'a [u8]) -> (bool, &'a [u8]) {
        match buffer.find_byte(b'\n') {
            None => {
                // No newline
                let buf_len = if buffer.len() > 0 && buffer[buffer.len() - 1] == b'\r' {
                    buffer.len() - 1
                } else {
                    buffer.len()
                };

                let out_buffer = &buffer[..buf_len];

                *buffer = &[];
                (false, out_buffer)
            }
            Some(pos) => {
                let mut bpos = pos;
                if bpos != 0 && buffer[bpos - 1] == b'\r' {
                    bpos -= 1;
                }
                let out_buffer = &buffer[..bpos];

                *buffer = &buffer[pos + 1..];
                (true, out_buffer)
            }
        }
    }

    pub fn process_lines(
        &mut self,
        file: impl AsRef<Path>,
        mut callback: impl FnMut(
            &[u8],
            bool, /* partial (line continues on next call) */
            bool, /* finished (last line) */
        ),
        remove: bool,
    ) {
        let mut line_pending = false;

        self.read_binary_file(
            file.as_ref(),
            |mut buffer: &[u8]| {
                // File finished
                if buffer.len() == 0 {
                    if line_pending {
                        eprintln!(
                            "WARNING: No newline at ending of file '{}'",
                            file.as_ref().display()
                        );
                    }
                    callback(&[], false, true);
                    return;
                }

                loop {
                    let (full, line) = Self::split_line(&mut buffer);

                    if full {
                        callback(line, false, false);
                    } else {
                        line_pending = line.len() > 0;
                        if line_pending {
                            callback(line, true, false);
                        }
                        break;
                    }
                }
            },
            remove,
        );
    }
}
