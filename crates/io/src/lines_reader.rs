use crate::raw_reader::{Codec, RawBytesReader, codec_of, open_input, wrap_decoder};
use bstr::ByteSlice;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use std::io::Read;
use std::path::Path;
use streaming_libdeflate_rs::decompress_file_buffered_callback;

pub(crate) enum LinesSource<'a> {
    File(&'a Path),
    Stream(&'a mut dyn Read, &'a Path),
}

pub struct LinesReader {
    raw: RawBytesReader,
}

impl LinesReader {
    pub fn new() -> Self {
        Self {
            raw: RawBytesReader::new(),
        }
    }

    #[inline(always)]
    fn read_stream_buffered(
        &mut self,
        stream: &mut dyn Read,
        mut callback: impl FnMut(&[u8]),
    ) -> Result<(), ()> {
        let result = self.raw.read_stream(stream, &mut callback);
        // The empty slice is how the line readers learn that the input ended.
        callback(&[]);
        result.map_err(|_| ())
    }

    fn read_binary_file(
        &mut self,
        path: impl AsRef<Path>,
        mut callback: impl FnMut(&[u8]),
        remove: bool,
    ) {
        let source = path.as_ref();
        let codec = codec_of(source);
        if codec == Codec::Gzip {
            if decompress_file_buffered_callback(
                source,
                |data| {
                    callback(data);
                    Ok(())
                },
                DEFAULT_OUTPUT_BUFFER_SIZE,
            )
            .is_err()
            {
                ggcat_logging::error!("WARNING: Error while reading file {}", source.display());
            }
            callback(&[]);
        } else {
            match wrap_decoder(codec, open_input(source)) {
                Ok(mut reader) => {
                    self.read_stream_buffered(&mut reader, callback)
                        .unwrap_or_else(|_| {
                            ggcat_logging::error!(
                                "WARNING: Error while reading file {}",
                                source.display()
                            );
                        });
                }
                Err(_) => {
                    ggcat_logging::error!("WARNING: Error while reading file {}", source.display());
                    callback(&[]);
                }
            }
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
        callback: impl FnMut(
            &[u8],
            bool, /* partial (line continues on next call) */
            bool, /* finished (last line) */
        ),
        remove: bool,
    ) {
        self.process_source(LinesSource::File(file.as_ref()), callback, remove);
    }

    pub(crate) fn process_source(
        &mut self,
        source: LinesSource<'_>,
        mut callback: impl FnMut(&[u8], bool, bool),
        remove: bool,
    ) {
        let file = match &source {
            LinesSource::File(path) | LinesSource::Stream(_, path) => *path,
        };
        let mut line_pending = false;
        let mut buffers = |mut buffer: &[u8]| {
            if buffer.is_empty() {
                if line_pending {
                    ggcat_logging::error!(
                        "WARNING: No newline at ending of file '{}'",
                        file.display()
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
                    line_pending = !line.is_empty();
                    if line_pending {
                        callback(line, true, false);
                    }
                    break;
                }
            }
        };
        match source {
            LinesSource::File(path) => self.read_binary_file(path, buffers, remove),
            LinesSource::Stream(reader, _) => {
                self.read_stream_buffered(reader, &mut buffers)
                    .unwrap_or_else(|_| panic!("Error while reading {}", file.display()));
            }
        }
    }
}
