use bstr::ByteSlice;
use libdeflate_rs::decompress_file_buffered;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub struct LinesReaderBufferLock {}

pub struct LinesReader {}

impl LinesReader {
    #[inline(always)]
    fn read_stream_buffered(
        mut stream: impl Read,
        mut callback: impl FnMut(&[u8]),
    ) -> Result<(), ()> {
        let mut buffer = [0; 1024 * 512];
        while let Ok(count) = stream.read(&mut buffer) {
            if count == 0 {
                callback(&[]);
                return Ok(());
            }
            callback(&buffer[0..count]);
        }
        Err(())
    }

    fn read_binary_file(path: impl AsRef<Path>, mut callback: impl FnMut(&[u8]), remove: bool) {
        if path.as_ref().extension().filter(|x| *x == "gz").is_some() {
            if let Err(_err) = decompress_file_buffered(
                &path,
                |data| {
                    callback(data);
                    Ok(())
                },
                1024 * 512,
            ) {
                println!(
                    "WARNING: Error while reading file {}",
                    path.as_ref().display()
                );
            }
            callback(&[]);
        } else if path.as_ref().extension().filter(|x| *x == "lz4").is_some() {
            let mut file = lz4::Decoder::new(
                File::open(&path).expect(&format!("Cannot open file {}", path.as_ref().display())),
            )
            .unwrap();
            Self::read_stream_buffered(file, callback).unwrap_or_else(|_| {
                println!(
                    "WARNING: Error while reading file {}",
                    path.as_ref().display()
                );
            });
        } else {
            let mut file =
                File::open(&path).expect(&format!("Cannot open file {}", path.as_ref().display()));
            Self::read_stream_buffered(file, callback).unwrap_or_else(|_| {
                println!(
                    "WARNING: Error while reading file {}",
                    path.as_ref().display()
                );
            });
        }

        if remove {
            std::fs::remove_file(path);
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
        file: impl AsRef<Path>,
        mut callback: impl FnMut(&[u8], bool),
        remove: bool,
    ) {
        let mut tmp_line = Vec::new();
        Self::read_binary_file(
            file,
            |mut buffer: &[u8]| {
                // File finished
                if buffer.len() == 0 {
                    callback(&[], true);
                    return;
                }

                loop {
                    let (full, line) = Self::split_line(&mut buffer);

                    if full {
                        callback(
                            if tmp_line.len() > 0 {
                                tmp_line.extend_from_slice(line);
                                &tmp_line
                            } else {
                                line
                            },
                            false,
                        );
                        tmp_line.clear();
                    } else {
                        tmp_line.extend_from_slice(line);
                        break;
                    }
                }
            },
            remove,
        );
    }
}
