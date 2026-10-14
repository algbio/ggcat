use crate::binary_reader::{BinaryEvent, GenericBinaryReader};
use bstr::ByteSlice;
use std::path::Path;

pub struct LinesReader {
    reader: GenericBinaryReader,
}

/// The lines of one chunk of a file, or the end of a file. Handing them over a chunk at a time
/// lets the receiver pick what to do with them once, rather than once per line.
pub struct ChunkLines<'a> {
    chunk: &'a [u8],
    end_of_file: bool,
}

impl<'a> ChunkLines<'a> {
    /// Calls `callback` with each line, as `(line, partial, finished)`: `partial` when the line
    /// continues in the next chunk, `finished` on the single empty line that ends a file.
    #[inline(always)]
    pub fn for_each(self, mut callback: impl FnMut(&'a [u8], bool, bool)) {
        if self.end_of_file {
            callback(&[], false, true);
            return;
        }
        let mut buffer = self.chunk;
        loop {
            let (full, line) = LinesReader::split_line(&mut buffer);

            if full {
                callback(line, false, false);
            } else {
                if line.len() > 0 {
                    callback(line, true, false);
                }
                break;
            }
        }
    }
}

impl LinesReader {
    pub fn new() -> Self {
        Self {
            reader: GenericBinaryReader::new(None),
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

    /// Whether `chunk` ends in the middle of a line.
    fn ends_mid_line(chunk: &[u8]) -> bool {
        let mut last = &chunk[chunk.rfind_byte(b'\n').map_or(0, |pos| pos + 1)..];
        LinesReader::split_line(&mut last).1.len() > 0
    }

    /// Ends the file `name` with its finished line.
    fn finish_file<T>(
        lines: &mut impl FnMut(&mut T, ChunkLines<'_>),
        state: &mut T,
        name: &str,
        line_pending: bool,
    ) {
        if line_pending {
            ggcat_logging::error!("WARNING: No newline at ending of file '{}'", name);
        }
        lines(
            state,
            ChunkLines {
                chunk: &[],
                end_of_file: true,
            },
        );
    }

    /// Reads `file` a chunk of lines at a time. Every file in it (just `file`, or each member of
    /// an archive) starts with a call to `new_file` with its name, whose result `lines` then
    /// receives along with each chunk of lines of that file, ending with the finished line.
    pub fn process_lines<T>(
        &mut self,
        file: impl AsRef<Path>,
        mut new_file: impl FnMut(Option<&str>) -> T,
        mut lines: impl FnMut(&mut T, ChunkLines<'_>),
        remove: bool,
    ) {
        let file = file.as_ref();
        let mut current: Option<T> = None;
        let mut current_name = String::new();
        let mut line_pending = false;

        let result = self.reader.read_file(file, |event| match event {
            BinaryEvent::FilePath(name) => {
                // The previous file is finished before the next one starts.
                if let Some(state) = &mut current {
                    Self::finish_file(&mut lines, state, &current_name, line_pending);
                }
                current_name.clear();
                current_name.push_str(name.unwrap_or_default());
                line_pending = false;
                current = Some(new_file(name));
            }
            BinaryEvent::Chunk(chunk) => {
                let state = current.as_mut().expect("chunks come after their file path");
                line_pending = Self::ends_mid_line(chunk);
                lines(
                    state,
                    ChunkLines {
                        chunk,
                        end_of_file: false,
                    },
                );
            }
            BinaryEvent::Extra { .. } => {}
        });
        if let Err(error) = result {
            ggcat_logging::error!(
                "WARNING: Error while reading file {}: {error:#}",
                file.display()
            );
        }

        if let Some(state) = &mut current {
            Self::finish_file(&mut lines, state, &current_name, line_pending);
        }

        if remove {
            std::fs::remove_file(file).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_file_is_finished_before_the_next_starts() {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in [("a.txt", &b"one\ntwo"[..]), ("b.txt", b"three\r\n")] {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            builder.append_data(&mut header, name, data).unwrap();
        }
        let directory =
            std::env::temp_dir().join(format!("ggcat-lines-reader-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("input.tar");
        std::fs::write(&path, builder.into_inner().unwrap()).unwrap();

        let mut events = Vec::new();
        let events_cell = std::cell::RefCell::new(&mut events);
        LinesReader::new().process_lines(
            &path,
            |name| {
                let name = name.unwrap().to_owned();
                events_cell.borrow_mut().push(format!("new {name}"));
                name
            },
            |name, lines| {
                lines.for_each(|line, partial, finished| {
                    let line = String::from_utf8_lossy(line);
                    events_cell
                        .borrow_mut()
                        .push(format!("{name}: {line:?} {partial} {finished}"));
                })
            },
            false,
        );
        std::fs::remove_dir_all(directory).unwrap();

        let a = format!("{}:a.txt", path.display());
        let b = format!("{}:b.txt", path.display());
        assert_eq!(
            events,
            [
                format!("new {a}"),
                format!("{a}: \"one\" false false"),
                format!("{a}: \"two\" true false"),
                format!("{a}: \"\" false true"),
                format!("new {b}"),
                format!("{b}: \"three\" false false"),
                format!("{b}: \"\" false true"),
            ]
        );
    }
}
