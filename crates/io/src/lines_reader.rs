use crate::binary_reader::{BinaryEvent, GenericBinaryReader};
use std::path::Path;

pub struct LinesReader {
    reader: GenericBinaryReader,
}

/// The lines of one chunk of a file, or the end of a file. Handing them over a chunk at a time
/// lets the receiver pick what to do with them once, rather than once per line.
pub struct ChunkLines<'a> {
    chunk: &'a [u8],
    end_of_file: bool,
    /// Whether the previous chunk of the file ended with a `\r`: a `\n` that starts this one is
    /// the rest of that line end, and skipped.
    after_carriage_return: bool,
}

impl<'a> ChunkLines<'a> {
    /// Calls `callback` with each line, as `(line, partial, finished)`: `partial` when the line
    /// continues in the next chunk, `finished` on the single empty line that ends a file. A line
    /// ends at a `\r`, and at a `\n` not right after one, as the SIMD parser reads them: the `\n`
    /// of a `\r\n` belongs to no line, so no line holds a `\r` or a `\n`.
    #[inline(always)]
    pub fn for_each(self, mut callback: impl FnMut(&'a [u8], bool, bool)) {
        if self.end_of_file {
            callback(&[], false, true);
            return;
        }
        let mut buffer = self.chunk;
        if self.after_carriage_return && buffer.first() == Some(&b'\n') {
            buffer = &buffer[1..];
        }
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
            reader: GenericBinaryReader::new(None, None),
        }
    }

    /// Takes the first line out of `buffer`: returns whether a line end ends it, rather than
    /// the end of `buffer`, and the line, without its line end. A line ends at a `\r`, and at a
    /// `\n` not right after one: the `\n` of a `\r\n` is taken out with it.
    #[inline]
    fn split_line<'a, 'b>(buffer: &'b mut &'a [u8]) -> (bool, &'a [u8]) {
        match memchr::memchr2(b'\r', b'\n', buffer) {
            None => {
                let line = *buffer;
                *buffer = &[];
                (false, line)
            }
            Some(pos) => {
                let line = &buffer[..pos];
                let crlf = buffer[pos] == b'\r' && buffer.get(pos + 1) == Some(&b'\n');
                *buffer = &buffer[pos + 1 + crlf as usize..];
                (true, line)
            }
        }
    }

    /// Whether `chunk` ends in the middle of a line: unless its last byte is a line end.
    fn ends_mid_line(chunk: &[u8]) -> bool {
        chunk
            .last()
            .is_some_and(|byte| !matches!(byte, b'\r' | b'\n'))
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
                after_carriage_return: false,
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
        // Whether the last chunk of the current file ended with a `\r`.
        let mut after_carriage_return = false;

        let result = self.reader.read_file(file, |event| match event {
            BinaryEvent::FilePath { name, .. } => {
                // The previous file is finished before the next one starts.
                if let Some(state) = &mut current {
                    Self::finish_file(&mut lines, state, &current_name, line_pending);
                }
                current_name.clear();
                current_name.push_str(name.unwrap_or_default());
                line_pending = false;
                after_carriage_return = false;
                current = Some(new_file(name));
            }
            BinaryEvent::Chunk(chunk) | BinaryEvent::ChunkWithCopybacks { chunk, .. } => {
                let state = current.as_mut().expect("chunks come after their file path");
                line_pending = Self::ends_mid_line(chunk);
                lines(
                    state,
                    ChunkLines {
                        chunk,
                        end_of_file: false,
                        after_carriage_return,
                    },
                );
                if let Some(&last) = chunk.last() {
                    after_carriage_return = last == b'\r';
                }
            }
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

    /// The lines of a file of `chunks`, as `process_lines` hands its chunks over: each line
    /// joined across the chunks it was split between.
    fn lines(chunks: &[&[u8]]) -> Vec<String> {
        let mut lines = Vec::new();
        let mut line = Vec::new();
        let mut after_carriage_return = false;
        for &chunk in chunks {
            let chunk_lines = ChunkLines {
                chunk,
                end_of_file: false,
                after_carriage_return,
            };
            chunk_lines.for_each(|part, partial, _| {
                line.extend_from_slice(part);
                if !partial {
                    lines.push(String::from_utf8(std::mem::take(&mut line)).unwrap());
                }
            });
            after_carriage_return = chunk.last() == Some(&b'\r');
        }
        if !line.is_empty() {
            lines.push(String::from_utf8(line).unwrap());
        }
        lines
    }

    /// A line ends at a `\r`, and at a `\n` not right after one: `\n\r`, `\r\r\n` and `\r\n\n`
    /// are two line ends, an empty line between them.
    #[test]
    fn carriage_returns_and_line_feeds_end_lines() {
        let cases: [(&[u8], &[&str]); 7] = [
            (b"a\rb\n", &["a", "b"]),
            (b"a\r\nb\n", &["a", "b"]),
            (b"a\n\rb\n", &["a", "", "b"]),
            (b"a\r\r\nb\n", &["a", "", "b"]),
            (b"a\r\n\nb\n", &["a", "", "b"]),
            (b"a\rb", &["a", "b"]),
            (b"\r\r", &["", ""]),
        ];
        for (input, expected) in cases {
            assert_eq!(lines(&[input]), expected, "{}", input.escape_ascii());
        }
    }

    /// A `\r` that ends a chunk ends its line, which is not partial: a `\n` that starts the next
    /// chunk is the rest of a `\r\n` split between them, and adds no line.
    #[test]
    fn a_carriage_return_ending_a_chunk_ends_its_line() {
        let mut parts = Vec::new();
        ChunkLines {
            chunk: b"ab\r",
            end_of_file: false,
            after_carriage_return: false,
        }
        .for_each(|line, partial, _| parts.push((line.to_vec(), partial)));
        assert_eq!(parts, [(b"ab".to_vec(), false)]);
        assert!(!LinesReader::ends_mid_line(b"ab\r"));
        assert!(!LinesReader::ends_mid_line(b"ab\n"));
        assert!(LinesReader::ends_mid_line(b"a\rb"));
        let cases: [(&[&[u8]], &[&str]); 6] = [
            (&[b"a\r", b"b\n"], &["a", "b"]),
            (&[b"a\r", b"\nb\n"], &["a", "b"]),
            (&[b"a\r", b"\n\nb\n"], &["a", "", "b"]),
            (&[b"a\r", b"\rb\n"], &["a", "", "b"]),
            (&[b"a", b"\r", b"\n", b"b\n"], &["a", "b"]),
            (&[b"a\n", b"\nb\n"], &["a", "", "b"]),
        ];
        for (chunks, expected) in cases {
            assert_eq!(lines(chunks), expected, "{chunks:?}");
        }
    }

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
