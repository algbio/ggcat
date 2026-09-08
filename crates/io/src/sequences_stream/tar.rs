//! Sequential archive inputs. Discovery is deliberately confined to `read`.
use super::SequenceInfo;
use super::general::{DynamicSequencesStream, GeneralSequenceBlockData};
use crate::sequences_reader::{DnaSequence, SequencesReader};
use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn strip_compression_suffix(name: &str) -> &str {
    for suffix in [".gz", ".bz2", ".xz", ".zst", ".zstd", ".lz4"] {
        if let Some(stem) = name.strip_suffix(suffix) {
            return stem;
        }
    }
    name
}

pub fn is_archive(path: &Path) -> bool {
    path.to_str().is_some_and(|name| {
        strip_compression_suffix(name).ends_with(".tar")
            || [".tgz", ".tbz2", ".txz"]
                .iter()
                .any(|ext| name.ends_with(ext))
    })
}

/// Split only at a colon following a recognized archive name.
pub fn split_archive_member(path: &Path) -> Option<(PathBuf, Option<PathBuf>)> {
    let name = path.to_str()?;
    for (index, _) in name.match_indices(':') {
        if is_archive(Path::new(&name[..index])) {
            return Some((
                PathBuf::from(&name[..index]),
                Some(PathBuf::from(&name[index + 1..])),
            ));
        }
    }
    is_archive(path).then(|| (path.to_owned(), None))
}

pub fn decoded_reader<'a>(reader: impl Read + 'a, name: &Path) -> Result<Box<dyn Read + 'a>> {
    let extension = name.extension().and_then(|x| x.to_str()).unwrap_or("");
    Ok(match extension {
        "gz" | "tgz" => Box::new(flate2::read::MultiGzDecoder::new(reader)),
        "bz2" | "tbz2" => Box::new(bzip2::read::MultiBzDecoder::new(reader)),
        "xz" | "txz" => Box::new(liblzma::read::XzDecoder::new_multi_decoder(reader)),
        "zst" | "zstd" => Box::new(zstd::stream::read::Decoder::new(reader)?),
        "lz4" => Box::new(CompleteLz4Reader(Some(lz4::Decoder::new(reader)?))),
        _ => Box::new(reader),
    })
}

// lz4::Decoder reports premature input EOF through finish(), not read().
struct CompleteLz4Reader<R: Read>(Option<lz4::Decoder<R>>);
impl<R: Read> Read for CompleteLz4Reader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let Some(decoder) = self.0.as_mut() else {
            return Ok(0);
        };
        let count = decoder.read(buffer)?;
        if count == 0 {
            self.0
                .take()
                .unwrap()
                .finish()
                .1
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, error))?;
        }
        Ok(count)
    }
}

#[derive(Default)]
pub struct InputColorRegistry {
    pub names: Vec<String>,
    members: HashMap<String, u32>,
    pub errors: Vec<String>,
}

pub type SharedInputColors = Arc<Mutex<InputColorRegistry>>;

impl InputColorRegistry {
    fn member_color(&mut self, name: String) -> Result<u32> {
        if let Some(&id) = self.members.get(&name) {
            return Ok(id);
        }
        let id = self
            .names
            .len()
            .try_into()
            .context("Too many input colors")?;
        self.names.push(name.clone());
        self.members.insert(name, id);
        Ok(id)
    }
}

pub struct TarSequenceBlock {
    pub path: PathBuf,
    pub color: Option<u32>,
    pub member_colors: HashMap<PathBuf, u32>,
    pub(crate) registry: SharedInputColors,
}

impl TarSequenceBlock {
    pub fn new(path: PathBuf, color: Option<u32>) -> Self {
        Self {
            path,
            color,
            member_colors: HashMap::new(),
            registry: Arc::default(),
        }
    }

    pub fn read(
        &self,
        reader: &mut SequencesReader,
        copy_ident: bool,
        copyback: Option<usize>,
        mut callback: impl FnMut(DnaSequence<&[u8]>, SequenceInfo),
    ) -> Result<()> {
        let file = std::fs::File::open(&self.path)
            .with_context(|| format!("Cannot open archive {}", self.path.display()))?;
        let mut archive = tar::Archive::new(decoded_reader(file, &self.path)?);
        let mut unmatched: HashSet<_> = self.member_colors.keys().cloned().collect();
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            let name = format!("{}:{}", self.path.display(), path.display());
            let reason = if !entry.header().entry_type().is_file() {
                Some("not a regular file")
            } else if SequencesReader::archive_file_type(&path).is_none() {
                Some("unsupported sequence format")
            } else {
                None
            };
            if let Some(reason) = reason {
                ggcat_logging::info!("Skipping {}: {}", name, reason);
                continue;
            }
            unmatched.remove(&path);
            let color = match self.member_colors.get(&path).copied().or(self.color) {
                Some(id) => id,
                None => self.registry.lock().member_color(name.clone())?,
            };
            let mut stream = decoded_reader(&mut entry, &path)
                .with_context(|| format!("Cannot decompress {}", name))?;
            // Keep parser errors in the normal Result path, including decoder failures.
            let mut checked = CheckedReader {
                inner: &mut stream,
                error: None,
            };
            reader.process_reader_extended(
                &mut checked,
                Path::new(&name),
                |seq| callback(seq, SequenceInfo { color: Some(color) }),
                copyback,
                copy_ident,
            );
            if let Some(error) = checked.error {
                bail!("Error reading {}: {}", name, error);
            }
        }
        // Consume compression trailers too: tar iteration stops at its end marker.
        std::io::copy(&mut archive.into_inner(), &mut std::io::sink())?;
        for path in unmatched {
            ggcat_logging::info!(
                "Unmatched archive color mapping: {}:{}",
                self.path.display(),
                path.display()
            );
        }
        Ok(())
    }
}

// The existing sequence callback API has no Result return. Remember read failures
// while giving its parser EOF, then return the failure to the archive caller.
struct CheckedReader<'a> {
    inner: &'a mut dyn Read,
    error: Option<std::io::Error>,
}
impl Read for CheckedReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.error.is_some() {
            return Ok(0);
        }
        match self.inner.read(buf) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => Err(error),
            Err(error) => {
                self.error = Some(error);
                Ok(0)
            }
            result => result,
        }
    }
}

// Archive grouping changes job positions. Preserve a dynamic block's original
// fallback color, while leaving explicit per-sequence colors untouched.
struct DefaultColorStream {
    inner: Arc<dyn DynamicSequencesStream>,
    color: u32,
}
impl DynamicSequencesStream for DefaultColorStream {
    fn read_block(
        &self,
        block: usize,
        copy_ident: bool,
        copyback: Option<usize>,
        callback: &mut dyn FnMut(DnaSequence<'_, &[u8]>, SequenceInfo),
    ) {
        self.inner
            .read_block(block, copy_ident, copyback, &mut |seq, mut info| {
                info.color = info.color.or(Some(self.color));
                callback(seq, info);
            });
    }
    fn estimated_base_count(&self, block: usize) -> u64 {
        self.inner.estimated_base_count(block)
    }
}

/// Prepare archive jobs and color reservations without opening any input files.
pub fn prepare_inputs(
    blocks: Vec<GeneralSequenceBlockData>,
    names: &[String],
) -> Result<(Vec<GeneralSequenceBlockData>, SharedInputColors)> {
    let registry = Arc::new(Mutex::new(InputColorRegistry {
        names: names.to_vec(),
        ..Default::default()
    }));
    if !blocks.iter().any(|block| match block {
        GeneralSequenceBlockData::TAR(_) => true,
        GeneralSequenceBlockData::FASTA((path, _)) => split_archive_member(path).is_some(),
        _ => false,
    }) {
        return Ok((blocks, registry));
    }
    {
        let mut colors = registry.lock();
        let mut reserve = |id: u32, name: String| -> Result<()> {
            let length = (id as usize)
                .checked_add(1)
                .context("Too many input colors")?;
            if colors.names.len() < length {
                let extra = length - colors.names.len();
                colors.names.try_reserve(extra)?;
                colors.names.resize(length, String::new());
            }
            if colors.names[id as usize].is_empty() {
                colors.names[id as usize] = name;
            }
            Ok(())
        };
        for (index, block) in blocks.iter().enumerate() {
            match block {
                GeneralSequenceBlockData::FASTA((path, Some(color))) => {
                    let name = if split_archive_member(path).is_some() {
                        path.to_string_lossy().into_owned()
                    } else {
                        path.file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned()
                    };
                    reserve(*color, name)?;
                }
                GeneralSequenceBlockData::TAR(block) => {
                    if let Some(color) = block.color {
                        reserve(color, block.path.display().to_string())?;
                    }
                    for (path, color) in &block.member_colors {
                        reserve(
                            *color,
                            format!("{}:{}", block.path.display(), path.display()),
                        )?;
                    }
                }
                GeneralSequenceBlockData::Dynamic(_) => reserve(
                    index.try_into().context("Too many input files")?,
                    index.to_string(),
                )?,
                _ => {}
            }
        }
    }
    let mut output = Vec::new();
    let mut archive_indices = HashMap::<PathBuf, usize>::new();
    for (index, block) in blocks.into_iter().enumerate() {
        let archive = match block {
            GeneralSequenceBlockData::FASTA((path, color)) => {
                if let Some((archive, member)) = split_archive_member(&path) {
                    let mut block =
                        TarSequenceBlock::new(archive, if member.is_none() { color } else { None });
                    if let Some(member) = member {
                        if member.as_os_str().is_empty() {
                            bail!("Empty archive member in {}", path.display());
                        }
                        if let Some(color) = color {
                            block.member_colors.insert(member, color);
                        }
                    }
                    block
                } else {
                    // Reserve implicit ordinary-file IDs before discovering members.
                    let id = match color {
                        Some(id) => id,
                        None if !names.is_empty() => {
                            index.try_into().context("Too many input files")?
                        }
                        None => registry
                            .lock()
                            .names
                            .len()
                            .try_into()
                            .context("Too many input colors")?,
                    };
                    let mut colors = registry.lock();
                    while colors.names.len() <= id as usize {
                        colors.names.push(String::new());
                    }
                    if colors.names[id as usize].is_empty() {
                        colors.names[id as usize] = path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned();
                    }
                    drop(colors);
                    output.push(GeneralSequenceBlockData::FASTA((path, Some(id))));
                    continue;
                }
            }
            GeneralSequenceBlockData::TAR(block) => block,
            GeneralSequenceBlockData::Dynamic((inner, block)) => {
                output.push(GeneralSequenceBlockData::Dynamic((
                    Arc::new(DefaultColorStream {
                        inner,
                        color: index.try_into().context("Too many input files")?,
                    }),
                    block,
                )));
                continue;
            }
            other => {
                output.push(other);
                continue;
            }
        };
        if let Some(&index) = archive_indices.get(&archive.path) {
            let GeneralSequenceBlockData::TAR(existing) = &mut output[index] else {
                unreachable!()
            };
            if let Some(color) = archive.color {
                if existing.color.is_some_and(|old| old != color) {
                    bail!("Conflicting colors for {}", archive.path.display());
                }
                existing.color = Some(color);
            }
            for (path, color) in archive.member_colors {
                if existing
                    .member_colors
                    .insert(path.clone(), color)
                    .is_some_and(|old| old != color)
                {
                    bail!(
                        "Conflicting colors for {}:{}",
                        archive.path.display(),
                        path.display()
                    );
                }
            }
        } else {
            archive_indices.insert(archive.path.clone(), output.len());
            output.push(GeneralSequenceBlockData::TAR(TarSequenceBlock {
                registry: registry.clone(),
                ..archive
            }));
        }
    }
    Ok((output, registry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequences_stream::{GenericSequencesStream, general::GeneralSequencesStream};
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "ggcat-tar-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn compressed(bytes: &[u8], ext: &str) -> Vec<u8> {
        match ext {
            "gz" | "tgz" => {
                let mut w = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                w.write_all(bytes).unwrap();
                w.finish().unwrap()
            }
            "bz2" | "tbz2" => {
                let mut w = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
                w.write_all(bytes).unwrap();
                w.finish().unwrap()
            }
            "xz" | "txz" => {
                let mut w = liblzma::write::XzEncoder::new(Vec::new(), 1);
                w.write_all(bytes).unwrap();
                w.finish().unwrap()
            }
            "zst" | "zstd" => zstd::stream::encode_all(bytes, 1).unwrap(),
            "lz4" => {
                let mut w = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
                w.write_all(bytes).unwrap();
                let (bytes, result) = w.finish();
                result.unwrap();
                bytes
            }
            "tar" => bytes.to_vec(),
            _ => unreachable!(),
        }
    }
    fn archive(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, bytes) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, path, *bytes).unwrap();
        }
        builder.into_inner().unwrap()
    }
    fn parse(block: &TarSequenceBlock) -> Result<Vec<(Vec<u8>, u32)>> {
        let mut seqs = Vec::new();
        block.read(&mut SequencesReader::new(), true, None, |seq, info| {
            seqs.push((seq.seq.to_vec(), info.color.unwrap()))
        })?;
        Ok(seqs)
    }

    #[test]
    fn all_compressions_at_both_layers() {
        let fixture = Fixture::new();
        for ext in [
            "tar", "gz", "bz2", "xz", "zst", "zstd", "lz4", "tgz", "tbz2", "txz",
        ] {
            let mut members = vec![
                ("plain.fasta".to_string(), b">a\nacgt\n".to_vec()),
                ("plain.fastq".into(), b"@b\ntgca\n+\n!!!!\n".to_vec()),
            ];
            for inner in ["gz", "bz2", "xz", "zst", "zstd", "lz4"] {
                members.push((
                    format!("nested/read.fa.{inner}"),
                    compressed(b">c\nAAAA\n", inner),
                ));
            }
            members.push(("notes.txt".into(), b"ignored".to_vec()));
            members.push((
                "nested.tar".into(),
                archive(&[("hidden.fa", b">hidden\nCCCC\n")]),
            ));
            let contents = archive(
                &members
                    .iter()
                    .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
                    .collect::<Vec<_>>(),
            );
            let name = if ["tgz", "tbz2", "txz", "tar"].contains(&ext) {
                format!("input.{ext}")
            } else {
                format!("input.tar.{ext}")
            };
            let path = fixture.write(&name, &compressed(&contents, ext));
            assert!(is_archive(&path));
            let block = TarSequenceBlock::new(path.clone(), None);
            let seqs = parse(&block).unwrap();
            assert_eq!(seqs.len(), 8, "{ext}");
            assert_eq!(seqs[0], (b"ACGT".to_vec(), 0));
            assert_eq!(seqs[1], (b"TGCA".to_vec(), 1));
            assert_eq!(block.registry.lock().names.len(), 8);
            assert_eq!(
                block.registry.lock().names[0],
                format!("{}:plain.fasta", path.display())
            );
        }
    }

    #[test]
    fn preparation_and_estimation_do_not_read_archive() {
        let fixture = Fixture::new();
        // Invalid compression and invalid tar headers must not matter until read.
        let path = fixture.write("invalid.tar.gz", b"not an archive");
        let (blocks, registry) =
            prepare_inputs(vec![GeneralSequenceBlockData::FASTA((path, None))], &[]).unwrap();
        assert_eq!(blocks.len(), 1);
        crate::compute_stats_from_input_blocks(&blocks).unwrap();
        assert!(registry.lock().names.is_empty());
        GeneralSequencesStream::new().read_block(&blocks[0], false, None, |_, _| {
            panic!("unexpected sequence")
        });
        assert_eq!(registry.lock().errors.len(), 1);
    }

    #[test]
    fn mapping_precedence_grouping_and_duplicates() {
        let fixture = Fixture::new();
        let path = fixture.write(
            "reads.tar",
            &archive(&[
                ("a.fa", b">a\nACGT\n"),
                ("b.fq", b"@b\nTTTT\n+\n!!!!\n"),
                ("empty.fa", b""),
                ("a.fa", b">a2\nGGGG\n"),
            ]),
        );
        let member = PathBuf::from(format!("{}:a.fa", path.display()));
        let names = vec!["all".to_string(), "specific".to_string()];
        let (blocks, registry) = prepare_inputs(
            vec![
                GeneralSequenceBlockData::FASTA((member.clone(), Some(1))),
                GeneralSequenceBlockData::FASTA((path.clone(), Some(0))),
            ],
            &names,
        )
        .unwrap();
        assert_eq!(blocks.len(), 1);
        let GeneralSequenceBlockData::TAR(block) = &blocks[0] else {
            panic!()
        };
        assert_eq!(
            parse(block)
                .unwrap()
                .iter()
                .map(|x| x.1)
                .collect::<Vec<_>>(),
            [1, 0, 1]
        );
        assert_eq!(registry.lock().names, names);
        let (blocks, registry) = prepare_inputs(
            vec![GeneralSequenceBlockData::FASTA((member.clone(), Some(1)))],
            &names,
        )
        .unwrap();
        let GeneralSequenceBlockData::TAR(block) = &blocks[0] else {
            panic!()
        };
        assert_eq!(
            parse(block)
                .unwrap()
                .iter()
                .map(|x| x.1)
                .collect::<Vec<_>>(),
            [1, 2, 1]
        );
        assert_eq!(registry.lock().names.len(), 4); // Empty recognized member has a color.
        assert!(
            prepare_inputs(
                vec![
                    GeneralSequenceBlockData::FASTA((member.clone(), Some(0))),
                    GeneralSequenceBlockData::FASTA((member, Some(1)))
                ],
                &names
            )
            .is_err()
        );
        assert!(
            prepare_inputs(
                vec![
                    GeneralSequenceBlockData::FASTA((path.clone(), Some(0))),
                    GeneralSequenceBlockData::FASTA((path.clone(), Some(1)))
                ],
                &names
            )
            .is_err()
        );
        let block = TarSequenceBlock::new(path, None);
        assert_eq!(
            parse(&block)
                .unwrap()
                .iter()
                .map(|x| x.1)
                .collect::<Vec<_>>(),
            [0, 1, 0]
        );
        assert_eq!(block.registry.lock().names.len(), 3);
    }

    #[test]
    fn empty_skipped_and_corrupt_inputs() {
        let fixture = Fixture::new();
        let path = fixture.write("empty.tar", &archive(&[]));
        assert!(
            parse(&TarSequenceBlock::new(path, None))
                .unwrap()
                .is_empty()
        );
        let mut builder = tar::Builder::new(Vec::new());
        for entry_type in [
            tar::EntryType::Directory,
            tar::EntryType::Symlink,
            tar::EntryType::Link,
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o755);
            header.set_entry_type(entry_type);
            header.set_link_name("target.fa").unwrap();
            header.set_cksum();
            builder
                .append_data(&mut header, "skip.fa", &[][..])
                .unwrap();
        }
        let path = fixture.write("links.tar", &builder.into_inner().unwrap());
        let block = TarSequenceBlock::new(path, None);
        assert!(parse(&block).unwrap().is_empty());
        assert!(block.registry.lock().names.is_empty());
        let mut contents = archive(&[("a.fa", b">a\nACGT\n")]);
        contents[0] ^= 0xff;
        let path = fixture.write("bad.tar", &contents);
        assert!(parse(&TarSequenceBlock::new(path, None)).is_err());
        let path = fixture.write("bad-member.tar", &archive(&[("a.fa.gz", b"not gzip")]));
        assert!(parse(&TarSequenceBlock::new(path, None)).is_err());
        let mut bytes = compressed(&archive(&[("a.fa", b">a\nACGT\n")]), "gz");
        bytes.truncate(bytes.len() - 5);
        let path = fixture.write("truncated.tar.gz", &bytes);
        assert!(parse(&TarSequenceBlock::new(path, None)).is_err());
    }

    #[test]
    fn truncated_compression_is_an_error_at_either_layer() {
        let fixture = Fixture::new();
        for extension in ["gz", "bz2", "xz", "zst", "zstd", "lz4"] {
            let mut bytes = compressed(&archive(&[("a.fa", b">a\nACGT\n")]), extension);
            bytes.truncate(bytes.len() - 4);
            let path = fixture.write(&format!("bad.tar.{extension}"), &bytes);
            assert!(
                parse(&TarSequenceBlock::new(path, None)).is_err(),
                "archive {extension}"
            );
            let mut bytes = compressed(b">a\nACGT\n", extension);
            bytes.truncate(bytes.len() - 4);
            let path = fixture.write(
                "bad-member.tar",
                &archive(&[(&format!("a.fa.{extension}"), &bytes)]),
            );
            assert!(
                parse(&TarSequenceBlock::new(path, None)).is_err(),
                "member {extension}"
            );
        }
    }

    #[test]
    fn concurrent_archives_reserve_ordinary_colors() {
        let fixture = Fixture::new();
        let a = fixture.write("a.tar", &archive(&[("a.fa", b">a\nACGT\n")]));
        let b = fixture.write("b.tar", &archive(&[("a.fa", b">b\nTGCA\n")]));
        let loose = fixture.write("loose.fa", b">c\nAAAA\n");
        let (blocks, registry) = prepare_inputs(
            vec![a, loose, b]
                .into_iter()
                .map(|path| GeneralSequenceBlockData::FASTA((path, None)))
                .collect(),
            &[],
        )
        .unwrap();
        std::thread::scope(|scope| {
            for block in &blocks {
                scope.spawn(|| {
                    GeneralSequencesStream::new().read_block(block, false, None, |_, _| {})
                });
            }
        });
        let registry = registry.lock();
        assert!(registry.errors.is_empty());
        assert_eq!(registry.names.len(), 3);
        assert_eq!(registry.names[0], "loose.fa");
        assert_ne!(registry.names[1], registry.names[2]);
    }

    #[test]
    fn explicit_archive_ids_are_reserved_without_supplied_names() {
        let fixture = Fixture::new();
        let path = fixture.write(
            "reads.tar",
            &archive(&[("a.fa", b">a\nACGT\n"), ("b.fa", b">b\nAAAA\n")]),
        );
        let mut block = TarSequenceBlock::new(path, None);
        block.member_colors.insert(PathBuf::from("a.fa"), 0);
        let (blocks, registry) =
            prepare_inputs(vec![GeneralSequenceBlockData::TAR(block)], &[]).unwrap();
        let GeneralSequenceBlockData::TAR(block) = &blocks[0] else {
            panic!()
        };
        assert_eq!(
            parse(block)
                .unwrap()
                .iter()
                .map(|seq| seq.1)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(registry.lock().names.len(), 2);
    }

    #[test]
    fn dynamic_colors_survive_archive_grouping() {
        struct Stream;
        impl DynamicSequencesStream for Stream {
            fn read_block(
                &self,
                _: usize,
                _: bool,
                _: Option<usize>,
                callback: &mut dyn FnMut(DnaSequence<'_, &[u8]>, SequenceInfo),
            ) {
                for color in [None, Some(1)] {
                    callback(
                        DnaSequence {
                            ident_data: b">dynamic",
                            seq: b"ACGT",
                            format: crate::sequences_reader::DnaSequencesFileType::FASTA,
                        },
                        SequenceInfo { color },
                    );
                }
            }
            fn estimated_base_count(&self, _: usize) -> u64 {
                8
            }
        }
        let fixture = Fixture::new();
        let path = fixture.write("reads.tar", &archive(&[("a.fa", b">a\nACGT\n")]));
        let names = vec!["archive".into(), "explicit".into(), "dynamic".into()];
        let (blocks, registry) = prepare_inputs(
            vec![
                GeneralSequenceBlockData::FASTA((path.clone(), Some(0))),
                GeneralSequenceBlockData::FASTA((path, Some(0))),
                GeneralSequenceBlockData::Dynamic((Arc::new(Stream), 0)),
            ],
            &names,
        )
        .unwrap();
        assert_eq!(blocks.len(), 2);
        let mut colors = Vec::new();
        GeneralSequencesStream::new()
            .read_block(&blocks[1], true, None, |_, info| colors.push(info.color));
        assert_eq!(colors, [Some(2), Some(1)]);
        assert_eq!(registry.lock().names, names);
    }

    #[test]
    fn member_parser_retains_fasta_copyback() {
        let fixture = Fixture::new();
        let sequence = "ACGT".repeat(config::DEFAULT_OUTPUT_BUFFER_SIZE / 2);
        let path = fixture.write(
            "long.tar",
            &archive(&[("long.fa", format!(">long\n{sequence}\n").as_bytes())]),
        );
        let block = TarSequenceBlock::new(path, None);
        let mut joined = Vec::new();
        block
            .read(&mut SequencesReader::new(), true, Some(30), |seq, _| {
                if !joined.is_empty() {
                    joined.truncate(joined.len() - 30);
                }
                joined.extend_from_slice(seq.seq);
                assert_eq!(seq.ident_data, b">long");
            })
            .unwrap();
        assert_eq!(joined, sequence.as_bytes());
    }
}
