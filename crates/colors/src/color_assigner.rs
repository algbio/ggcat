//! The colours of a coloured graph's inputs, decided as the inputs are read: an archive's
//! members can each have a colour of their own, so the colours, and their names for the
//! colormap, are all known only once the inputs are read.

use crate::bucket_colors::{ColorRun, canonicalize};
use anyhow::{Result, bail};
use config::ColorIndexType;
use io::binary_reader::{archive_prefixes, split_archive_member};
use io::sequences_stream::general::GeneralSequenceBlockData;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// The colours decided before reading, and one range preallocated for each archive whose
/// members can get new colours, are kept under this many.
const PREALLOCATION_BOUND: u64 = 1 << 29;
/// The colours of a range preallocated for an archive's members: a power of two, at most and at
/// least these.
const MAX_PREALLOCATION: ColorIndexType = 2048;
const MIN_PREALLOCATION: ColorIndexType = 8;

/// The colours of the files of a coloured graph's inputs, and their names.
///
/// The colours decided before reading come first, numbered in the order of the colour list, or
/// without one in the order of the inputs:
/// - a file has its own colour, or the one the list maps it to;
/// - an archive, written `x.tar` in the list, the colour of all its members;
/// - a member, written `x.tar:dir/a.fa`, its own colour over the archive's;
/// - an archive nested in it, written `x.tar:dir/inner.tgz`, the colour of all its own members,
///   over the outer archive's, at any depth: `x.tar:dir/inner.tgz:a.fa` is one of them.
///
/// A member takes the most specific of these that the list has. The members that the list maps
/// to no colour are each given a new one as they are read, named `x.tar:dir/a.fa`, after all
/// the others. Those of an archive, nested ones included, get consecutive colours in the order
/// they are read in: from a range of colours preallocated for the archive when it needs one,
/// and the next range when it is used up, which other archives' ranges can come before.
///
/// Once the inputs are read, [`Self::packed_colors`] closes the gaps the ranges leave and puts
/// the archives' colours in the order of the inputs, whatever order they were read in.
pub struct ColorAssigner {
    /// By the index of the input in the inputs to read, as [`Self::new`] gives them.
    inputs: Vec<InputColors>,
    /// The colours decided before reading: those below this.
    decided: ColorIndexType,
    /// The colours of each range preallocated for an archive, a power of two: each range is
    /// a block of this many colours, from a multiple of it.
    prealloc_count: ColorIndexType,
    named: Mutex<NamedColors>,
}

/// How the files of an input get their colours.
enum InputColors {
    /// All of them this one: a file's, or that of a stream's records that bring none.
    Fixed(ColorIndexType),
    /// The members of an archive, named `prefix` followed by their path in it: each its own
    /// colour if mapped one, otherwise that of the innermost archive it is nested in that is
    /// mapped one, otherwise the archive's if mapped one, otherwise a new one.
    Archive {
        prefix: String,
        color: Option<ColorIndexType>,
        members: HashMap<PathBuf, MappedMember>,
    },
}

/// The colour a member, or a nested archive, is mapped to, and whether a file read was it or
/// in it.
struct MappedMember {
    color: ColorIndexType,
    found: AtomicBool,
}

/// The names of the colours so far, by their index, those of the ranges preallocated for
/// archives included, empty until given; the colours given to members as they were read, by the
/// members' names; and the ranges of each archive, by its input.
struct NamedColors {
    names: Vec<String>,
    members: HashMap<String, ColorIndexType>,
    archives: HashMap<usize, ArchiveRanges>,
}

/// The ranges an archive took for its members' colours, in order, by their blocks, and the
/// colours left of the last.
#[derive(Default)]
struct ArchiveRanges {
    blocks: Vec<ColorIndexType>,
    left: Range<ColorIndexType>,
}

/// Where the colours that a [`ColorAssigner`] gave go once packed, by
/// [`ColorAssigner::packed_colors`]: each block of `1 << shift` colours, as large as the ranges
/// preallocated for archives, moves by an offset of its own, so that a colour takes a single
/// lookup. The offsets are added wrapping, as a block can move down.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColorRemap {
    shift: u32,
    offsets: Vec<ColorIndexType>,
}

impl ColorRemap {
    /// The remap of the blocks of `1 << shift` colours by `offsets`, as [`Self::parts`] gives
    /// them.
    pub fn from_parts(shift: u32, offsets: Vec<ColorIndexType>) -> Self {
        Self { shift, offsets }
    }

    /// The blocks' size, as a shift, and their offsets: what [`Self::from_parts`] takes.
    pub fn parts(&self) -> (u32, &[ColorIndexType]) {
        (self.shift, &self.offsets)
    }

    /// Where `color` goes. Only for the colours given: those of a range that no member got
    /// go anywhere.
    #[inline(always)]
    pub fn map(&self, color: ColorIndexType) -> ColorIndexType {
        color.wrapping_add(self.offsets[(color >> self.shift) as usize])
    }

    /// The set of `runs` remapped, in `out`, in its canonical form: each run cut at the bounds
    /// of the blocks, which can move apart.
    pub fn map_runs<'a>(&self, runs: &[ColorRun], out: &'a mut Vec<ColorRun>) -> &'a [ColorRun] {
        out.clear();
        for run in runs {
            let mut start = run.start() as u64;
            while start < run.end() {
                let end = run.end().min(((start >> self.shift) + 1) << self.shift);
                let mapped = self.map(start as ColorIndexType);
                out.push(ColorRun::from_bounds(mapped, mapped as u64 + (end - start)));
                start = end;
            }
        }
        canonicalize(out)
    }
}

impl ColorAssigner {
    /// The colours of `blocks`, the graph's inputs, as their colours and `names` decide them, and
    /// the inputs to read: each archive once, in the place of its first mention, the blocks that
    /// name its members (`x.tar:dir/a.fa`) merged into it.
    ///
    /// A file with no colour has its index in `blocks` if `names` are given, as their order
    /// matches the blocks', otherwise the colour after all those decided so far. A stream of
    /// records gives its index in `blocks` to the records that bring no colour. A colour with no
    /// name is named after its file, archive, member or stream. An archive mapped two colours,
    /// or one of its members two, is an error.
    pub fn new(
        blocks: Vec<GeneralSequenceBlockData>,
        names: &[String],
    ) -> Result<(Self, Vec<GeneralSequenceBlockData>)> {
        let mut named = names.to_vec();
        // The colours mapped are taken first, so that the files with none get those after.
        for block in &blocks {
            if let GeneralSequenceBlockData::FILE((path, Some(color))) = block {
                reserve(&mut named, *color, || match split_archive_member(path) {
                    Some(_) => path.to_string_lossy().into_owned(),
                    None => file_name(path),
                });
            }
        }

        let (mut inputs, mut to_read) = (Vec::new(), Vec::new());
        let mut archives = HashMap::new();
        for (index, block) in blocks.into_iter().enumerate() {
            let (path, color) = match block {
                GeneralSequenceBlockData::FILE(file) => file,
                dynamic @ GeneralSequenceBlockData::Dynamic(_) => {
                    let color = index as ColorIndexType;
                    reserve(&mut named, color, || index.to_string());
                    inputs.push(InputColors::Fixed(color));
                    to_read.push(dynamic);
                    continue;
                }
            };
            let Some((archive, member)) = split_archive_member(&path) else {
                let color = color.unwrap_or(if names.is_empty() {
                    named.len() as ColorIndexType
                } else {
                    index as ColorIndexType
                });
                reserve(&mut named, color, || file_name(&path));
                inputs.push(InputColors::Fixed(color));
                to_read.push(GeneralSequenceBlockData::FILE((path, Some(color))));
                continue;
            };
            if member
                .as_ref()
                .is_some_and(|member| member.as_os_str().is_empty())
            {
                bail!("Empty archive member in {}", path.display());
            }
            let input = *archives.entry(archive.clone()).or_insert_with(|| {
                inputs.push(InputColors::Archive {
                    prefix: format!("{}:", archive.to_string_lossy()),
                    color: None,
                    members: HashMap::new(),
                });
                to_read.push(GeneralSequenceBlockData::FILE((archive.clone(), None)));
                inputs.len() - 1
            });
            let InputColors::Archive {
                color: archive_color,
                members,
                ..
            } = &mut inputs[input]
            else {
                unreachable!("the input of an archive");
            };
            match (member, color) {
                // Mapped to no colour, its members get theirs as they are read.
                (_, None) => {}
                (None, Some(color)) => {
                    if archive_color
                        .replace(color)
                        .is_some_and(|other| other != color)
                    {
                        bail!("Conflicting colors for {}", archive.display());
                    }
                }
                (Some(member), Some(color)) => match members.entry(member) {
                    Entry::Vacant(entry) => {
                        entry.insert(MappedMember {
                            color,
                            found: AtomicBool::new(false),
                        });
                    }
                    Entry::Occupied(entry) => {
                        if entry.get().color != color {
                            bail!("Conflicting colors for {}", path.display());
                        }
                    }
                },
            }
        }

        let reading_archives = inputs
            .iter()
            .filter(|input| matches!(input, InputColors::Archive { color: None, .. }))
            .count();
        let assigner = Self {
            prealloc_count: prealloc_count(named.len(), reading_archives),
            decided: named.len() as ColorIndexType,
            inputs,
            named: Mutex::new(NamedColors {
                names: named,
                members: HashMap::new(),
                archives: HashMap::new(),
            }),
        };
        Ok((assigner, to_read))
    }

    /// The colour of the file `name` of input `input`, as it starts being read. A member of an
    /// archive, named `archive:member`, has the colour mapped to it, or else to the innermost
    /// archive it is nested in that is mapped one, or else to the archive, or else a new one
    /// named `name`, the next of the archive's range: the same for every member of that name.
    pub fn file_color(&self, input: usize, name: &str) -> ColorIndexType {
        let (prefix, color, members) = match &self.inputs[input] {
            InputColors::Fixed(color) => return *color,
            InputColors::Archive {
                prefix,
                color,
                members,
            } => (prefix, color, members),
        };
        if let Some(member) = name.strip_prefix(prefix.as_str())
            && !members.is_empty()
        {
            // The member, and every archive it is nested in, outermost first: each mapping
            // found is marked, and the innermost wins.
            let mut mapped_color = None;
            for path in archive_prefixes(member).chain([member]) {
                if let Some(mapped) = members.get(Path::new(path)) {
                    mapped.found.store(true, Ordering::Relaxed);
                    mapped_color = Some(mapped.color);
                }
            }
            if let Some(color) = mapped_color {
                return color;
            }
        }
        if let Some(color) = color {
            return *color;
        }
        let mut named = self.named.lock();
        let NamedColors {
            names,
            members,
            archives,
        } = &mut *named;
        if let Some(&color) = members.get(name) {
            return color;
        }
        let ranges = archives.entry(input).or_default();
        if ranges.left.is_empty() {
            // A range after all the colours so far, from a multiple of its size, so that its
            // block is a colour shifted. Its colours have no name until given.
            let size = self.prealloc_count;
            let start = names.len().next_multiple_of(size as usize);
            names.resize(start + size as usize, String::new());
            let start = start as ColorIndexType;
            ranges.blocks.push(start / size);
            ranges.left = start..start + size;
        }
        let color = ranges.left.start;
        ranges.left.start += 1;
        names[color as usize] = name.to_owned();
        members.insert(name.to_owned(), color);
        color
    }

    /// The colour of the sequences of input `input` that come with none and from no file
    /// named to [`Self::file_color`]: a stream's records. An archive's members always get
    /// theirs from [`Self::file_color`].
    pub fn input_color(&self, input: usize) -> ColorIndexType {
        match &self.inputs[input] {
            InputColors::Fixed(color) => *color,
            InputColors::Archive { color, .. } => color.unwrap_or_default(),
        }
    }

    /// Whether some colours are given only as the inputs are read: a run resumed past the
    /// reading cannot know them.
    pub fn assigns_while_reading(&self) -> bool {
        self.inputs
            .iter()
            .any(|input| matches!(input, InputColors::Archive { color: None, .. }))
    }

    /// The names of all the colours so far, by their index, as [`Self::file_color`] gave them,
    /// those of the ranges that no member got empty.
    #[cfg(test)]
    fn color_names(&self) -> Vec<String> {
        self.named.lock().names.clone()
    }

    /// The colours packed, once the inputs are read: the remap of those that
    /// [`Self::file_color`] gave, if any moves, and the names of the packed colours, by their
    /// index, with no colour unnamed between them. The colours decided before reading keep
    /// theirs. The archives' members follow them, archive by archive in the order of the inputs,
    /// each archive's in the order they were read in: an order that does not depend on the
    /// order the archives were read in.
    pub fn packed_colors(&self) -> (Option<ColorRemap>, Vec<String>) {
        let named = self.named.lock();
        if named.archives.is_empty() {
            return (None, named.names.clone());
        }
        let size = self.prealloc_count;
        let mut offsets = vec![0; named.names.len() / size as usize];
        let mut names = named.names[..self.decided as usize].to_vec();
        for input in 0..self.inputs.len() {
            let Some(ranges) = named.archives.get(&input) else {
                continue;
            };
            for (index, &block) in ranges.blocks.iter().enumerate() {
                let start = block * size;
                // All of each range is given, but what is left of the last.
                let end = match index + 1 == ranges.blocks.len() {
                    true => ranges.left.start,
                    false => start + size,
                };
                offsets[block as usize] = (names.len() as ColorIndexType).wrapping_sub(start);
                names.extend_from_slice(&named.names[start as usize..end as usize]);
            }
        }
        let remap = ColorRemap {
            shift: size.trailing_zeros(),
            offsets,
        };
        (Some(remap), names)
    }

    /// The members and nested archives mapped to a colour that no file read was or was in, as
    /// `archive:member`, once the inputs are read: their colours have no sequence.
    pub fn unmatched_mappings(&self) -> Vec<String> {
        let mut unmatched = Vec::new();
        for input in &self.inputs {
            if let InputColors::Archive {
                prefix, members, ..
            } = input
            {
                for (member, mapped) in members {
                    if !mapped.found.load(Ordering::Relaxed) {
                        unmatched.push(format!("{prefix}{}", member.display()));
                    }
                }
            }
        }
        unmatched.sort();
        unmatched
    }
}

/// The colours of each range preallocated for an archive's members, with `assigned` colours
/// decided and `archives` archives whose members can get new colours: the largest power of two
/// up to [`MAX_PREALLOCATION`] that keeps all of them, and one range for each archive, under
/// [`PREALLOCATION_BOUND`], but [`MIN_PREALLOCATION`] at least.
fn prealloc_count(assigned: usize, archives: usize) -> ColorIndexType {
    let mut count = MAX_PREALLOCATION;
    while count > MIN_PREALLOCATION
        && assigned as u64 + archives as u64 * count as u64 >= PREALLOCATION_BOUND
    {
        count /= 2;
    }
    count
}

/// Makes `color` one of `names`, named `name()` unless it has a name already.
fn reserve(names: &mut Vec<String>, color: ColorIndexType, name: impl FnOnce() -> String) {
    let index = color as usize;
    if names.len() <= index {
        names.resize(index + 1, String::new());
    }
    if names[index].is_empty() {
        names[index] = name();
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::sequences_stream::SequenceInfo;
    use io::sequences_stream::general::DynamicSequencesStream;
    use std::sync::Arc;
    use utils::dna_sequence::DnaSequence;

    fn file(path: &str, color: Option<ColorIndexType>) -> GeneralSequenceBlockData {
        GeneralSequenceBlockData::FILE((PathBuf::from(path), color))
    }

    /// The paths and colours of the files to read.
    fn files(blocks: &[GeneralSequenceBlockData]) -> Vec<(&str, Option<ColorIndexType>)> {
        blocks
            .iter()
            .map(|block| match block {
                GeneralSequenceBlockData::FILE((path, color)) => (path.to_str().unwrap(), *color),
                GeneralSequenceBlockData::Dynamic(_) => ("dynamic", None),
            })
            .collect()
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// The colours that have a name, and how many colours there are, unnamed ones included.
    fn named(assigner: &ColorAssigner) -> (Vec<(usize, String)>, usize) {
        let all = assigner.color_names();
        let named = all.iter().cloned().enumerate();
        (
            named.filter(|(_, name)| !name.is_empty()).collect(),
            all.len(),
        )
    }

    fn indexed(names: &[(usize, &str)]) -> Vec<(usize, String)> {
        names
            .iter()
            .map(|(color, name)| (*color, name.to_string()))
            .collect()
    }

    #[test]
    fn files_take_the_input_order_and_members_new_colors_as_they_are_read() {
        let blocks = vec![
            file("a.fa", None),
            file("d/x.tar", None),
            file("b.fq.gz", None),
        ];
        let (assigner, to_read) = ColorAssigner::new(blocks, &[]).unwrap();
        assert_eq!(
            files(&to_read),
            [("a.fa", Some(0)), ("d/x.tar", None), ("b.fq.gz", Some(1))]
        );
        assert_eq!(assigner.file_color(0, "a.fa"), 0);
        // The members' colours come from a range of 2048, from a multiple of 2048, the rest of
        // it unnamed.
        assert_eq!(assigner.file_color(1, "d/x.tar:m.fa"), 2048);
        assert_eq!(assigner.file_color(1, "d/x.tar:dir/n.fq"), 2049);
        // A member of the same name again: the same colour.
        assert_eq!(assigner.file_color(1, "d/x.tar:m.fa"), 2048);
        assert_eq!(assigner.file_color(2, "b.fq.gz"), 1);
        assert_eq!(
            named(&assigner),
            (
                indexed(&[
                    (0, "a.fa"),
                    (1, "b.fq.gz"),
                    (2048, "d/x.tar:m.fa"),
                    (2049, "d/x.tar:dir/n.fq")
                ]),
                2 * 2048
            )
        );
        // Packed: no colour without a name.
        let (remap, packed) = assigner.packed_colors();
        let remap = remap.unwrap();
        assert_eq!(
            packed,
            names(&["a.fa", "b.fq.gz", "d/x.tar:m.fa", "d/x.tar:dir/n.fq"])
        );
        assert_eq!(
            [0, 1, 2048, 2049].map(|color| remap.map(color)),
            [0, 1, 2, 3]
        );
        assert!(assigner.assigns_while_reading());
        assert!(assigner.unmatched_mappings().is_empty());
    }

    #[test]
    fn a_list_maps_archives_and_members_and_the_other_members_get_their_own() {
        let blocks = vec![
            file("d/x.tar.gz", Some(0)),
            file("d/x.tar.gz:s/a.fa", Some(1)),
            file("d/l.fa", Some(2)),
            file("d/y.tar:b.fa", Some(1)),
            file("d/x.tar.gz:missing.fa", Some(2)),
            file("d/y.tar:dir//c.fa", Some(0)),
        ];
        let list = names(&["all", "special", "loose"]);
        let (assigner, to_read) = ColorAssigner::new(blocks, &list).unwrap();
        // Each archive once, where it is first named.
        assert_eq!(
            files(&to_read),
            [("d/x.tar.gz", None), ("d/l.fa", Some(2)), ("d/y.tar", None)]
        );
        assert_eq!(assigner.file_color(0, "d/x.tar.gz:s/a.fa"), 1);
        assert_eq!(assigner.file_color(0, "d/x.tar.gz:other.fa"), 0);
        assert_eq!(assigner.file_color(1, "d/l.fa"), 2);
        assert_eq!(assigner.file_color(2, "d/y.tar:b.fa"), 1);
        // Member paths are compared by their components.
        assert_eq!(assigner.file_color(2, "d/y.tar:dir/c.fa"), 0);
        assert_eq!(assigner.file_color(2, "d/y.tar:e.fa"), 2048);
        assert_eq!(
            named(&assigner),
            (
                indexed(&[
                    (0, "all"),
                    (1, "special"),
                    (2, "loose"),
                    (2048, "d/y.tar:e.fa")
                ]),
                2 * 2048
            )
        );
        let (remap, packed) = assigner.packed_colors();
        assert_eq!(packed, names(&["all", "special", "loose", "d/y.tar:e.fa"]));
        assert_eq!(remap.unwrap().map(2048), 3);
        assert_eq!(assigner.unmatched_mappings(), ["d/x.tar.gz:missing.fa"]);
        assert!(assigner.assigns_while_reading());
    }

    #[test]
    fn a_member_takes_the_mapping_of_the_innermost_archive_it_is_nested_in() {
        let blocks = vec![
            file("d/x.tar", Some(0)),
            file("d/x.tar:in.tgz", Some(1)),
            file("d/x.tar:in.tgz:a:b/deep.tar.xz", Some(2)),
            file("d/x.tar:in.tgz:a:b/deep.tar.xz:own.fa", Some(3)),
            file("d/x.tar:never.tar", Some(3)),
            file("d/y.tar:dir//in.tar:own.fa", Some(1)),
            file("d/y.tar:dir/in.tar", Some(2)),
        ];
        let list = names(&["outer", "inner", "deep", "own"]);
        let (assigner, to_read) = ColorAssigner::new(blocks, &list).unwrap();
        assert_eq!(files(&to_read), [("d/x.tar", None), ("d/y.tar", None)]);
        assert_eq!(assigner.file_color(0, "d/x.tar:a.fa"), 0);
        assert_eq!(assigner.file_color(0, "d/x.tar:in.tgz:b.fa"), 1);
        assert_eq!(
            assigner.file_color(0, "d/x.tar:in.tgz:a:b/deep.tar.xz:c.fa"),
            2
        );
        assert_eq!(
            assigner.file_color(0, "d/x.tar:in.tgz:a:b/deep.tar.xz:own.fa"),
            3
        );
        // A directory's colon ends no archive: `a` is not one.
        assert_eq!(assigner.file_color(0, "d/x.tar:in.tgz:a:c.fa"), 1);
        // With no colour for the outer archive, its other members get their own.
        assert_eq!(assigner.file_color(1, "d/y.tar:dir/in.tar:own.fa"), 1);
        assert_eq!(assigner.file_color(1, "d/y.tar:dir/in.tar:b.fa"), 2);
        assert_eq!(assigner.file_color(1, "d/y.tar:c.fa"), 2048);
        assert_eq!(assigner.unmatched_mappings(), ["d/x.tar:never.tar"]);
        let (remap, packed) = assigner.packed_colors();
        assert_eq!(
            packed,
            names(&["outer", "inner", "deep", "own", "d/y.tar:c.fa"])
        );
        assert_eq!(remap.unwrap().map(2048), 4);
    }

    #[test]
    fn an_archive_or_member_mapped_two_colors_is_an_error() {
        let new = |blocks| ColorAssigner::new(blocks, &[]).map(|(_, to_read)| to_read.len());
        assert!(new(vec![file("x.tar", Some(0)), file("x.tar", Some(1))]).is_err());
        assert!(
            new(vec![
                file("x.tar:a.fa", Some(0)),
                file("x.tar:a.fa", Some(1))
            ])
            .is_err()
        );
        assert!(new(vec![file("x.tar:", Some(0))]).is_err());
        // The same colour twice is the same mapping.
        assert_eq!(
            new(vec![file("x.tar", Some(0)), file("x.tar", Some(0))]).unwrap(),
            1
        );
        let (assigner, _) = ColorAssigner::new(
            vec![file("x.tar", Some(0)), file("x.tar:a.fa", Some(1))],
            &[],
        )
        .unwrap();
        assert!(!assigner.assigns_while_reading());
    }

    struct NoRecords;

    impl DynamicSequencesStream for NoRecords {
        fn read_block(
            &self,
            _block: usize,
            _copy_ident_data: bool,
            _partial_read_copyback: Option<usize>,
            _callback: &mut dyn FnMut(DnaSequence<'_, &[u8]>, SequenceInfo),
        ) {
        }

        fn estimated_base_count(&self, _block: usize) -> u64 {
            0
        }
    }

    #[test]
    fn with_names_given_colorless_files_and_streams_take_their_index() {
        let blocks = vec![
            file("a.fa", None),
            file("x.tar", None),
            GeneralSequenceBlockData::Dynamic((Arc::new(NoRecords), 0)),
            file("b.fa", None),
        ];
        let given = names(&["n0", "n1", "n2", "n3"]);
        let (assigner, to_read) = ColorAssigner::new(blocks, &given).unwrap();
        assert_eq!(
            files(&to_read),
            [
                ("a.fa", Some(0)),
                ("x.tar", None),
                ("dynamic", None),
                ("b.fa", Some(3))
            ]
        );
        assert_eq!(assigner.input_color(2), 2);
        assert_eq!(assigner.file_color(1, "x.tar:m.fa"), 2048);
        let (remap, packed) = assigner.packed_colors();
        assert_eq!(packed, names(&["n0", "n1", "n2", "n3", "x.tar:m.fa"]));
        assert_eq!(remap.unwrap().map(2048), 4);
    }

    #[test]
    fn the_ranges_are_as_large_as_the_bound_allows_from_8_to_2048() {
        assert_eq!(prealloc_count(0, 1), 2048);
        assert_eq!(prealloc_count(100_000, 1000), 2048);
        // 2^18 archives: 2048 colours each would reach 2^29 exactly.
        assert_eq!(prealloc_count(0, 1 << 18), 1024);
        // One archive fewer leaves room for 2048 colours decided, not one more.
        assert_eq!(prealloc_count(2047, (1 << 18) - 1), 2048);
        assert_eq!(prealloc_count(2048, (1 << 18) - 1), 1024);
        // At least 8, even past the bound.
        assert_eq!(prealloc_count(0, 1 << 26), 8);
        assert_eq!(prealloc_count((1 << 29) - 1, 1), 8);
        assert_eq!(prealloc_count(0, 0), 2048);
    }

    /// `a.fa`, then the archives `x.tar` and `y.tar`, read with their members interleaved:
    /// x's first 2048 and one more, y's three, in `x_first` order or y's first.
    fn read_interleaved(x_first: bool) -> ColorAssigner {
        let blocks = vec![file("a.fa", None), file("x.tar", None), file("y.tar", None)];
        let (assigner, _) = ColorAssigner::new(blocks, &[]).unwrap();
        let mut x = vec!["x.tar:m0.fa".to_string(), "x.tar:m1.fa".to_string()];
        // A nested archive's members are the outer archive's.
        x.push("x.tar:in.tgz:a.fa".to_string());
        x.extend((3..2048).map(|member| format!("x.tar:m{member}.fa")));
        x.push("x.tar:last.fa".to_string());
        let y = ["y.tar:n0.fa", "y.tar:n1.fa", "y.tar:n2.fa"];
        let (mut x, mut y) = (x.iter(), y.iter());
        loop {
            let (first, second) = match x_first {
                true => (
                    x.next().map(|name| (1, name.as_str())),
                    y.next().map(|name| (2, *name)),
                ),
                false => (
                    y.next().map(|name| (2, *name)),
                    x.next().map(|name| (1, name.as_str())),
                ),
            };
            if first.is_none() && second.is_none() {
                break;
            }
            for (input, name) in first.into_iter().chain(second) {
                assigner.file_color(input, name);
            }
        }
        assigner
    }

    #[test]
    fn each_archive_gives_its_members_consecutive_colors_range_after_range() {
        let assigner = read_interleaved(true);
        let color = |input, name| assigner.file_color(input, name);
        // x's first range, after a's block, then y's, then x's second.
        assert_eq!(color(1, "x.tar:m0.fa"), 2048);
        assert_eq!(color(2, "y.tar:n0.fa"), 2 * 2048);
        assert_eq!(color(1, "x.tar:m1.fa"), 2049);
        assert_eq!(color(1, "x.tar:in.tgz:a.fa"), 2050);
        assert_eq!(color(1, "x.tar:m2047.fa"), 2 * 2048 - 1);
        assert_eq!(color(2, "y.tar:n2.fa"), 2 * 2048 + 2);
        assert_eq!(color(1, "x.tar:last.fa"), 3 * 2048);
        let names = assigner.color_names();
        assert_eq!(names.len(), 4 * 2048);
        assert!(names[1..2048].iter().all(String::is_empty));
        assert!(names[2 * 2048 + 3..3 * 2048].iter().all(String::is_empty));
        assert!(names[3 * 2048 + 1..].iter().all(String::is_empty));
    }

    #[test]
    fn packed_colors_follow_the_inputs_with_no_gap_whatever_order_they_are_read_in() {
        let x_first = read_interleaved(true);
        let y_first = read_interleaved(false);
        let (remap, packed) = x_first.packed_colors();
        let remap = remap.unwrap();
        // a, then x's 2049 members in order, then y's 3.
        assert_eq!(packed.len(), 1 + 2049 + 3);
        assert_eq!(packed[0], "a.fa");
        assert_eq!(packed[3], "x.tar:in.tgz:a.fa");
        assert_eq!(packed[2049], "x.tar:last.fa");
        assert_eq!(
            packed[2050..],
            names(&["y.tar:n0.fa", "y.tar:n1.fa", "y.tar:n2.fa"])
        );
        let (other_remap, other_packed) = y_first.packed_colors();
        let other_remap = other_remap.unwrap();
        assert_eq!(packed, other_packed);
        for (packed_color, name) in packed.iter().enumerate().skip(1) {
            let input = if name.starts_with("x.tar") { 1 } else { 2 };
            let color = x_first.file_color(input, name);
            let other_color = y_first.file_color(input, name);
            assert_eq!(remap.map(color) as usize, packed_color, "{name}");
            assert_eq!(
                other_remap.map(other_color) as usize,
                packed_color,
                "{name}"
            );
        }
    }

    #[test]
    fn remapped_runs_are_cut_at_the_blocks_and_canonical() {
        let assigner = read_interleaved(true);
        let remap = assigner.packed_colors().0.unwrap();
        // a; x's m2047 and y's n0, consecutive before the remap only; x's last.
        let runs = [
            ColorRun::new(0, 1),
            ColorRun::new(2 * 2048 - 1, 2),
            ColorRun::new(3 * 2048, 1),
        ];
        let mut out = Vec::new();
        // a stays; m2047, last and n0 become 2048, 2049 and 2050: one run.
        assert_eq!(
            remap.map_runs(&runs, &mut out),
            [ColorRun::new(0, 1), ColorRun::new(2048, 3)]
        );
        // y's n0 and n1 alone: one run both before and after.
        assert_eq!(
            remap.map_runs(&[ColorRun::new(2 * 2048, 2)], &mut out),
            [ColorRun::new(2050, 2)]
        );
    }

    #[test]
    fn with_no_archive_read_nothing_is_remapped() {
        let blocks = vec![file("a.fa", None), file("b.fa", None)];
        let (assigner, _) = ColorAssigner::new(blocks, &[]).unwrap();
        assert_eq!(assigner.packed_colors(), (None, names(&["a.fa", "b.fa"])));
    }
}
