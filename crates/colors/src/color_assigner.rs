//! The colours of a coloured graph's inputs, decided as the inputs are read: an archive's
//! members can each have a colour of their own, so the colours, and their names for the
//! colormap, are all known only once the inputs are read.

use anyhow::{Result, bail};
use config::ColorIndexType;
use io::binary_reader::{archive_prefixes, split_archive_member};
use io::sequences_stream::general::GeneralSequenceBlockData;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

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
/// the others. They are numbered in the order they
/// are read in, the same within an archive, but not across archives from one run to the next.
pub struct ColorAssigner {
    /// By the index of the input in the inputs to read, as [`Self::new`] gives them.
    inputs: Vec<InputColors>,
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

/// The names of the colours so far, by their index, and the colours given to members as they
/// were read, by the members' names.
struct NamedColors {
    names: Vec<String>,
    members: HashMap<String, ColorIndexType>,
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

        let assigner = Self {
            inputs,
            named: Mutex::new(NamedColors {
                names: named,
                members: HashMap::new(),
            }),
        };
        Ok((assigner, to_read))
    }

    /// The colour of the file `name` of input `input`, as it starts being read. A member of an
    /// archive, named `archive:member`, has the colour mapped to it, or else to the innermost
    /// archive it is nested in that is mapped one, or else to the archive, or else a new one
    /// named `name`: the same for every member of that name.
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
        let NamedColors { names, members } = &mut *named;
        *members.entry(name.to_owned()).or_insert_with(|| {
            names.push(name.to_owned());
            (names.len() - 1) as ColorIndexType
        })
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

    /// The names of all the colours so far, by their index: once the inputs are read, all of
    /// them, those given to members included.
    pub fn color_names(&self) -> Vec<String> {
        self.named.lock().names.clone()
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
        assert_eq!(assigner.file_color(1, "d/x.tar:m.fa"), 2);
        assert_eq!(assigner.file_color(1, "d/x.tar:dir/n.fq"), 3);
        // A member of the same name again: the same colour.
        assert_eq!(assigner.file_color(1, "d/x.tar:m.fa"), 2);
        assert_eq!(assigner.file_color(2, "b.fq.gz"), 1);
        assert_eq!(
            assigner.color_names(),
            names(&["a.fa", "b.fq.gz", "d/x.tar:m.fa", "d/x.tar:dir/n.fq"])
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
        assert_eq!(assigner.file_color(2, "d/y.tar:e.fa"), 3);
        assert_eq!(
            assigner.color_names(),
            names(&["all", "special", "loose", "d/y.tar:e.fa"])
        );
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
        assert_eq!(assigner.file_color(1, "d/y.tar:c.fa"), 4);
        assert_eq!(assigner.unmatched_mappings(), ["d/x.tar:never.tar"]);
        assert_eq!(
            assigner.color_names(),
            names(&["outer", "inner", "deep", "own", "d/y.tar:c.fa"])
        );
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
        assert_eq!(assigner.file_color(1, "x.tar:m.fa"), 4);
        assert_eq!(
            assigner.color_names(),
            names(&["n0", "n1", "n2", "n3", "x.tar:m.fa"])
        );
    }
}
