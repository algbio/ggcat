use std::ops::Range;

use colors::colors_manager::{
    ColorsManager, ColorsMergeManager, color_types::PartialUnitigsColorStructure,
};
use config::{DEFAULT_PER_CPU_BUFFER_SIZE, MAX_INLINE_UNITIG_SIZE};
use hashes::HashableSequence;
use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::extra_data::{
        SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement, TempBuffer,
    },
    concurrent_filewriter::ConcurrentFileWriter,
    partial_unitigs_extra_data::{IndirectReadInfo, PartialUnitigExtraData, PartialUnitigMode},
};

struct OversizeTempData<CX: ColorsManager> {
    oversize_temp_buffer: Vec<u8>,
    oversize_color_buffer: TempBuffer<PartialUnitigsColorStructure<CX>>,
    range_removal_temp_buffer: Vec<u8>,
}

impl<CX: ColorsManager> OversizeTempData<CX> {
    pub fn new() -> Self {
        Self {
            oversize_temp_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            oversize_color_buffer: PartialUnitigsColorStructure::<CX>::new_temp_buffer(),
            range_removal_temp_buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
        }
    }
}

fn write_oversize_unitigs<CX: ColorsManager>(
    oversize_unitigs_file: &ConcurrentFileWriter,
    oversize_data: &mut OversizeTempData<CX>,
    oversize_unitig_part: CompressedRead,
    joined_color_buffer: &TempBuffer<PartialUnitigsColorStructure<CX>>,
    splice_start_colors_offset: usize,
    splice_end_colors_offset: usize,
) -> IndirectReadInfo {
    // Colors from splice_start - k + 1 to splice_end_colors_offset
    oversize_data.oversize_temp_buffer.clear();

    let extra_length = if CX::COLORS_ENABLED {
        let oversize_color = {
            PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                &mut oversize_data.oversize_color_buffer,
            );

            let joined_color = CX::ColorsMergeManagerType::get_color_from_fully_joined_buffer(
                &joined_color_buffer,
            );

            // Copy only required colors
            CX::ColorsMergeManagerType::join_structures::<false>(
                &mut oversize_data.oversize_color_buffer,
                &joined_color,
                &joined_color_buffer,
                splice_start_colors_offset,
                Some(splice_end_colors_offset - splice_start_colors_offset),
            );

            CX::ColorsMergeManagerType::get_color_from_fully_joined_buffer(
                &oversize_data.oversize_color_buffer,
            )
        };

        oversize_color.encode_extended(
            &oversize_data.oversize_color_buffer,
            &mut oversize_data.oversize_temp_buffer,
            Default::default(),
            0,
            false,
            0,
        );
        oversize_data.oversize_temp_buffer.len()
    } else {
        0
    };

    oversize_unitig_part.copy_to_buffer(&mut oversize_data.oversize_temp_buffer);
    let position = oversize_unitigs_file
        .write(&oversize_data.oversize_temp_buffer[..])
        .unwrap();

    IndirectReadInfo::new(
        position as usize,
        extra_length,
        oversize_unitig_part.bases_count(),
    )
}

pub struct AlignedDynamicCompressedRead {
    buffer: Vec<u8>,
    pub bases_count: usize,
}

impl AlignedDynamicCompressedRead {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            bases_count: 0,
        }
    }

    pub fn remove_range(
        &mut self,
        range_remove_temp_buffer: &mut Vec<u8>,
        splice_start: usize,
        splice_end: usize,
    ) {
        let suffix = self.sub_slice(splice_end..self.bases_count);
        let suffix_bases_count = suffix.bases_count();
        range_remove_temp_buffer.clear();
        suffix.copy_to_buffer(range_remove_temp_buffer);

        self.trim_at(splice_start);

        let suffix_read =
            CompressedRead::new_from_compressed(range_remove_temp_buffer, suffix_bases_count);
        self.append_read(suffix_read, false);
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.bases_count = 0;
    }

    #[inline(always)]
    pub fn get_reference<'a>(&'a self) -> CompressedRead<'a> {
        CompressedRead::new_from_compressed(&self.buffer, self.bases_count)
    }

    pub fn sub_slice<'a>(&'a self, range: Range<usize>) -> CompressedRead<'a> {
        self.get_reference().sub_slice(range)
    }

    pub fn trim_at(&mut self, bases_count: usize) {
        debug_assert!(self.bases_count >= bases_count);
        let bytes_count = bases_count.div_ceil(4);
        self.buffer.truncate(bytes_count);
        self.bases_count = bases_count;
    }

    pub fn append_read(&mut self, read: CompressedRead, rc: bool) {
        // self.append_read(read, rc);
        read.copy_to_buffer_with_offset(&mut self.buffer, self.bases_count % 4, rc);
        self.bases_count += read.bases_count();
    }
}

#[track_caller]
fn splice_join_operation<'a, CX: ColorsManager>(
    oversize_unitigs_file: &ConcurrentFileWriter,
    oversize_temp_data: &'a mut OversizeTempData<CX>,
    k: usize,
    joined_read: &mut AlignedDynamicCompressedRead,
    splice_start: usize,
    splice_end: usize,
    splice_end_colors_offset: usize,
    joined_color: &mut TempBuffer<PartialUnitigsColorStructure<CX>>,
) -> IndirectReadInfo {
    let indirect_middle = write_oversize_unitigs::<CX>(
        oversize_unitigs_file,
        oversize_temp_data,
        joined_read.sub_slice(splice_start..splice_end),
        joined_color,
        splice_start - k + 1,
        splice_end_colors_offset,
    );

    joined_read.remove_range(
        &mut oversize_temp_data.range_removal_temp_buffer,
        splice_start,
        splice_end,
    );

    // Remove processed range colors
    CX::ColorsMergeManagerType::remove_colors_range(
        joined_color,
        (splice_start - k + 1)..splice_end_colors_offset,
    );

    indirect_middle
}

pub struct JoinedSequence<'a, CX: ColorsManager> {
    pub sequence: CompressedRead<'a>,
    pub extra: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    pub extra_buffer: &'a TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
}

pub struct IndirectSequencesJoiner<CX: ColorsManager> {
    k: usize,
    oversize_unitigs_file: ConcurrentFileWriter,
    pub sequence: AlignedDynamicCompressedRead,
    indirection_start: usize,
    pub extra_buffer: (
        TempBuffer<PartialUnitigsColorStructure<CX>>,
        Vec<IndirectReadInfo>,
    ),
    #[cfg(feature = "support_kmer_counters")]
    counters: io::partial_unitigs_extra_data::SequenceAbundance,
    oversize_buffers: OversizeTempData<CX>,
}

impl<CX: ColorsManager> IndirectSequencesJoiner<CX> {
    pub fn new(k: usize, oversize_unitigs_file: ConcurrentFileWriter) -> Self {
        Self {
            k,
            oversize_unitigs_file,
            sequence: AlignedDynamicCompressedRead::new(),
            indirection_start: 0,
            extra_buffer:
                PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::new_temp_buffer(),
            oversize_buffers: OversizeTempData::<CX>::new(),
            #[cfg(feature = "support_kmer_counters")]
            counters: Default::default(),
        }
    }

    pub fn get_sequence_ref(&self) -> &AlignedDynamicCompressedRead {
        &self.sequence
    }

    pub fn reset(&mut self) {
        self.sequence.clear();
        self.indirection_start = 0;
        self.extra_buffer.1.clear();
        #[cfg(feature = "support_kmer_counters")]
        {
            self.counter = Default::default();
        }
        PartialUnitigsColorStructure::<CX>::clear_temp_buffer(&mut self.extra_buffer.0);
    }

    pub fn append_sequence(
        &mut self,
        sequence: CompressedRead,
        extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        overlapping_characters: usize,
        is_rc: bool,
        extra_buffer: &TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
        colors_count: Option<usize>,
    ) {
        #[cfg(feature = "support_kmer_counters")]
        {
            let (other_first, other_last) = if is_rc {
                (extra.counters.last, extra.counters.first)
            } else {
                (extra.counters.first, extra.counters.last)
            };

            self.counters.first = if self.sequence.bases_count == 0 {
                other_first
            } else {
                self.counters.first
            };
            self.counters.sum += extra.counters.sum - other_first;
            self.counters.last = other_last;
        }

        // INDIRECTION:
        // only one inline: join and update the indirect data, make another indirection if inline size exceeds threshold
        // both indirect: make an indirection of the middle part, create a single indirect unitig.

        let indirection_threshold = (self.k * 4).max(MAX_INLINE_UNITIG_SIZE);
        let left_read_bases_count = self.sequence.bases_count;
        let right_sequence = if is_rc {
            sequence.sub_slice(0..sequence.bases_count() - overlapping_characters)
        } else {
            sequence.sub_slice(overlapping_characters..sequence.bases_count())
        };
        let right_skip_colors = if CX::COLORS_ENABLED && overlapping_characters > 0 {
            overlapping_characters - self.k + 1
        } else {
            0
        };

        self.sequence.append_read(right_sequence, is_rc);

        if is_rc {
            // Copy only required colors
            CX::ColorsMergeManagerType::join_structures::<true>(
                &mut self.extra_buffer.0,
                &extra.colors,
                &extra_buffer.0,
                right_skip_colors,
                colors_count,
            );
        } else {
            // Copy only required colors
            CX::ColorsMergeManagerType::join_structures::<false>(
                &mut self.extra_buffer.0,
                &extra.colors,
                &extra_buffer.0,
                right_skip_colors,
                colors_count,
            );
        }

        let joined_sequence_bases_count = self.sequence.bases_count;

        match (self.extra_buffer.1.len(), extra.mode.clone()) {
            (0, PartialUnitigMode::Inline) => {
                let total_bases_count = self.sequence.bases_count;

                // INDIRECTION:
                // both inline: join and check if threshold reached, in case make the unitig indirect
                if total_bases_count > indirection_threshold {
                    // Splice the current read middle part
                    let indirect_reference = splice_join_operation::<CX>(
                        &self.oversize_unitigs_file,
                        &mut self.oversize_buffers,
                        self.k,
                        &mut self.sequence,
                        self.k,
                        joined_sequence_bases_count - self.k,
                        joined_sequence_bases_count - self.k,
                        &mut self.extra_buffer.0,
                    );

                    self.extra_buffer.1.push(indirect_reference);
                    self.indirection_start = self.k;
                }
            }
            (
                0,
                PartialUnitigMode::Indirect {
                    indirection_start,
                    indirections_range,
                },
            ) => {
                let indirection_start = if is_rc {
                    sequence.bases_count() - indirection_start //sequence.bases_count() - indirection_start
                } else {
                    indirection_start
                } - overlapping_characters;

                let new_indirection_start = indirection_start + left_read_bases_count;
                if new_indirection_start > indirection_threshold {
                    // Add another left indirection
                    let indirect_reference = splice_join_operation::<CX>(
                        &self.oversize_unitigs_file,
                        &mut self.oversize_buffers,
                        self.k,
                        &mut self.sequence,
                        self.k,
                        new_indirection_start,
                        new_indirection_start - self.k + 1,
                        &mut self.extra_buffer.0,
                    );

                    self.extra_buffer.1.push(indirect_reference);
                    let right_range_start = self.extra_buffer.1.len();
                    self.extra_buffer
                        .1
                        .extend_from_slice(&extra_buffer.1[indirections_range.clone()]);
                    if is_rc {
                        self.extra_buffer.1[right_range_start..].reverse();
                        self.extra_buffer.1[right_range_start..]
                            .iter_mut()
                            .for_each(|ic| ic.flip_rc());
                    }

                    self.indirection_start = self.k;
                } else {
                    self.indirection_start = new_indirection_start;

                    let right_range_start = self.extra_buffer.1.len();
                    self.extra_buffer
                        .1
                        .extend_from_slice(&extra_buffer.1[indirections_range.clone()]);
                    if is_rc {
                        self.extra_buffer.1[right_range_start..].reverse();
                        self.extra_buffer.1[right_range_start..]
                            .iter_mut()
                            .for_each(|ic| ic.flip_rc());
                    }
                }
            }
            (_, PartialUnitigMode::Inline) => {
                let right_size = self.sequence.bases_count - self.indirection_start;

                if right_size > indirection_threshold {
                    // Add another indirection
                    let indirect_reference = splice_join_operation::<CX>(
                        &self.oversize_unitigs_file,
                        &mut self.oversize_buffers,
                        self.k,
                        &mut self.sequence,
                        self.indirection_start,
                        joined_sequence_bases_count - self.k,
                        (joined_sequence_bases_count - self.k) - self.k + 1,
                        &mut self.extra_buffer.0,
                    );

                    self.extra_buffer.1.push(indirect_reference);
                }
            }
            (
                _,
                PartialUnitigMode::Indirect {
                    indirection_start: right_indirection_start,
                    indirections_range: right_indirections_range,
                },
            ) => {
                let new_indirection_start = left_read_bases_count
                    + if is_rc {
                        sequence.bases_count() - right_indirection_start //sequence.bases_count() - indirection_start
                    } else {
                        right_indirection_start
                    }
                    - overlapping_characters;

                let indirect_reference = splice_join_operation::<CX>(
                    &self.oversize_unitigs_file,
                    &mut self.oversize_buffers,
                    self.k,
                    &mut self.sequence,
                    self.indirection_start,
                    new_indirection_start,
                    new_indirection_start - self.k * 2 + 2,
                    &mut self.extra_buffer.0,
                );

                self.extra_buffer.1.push(indirect_reference);
                let right_range_start = self.extra_buffer.1.len();
                self.extra_buffer
                    .1
                    .extend_from_slice(&extra_buffer.1[right_indirections_range.clone()]);
                if is_rc {
                    self.extra_buffer.1[right_range_start..].reverse();
                    self.extra_buffer.1[right_range_start..]
                        .iter_mut()
                        .for_each(|ic| ic.flip_rc());
                }
            }
        };
    }

    pub fn get_sequence<'a>(&'a self) -> JoinedSequence<'a, CX> {
        let colors =
            CX::ColorsMergeManagerType::get_color_from_fully_joined_buffer(&self.extra_buffer.0);

        JoinedSequence {
            sequence: self.sequence.get_reference(),
            extra: PartialUnitigExtraData {
                colors,
                mode: if self.extra_buffer.1.len() > 0 {
                    PartialUnitigMode::Indirect {
                        indirection_start: self.indirection_start,
                        indirections_range: 0..self.extra_buffer.1.len(),
                    }
                } else {
                    PartialUnitigMode::Inline
                },
                #[cfg(feature = "support_kmer_counters")]
                counters: self.counters,
            },
            extra_buffer: &self.extra_buffer,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use colors::non_colored::NonColoredManager;
    use io::{
        compressed_read::CompressedReadIndipendent,
        concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
        concurrent_filewriter::ConcurrentFileWriter,
    };

    use super::IndirectSequencesJoiner;
    use io::partial_unitigs_extra_data::{PartialUnitigExtraData, PartialUnitigMode};

    fn inline_extra() -> PartialUnitigExtraData<NonColoredManager> {
        PartialUnitigExtraData {
            colors: NonColoredManager,
            mode: PartialUnitigMode::Inline,
            #[cfg(feature = "support_kmer_counters")]
            counters: sequence_output::structured_sequences::SequenceAbundance {
                first: 1,
                sum: 1,
                last: 1,
            },
        }
    }

    fn new_temp_path(prefix: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{}_{}.bin", std::process::id(), now))
    }

    fn append_plain(
        joiner: &mut IndirectSequencesJoiner<NonColoredManager>,
        seq: &str,
        overlap: usize,
        is_rc: bool,
    ) {
        let mut storage = Vec::new();
        let read = CompressedReadIndipendent::from_plain(seq.as_bytes(), &mut storage);
        let extra = inline_extra();
        let extra_buffer = <PartialUnitigExtraData<NonColoredManager>>::new_temp_buffer();
        joiner.append_sequence(
            read.as_reference(&storage),
            &extra,
            overlap,
            is_rc,
            &extra_buffer,
            None,
        );
    }

    #[test]
    fn inline_join_various_lengths_forward() {
        let k = 5;
        let cases = [
            ("AAACCCCTA", "CCCTAT", "AAACCCCTAT"),
            ("GATTACAGT", "ACAGTTTGGGA", "GATTACAGTTTGGGA"),
            ("TGCATGCATGCA", "ATGCATTT", "TGCATGCATGCATTT"),
        ];

        for (left, right, expected) in cases {
            let temp_path = new_temp_path("joiner_inline_forward");
            let writer = ConcurrentFileWriter::create(&temp_path).unwrap();
            let mut joiner = IndirectSequencesJoiner::<NonColoredManager>::new(k, writer.clone());

            append_plain(&mut joiner, left, 0, false);
            append_plain(&mut joiner, right, k, false);

            let joined = joiner.get_sequence();

            assert_eq!(joined.sequence.to_string(), expected);
            assert!(matches!(joined.extra.mode, PartialUnitigMode::Inline));
            assert!(joined.extra_buffer.1.is_empty());
            assert_eq!(writer.file().metadata().unwrap().len(), 0);

            fs::remove_file(&temp_path).unwrap();
        }
    }

    #[test]
    fn inline_join_reverse_complement() {
        let k = 4;
        let temp_path = new_temp_path("joiner_inline_rc");
        let writer = ConcurrentFileWriter::create(&temp_path).unwrap();
        let mut joiner = IndirectSequencesJoiner::<NonColoredManager>::new(k, writer.clone());

        // Right sequence shares one k-mer with left only when used in reverse complement.
        append_plain(&mut joiner, "AACCTTTT", 0, false);
        append_plain(&mut joiner, "CGTAAAAA", k, true);

        let joined = joiner.get_sequence();

        assert_eq!(joined.sequence.to_string(), "AACCTTTTTACG");
        assert!(matches!(joined.extra.mode, PartialUnitigMode::Inline));
        assert!(joined.extra_buffer.1.is_empty());
        assert_eq!(writer.file().metadata().unwrap().len(), 0);

        fs::remove_file(&temp_path).unwrap();
    }
}
