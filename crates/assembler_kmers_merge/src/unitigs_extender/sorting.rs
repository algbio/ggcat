use std::{iter::repeat, mem::take, ops::Range};

use binary_heap_plus::BinaryHeap;
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::temp_reads::creads_utils::DeserializedReadIndependent;

struct SuffixStackElement {
    /// The start index of the current chunk of reads being processed
    index: usize,
    /// The exclusive last index of the current chunk of reads being processed
    last_index: usize,
    /// The start index of the sorted chunks subslice referred to the current stack element
    sorted_chunks_start: usize,
}

struct PrefixStackElement {
    index: usize,
    last_index: usize,
    /// The suffix length of the reads in this stack element.
    suffix_length: usize,
}

#[derive(Debug)]
struct SortedChunk {
    start: usize,
    end: usize,
    is_sorted: bool,
}

#[derive(Debug)]
struct SortingHeapElement {
    index: usize,
    last: usize,
}

#[derive(Clone)]
struct Supertig {
    read: CompressedReadIndipendent,
    superkmers_count: usize,
    multiplicity: usize,
    next: usize,
}

#[derive(Default)]
pub struct SortingExtender {
    /// Holds the skip values for each read. A skip value x greater than 1 means that the next x reads
    /// were already processed and thus are sorted.
    /// This can be used to immediately create a sorted chunk without reading again all the reads.
    forward_skip: Vec<usize>,

    /// The unified array of sorted chunks. Each sorted chunk belongs to a specific processing stack element.
    sorted_chunks: Vec<SortedChunk>,

    /// The size of the suffixes for each read, that is the number of bases after the minimizer position.
    /// This value must be updated as soon as some suffix kmers are used,
    /// in practice reducing the suffix (and read) size and
    /// landing on a reduced problem.
    suffix_sizes: Vec<usize>,

    /// An array to map prefix-sorted elements to their original indices.
    elements_mapping: Vec<usize>,

    /// Adjacent Longest Common Centered Suffix (LCCS) array in the suffix sorted reads.
    /// lcs_arra[0] contains the LCS between reads[0] and reads[1].
    lccs_array: Vec<usize>,

    /// Adjacent Longest Common Centered Prefix (LCCP) array in the prefix sorted reads.
    /// lcp_array[0] contains the LCCP between reads[mapping[0]] and reads[mapping[1]].
    lccp_array: Vec<usize>,

    /// The list of supertigs
    supertigs: Vec<Supertig>,

    /// The mapping between supertigs and superkmers. This mapping is in the same order as the sorted supertigs,
    /// so to access it the elements_mapping must be used.
    supertigs_mapping: Vec<usize>,

    /// The stack of current portions of processed elements.
    processing_stack: Vec<SuffixStackElement>,

    /// The stack of current prefixes.
    prefix_stack: Vec<PrefixStackElement>,

    /// Min heap storage to allow merge sorting of the sorted chunks.
    sorting_heap_data: Vec<SortingHeapElement>,

    /// Prefix merge sort support vector. element indices are copied here
    /// to allow consistent access fo the various chunks during sorting.
    sorting_support_vec: Vec<usize>,
}

// fn check_kmers<E>(
//     k: usize,
//     reads: &[DeserializedReadIndependent<E>],
//     superkmers_storage: &Vec<u8>,
//     supertigs: &[(CompressedRead, usize)],
// ) {
//     let mut kmers = vec![];
//     let mut stkmers = vec![];
//     for read in reads.iter() {
//         let kmers_count = read.read.bases_count() - k + 1;
//         for i in 0..kmers_count {
//             let kmer = read
//                 .read
//                 .as_reference(superkmers_storage)
//                 .sub_slice(i..i + k);
//             kmers.push((kmer.to_string(), read.multiplicity));
//         }
//     }

//     for st in supertigs.iter() {
//         let kmers_count = st.0.bases_count() - k + 1;
//         for i in 0..kmers_count {
//             let kmer = st.0.sub_slice(i..i + k);
//             stkmers.push((kmer.to_string(), st.1));
//         }
//     }

//     kmers.sort_by_cached_key(|k| k.0.to_string());
//     stkmers.sort_by_cached_key(|k| k.0.to_string());

//     for kmer in stkmers.windows(2) {
//         if kmer[0].0 == kmer[1].0 {
//             for read in reads.iter() {
//                 println!(
//                     "READ: {} with multiplicity {} and offset {}",
//                     read.read.as_reference(superkmers_storage).to_string(),
//                     read.multiplicity,
//                     read.minimizer_pos
//                 );
//             }

//             for supertig in supertigs.iter() {
//                 println!(
//                     "Supertig: {} with multiplicity {}",
//                     supertig.0.to_string(),
//                     supertig.1
//                 );
//             }

//             panic!(
//                 "Found duplicate supertig kmer: {} with multiplicities {} and {}",
//                 kmer[0].0, kmer[0].1, kmer[1].1
//             );
//         }
//     }

//     kmers.dedup_by_key(|k| k.0.to_string());
//     stkmers.dedup_by_key(|k| k.0.to_string());

//     // println!(
//     //     "Kmers: {} stk: {} total: {}",
//     //     kmers.len(),
//     //     stkmers.len(),
//     //     supertigs.len()
//     // );
//     assert_eq!(kmers.len(), stkmers.len());
//     // todo!();
// }

impl SortingExtender {
    /// Processes elements in range, assuming they are prefix sorted and share the same suffix of length `suffix_length`.
    /// Reduces every read suffix to exactly `target_suffix_length` bases, and outputs the used kmers
    pub fn process_reads_block<'a, E: Copy>(
        &mut self,
        reads: &[DeserializedReadIndependent<E>],
        superkmers_storage: &'a Vec<u8>,
        range: Range<usize>,
        k: usize,
        suffix_length: usize,
        target_suffix_length: usize,
        mut output_read: impl FnMut(CompressedRead<'a>, usize),
    ) {
        // Compute the LCCP array for the current range of reads.
        {
            self.lccp_array.clear();
            for idx in range.clone().skip(1) {
                let a = &reads[self.elements_mapping[idx - 1]];
                let b = &reads[self.elements_mapping[idx]];
                self.lccp_array.push(unsafe {
                    a.read
                        .get_centered_prefix_difference(
                            &b.read,
                            superkmers_storage,
                            a.minimizer_pos as usize,
                            b.minimizer_pos as usize,
                        )
                        .0
                })
            }
            // Last one has no matches with the following element
            self.lccp_array.push(0);
        }

        self.prefix_stack.clear();
        self.prefix_stack.push(PrefixStackElement {
            index: range.start,
            last_index: range.end,
            suffix_length: suffix_length,
        });

        while let Some(prefix_element) = self.prefix_stack.pop() {
            let mut element_target_index = prefix_element.index;
            let target_index_end = prefix_element.last_index;
            let shared_suffix = prefix_element.suffix_length;

            while element_target_index < target_index_end {
                let reference = &reads[self.elements_mapping[element_target_index]];
                let reference_index = element_target_index;

                element_target_index += 1;

                let mut minimum_prefix_share = reference.minimizer_pos as usize;
                if minimum_prefix_share + shared_suffix < k {
                    // No more kmers to output, skip this read
                    self.suffix_sizes[reference_index] = 0;
                    continue;
                }

                let mut multiplicity = reference.multiplicity as usize;

                while element_target_index < target_index_end {
                    let next_read = &reads[self.elements_mapping[element_target_index]];

                    let left_matching = self.lccp_array[element_target_index - 1 - range.start];

                    let total_matching = left_matching + shared_suffix;
                    if total_matching < k {
                        // Found a superkmer not sharing the kmer, break the loop
                        break;
                    }

                    minimum_prefix_share = minimum_prefix_share.min(left_matching);
                    multiplicity += next_read.multiplicity as usize;
                    element_target_index += 1;
                }

                // reference_index..target_index_start contains all the reads that match at least k characters.

                let prefix_limited_suffix = k - minimum_prefix_share;
                let suffix_limited_suffix = target_suffix_length + 1;
                let leftmost_allowed_suffix = prefix_limited_suffix.max(suffix_limited_suffix);

                output_read(
                    reference.read.as_reference(superkmers_storage).sub_slice(
                        (reference.minimizer_pos as usize + leftmost_allowed_suffix - k)
                            ..(reference.minimizer_pos as usize + shared_suffix),
                    ),
                    multiplicity,
                );

                if leftmost_allowed_suffix != target_suffix_length + 1 {
                    self.prefix_stack.push(PrefixStackElement {
                        index: reference_index,
                        last_index: element_target_index,
                        suffix_length: leftmost_allowed_suffix - 1,
                    });
                }
            }
        }

        for idx in range {
            self.suffix_sizes[idx] = target_suffix_length;
        }
    }

    pub fn process_reads<E: Copy>(
        &mut self,
        reads: &mut [DeserializedReadIndependent<E>],
        superkmers_storage: &Vec<u8>,
        k: usize,
        mut output_read: impl FnMut(CompressedRead, usize),
    ) {
        // 0. Sort (reversed) by suffix and compute a lccs array
        {
            self.lccs_array.clear();
            reads.sort_unstable_by(|a, b| unsafe {
                let ar = a
                    .read
                    .get_centered_suffix_difference(
                        &b.read,
                        superkmers_storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse();

                let br = a
                    .read
                    .get_centered_suffix_difference(
                        &b.read,
                        superkmers_storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse();
                assert_eq!(ar, br);
                ar
            });

            for idx in 1..reads.len() {
                let a = &reads[idx - 1];
                let b = &reads[idx];
                self.lccs_array.push(unsafe {
                    a.read
                        .get_centered_suffix_difference(
                            &b.read,
                            superkmers_storage,
                            a.minimizer_pos as usize,
                            b.minimizer_pos as usize,
                        )
                        .0
                })
            }
            // Last one has no matches with the following element
            self.lccs_array.push(0);
        }

        // 1. Save the suffixes lengths for each read
        {
            self.suffix_sizes.clear();
            for el in reads.iter() {
                self.suffix_sizes
                    .push(el.read.bases_count() - el.minimizer_pos as usize);
            }
        }

        // 2. Reinitialize the arrays
        {
            self.elements_mapping.clear();
            self.elements_mapping.extend(0..reads.len());

            self.supertigs.clear();

            self.supertigs_mapping.clear();
            self.supertigs_mapping
                .extend(repeat(usize::MAX).take(reads.len()));

            self.forward_skip.clear();
            self.forward_skip.extend(repeat(1).take(reads.len()));
        }

        self.sorted_chunks.clear();

        let mut next_position = 0;

        let remapped_minimizer_elements = reads;

        'process_elements: while self.processing_stack.len() > 0
            || next_position < self.elements_mapping.len()
        {
            if self.processing_stack.is_empty() {
                // The stack is empty, so we reached a 'split' point that cleanly separates the superkmers.
                // Add the next element in the stack (along with its one-sized sorted chunk).
                self.processing_stack.push(SuffixStackElement {
                    index: next_position,
                    last_index: next_position + 1,
                    sorted_chunks_start: self.sorted_chunks.len(),
                });
                self.sorted_chunks.push(SortedChunk {
                    start: next_position,
                    end: next_position + 1,
                    is_sorted: false,
                });
            }

            let last_stack_element = self.processing_stack.last_mut().unwrap();

            let suffix_stack_block_start = last_stack_element.index;
            let suffix_stack_block_last = &mut last_stack_element.last_index;
            let sorted_chunks_start = last_stack_element.sorted_chunks_start;

            // The needed suffix is the minimum suffix size required
            // to have a chance of having a full kmer match in the current stack element
            let needed_suffix = self.suffix_sizes[suffix_stack_block_start];

            // Try to extend the current stack elements until the suffix is not enough
            while *suffix_stack_block_last < self.elements_mapping.len()
                && self.lccs_array[*suffix_stack_block_last - 1] >= needed_suffix
            {
                // A longer suffix is found, process it before continuing
                if self.suffix_sizes[*suffix_stack_block_last] > needed_suffix {
                    let next_position = *suffix_stack_block_last;

                    self.processing_stack.push(SuffixStackElement {
                        index: next_position,
                        last_index: next_position + 1,
                        sorted_chunks_start: self.sorted_chunks.len(),
                    });
                    self.sorted_chunks.push(SortedChunk {
                        start: next_position,
                        end: next_position + 1,
                        is_sorted: false,
                    });

                    continue 'process_elements;
                }

                const MIN_SORTED_SIZE: usize = 64;
                let last_sorted_chunk: &mut SortedChunk;

                // If the last sorted chunk is not sorted, or the next suffix is not large enough to be sorted,
                // set the current sorted chunk as not sorted and extend it with the next suffix chunk.
                if {
                    last_sorted_chunk = self.sorted_chunks.last_mut().unwrap();

                    let next_sorted =
                        self.forward_skip[*suffix_stack_block_last] >= MIN_SORTED_SIZE;

                    (!last_sorted_chunk.is_sorted && !next_sorted)
                        || (last_sorted_chunk.end - last_sorted_chunk.start
                            + self.forward_skip[*suffix_stack_block_last])
                            < MIN_SORTED_SIZE
                } {
                    last_sorted_chunk.end =
                        *suffix_stack_block_last + self.forward_skip[*suffix_stack_block_last];
                    last_sorted_chunk.is_sorted = false;
                }
                // Else, push a new sorted chunk with the current suffix position
                else {
                    self.sorted_chunks.push(SortedChunk {
                        start: *suffix_stack_block_last,
                        end: *suffix_stack_block_last + self.forward_skip[*suffix_stack_block_last],
                        is_sorted: self.forward_skip[*suffix_stack_block_last] >= MIN_SORTED_SIZE,
                    });
                }
                *suffix_stack_block_last += self.forward_skip[*suffix_stack_block_last];
            }

            // We have now to sort the prefixes of the reads in the current suffix range and
            // then find all the kmers sharing the whole needed suffix.

            self.sorting_heap_data.clear();
            let suffix_stack_block_last = *suffix_stack_block_last;

            // Ensure that all the sorted chunks are actually sorted
            for chunk in &mut self.sorted_chunks[sorted_chunks_start..] {
                if !chunk.is_sorted {
                    self.elements_mapping[chunk.start..chunk.end].sort_unstable_by(|a, b| unsafe {
                        let a = &remapped_minimizer_elements[*a];
                        let b = &remapped_minimizer_elements[*b];
                        a.read
                            .get_centered_prefix_difference(
                                &b.read,
                                superkmers_storage,
                                a.minimizer_pos as usize,
                                b.minimizer_pos as usize,
                            )
                            .1
                            .reverse()
                    });
                    chunk.is_sorted = true;
                }

                // Load the chunks into the heap used for merge sorting all the chunks together
                self.sorting_heap_data.push(SortingHeapElement {
                    index: chunk.start,
                    last: chunk.end,
                });
            }

            // Merge sort all the chunks together
            if self.sorting_heap_data.len() > 1 {
                self.sorting_support_vec.clear();
                self.sorting_support_vec.extend_from_slice(
                    &self.elements_mapping[suffix_stack_block_start..suffix_stack_block_last],
                );

                let mut sorting_heap = BinaryHeap::from_vec_cmp(
                    take(&mut self.sorting_heap_data),
                    |a: &SortingHeapElement, b: &SortingHeapElement| {
                        let a = &remapped_minimizer_elements
                            [self.sorting_support_vec[a.index - suffix_stack_block_start]];
                        let b = &remapped_minimizer_elements
                            [self.sorting_support_vec[b.index - suffix_stack_block_start]];
                        unsafe {
                            a.read
                                .get_centered_prefix_difference(
                                    &b.read,
                                    superkmers_storage,
                                    a.minimizer_pos as usize,
                                    b.minimizer_pos as usize,
                                )
                                .1
                        }
                    },
                );

                // Sort the suffixes by merging chunks (keep track of the chunks in the stack using a common vector)
                let mut write_index = suffix_stack_block_start;
                while sorting_heap.len() > 0 {
                    let mut element = sorting_heap.peek_mut().unwrap();
                    self.elements_mapping[write_index] =
                        self.sorting_support_vec[element.index - suffix_stack_block_start];
                    if element.index + 1 >= element.last {
                        // A chunk was completely sorted
                        drop(element);
                        sorting_heap.pop();
                    } else {
                        element.index += 1;
                    }

                    write_index += 1;
                }

                self.sorting_heap_data = sorting_heap.into_vec();
            }

            // At this point we have all the reads matching at least the needed suffix size in chunks,
            // in the array sorted_chunks[sorted_chunks_start..]
            // For each of such kmers, find the leftmost position that is common to all the reads in the current suffix range.

            // The parent suffix size, we must not exceed this value as there could be other reads sharing a kmer if we overshoot it.
            let max_allowed_suffix_prev = if self.processing_stack.len() <= 1 {
                0
            } else {
                self.suffix_sizes[self.processing_stack[self.processing_stack.len() - 2].index]
            };

            // The post suffix size, that is the suffix share of this block with the next read.
            let max_allowed_suffix_post = self.lccs_array[suffix_stack_block_last - 1];

            let max_allowed_suffix = max_allowed_suffix_prev.max(max_allowed_suffix_post);

            assert!(max_allowed_suffix < needed_suffix);

            self.process_reads_block(
                remapped_minimizer_elements,
                superkmers_storage,
                suffix_stack_block_start..suffix_stack_block_last,
                k,
                needed_suffix,
                max_allowed_suffix,
                |a, b| output_read(a, b),
            );

            self.forward_skip[suffix_stack_block_start] =
                suffix_stack_block_last - suffix_stack_block_start;

            next_position = suffix_stack_block_last;

            if max_allowed_suffix == max_allowed_suffix_prev {
                let last_stack_element = self.processing_stack.pop().unwrap();
                self.sorted_chunks
                    .truncate(last_stack_element.sorted_chunks_start);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use io::compressed_read::CompressedReadIndipendent;

    use super::*;

    fn create_read(
        read: &[u8],
        mpos: u16,
        storage: &mut Vec<u8>,
    ) -> DeserializedReadIndependent<()> {
        DeserializedReadIndependent {
            read: CompressedReadIndipendent::from_plain(read, storage),
            minimizer_pos: mpos,
            multiplicity: 1,
            ..Default::default()
        }
    }

    #[test]
    fn test_sorting() {
        let mut storage = vec![];

        let mut reads = vec![
            create_read(b"TAAGTTCCAATTTCAGACCATCTCTTTGTGAA", 0, &mut storage),
            create_read(b"TAAGTTCCAATTCCAAACTCTTTGTGAATGCA", 0, &mut storage),
        ];

        reads.sort_unstable_by(|a, b| unsafe {
            println!(
                "Comparing {} with {} result: {:?}",
                a.read
                    .as_reference(&storage)
                    .sub_slice(a.minimizer_pos as usize..a.read.bases_count())
                    .to_string(),
                b.read
                    .as_reference(&storage)
                    .sub_slice(b.minimizer_pos as usize..b.read.bases_count())
                    .to_string(),
                a.read
                    .get_centered_suffix_difference(
                        &b.read,
                        &storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse()
            );

            a.read
                .get_centered_suffix_difference(
                    &b.read,
                    &storage,
                    a.minimizer_pos as usize,
                    b.minimizer_pos as usize,
                )
                .1
                .reverse()
        });

        println!("DEBUG NOW!!!!");
        for i in 0..1 {
            let a = &reads[i];
            let b = &reads[i + 1];
            println!(
                "A: {}",
                a.read
                    .as_reference(&storage)
                    .sub_slice(a.minimizer_pos as usize..a.read.bases_count())
                    .to_string()
            );
            println!(
                "B: {}",
                b.read
                    .as_reference(&storage)
                    .sub_slice(b.minimizer_pos as usize..b.read.bases_count())
                    .to_string()
            );
            println!("Compare at indes: {}", i);
            let res = unsafe {
                a.read
                    .get_centered_suffix_difference(
                        &b.read,
                        &storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse()
            };
            println!("Sorting result: {:?}", res);
        }

        let mut correct_reads = reads.clone();

        correct_reads.sort_unstable_by(|a, b| unsafe {
            let a = a
                .read
                .as_reference(&storage)
                .sub_slice(a.minimizer_pos as usize..a.read.bases_count())
                .to_string();
            let b = b
                .read
                .as_reference(&storage)
                .sub_slice(b.minimizer_pos as usize..b.read.bases_count())
                .to_string();
            a.cmp(&b).reverse()
        });

        println!("Sorted reads:");
        for (idx, read) in reads.iter().enumerate() {
            println!(
                "suffix: \t{}  {}: {} (minimizer_pos: {}, multiplicity: {})",
                read.read
                    .as_reference(&storage)
                    .sub_slice(read.minimizer_pos as usize..read.read.bases_count())
                    .to_string(),
                idx,
                read.read.as_reference(&storage).to_string(),
                read.minimizer_pos,
                read.multiplicity,
            );
        }

        println!("Correct reads:");
        for (idx, read) in correct_reads.iter().enumerate() {
            println!(
                "suffix: \t{}  {}: {} (minimizer_pos: {}, multiplicity: {})",
                read.read
                    .as_reference(&storage)
                    .sub_slice(read.minimizer_pos as usize..read.read.bases_count())
                    .to_string(),
                idx,
                read.read.as_reference(&storage).to_string(),
                read.minimizer_pos,
                read.multiplicity,
            );
        }

        assert_eq!(reads.len(), 17);
    }
}
