use std::{iter::repeat, mem::take, ops::Range};

use binary_heap_plus::BinaryHeap;
use hashes::HashableSequence;
use io::compressed_read::CompressedRead;
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

// macro_rules! println {
//     ($($v:tt)*) => {};
// }

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

    /// Adjacent Longest Common Reversed Suffix (LCS) array in the suffix sorted reads.
    /// lcs_arra[0] contains the LCS between reads[0] and reads[1].
    lcs_array: Vec<usize>,

    /// Adjacent Longest Common Reversed Prefix (LCP) array in the prefix sorted reads.
    /// lcp_array[0] contains the LCP between reads[mapping[0]] and reads[mapping[1]].
    lcp_array: Vec<usize>,

    /// Unused?
    // rev_lcp_array: Vec<usize>,

    /// Helper for radix sort, not used now
    // reads_copy: Vec<DeserializedReadIndependent<E>>,

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

fn check_kmers<E>(
    k: usize,
    reads: &[DeserializedReadIndependent<E>],
    superkmers_storage: &Vec<u8>,
    supertigs: &[(CompressedRead, usize)],
) {
    let mut kmers = vec![];
    let mut stkmers = vec![];
    for read in reads.iter() {
        let kmers_count = read.read.bases_count() - k + 1;
        for i in 0..kmers_count {
            let kmer = read
                .read
                .as_reference(superkmers_storage)
                .sub_slice(i..i + k);
            kmers.push((kmer.to_string(), read.multiplicity));
        }
    }

    for st in supertigs.iter() {
        let kmers_count = st.0.bases_count() - k + 1;
        for i in 0..kmers_count {
            let kmer = st.0.sub_slice(i..i + k);
            stkmers.push((kmer.to_string(), st.1));
        }
    }

    kmers.sort_by_cached_key(|k| k.0.to_string());
    stkmers.sort_by_cached_key(|k| k.0.to_string());

    for kmer in stkmers.windows(2) {
        if kmer[0].0 == kmer[1].0 {
            for read in reads.iter() {
                println!(
                    "READ: {} with multiplicity {} and offset {}",
                    read.read.as_reference(superkmers_storage).to_string(),
                    read.multiplicity,
                    read.minimizer_pos
                );
            }

            for supertig in supertigs.iter() {
                println!(
                    "Supertig: {} with multiplicity {}",
                    supertig.0.to_string(),
                    supertig.1
                );
            }

            panic!(
                "Found duplicate supertig kmer: {} with multiplicities {} and {}",
                kmer[0].0, kmer[0].1, kmer[1].1
            );
        }
    }

    kmers.dedup_by_key(|k| k.0.to_string());
    stkmers.dedup_by_key(|k| k.0.to_string());

    println!(
        "Kmers: {} stk: {} total: {}",
        kmers.len(),
        stkmers.len(),
        supertigs.len()
    );
    assert_eq!(kmers.len(), stkmers.len());
    // todo!();
}

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
        // Compute the LCP array for the current range of reads.
        {
            self.lcp_array.clear();
            for idx in range.clone().skip(1) {
                let a = &reads[self.elements_mapping[idx - 1]];
                let b = &reads[self.elements_mapping[idx]];
                self.lcp_array.push(unsafe {
                    a.read
                        .get_prefix_difference(
                            &b.read,
                            superkmers_storage,
                            a.minimizer_pos as usize,
                            b.minimizer_pos as usize,
                        )
                        .0
                })
            }
            // Last one has no matches with the following element
            self.lcp_array.push(0);
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

            // println!("Start stack wit shared suffix: {}", shared_suffix);

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

                    let left_matching = self.lcp_array[element_target_index - 1 - range.start];

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

                // println!("Dumping reads now from: {}", reference_index);

                // for i in reference_index..element_target_index {
                //     let crt = &reads[self.elements_mapping[i]];

                //     assert!(crt.minimizer_pos as usize + shared_suffix <= crt.read.bases_count());

                //     println!(
                //         "Read {}: {} full: {} suffix: {}",
                //         i,
                //         crt.read
                //             .as_reference(superkmers_storage)
                //             .sub_slice(
                //                 (crt.minimizer_pos as usize + leftmost_allowed_suffix - k)
                //                     ..(crt.minimizer_pos as usize + shared_suffix)
                //             )
                //             .to_string(),
                //         crt.read.as_reference(superkmers_storage).to_string(),
                //         crt.read
                //             .as_reference(superkmers_storage)
                //             .sub_slice(
                //                 crt.minimizer_pos as usize
                //                     ..(crt.minimizer_pos as usize + shared_suffix)
                //             )
                //             .to_string(),
                //     );
                // }

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
        let debug_reads = reads.to_vec();
        let mut debug_supertigs = vec![];
        // println!("********************************** STARTED **********************************");

        // println!("Reads:");
        // for (idx, read) in reads.iter().enumerate() {
        //     println!(
        //         "  {}: {} (minimizer_pos: {}, multiplicity: {})",
        //         idx,
        //         read.read.as_reference(superkmers_storage).to_string(),
        //         read.minimizer_pos,
        //         read.multiplicity
        //     );
        // }

        // if reads.len() < 55 || reads.len() > 60 {
        //     return;
        // }

        // let mut count = 0;

        // 0. Sort (reversed) by suffix and compute a lcs array
        {
            self.lcs_array.clear();
            // elements_mapping
            //     .extend((0..reads.len()).map(|i| ReadIndex::new(i)));

            // self.reads_copy.clear();
            // self.reads_copy.extend_from_slice(reads);

            // if reads.len() > 128 {
            //     radix_sort_reads(&mut self.reads_copy, reads, superkmers_storage, 0);
            // } else {

            reads.sort_unstable_by(|a, b| unsafe {
                let ar = a
                    .read
                    .get_suffix_difference(
                        &b.read,
                        superkmers_storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse();

                let br = a
                    .read
                    .get_suffix_difference(
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
            // }

            for idx in 1..reads.len() {
                let a = &reads[idx - 1];
                let b = &reads[idx];
                self.lcs_array.push(unsafe {
                    a.read
                        .get_suffix_difference(
                            &b.read,
                            superkmers_storage,
                            a.minimizer_pos as usize,
                            b.minimizer_pos as usize,
                        )
                        .0
                })
            }
            // Last one has no matches with the following element
            self.lcs_array.push(0);

            // println!("LCS: {} {:?}", self.lcs_array.len(), self.lcs_array);
        }

        // 1. Save the suffixes lengths for each read
        {
            self.suffix_sizes.clear();
            for el in reads.iter() {
                self.suffix_sizes
                    .push(el.read.bases_count() - el.minimizer_pos as usize);
            }
        }

        // println!("Sorted Reads:");
        // for (idx, read) in reads.iter().enumerate() {
        //     println!(
        //         "  {}: {} (minimizer_pos: {}, multiplicity: {})",
        //         idx,
        //         read.read.as_reference(superkmers_storage).to_string(),
        //         read.minimizer_pos,
        //         read.multiplicity
        //     );
        // }

        // // 2. Sort the elements indices for the prefixes and compute a (centered/reversed) lcp array
        {
            //     self.rev_lcp_array.clear();
            self.elements_mapping.clear();

            self.elements_mapping.extend(0..reads.len());

            //     self.elements_mapping.sort_unstable_by(|a, b| unsafe {
            //         let a = &reads[*a];
            //         let b = &reads[*b];
            //         a.read
            //             .get_prefix_difference(
            //                 &b.read,
            //                 superkmers_storage,
            //                 a.minimizer_pos as usize,
            //                 b.minimizer_pos as usize,
            //             )
            //             .1
            //             .reverse()
            //     });

            //     for idx in 1..self.elements_mapping.len() {
            //         let a = &reads[self.elements_mapping[idx - 1]];
            //         let b = &reads[self.elements_mapping[idx]];
            //         self.rev_lcp_array.push(unsafe {
            //             a.read
            //                 .get_prefix_difference(
            //                     &b.read,
            //                     superkmers_storage,
            //                     a.minimizer_pos as usize,
            //                     b.minimizer_pos as usize,
            //                 )
            //                 .0
            //         })
            //     }

            //     self.rev_lcp_array.push(0);
        }

        // Reinitialize the skip array
        self.forward_skip.clear();
        self.forward_skip.extend(repeat(1).take(reads.len()));

        self.sorted_chunks.clear();

        let mut next_position = 0;

        let remapped_minimizer_elements = reads;

        // for (idx, read) in remapped_minimizer_elements.iter().enumerate() {
        //     println!(
        //         "Read: {} - {}",
        //         idx,
        //         read.read.as_reference(superkmers_storage).to_string()
        //     );
        // }

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

            // println!("Start processing stack elements.");

            // The needed suffix is the minimum suffix size required
            // to have a chance of having a full kmer match in the current stack element
            let needed_suffix = self.suffix_sizes[suffix_stack_block_start];

            // Try to extend the current stack elements until the suffix is not enough
            while *suffix_stack_block_last < self.elements_mapping.len()
                && self.lcs_array[*suffix_stack_block_last - 1] >= needed_suffix
            {
                // println!(
                //     "{} vs {} with position: {} {} and {}",
                //     self.lcs_array[*suffix_stack_block_last - 1],
                //     needed_suffix,
                //     self.suffix_sizes[*suffix_stack_block_last],
                //     reads[suffix_stack_block_start]
                //         .read
                //         .as_reference(superkmers_storage)
                //         .to_string(),
                //     reads[*suffix_stack_block_last]
                //         .read
                //         .as_reference(superkmers_storage)
                //         .to_string()
                // );

                // A longer suffix is found, process it before continuing
                if self.suffix_sizes[*suffix_stack_block_last] > needed_suffix {
                    let next_position = *suffix_stack_block_last;
                    // println!(
                    //     "Pushing value: {} vs needed: {}/{}",
                    //     next_position, self.suffix_sizes[*suffix_stack_block_last], needed_suffix
                    // );

                    // println!(
                    //     "Pushing stack element: {} with suffix size: {}",
                    //     next_position, self.suffix_sizes[*suffix_stack_block_last]
                    // );
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
                    // println!(
                    //     "Add index {} with skip {} to sorted chunks",
                    //     suffix_stack_block_last, self.forward_skip[*suffix_stack_block_last]
                    // );
                }
                // Else, push a new sorted chunk with the current suffix position
                else {
                    // println!(
                    //     "Push new sorted chunk at position: {} with size: {}",
                    //     *suffix_stack_block_last, self.forward_skip[*suffix_stack_block_last]
                    // );
                    self.sorted_chunks.push(SortedChunk {
                        start: *suffix_stack_block_last,
                        end: *suffix_stack_block_last + self.forward_skip[*suffix_stack_block_last],
                        is_sorted: self.forward_skip[*suffix_stack_block_last] >= MIN_SORTED_SIZE,
                    });
                }
                *suffix_stack_block_last += self.forward_skip[*suffix_stack_block_last];
            }

            // println!("Needed suffix: {}", needed_suffix);
            // println!("Lcs: {}", self.lcs_array[*suffix_stack_block_last - 1]);

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
                            .get_prefix_difference(
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

                // println!("Sorted chunk {}..{}:", chunk.start, chunk.end);
                // for i in chunk.start..chunk.end {
                //     let idx = self.elements_mapping[i];
                //     let read = &remapped_minimizer_elements[idx];
                //     println!(
                //         "  {}: {} first: {} second: {}",
                //         idx,
                //         read.read.as_reference(superkmers_storage).to_string(),
                //         read.read
                //             .as_reference(superkmers_storage)
                //             .sub_slice(0..(read.minimizer_pos as usize))
                //             .to_string(),
                //         read.read
                //             .as_reference(superkmers_storage)
                //             .sub_slice((read.minimizer_pos as usize)..read.read.bases_count())
                //             .to_string()
                //     );
                // }

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
                        // a.minimizer_pos.cmp(&b.minimizer_pos)
                        unsafe {
                            a.read
                                .get_prefix_difference(
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
            let max_allowed_suffix_post = self.lcs_array[suffix_stack_block_last - 1];

            /*
               WWWWWWWWWW#######YYYYY<allowed_prev>ZZZZZZ
               XXXXXXXXXX#######YYYYYYYYYYY
               PPPPPPXXXX#######YYYYYYYYYYY
               HHHHXXXXXX#######YYYYYYYYYYY
               WWWWWWWXXX#######YYYYYYYYYYY
               ZZZZZZXXXX#######YYYYYYY<allowed_post>WWWW
            */

            let max_allowed_suffix = max_allowed_suffix_prev.max(max_allowed_suffix_post);

            assert!(max_allowed_suffix < needed_suffix);

            self.process_reads_block(
                remapped_minimizer_elements,
                superkmers_storage,
                suffix_stack_block_start..suffix_stack_block_last,
                k,
                needed_suffix,
                max_allowed_suffix,
                // &mut output_read,
                |a, b| {
                    debug_supertigs.push((a, b));
                    output_read(a, b)
                },
            );

            self.forward_skip[suffix_stack_block_start] =
                suffix_stack_block_last - suffix_stack_block_start;

            // println!("Next position: {}", next_position);
            // println!("Max allowed suffix: {}", max_allowed_suffix);
            // println!("Max allowed suffix prev: {}", max_allowed_suffix_prev);
            // println!("Max allowed suffix post: {}", max_allowed_suffix_post);
            // println!("Processed stack: {:?}", self.processing_stack.len());

            next_position = suffix_stack_block_last;

            if max_allowed_suffix == max_allowed_suffix_prev {
                let last_stack_element = self.processing_stack.pop().unwrap();
                self.sorted_chunks
                    .truncate(last_stack_element.sorted_chunks_start);
            }
        }

        check_kmers(k, &debug_reads, superkmers_storage, &debug_supertigs);
        // println!(
        //     "AAA Written Count {}/{}",
        //     count,
        //     remapped_minimizer_elements.len()
        // );
        // if count + 10 < remapped_minimizer_elements.len() {
        //     panic!("AAAA")
        // }

        //         // Supertig: TACTTCTATGTCCTGTAATGAGAATCCGTTTTCCTCCTGACTG with multiplicity: 5
        //         // Supertig: TACTTCTATGTCCTGTAATGAGAATCCGTTTTCCTCCTGACTG with multiplicity: 11

        //         // Debug TACTTCTATGTCCTGTAATGAGAATCCGTTTTCCTCCTGACTG
        //         // Supertig: TACTTCTATGTCCTGTAATGAGAATCCGTTTTCCTCCTGACTG with multiplicity: 11 expected 16? RC: CAGTCAGGAGGAAAACGGATTCTCATTACAGGACATAGAAGTA

        //         // println!("Chunk size: {} elcount: {}", lower_bound - start_element_idx, reads.len());
        //         // println!(
        //         //     "Supertig: {} with multiplicity: {} and size: {} gc: {} offset: {} minimizer: {} count: {}/{}",
        //         //     supertig.to_string(),
        //         //     multiplicity,
        //         //     needed_suffix,
        //         //     global_counter,
        //         //     supertig_offset,
        //         //     prefix_element
        //         //         .read
        //         //         .as_reference(superkmers_storage)
        //         //         .sub_slice(
        //         //             supertig_offset..supertig_offset + global_data.m,
        //         //         ).to_string(),
        //         //         consumed_kmers,
        //         //     lower_bound - start_element_idx
        //         // );

        //         global_counter += 1;

        //    Supertig: an unitig fully contained in all super-kmers where one of its k-mers is present

        //    1. Find all supertigs starting from the rightmost and going left
        //    2. Save info about supertigs in each super-kmer they belong
        //    3. If a right-supertig is found, check if its multiplicity matches the found one and its unique, in case mark it as joinable using a linked list
        //    4. Keep all the linked list heads
        //    5. At halfpoint, use the backward index to avoid including too many kmers in the search (only if worth it)
        //    6. Iterate each list and join the supertigs in an unitig
        //    7. PROFIT
        // */
        // TODO:

        // If there are multiple matches when going left, it means that the current kmer is branching. Else it is guaranteed to be singly extendable to the left.
        // Keep rightmost last used kmer, to handle already taken kmers.
        // For simplitigs, in case of branching follow the reference super-kmer
        // Keep a rolling window sum of multiplicities, to allow immediate check for abundance thresholds
        //
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
                    .get_suffix_difference(
                        &b.read,
                        &storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse()
            );

            a.read
                .get_suffix_difference(
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
                    .get_suffix_difference(
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
