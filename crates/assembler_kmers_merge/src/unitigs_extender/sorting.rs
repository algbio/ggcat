use std::{iter::repeat, mem::take};

use binary_heap_plus::BinaryHeap;
use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::creads_utils::DeserializedReadIndependent,
};

use crate::sorting::radix_sort_reads;

struct ProcessingStackElement {
    index: usize,
    last_index: usize,
    sorted_chunks_start: usize,
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
struct PrefixChunkStackElement {
    prefix_start_index: usize,
    prefix_end_index: usize,
    suffix_rightmost_remaining: usize,
}

#[derive(Debug)]
struct SortingHeapElement {
    index: usize,
    last: usize,
}

#[derive(Default)]
pub struct SortingExtender<E> {
    forward_skip: Vec<usize>,
    sorted_chunks: Vec<SortedChunk>,
    suffix_sizes: Vec<usize>,
    elements_mapping: Vec<usize>,
    lcs_array: Vec<usize>,
    rev_lcp_array: Vec<usize>,
    reads_copy: Vec<DeserializedReadIndependent<E>>,
    processing_stack: Vec<ProcessingStackElement>,
    sorting_heap_data: Vec<SortingHeapElement>,
    sorting_support_vec: Vec<usize>,
    prefix_stack: Vec<PrefixChunkStackElement>,
}

impl<E: Copy> SortingExtender<E> {
    pub fn process_reads(
        &mut self,
        reads: &mut [DeserializedReadIndependent<E>],
        superkmers_storage: &Vec<u8>,
        k: usize,
        mut output_read: impl FnMut(CompressedRead, usize),
    ) {
        println!("********************************** STARTED **********************************");

        println!("Reads:");
        for (idx, read) in reads.iter().enumerate() {
            println!(
                "  {}: {} (minimizer_pos: {}, multiplicity: {})",
                idx,
                read.read.as_reference(superkmers_storage).to_string(),
                read.minimizer_pos,
                read.multiplicity
            );
        }

        // if reads.len() < 55 || reads.len() > 60 {
        //     return;
        // }

        let mut count = 0;

        // 0. Save the suffixes lengths for each read
        {
            self.suffix_sizes.clear();
            for el in reads.iter() {
                self.suffix_sizes
                    .push(el.read.bases_count() - el.minimizer_pos as usize);
            }
        }

        // 1. Sort (reversed) by suffix and compute a lcs array
        {
            self.lcs_array.clear();
            // elements_mapping
            //     .extend((0..reads.len()).map(|i| ReadIndex::new(i)));

            self.reads_copy.clear();
            self.reads_copy.extend_from_slice(reads);

            // if reads.len() > 128 {
            //     radix_sort_reads(&mut self.reads_copy, reads, superkmers_storage, 0);
            // } else {
            reads.sort_unstable_by(|a, b| unsafe {
                a.read
                    .get_suffix_difference(
                        &b.read,
                        superkmers_storage,
                        a.minimizer_pos as usize,
                        b.minimizer_pos as usize,
                    )
                    .1
                    .reverse()
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

        println!("Sorted Reads:");
        for (idx, read) in reads.iter().enumerate() {
            println!(
                "  {}: {} (minimizer_pos: {}, multiplicity: {})",
                idx,
                read.read.as_reference(superkmers_storage).to_string(),
                read.minimizer_pos,
                read.multiplicity
            );
        }

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
                self.processing_stack.push(ProcessingStackElement {
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

            let suffix_element_index = last_stack_element.index;
            let suffix_last_position = &mut last_stack_element.last_index;
            let sorted_chunks_start = last_stack_element.sorted_chunks_start;
            let needed_suffix = self.suffix_sizes[suffix_element_index];

            while *suffix_last_position < self.elements_mapping.len()
                && self.lcs_array[*suffix_last_position - 1] >= needed_suffix
            {
                // println!(
                //     "{} vs {} with position: {} {} and {}",
                //     self.lcs_array[*suffix_last_position - 1],
                //     needed_suffix,
                //     self.suffix_sizes[*suffix_last_position],
                //     reads[suffix_element_index]
                //         .read
                //         .as_reference(superkmers_storage)
                //         .to_string(),
                //     reads[*suffix_last_position]
                //         .read
                //         .as_reference(superkmers_storage)
                //         .to_string()
                // );

                // A longer suffix is found, process it before continuing
                if self.suffix_sizes[*suffix_last_position] > needed_suffix {
                    let next_position = *suffix_last_position;
                    // println!(
                    //     "Pushing value: {} vs needed: {}/{}",
                    //     next_position, self.suffix_sizes[*suffix_last_position], needed_suffix
                    // );

                    self.processing_stack.push(ProcessingStackElement {
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
                if {
                    last_sorted_chunk = self.sorted_chunks.last_mut().unwrap();

                    let next_sorted = self.forward_skip[*suffix_last_position] >= MIN_SORTED_SIZE;

                    (!last_sorted_chunk.is_sorted && !next_sorted)
                        || (last_sorted_chunk.end - last_sorted_chunk.start
                            + self.forward_skip[*suffix_last_position])
                            < MIN_SORTED_SIZE
                } {
                    last_sorted_chunk.end =
                        *suffix_last_position + self.forward_skip[*suffix_last_position];
                    last_sorted_chunk.is_sorted = false;
                    // println!(
                    //     "Add index {} with skip {} to sorted chunks",
                    //     suffix_last_position, self.forward_skip[*suffix_last_position]
                    // );
                } else {
                    // println!(
                    //     "Push new sorted chunk at position: {} with size: {}",
                    //     *suffix_last_position, self.forward_skip[*suffix_last_position]
                    // );
                    self.sorted_chunks.push(SortedChunk {
                        start: *suffix_last_position,
                        end: *suffix_last_position + self.forward_skip[*suffix_last_position],
                        is_sorted: self.forward_skip[*suffix_last_position] >= MIN_SORTED_SIZE,
                    });
                }
                *suffix_last_position += self.forward_skip[*suffix_last_position];
            }

            let suffix_last_position = *suffix_last_position;
            let max_allowed_suffix_prev = if self.processing_stack.len() <= 1 {
                0
            } else {
                self.suffix_sizes[self.processing_stack[self.processing_stack.len() - 2].index]
            };
            let max_allowed_suffix_post = self.lcs_array[suffix_last_position - 1];

            // Here we have all the possible matches in range suffix_element_index..suffix_last_position
            let max_allowed_suffix = max_allowed_suffix_prev.max(max_allowed_suffix_post);

            for idx in suffix_element_index..suffix_last_position {
                self.suffix_sizes[idx] = max_allowed_suffix;
            }
            self.forward_skip[suffix_element_index] = suffix_last_position - suffix_element_index;

            // println!("Chunks: {:?}", &self.sorted_chunks[sorted_chunks_start..]);
            // println!("Remappings: {:?}", self.elements_mapping);
            self.sorting_heap_data.clear();

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

                // Load the chunks into the heap
                self.sorting_heap_data.push(SortingHeapElement {
                    index: chunk.start,
                    last: chunk.end,
                });
            }

            // Merge sort all the chunks together
            if self.sorting_heap_data.len() > 1 {
                self.sorting_support_vec.clear();
                self.sorting_support_vec.extend_from_slice(
                    &self.elements_mapping[suffix_element_index..suffix_last_position],
                );

                let mut sorting_heap = BinaryHeap::from_vec_cmp(
                    take(&mut self.sorting_heap_data),
                    |a: &SortingHeapElement, b: &SortingHeapElement| {
                        let a = &remapped_minimizer_elements
                            [self.sorting_support_vec[a.index - suffix_element_index]];
                        let b = &remapped_minimizer_elements
                            [self.sorting_support_vec[b.index - suffix_element_index]];
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
                let mut write_index = suffix_element_index;
                while sorting_heap.len() > 0 {
                    let mut element = sorting_heap.peek_mut().unwrap();
                    self.elements_mapping[write_index] =
                        self.sorting_support_vec[element.index - suffix_element_index];
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

            // println!(
            //     "FInal ordering for {}..{}:",
            //     suffix_element_index, suffix_last_position
            // );

            // for i in suffix_element_index..suffix_last_position {
            //     println!(
            //         "  {}: {}",
            //         i,
            //         remapped_minimizer_elements[self.elements_mapping[i]]
            //             .read
            //             .as_reference(superkmers_storage)
            //             .to_string()
            //     );
            // }

            self.prefix_stack.push(PrefixChunkStackElement {
                prefix_start_index: suffix_element_index,
                prefix_end_index: suffix_last_position,
                suffix_rightmost_remaining: needed_suffix,
            });

            while let Some(prefix_range) = self.prefix_stack.pop() {
                let PrefixChunkStackElement {
                    prefix_start_index,
                    prefix_end_index,
                    suffix_rightmost_remaining,
                } = prefix_range;

                let mut target_index_start = prefix_start_index;
                while target_index_start < prefix_end_index {
                    let reference =
                        &remapped_minimizer_elements[self.elements_mapping[target_index_start]];

                    let mut rightmost_left_match = reference.minimizer_pos as usize;

                    if rightmost_left_match + suffix_rightmost_remaining < k {
                        target_index_start += 1;
                        // println!(
                        //     "Skipping read [too short]: {} [{} + {} < {}]",
                        //     reference.read.as_reference(superkmers_storage).to_string(),
                        //     rightmost_left_match,
                        //     suffix_rightmost_remaining,
                        //     k
                        // );
                        continue;
                    }

                    let mut target_index_end = target_index_start + 1;
                    let mut multiplicity = reference.multiplicity as usize;

                    while target_index_end < prefix_end_index {
                        let next_read =
                            &remapped_minimizer_elements[self.elements_mapping[target_index_end]];

                        let left_matching = unsafe {
                            reference
                                .read
                                .get_prefix_difference(
                                    &next_read.read,
                                    superkmers_storage,
                                    reference.minimizer_pos as usize,
                                    next_read.minimizer_pos as usize,
                                )
                                .0
                        };
                        let total_matching = left_matching + suffix_rightmost_remaining;

                        // The last supertig containing the current kmer was reached, exit and process the current reference
                        if total_matching < k {
                            break;
                        }

                        rightmost_left_match = rightmost_left_match.min(left_matching);
                        multiplicity += next_read.multiplicity as usize;
                        target_index_end += 1;
                    }

                    // Process reads
                    let max_leftmost =
                        (reference.minimizer_pos as usize + max_allowed_suffix).saturating_sub(k);
                    let crt_leftmost = reference.minimizer_pos as usize - rightmost_left_match;

                    let leftmost_index = max_leftmost.max(crt_leftmost);

                    // println!("EXEC!");
                    // if max_leftmost + 1 < crt_leftmost {
                    //     // println!(
                    //     //     "Remaining before {} vs now: {} vs needed: {} count: {}..{} min: {}",
                    //     //     suffix_rightmost_remaining,
                    //     //     k - rightmost_left_match,
                    //     //     max_allowed_suffix,
                    //     //     target_index_start,
                    //     //     target_index_end,
                    //     //     reference.minimizer_pos
                    //     // );
                    //     self.prefix_stack.push(PrefixChunkStackElement {
                    //         prefix_start_index: target_index_start,
                    //         prefix_end_index: target_index_end,
                    //         suffix_rightmost_remaining: k - rightmost_left_match,
                    //     });
                    // }

                    // println!("Leftmost: {}", leftmost_index);
                    // println!(
                    //     "Output read: {} - {} with multiplicity: {} and match {}..{} max allowed: {}",
                    //     target_index_start,
                    //     reference
                    //         .read
                    //         .as_reference(superkmers_storage)
                    //         .sub_slice(
                    //             leftmost_index..(reference.minimizer_pos as usize + suffix_rightmost_remaining),
                    //         )
                    //         .to_string(),
                    //     multiplicity,
                    //     leftmost_index,
                    //     (reference.minimizer_pos as usize + suffix_rightmost_remaining),
                    //     max_allowed_suffix,
                    // );
                    count += 1;
                    output_read(
                        reference.read.as_reference(superkmers_storage).sub_slice(
                            leftmost_index
                                ..(reference.minimizer_pos as usize + suffix_rightmost_remaining),
                        ),
                        multiplicity,
                    );

                    target_index_start = target_index_end;
                }
            }

            next_position = suffix_last_position;

            if max_allowed_suffix == max_allowed_suffix_prev {
                let last_stack_element = self.processing_stack.pop().unwrap();
                self.sorted_chunks
                    .truncate(last_stack_element.sorted_chunks_start);
            }
        }

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
