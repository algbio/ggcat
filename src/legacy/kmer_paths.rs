use crate::compressed_read::CompressedRead;
use crate::reads_freezer::ReadsWriter;
use crate::sequences_reader::FastaSequence;
use hashbrown::HashMap;
use nthash::NtSequence;
use std::num::NonZeroU32;

pub struct KmerNode {
    next: [Option<NonZeroU32>; 4],
}

pub struct KmerPaths {
    pub nodes_backward: Vec<KmerNode>,
    pub counters_forward: Vec<u32>,

    pub nodes_forward: Vec<KmerNode>,
    pub counters_backward: Vec<u32>,
    pub reads_map: HashMap<(u32, u32), u32>,
}

const EMPTY_NODE: KmerNode = KmerNode { next: [None; 4] };

impl KmerPaths {
    pub fn new() -> KmerPaths {
        KmerPaths {
            nodes_backward: vec![EMPTY_NODE],
            counters_forward: vec![0],
            nodes_forward: vec![EMPTY_NODE],
            counters_backward: vec![0],
            reads_map: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.nodes_backward.clear();
        self.nodes_forward.clear();
        self.nodes_backward.push(EMPTY_NODE);
        self.nodes_forward.push(EMPTY_NODE);
        self.counters_backward.clear();
        self.counters_forward.clear();
        self.counters_backward.push(0);
        self.counters_forward.push(0);
    }

    pub fn add_kmer(&mut self, read: CompressedRead, back_start: usize, forward_start: usize) {
        let mut node_index = 0;

        for i in (0..back_start).rev() {
            let base = unsafe { read.get_base_unchecked(i) };

            let next_index = self.nodes_backward[node_index].next[base as usize];
            node_index = match next_index {
                None => {
                    self.nodes_backward.push(EMPTY_NODE);
                    self.counters_backward.push(0);
                    let next_node = self.nodes_backward.len() as u32 - 1;
                    self.nodes_backward[node_index].next[base as usize] =
                        Some(unsafe { NonZeroU32::new_unchecked(next_node) });
                    next_node as usize
                }
                Some(index) => index.get() as usize,
            };
            self.counters_backward[node_index] += 1;
        }

        let mut start_index = node_index as u32;

        node_index = 0;
        for i in forward_start..read.bases_count() {
            let base = unsafe { read.get_base_unchecked(i) };

            let next_index = self.nodes_forward[node_index].next[base as usize];
            node_index = match next_index {
                None => {
                    self.nodes_forward.push(EMPTY_NODE);
                    self.counters_forward.push(0);
                    let next_node = self.nodes_forward.len() as u32 - 1;
                    self.nodes_forward[node_index].next[base as usize] =
                        Some(unsafe { NonZeroU32::new_unchecked(next_node) });
                    next_node as usize
                }
                Some(index) => index.get() as usize,
            };
            self.counters_forward[node_index] += 1;
        }

        let mut end_index = node_index as u32;

        if back_start != 0 && forward_start != read.bases_count() {
            *self.reads_map.entry((start_index, end_index)).or_insert(0) += 1;
        }
    }

    pub fn iterate(&mut self, writer: &mut ReadsWriter, forward: bool) -> usize {
        let mut stack = vec![];
        let mut read = vec![];
        stack.reserve(64);
        read.reserve(64);

        stack.push((0u32, 0u8));
        read.push(4); // Out of bounds value

        const LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

        let mut count = 0;

        let (nodes, counters) = if forward {
            (&self.nodes_forward, &self.counters_forward)
        } else {
            (&self.nodes_backward, &self.counters_backward)
        };

        while stack.len() > 0 {
            let mut top = *stack.last_mut().unwrap();
            if top.1 > 3 {
                read.pop();
                stack.pop();
                continue;
            }

            let end_node = top.1 == 0;
            while top.1 < 4 {
                if let Some(node) = nodes[top.0 as usize].next[top.1 as usize] {
                    if counters[node.get() as usize] >= 2 {
                        read.push(LETTERS[top.1 as usize]);
                        top.1 += 1;
                        *stack.last_mut().unwrap() = top;
                        stack.push((node.get(), 0));
                        break;
                    } else {
                        top.1 += 1;
                        *stack.last_mut().unwrap() = top;
                    }
                } else {
                    top.1 += 1;
                    *stack.last_mut().unwrap() = top;
                }
            }

            if end_node && top.1 == 4 && read.len() > 1 {
                writer.add_read(FastaSequence {
                    ident: &[],
                    seq: &read[1..],
                    qual: None,
                });
                count += 1;
                // println!(
                //     "EndNode{}/{}! {}",
                //     top.0,
                //     top.1,
                //     std::str::from_utf8(&read[1..]).unwrap()
                // );
            }
        }
        count
    }
}
