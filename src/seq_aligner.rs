// use std::collections::HashMap;
// use crate::gzip_fasta_reader::FastaSequence;
// use bstr::ByteSlice;
//
// struct FastaOwnSeq {
//     seq: Vec<u8>,
//     qual: Vec<u8>
// }
//
// pub struct SeqAligner {
//     hashmap: HashMap<u64, Vec<FastaOwnSeq>>
// }
//
// impl SeqAligner {
//     pub fn new() -> SeqAligner {
//         SeqAligner {
//             hashmap: HashMap::new()
//         }
//     }
//
//     pub fn add_read(&mut self, hash: u64, read: FastaSequence) {
//         let read_cpy = FastaOwnSeq {
//             seq: Vec::from(read.seq),
//             qual: Vec::from(read.qual)
//         };
//
//         if let Some(vec) = self.hashmap.get_mut(&hash) {
//             vec.push(read_cpy);
//         }
//         else {
//             let mut vec = Vec::new();
//             vec.push(read_cpy);
//             self.hashmap.insert(hash, vec);
//         }
//     }
//
//     pub fn process(&mut self) {
//         for (hash, vector) in &self.hashmap {
//             for element in vector {
//
//             }
//         }
//     }
// }
