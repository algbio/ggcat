// use crate::io::concurrent::intermediate_storage::IntermediateReadsReader;
// use crate::query_pipeline::QueryPipeline;
// use crate::types::BucketIndexType;
// use crate::KEEP_FILES;
// use crossbeam::queue::SegQueue;
// use std::path::PathBuf;
// use std::sync::atomic::Ordering;
// use std::sync::Arc;
//
// pub const QUERY_BUCKETS_COUNT: usize = 256;
//
// enum ReadType {
//     Graph,
//     Query,
// }
//
// #[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
// struct QueryReadRef {
//     pub read_start: usize,
//     pub hash: u64,
//     pub read_type: ReadType,
// }
//
// impl QueryPipeline {
//     pub fn kmers_intersect(graph_buckets: Vec<PathBuf>, query_buckets: Vec<PathBuf>) {
//         let process_queue = Arc::new(SegQueue::new());
//
//         for pair in graph_buckets.iter().zip(query_buckets.iter()) {
//             process_queue.push(pair);
//         }
//
//         crossbeam::thread::scope(|s| {
//             for _ in 0..10 {
//                 s.spawn(|_| {
//                     while let Some((graph_path, query_path)) = process_queue.pop() {
//                         let graph_reader = IntermediateReadsReader::<
//                             KmersFlags<CX::MinimizerBucketingSeqColorDataType>,
//                         >::new(
//                             graph_path, !KEEP_FILES.load(Ordering::Relaxed)
//                         );
//
//                         let query_reader = IntermediateReadsReader::<
//                             KmersFlags<CX::MinimizerBucketingSeqColorDataType>,
//                         >::new(
//                             graph_path, !KEEP_FILES.load(Ordering::Relaxed)
//                         );
//
//                         let mut buckets: Vec<Vec<u8>> = vec![Vec::new(); QUERY_BUCKETS_COUNT];
//                         let mut reads: Vec<Vec<QueryReadRef>> =
//                             vec![Vec::new(); QUERY_BUCKETS_COUNT];
//
//                         graph_reader.for_each(|extra, seq| {
//                             let hashes = H::new(seq, m);
//                             let minimizer = hashes
//                                 .iter()
//                                 .min_by_key(|k| H::get_minimizer(k.to_unextendable()))
//                                 .unwrap();
//
//                             let bucket_index = H::get_second_bucket(minimizer.to_unextendable())
//                                 % (QUERY_BUCKETS_COUNT as BucketIndexType);
//                         });
//                     }
//                 });
//             }
//         });
//     }
// }
