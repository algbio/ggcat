// use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
// use crate::pipeline_common::kmers_transform::{
//     KmersTransformContext, KmersTransformExecutorFactory,
// };
// use crate::KEEP_FILES;
// use parallel_processor::buckets::readers::async_binary_reader::AsyncBinaryReader;
// use parallel_processor::execution_manager::executor::{Executor, ExecutorType};
// use parallel_processor::execution_manager::executor_address::ExecutorAddress;
// use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
// use parallel_processor::execution_manager::packet::Packet;
// use parallel_processor::memory_fs::RemoveFileMode;
// use std::marker::PhantomData;
// use std::path::PathBuf;
// use std::sync::atomic::Ordering;
// use std::sync::Arc;
//
// pub struct KmersTransformMapProcessor<F: KmersTransformExecutorFactory> {
//     context: Option<Arc<KmersTransformContext<F>>>,
//     buffers: Vec<Packet<ReadsBuffer<F>>>,
//     _phantom: PhantomData<F>,
// }
//
// impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformMapProcessor<F> {
//     type InitData = ();
//
//     fn allocate_new(_init_data: &Self::InitData) -> Self {
//         Self {
//             context: None,
//             _phantom: Default::default(),
//         }
//     }
//
//     fn reset(&mut self) {
//         self.context.take();
//     }
// }
//
// impl<F: KmersTransformExecutorFactory> Executor for KmersTransformMapProcessor<F> {
//     const EXECUTOR_TYPE: ExecutorType = ExecutorType::MultipleCommonPacketUnits;
//
//     type InputPacket = PathBuf;
//     type OutputPacket = ReadsBuffer<F::AssociatedExtraData>;
//     type GlobalParams = KmersTransformContext<F>;
//     type MemoryParams = ();
//     type BuildParams = (Arc<KmersTransformContext<F>>, AsyncBinaryReader);
//
//     fn allocate_new_group(
//         global_params: Arc<Self::GlobalParams>,
//         _memory_params: Option<Self::MemoryParams>,
//         common_packet: Option<Packet<Self::InputPacket>>,
//     ) -> Self::BuildParams {
//         let file = common_packet.unwrap();
//
//         (
//             global_params,
//             AsyncBinaryReader::new(
//                 file.get_value(),
//                 true,
//                 RemoveFileMode::Remove {
//                     remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
//                 },
//             ),
//         )
//     }
//
//     fn get_maximum_concurrency(&self) -> usize {
//         todo!()
//     }
//
//     fn reinitialize<P: FnMut() -> Packet<Self::OutputPacket>>(
//         &mut self,
//         reinit_params: &Self::BuildParams,
//         packet_alloc: P,
//     ) {
//         self.context = Some(reinit_params.0.clone());
//
//         // let input_queue: Receiver<AsyncBinaryReader> = read_input_queues[i].1.clone();
//         // let output_queue = read_output_queue.0.clone();
//         // let buffers_pool = buffers_pool.clone();
//         // let queues = queues.clone();
//         //         let async_reader = self.execution_context.async_readers.get();
//         //         let preprocessor = F::new_preprocessor(&self.execution_context.global_extra_data);
//         //
//     }
//
//     fn execute<
//         P: FnMut() -> Packet<Self::OutputPacket>,
//         S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
//     >(
//         &mut self,
//         input_packet: Packet<Self::InputPacket>,
//         packet_alloc: P,
//         packet_send: S,
//     ) {
//         // while let Ok(reader) = input_queue.recv() {
//         //     reader.decode_all_bucket_items::<CompressedReadsBucketHelper<
//         //         F::AssociatedExtraData,
//         //         F::FLAGS_COUNT,
//         //         { USE_SECOND_BUCKET },
//         //         false,
//         //     >, _>(async_reader.clone(), Vec::new(), |read_info| {
//         //         let bucket = preprocessor
//         //             .get_sequence_bucket(&self.execution_context.global_extra_data, &read_info)
//         //             as usize
//         //             % executors_count;
//         //
//         //         let (flags, _second_bucket, extra_data, read) = read_info;
//         //
//         //         let ind_read = CompressedReadIndipendent::from_read(
//         //             &read,
//         //             &mut reads_mt_buffers[bucket].reads_buffer,
//         //         );
//         //         reads_mt_buffers[bucket]
//         //             .reads
//         //             .push((flags, extra_data, ind_read));
//         //         if reads_mt_buffers[bucket].reads.len() == reads_mt_buffers[bucket].reads.capacity()
//         //         {
//         //             queues[bucket]
//         //                 .0
//         //                 .send(ReadsMode::AddBatch(std::mem::replace(
//         //                     &mut reads_mt_buffers[bucket],
//         //                     buffers_pool.1.recv().unwrap(),
//         //                 )))
//         //                 .unwrap();
//         //             reads_mt_buffers[bucket].reads.clear();
//         //             reads_mt_buffers[bucket].reads_buffer.clear();
//         //         }
//         //     });
//         //
//         //     for e in 0..executors_count {
//         //         if reads_mt_buffers[e].reads.len() > 0 {
//         //             queues[e]
//         //                 .0
//         //                 .send(ReadsMode::AddBatch(std::mem::replace(
//         //                     &mut reads_mt_buffers[e],
//         //                     buffers_pool.1.recv().unwrap(),
//         //                 )))
//         //                 .unwrap();
//         //             reads_mt_buffers[e].reads.clear();
//         //             reads_mt_buffers[e].reads_buffer.clear();
//         //         }
//         //     }
//         //     output_queue.send(());
//         // }
//     }
//
//     fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, packet_send: S) {
//         todo!()
//     }
//
//     fn get_total_memory(&self) -> u64 {
//         todo!()
//     }
//
//     fn get_current_memory_params(&self) -> Self::MemoryParams {
//         todo!()
//     }
// }
