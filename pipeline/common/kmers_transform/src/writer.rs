// use crate::{
//     KmersTransformContext, KmersTransformExecutorFactory, KmersTransformFinalExecutor,
//     KmersTransformMapProcessor,
// };
// use parallel_processor::mt_debug_counters::counter::{AtomicCounter, SumMode};
// use parallel_processor::mt_debug_counters::declare_counter_i64;
// use parallel_processor::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
// use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
// use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
// use parallel_processor::execution_manager::packet::Packet;
// use std::future::Future;
// use std::marker::PhantomData;
// use std::sync::Arc;
// use utils::track;
//
// static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
//     declare_counter_i64!("kt_addr_wait_writer", SumMode, false);
//
// static PACKET_WAITING_COUNTER: AtomicCounter<SumMode> =
//     declare_counter_i64!("kt_packet_wait_writer", SumMode, false);
//
// static PACKET_ALLOC_COUNTER: AtomicCounter<SumMode> =
//     declare_counter_i64!("kt_packet_alloc_writer", SumMode, false);
//
// pub struct KmersTransformWriter<F: KmersTransformExecutorFactory>(PhantomData<F>);
//
// impl<F: KmersTransformExecutorFactory> AsyncExecutor for KmersTransformWriter<F> {
//     type InputPacket = <F::MapProcessorType as KmersTransformMapProcessor<F>>::MapStruct;
//     type OutputPacket = ();
//     type GlobalParams = KmersTransformContext<F>;
//     type InitData = ();
//
//     type AsyncExecutorFuture<'a> = impl Future<Output = ()> + 'a;
//
//     fn new() -> Self {
//         Self(PhantomData)
//     }
//
//     fn async_executor_main<'a>(
//         &'a mut self,
//         global_context: &'a Self::GlobalParams,
//         mut receiver: ExecutorReceiver<Self>,
//         memory_tracker: MemoryTracker<Self>,
//     ) -> Self::AsyncExecutorFuture<'a> {
//         async move {
//             // Only one address
//             let (address, _) = track!(
//                 receiver.obtain_address().await.unwrap(),
//                 ADDR_WAITING_COUNTER
//             );
//
//             let mut spawner = address.make_spawner();
//
//             for _ in 0..global_context.compute_threads_count {
//                 spawner.spawn_executor(async {
//                     let mut final_executor =
//                         F::new_final_executor(&global_context.global_extra_data);
//                     while let Some(packet) =
//                         track!(address.receive_packet().await, PACKET_WAITING_COUNTER)
//                     {
//                         final_executor.process_map(&global_context.global_extra_data, packet);
//                     }
//                     final_executor.finalize(&global_context.global_extra_data);
//                 });
//             }
//             spawner.executors_await().await;
//         }
//     }
// }
// //     const MEMORY_FIELDS_COUNT: usize = 1;
// //     const MEMORY_FIELDS: &'static [&'static str] = &["BUFFER_SIZES"];
