use crate::execution_manager::packet::Packet;

pub enum ExecutorType {
    SingleUnit,
    MultipleUnits,
}

pub trait Executor {
    const EXECUTOR_TYPE: ExecutorType;

    type InputPacket;
    type OutputPacket;
    type MemoryParams;

    type ChannelParams;

    fn new(params: Option<Self::MemoryParams>) -> Self;

    fn reinitialize<P: FnMut() -> Packet<Self::OutputPacket>>(
        ch_params: Self::ChannelParams,
        params: Self::MemoryParams,
        packet_alloc: P,
    );

    fn execute<P: FnMut() -> Packet<Self::OutputPacket>, S: FnMut(Packet<Self::OutputPacket>)>(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        packet_alloc: P,
        packet_send: S,
    );

    fn finalize<S: FnMut(Packet<Self::OutputPacket>)>(&mut self, packet_send: S);

    fn get_total_memory(&self) -> u64;
    fn get_current_memory_params(&self) -> Self::MemoryParams;
}

// fn execute_mbp() {
//
//     let disk_thread_pool = ExecThreadPool::new(4);
//     let compute_thread_pool = ExecThreadPool::new(16);
//
//     let input_files = ExecutorInput::from_iter(files);
//
//     let file_readers = ExecutorsList::<MBFileReader>::new(ExecutorAllocMode::fixed(4), &disk_thread_pool);
//     file_readers.set_input(input_files);
//
//     let bucket_writers = ExecutorsList::<MBucketWriter>::new(ExecutorAllocMode::fixed(16), &compute_thread_pool);
//     bucket_writers.set_input(file_readers);
//
//     disk_thread_pool.start();
//     compute_thread_pool.start();
// }
//
// fn execute_kmers_merge() {
//
//     let disk_thread_pool = ExecThreadPool::new(4);
//     let compute_thread_pool = ExecThreadPool::new(16);
//
//     let input_buckets = ExecutorInput::from_iter(files);
//
//     let bucket_readers = ExecutorsList::<KMBucketReader>::new(ExecutorAllocMode::fixed(4), &disk_thread_pool);
//     input_buckets.set_output(bucket_readers, InputMode::FIFO);
//
//     let bucket_resplitters = ExecutorsList::<KMBucketReader>::new(ExecutorAllocMode::fixed(16), &compute_thread_pool);
//     bucket_readers.set_output(bucket_resplitters, InputMode::FIFO);
//     bucket_resplitters.set_output(bucket_readers, InputMode::LIFO);
//
//     let hmap_builders = ExecutorsList::<KmersHmapBuilding>::new(ExecutorAllocMode::limit_memory(1024MB), &compute_thread_pool);
//     bucket_readers.set_output(hmap_builders, InputMode::FIFO);
//
//     let kmers_extenders = ExecutorsList::<KmersExtenders>::new(ExecutorAllocMode::fixed(16), &compute_thread_pool);
//     hmap_builders.set_output(kmers_extenders, InputMode::FIFO);
//
//
//     disk_thread_pool.start();
//     compute_thread_pool.start();
// }
