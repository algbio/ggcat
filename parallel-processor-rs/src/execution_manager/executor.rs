use crate::execution_manager::executor_address::ExecutorAddress;
use crate::execution_manager::manager::{ExecutionManager, ExecutionManagerTrait};
use crate::execution_manager::objects_pool::{ObjectsPool, PoolObject, PoolObjectTrait};
use crate::execution_manager::packet::Packet;
use crate::execution_manager::packet::PacketTrait;
use parking_lot::RwLock;
use std::any::Any;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub enum ExecutorType {
    SingleUnit,
    MultipleCommonPacketUnits,
    MultipleUnits,
}

static EXECUTOR_GLOBAL_ID: AtomicU64 = AtomicU64::new(0);

pub trait Executor: PoolObjectTrait<InitData = ()> + Sync + Send {
    const EXECUTOR_TYPE: ExecutorType;

    type InputPacket: Send + Sync;
    type OutputPacket: Send + Sync + PacketTrait;
    type GlobalParams: Send + Sync;
    type MemoryParams: Send + Sync;
    type BuildParams: Send + Sync;

    fn generate_new_address() -> ExecutorAddress {
        ExecutorAddress {
            executor_keeper: Arc::new(RwLock::new(None)),
            executor_type_id: std::any::TypeId::of::<Self>(),
            executor_internal_id: EXECUTOR_GLOBAL_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn allocate_new_group(
        global_params: Arc<Self::GlobalParams>,
        memory_params: Option<Self::MemoryParams>,
        common_packet: Option<Packet<Self::InputPacket>>,
    ) -> Self::BuildParams;

    fn get_maximum_concurrency(&self) -> usize;

    fn reinitialize<P: FnMut() -> Packet<Self::OutputPacket>>(
        &mut self,
        reinit_params: &Self::BuildParams,
        packet_alloc: P,
    );

    fn pre_execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        packet_alloc: P,
        packet_send: S,
    );

    fn execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        packet_alloc: P,
        packet_send: S,
    );

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, packet_send: S);

    fn get_total_memory(&self) -> u64;
    fn get_current_memory_params(&self) -> Self::MemoryParams;
}

mod virt {
    use crate::execution_manager::executors_list::{
        ExecOutputMode, ExecutorAllocMode, ExecutorsList,
    };
    use crate::execution_manager::thread_pool::ExecThreadPool;
    use crate::execution_manager::units_io::ExecutorInput;

    // fn execute_kmers_merge() {
    //
    //     let disk_thread_pool = ExecThreadPool::new(4);
    //     let compute_thread_pool = ExecThreadPool::new(16);
    //
    //     let input_buckets = ExecutorInput::from_iter(files);
    //
    //     let bucket_readers = ExecutorsList::<KMBucketReader>::new(ExecutorAllocMode::Fixed(4), &disk_thread_pool);
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
}
