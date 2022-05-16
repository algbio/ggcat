use crate::execution_manager::executor_address::ExecutorAddress;
use crate::execution_manager::memory_tracker::MemoryTracker;
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::packet::Packet;
use crate::execution_manager::packet::PacketTrait;
use crate::execution_manager::work_scheduler::ExecutorDropper;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Eq, PartialEq)]
pub enum ExecutorType {
    SimplePacketsProcessing,
    NeedsInitPacket,
}

static EXECUTOR_GLOBAL_ID: AtomicU64 = AtomicU64::new(0);

pub trait ExecutorOperations<E: Executor> {
    fn declare_addresses(&mut self, addresses: Vec<ExecutorAddress>);
    fn packet_alloc(&mut self) -> Packet<E::OutputPacket>;
    fn packet_alloc_force(&mut self) -> Packet<E::OutputPacket>;
    fn packet_send(&mut self, address: ExecutorAddress, packet: Packet<E::OutputPacket>);
}

pub trait Executor:
    Sized + PoolObjectTrait<InitData = (Arc<Self::GlobalParams>, MemoryTracker<Self>)> + Sync + Send
{
    const EXECUTOR_TYPE: ExecutorType;

    const MEMORY_FIELDS_COUNT: usize;
    const MEMORY_FIELDS: &'static [&'static str];

    const BASE_PRIORITY: u64;
    const PACKET_PRIORITY_MULTIPLIER: u64;
    const STRICT_POOL_ALLOC: bool;

    type InputPacket: Send + Sync;
    type OutputPacket: Send + Sync + PacketTrait;
    type GlobalParams: Send + Sync;
    type MemoryParams: Send + Sync;
    type BuildParams: Send + Sync + Clone;

    fn generate_new_address() -> ExecutorAddress {
        let exec = ExecutorAddress {
            executor_keeper: Arc::new(ExecutorDropper::new()),
            executor_type_id: std::any::TypeId::of::<Self>(),
            executor_internal_id: EXECUTOR_GLOBAL_ID.fetch_add(1, Ordering::Relaxed),
        };
        exec
    }

    fn allocate_new_group<E: ExecutorOperations<Self>>(
        global_params: Arc<Self::GlobalParams>,
        memory_params: Option<Self::MemoryParams>,
        common_packet: Option<Packet<Self::InputPacket>>,
        ops: E,
    ) -> (Self::BuildParams, usize);

    fn required_pool_items(&self) -> u64;

    fn pre_execute<E: ExecutorOperations<Self>>(
        &mut self,
        reinit_params: Self::BuildParams,
        ops: E,
    );
    fn execute<E: ExecutorOperations<Self>>(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        ops: E,
    );
    fn finalize<E: ExecutorOperations<Self>>(&mut self, ops: E);

    fn is_finished(&self) -> bool;

    fn get_current_memory_params(&self) -> Self::MemoryParams;
}
