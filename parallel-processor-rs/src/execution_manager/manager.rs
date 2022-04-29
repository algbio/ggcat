use crate::execution_manager::executor::Executor;
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::packet::{Packet, PacketsPool};
use parking_lot::Mutex;
use std::sync::Arc;

pub type GenericExecutor<I, O> =
    Arc<Mutex<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>>;

pub trait ExecutionManagerTrait {
    type InputPacket;
    type OutputPacket;
    fn process_packet(
        &self,
        context: *mut (),
        packet: Packet<Self::InputPacket>,
        output_fn: fn(*mut (), ExecutorAddress, Packet<Self::OutputPacket>),
    );
}

pub struct ExecutionManager<E: Executor> {
    executor: Mutex<E>,
    weak_address: WeakExecutorAddress,
    pool: Arc<PacketsPool<E::OutputPacket>>,
}

impl<E: Executor + 'static> ExecutionManager<E> {
    pub fn new(
        mut executor: E,
        build_info: &E::BuildParams,
        pool: Arc<PacketsPool<E::OutputPacket>>,
        address: ExecutorAddress,
    ) -> Arc<dyn ExecutionManagerTrait<InputPacket = E::InputPacket, OutputPacket = E::OutputPacket>>
    {
        executor.reinitialize(build_info, || pool.alloc_packet());

        let self_ = Arc::new(Self {
            executor: Mutex::new(executor),
            weak_address: address.to_weak(),
            pool,
        });
        *address.executor_keeper.write() = Some(self_.clone());

        self_
    }
}

impl<E: Executor> ExecutionManagerTrait for ExecutionManager<E> {
    type InputPacket = E::InputPacket;
    type OutputPacket = E::OutputPacket;

    fn process_packet(
        &self,
        context: *mut (),
        packet: Packet<Self::InputPacket>,
        output_fn: fn(*mut (), ExecutorAddress, Packet<Self::OutputPacket>),
    ) {
        let mut executor = self.executor.lock();
        executor.execute(
            packet,
            || self.pool.alloc_packet(),
            |addr, packet| output_fn(context, addr, packet),
        );
    }
}
