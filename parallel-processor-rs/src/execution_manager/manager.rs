use crate::execution_manager::executor::{Executor, ExecutorAddress};
use crate::execution_manager::packet::{Packet, PacketsPool};
use parking_lot::Mutex;
use std::sync::Arc;

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
