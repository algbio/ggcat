use crate::execution_manager::executor::{Executor, ExecutorAddress};
use crate::execution_manager::executors_list::ExecutorsList;
use crate::execution_manager::manager::ExecutionManagerTrait;
use crate::execution_manager::packet::Packet;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct WorkManager<I, O> {
    _phantom: PhantomData<(I, O)>,
}

impl<I, O> WorkManager<I, O> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    pub fn add_executors_list<E: Executor>(&mut self, executors_list: ExecutorsList<E>) {}

    pub fn add_input_packet(&self, address: ExecutorAddress, packet: Packet<I>) {}

    pub fn find_work(
        executor: Option<Arc<Mutex<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>>>,
    ) -> (
        Packet<I>,
        Arc<Mutex<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>>,
    ) {
        todo!()
    }
}
