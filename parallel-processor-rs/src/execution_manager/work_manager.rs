use crate::execution_manager::executor::Executor;
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::executors_list::{ExecutorAllocMode, ExecutorsList};
use crate::execution_manager::manager::{ExecutionManager, ExecutionManagerTrait, GenericExecutor};
use crate::execution_manager::packet::Packet;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Weak};

pub struct WorkManager<I, O> {
    dummy_packets_map: Mutex<HashMap<ExecutorAddress, Vec<Packet<I>>>>,
    dummy_executors_list: Mutex<Vec<ExecutorAddress>>,
    dummy_executors_map: Mutex<
        HashMap<
            WeakExecutorAddress,
            Weak<Mutex<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>>,
        >,
    >,
    _phantom: PhantomData<O>,
}

impl<I, O> WorkManager<I, O> {
    pub fn new() -> Self {
        Self {
            dummy_packets_map: Mutex::new(HashMap::new()),
            dummy_executors_list: Mutex::new(Vec::new()),
            dummy_executors_map: Mutex::new(HashMap::new()),
            _phantom: PhantomData,
        }
    }

    pub fn add_executors<E: Executor>(
        &mut self,
        alloc_mode: ExecutorAllocMode,
        global_params: Arc<E::GlobalParams>,
    ) {
        let build_params = E::allocate_new(global_params, None);
        let exec = E::new();
        // ExecutionManager::new(exec, &build_params,)
    }

    pub fn add_input_packet(&self, address: ExecutorAddress, packet: Packet<I>) {
        self.dummy_packets_map
            .lock()
            .entry(address)
            .or_insert(Vec::new())
            .push(packet);
    }

    fn alloc_executor_typed<E: Executor>(
        address: ExecutorAddress,
        executor: E,
    ) -> GenericExecutor<I, O> {
        // ExecutionManager::new(E)
        todo!()
    }

    pub fn find_work(
        &self,
        executor: Option<(
            ExecutorAddress,
            Arc<Mutex<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>>,
        )>,
    ) -> (
        Packet<I>,
        ExecutorAddress,
        Arc<Mutex<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>>,
    ) {
        {
            let mut packets_map = self.dummy_packets_map.lock();
            if let Some((addr, exec)) = executor {
                if let Some(vec) = packets_map.get_mut(&addr) {
                    let packet = vec.pop().unwrap();
                    if vec.len() == 0 {
                        packets_map.remove(&addr).unwrap();
                    }
                    return (packet, addr, exec);
                }
            }

            let mut executors_list = self.dummy_executors_list.lock();
            if let Some(addr) = executors_list.pop() {
                let mut executors_list = self.dummy_executors_list.lock();
                let packet = packets_map.get_mut(&addr).unwrap().pop().unwrap();
                // return (packet, addr, exec);
            }
        }
        todo!()
    }
}
