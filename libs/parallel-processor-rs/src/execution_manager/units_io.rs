use crate::execution_manager::executor::Executor;
use crate::execution_manager::executors_list::ExecOutputMode;
use crate::execution_manager::packet::Packet;
use crate::execution_manager::thread_pool::{ExecThreadPool, ExecThreadPoolDataAddTrait};
use std::sync::Arc;

pub enum ExecutorInputAddressMode {
    Single,
    Multiple,
}

pub struct ExecutorInput<T, I: Iterator<Item = T>> {
    iterator: I,
    addr_mode: ExecutorInputAddressMode,
}

impl<T, I: Iterator<Item = T>> ExecutorInput<T, I> {
    pub fn from_iter(iterator: I, addr_mode: ExecutorInputAddressMode) -> Self {
        Self {
            iterator,
            addr_mode,
        }
    }
}

impl<T: Send + Sync + 'static, I: Iterator<Item = T>> ExecutorInput<T, I> {
    pub fn set_output_pool<E: Executor>(
        &mut self,
        output_pool: &Arc<ExecThreadPool>,
        _output_mode: ExecOutputMode,
    ) {
        let mut address = E::generate_new_address();
        let mut addresses = vec![];

        let mut data = vec![];

        for value in &mut self.iterator {
            data.push((address.clone(), Packet::new_simple(value).upcast()));
            if let ExecutorInputAddressMode::Multiple = &self.addr_mode {
                addresses.push(address.clone());
                address = E::generate_new_address();
            }
        }

        if let ExecutorInputAddressMode::Single = &self.addr_mode {
            addresses.push(address);
        }

        output_pool.add_executors_batch(addresses);

        for (addr, packet) in data {
            output_pool.add_data(addr.to_weak(), packet);
        }
    }
}
