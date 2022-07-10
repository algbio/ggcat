use crate::execution_manager::execution_context::ExecutionContext;
use crate::execution_manager::executor::AsyncExecutor;
use crate::execution_manager::packet::Packet;
use crate::execution_manager::thread_pool::ExecThreadPool;
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
    pub fn set_output_executor<E: AsyncExecutor<InputPacket = T>>(
        &mut self,
        context: &Arc<ExecutionContext>,
        init_data: E::InitData,
        priority: usize,
    ) {
        let mut address = E::generate_new_address(init_data.clone());
        let mut addresses = vec![];

        let mut data = vec![];

        for value in &mut self.iterator {
            data.push((address.clone(), Packet::new_simple(value).upcast()));
            if let ExecutorInputAddressMode::Multiple = &self.addr_mode {
                addresses.push(address.clone());
                address = E::generate_new_address(init_data.clone());
            }
        }

        if let ExecutorInputAddressMode::Single = &self.addr_mode {
            addresses.push(address);
        }

        context.register_executors_batch(addresses, priority);

        for (addr, packet) in data {
            context.add_input_packet(addr, packet);
        }
    }
}
