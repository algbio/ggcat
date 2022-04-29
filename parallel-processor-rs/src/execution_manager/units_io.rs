use crate::execution_manager::executor::{Executor, Packet};
use crate::execution_manager::executors_list::{ExecOutputMode, ExecutorsList};
use crate::execution_manager::thread_pool::ExecThreadPoolDataAddTrait;
use std::marker::PhantomData;

pub trait ExecOutput {
    type OutputPacket;
    fn set_output_executors<W: Executor<InputPacket = Self::OutputPacket>>(
        &mut self,
        exec_list: &ExecutorsList<W>,
        output_mode: ExecOutputMode,
    );
}

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

impl<T: Send + Sync + 'static, I: Iterator<Item = T>> ExecOutput for ExecutorInput<T, I> {
    type OutputPacket = T;

    fn set_output_executors<W: Executor<InputPacket = Self::OutputPacket>>(
        &mut self,
        exec_list: &ExecutorsList<W>,
        output_mode: ExecOutputMode,
    ) {
        let mut address = W::generate_new_address();
        for value in &mut self.iterator {
            exec_list
                .thread_pool
                .add_data(address.clone(), Packet::new_simple(value));
            if let ExecutorInputAddressMode::Multiple = &self.addr_mode {
                address = W::generate_new_address();
            }
        }
    }
}
