use crate::execution_manager::executor::Executor;
use crate::execution_manager::thread_pool::ExecThreadPool;
use crate::execution_manager::units_io::ExecOutput;
use crate::memory_data_size::MemoryDataSize;
use std::marker::PhantomData;

pub enum ExecutorAllocMode {
    Fixed(usize),
    MemoryLimited {
        min_count: usize,
        max_memory: MemoryDataSize,
    },
}

pub enum ExecOutputMode {
    LIFO,
    FIFO,
}

pub struct ExecutorsList<E: Executor> {
    _phantom: PhantomData<E>,
}

impl<E: Executor> ExecutorsList<E> {
    pub fn new(
        alloc_mode: ExecutorAllocMode,
        global_params: &E::GlobalParams,
        thread_pool: &ExecThreadPool<E::InputPacket, E::OutputPacket>,
    ) -> Self {
        todo!()
    }
}

impl<E: Executor> ExecOutput for ExecutorsList<E> {
    type OutputPacket = E::OutputPacket;

    fn set_output_executors<W: Executor<InputPacket = E::OutputPacket>>(
        &mut self,
        exec_list: &ExecutorsList<W>,
        output_mode: ExecOutputMode,
    ) {
    }
}
