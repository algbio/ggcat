use crate::execution_manager::executor::Executor;
use crate::execution_manager::thread_pool::{ExecThreadPool, ExecThreadPoolDataAddTrait};
use crate::execution_manager::units_io::ExecOutput;
use crate::memory_data_size::MemoryDataSize;
use std::marker::PhantomData;
use std::sync::Arc;

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
    thread_pool: Arc<ExecThreadPool<E::InputPacket, E::OutputPacket>>,
    output_thread_pool: Option<(
        Arc<dyn ExecThreadPoolDataAddTrait<InputPacket = E::OutputPacket>>,
        ExecOutputMode,
    )>,
}

impl<E: Executor> ExecutorsList<E> {
    pub fn new(
        alloc_mode: ExecutorAllocMode,
        global_params: &Arc<E::GlobalParams>,
        thread_pool: &Arc<ExecThreadPool<E::InputPacket, E::OutputPacket>>,
    ) -> Self {
        thread_pool
            .work_manager
            .write()
            .add_executors::<E>(alloc_mode, global_params.clone());
        Self {
            thread_pool: thread_pool.clone(),
            output_thread_pool: None,
        }
    }
}

impl<E: Executor> ExecOutput for ExecutorsList<E> {
    type OutputPacket = E::OutputPacket;

    fn set_output_executors<W: Executor<InputPacket = E::OutputPacket>>(
        &mut self,
        exec_list: &ExecutorsList<W>,
        output_mode: ExecOutputMode,
    ) {
        self.output_thread_pool = Some((exec_list.thread_pool.clone(), output_mode));
    }
}
