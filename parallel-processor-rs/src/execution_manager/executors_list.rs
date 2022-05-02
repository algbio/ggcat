use crate::execution_manager::executor::Executor;
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::thread_pool::{ExecThreadPool, ExecThreadPoolDataAddTrait};
use crate::execution_manager::units_io::ExecOutput;
use crate::memory_data_size::MemoryDataSize;
use std::marker::PhantomData;
use std::sync::Arc;

pub enum ExecutorAllocMode {
    Fixed(usize),
    MemoryLimited {
        min_count: usize,
        max_count: usize,
        max_memory: MemoryDataSize,
    },
}

pub enum PoolAllocMode {
    None,
    Shared { capacity: usize },
    Instance { capacity: usize },
}

pub enum ExecOutputMode {
    LIFO,
    FIFO,
}

pub struct ExecutorsList<E: Executor> {
    pub(crate) thread_pool: Arc<ExecThreadPool>,
    _phantom: PhantomData<E>,
}

impl<E: Executor> ExecutorsList<E> {
    pub fn new(
        alloc_mode: ExecutorAllocMode,
        pool_alloc_mode: PoolAllocMode,
        pool_init_data: <E::OutputPacket as PoolObjectTrait>::InitData,
        global_params: &Arc<E::GlobalParams>,
        thread_pool: &Arc<ExecThreadPool>,
    ) -> Self {
        thread_pool.work_manager.write().add_executors::<E>(
            alloc_mode,
            pool_alloc_mode,
            pool_init_data,
            global_params.clone(),
        );
        Self {
            thread_pool: thread_pool.clone(),
            _phantom: PhantomData,
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
        self.thread_pool.set_output(&exec_list.thread_pool)
    }
}
