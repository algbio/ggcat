use crate::execution_manager::executor::Executor;
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::thread_pool::{ExecThreadPool, ExecThreadPoolBuilder};
use crate::execution_manager::work_scheduler::ExecutionManagerInfo;
use crate::memory_data_size::MemoryDataSize;
use parking_lot::RwLock;
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
    Distinct { capacity: usize },
}

pub enum ExecOutputMode {
    HighPriority,
    LowPriority,
}

pub struct ExecutorsList<E: Executor> {
    executor_info: Arc<RwLock<ExecutionManagerInfo>>,
    _phantom: PhantomData<E>,
}

impl<E: Executor> ExecutorsList<E> {
    pub fn new(
        alloc_mode: ExecutorAllocMode,
        pool_alloc_mode: PoolAllocMode,
        pool_init_data: <E::OutputPacket as PoolObjectTrait>::InitData,
        global_params: &Arc<E::GlobalParams>,
        thread_pool: &mut ExecThreadPoolBuilder,
    ) -> Self {
        // println!(
        //     "********* TYPE ID FOR: {} ==> {:?}",
        //     std::any::type_name::<E>(),
        //     TypeId::of::<E>()
        // );
        let executor_info = thread_pool.work_scheduler.add_executors::<E>(
            alloc_mode,
            pool_alloc_mode,
            pool_init_data,
            global_params.clone(),
        );
        Self {
            executor_info,
            _phantom: PhantomData,
        }
    }

    pub fn set_output_pool(
        &mut self,
        output_pool: &Arc<ExecThreadPool>,
        _output_mode: ExecOutputMode, // TODO (maybe)
    ) {
        let mut exec_info = self.executor_info.write();
        exec_info.output_pool = Some(output_pool.clone());
    }
}
