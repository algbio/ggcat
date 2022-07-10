use crate::execution_manager::execution_context::{ExecutionContext, PoolAllocMode};
use crate::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::packet::{PacketAny, PacketTrait};
use flame::{FlameLibrary, FLAME_LIBRARY};
use parking_lot::Mutex;
use std::any::TypeId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

pub struct ExecThreadPool {
    context: Arc<ExecutionContext>,
    executors: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    runtime: Runtime,
    threads_count: usize,
}

pub struct ExecutorsHandle<E: AsyncExecutor>(PhantomData<E>);
impl<E: AsyncExecutor> Clone for ExecutorsHandle<E> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<E: AsyncExecutor> Copy for ExecutorsHandle<E> {}

impl ExecThreadPool {
    pub fn new(context: &Arc<ExecutionContext>, threads_count: usize, name: &str) -> Self {
        Self {
            context: context.clone(),
            executors: Mutex::new(Vec::new()),
            runtime: Builder::new_multi_thread()
                .thread_name(name)
                .worker_threads(threads_count)
                .build()
                .unwrap(),
            threads_count,
        }
    }

    pub fn register_executors<E: AsyncExecutor>(
        &self,
        count: usize,
        pool_alloc_mode: PoolAllocMode,
        pool_init_data: <E::OutputPacket as PoolObjectTrait>::InitData,
        global_params: &Arc<E::GlobalParams>,
    ) -> ExecutorsHandle<E> {
        self.context
            .register_executor_type::<E>(count, pool_alloc_mode, pool_init_data);

        let addresses_channel = self
            .context
            .waiting_addresses
            .lock()
            .get(&TypeId::of::<E>())
            .unwrap()
            .clone();

        let mut executors = self.executors.lock();

        for _ in 0..count {
            let context = self.context.clone();
            let addresses_channel = addresses_channel.clone();
            let global_params = global_params.clone();

            executors.push(self.runtime.spawn(async move {
                FLAME_LIBRARY
                    .scope(RefCell::new(FlameLibrary::new()), async {
                        let context_ = context.clone();
                        let mut executor = E::new();
                        let sem_lock = context_.start_semaphore.acquire().await;
                        let memory_tracker = context.memory_tracker.get_executor_instance();
                        executor
                            .async_executor_main(
                                &global_params,
                                ExecutorReceiver {
                                    context,
                                    addresses_channel,
                                    _phantom: PhantomData,
                                },
                                memory_tracker,
                            )
                            .await;

                        drop(sem_lock);
                        context_.wait_condvar.notify_all();
                    })
                    .await;
            }));
        }
        ExecutorsHandle(PhantomData)
    }

    async fn join(&self) {
        let mut executors = self.executors.lock();

        for executor in executors.drain(..) {
            executor.await.unwrap()
        }
    }

    // pub fn debug_print_memory(&self) {
    //     self.work_scheduler.print_debug_memory()
    // }
    //
    // pub fn debug_print_queue(&self) {
    //     self.work_scheduler.print_debug_executors()
    // }
    //
}
