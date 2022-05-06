use crate::execution_manager::executor::Executor;
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::executors_list::ExecutorsList;
use crate::execution_manager::manager::ExecutionStatus;
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::packet::PacketAny;
use crate::execution_manager::work_scheduler::WorkScheduler;
use parking_lot::{Mutex, RwLock};
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub trait ExecThreadPoolDataAddTrait: Send + Sync {
    fn add_data(&self, addr: WeakExecutorAddress, packet: PacketAny);
    fn add_executors_batch(&self, executors: Vec<ExecutorAddress>);
}

pub struct ExecThreadPoolBuilder {
    pub(crate) work_scheduler: WorkScheduler,
    threads_count: usize,
}

impl ExecThreadPoolBuilder {
    pub fn new(threads_count: usize) -> Self {
        Self {
            work_scheduler: WorkScheduler::new(threads_count * 2),
            threads_count,
        }
    }

    pub fn build(self) -> Arc<ExecThreadPool> {
        Arc::new(ExecThreadPool {
            work_scheduler: Arc::new(self.work_scheduler),
            is_joining: AtomicBool::new(false),
            threads_count: self.threads_count,
            thread_handles: Mutex::new(Vec::new()),
        })
    }
}

pub struct ExecThreadPool {
    work_scheduler: Arc<WorkScheduler>,
    is_joining: AtomicBool,
    threads_count: usize,
    thread_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl ExecThreadPool {
    pub fn start(self: &Arc<Self>, name: &'static str) {
        let mut handles = self.thread_handles.lock();
        assert_eq!(handles.len(), 0);
        for _ in 0..self.threads_count {
            let self_ = self.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(name.to_string())
                    .spawn(move || {
                        self_.thread();
                    })
                    .unwrap(),
            );
        }
    }

    fn thread(&self) {
        let mut executor = None;
        let mut last_status = (false, false);

        while !self.is_joining.load(Ordering::Relaxed) {
            loop {
                self.work_scheduler.maybe_change_work(&mut executor);
                if let Some(executor) = &mut executor {
                    let return_status = executor.execute(last_status.0 && last_status.1);
                    last_status = (
                        return_status == ExecutionStatus::OutputPoolFull,
                        last_status.0,
                    );

                    let should_wait = last_status.0 && last_status.1;

                    if should_wait || (return_status == ExecutionStatus::NoMorePacketsNoProgress) {
                        self.work_scheduler.wait_for_progress();
                    }
                } else {
                    break;
                }
            }
        }
    }

    pub fn wait_for_executors<E: Executor>(&self, _: &ExecutorsList<E>) {
        while self
            .work_scheduler
            .get_allocated_executors(&TypeId::of::<E>())
            > 0
        {
            println!(
                "Waiting for: {:?}/{} ==> {}",
                TypeId::of::<E>(),
                std::any::type_name::<E>(),
                self.work_scheduler
                    .get_allocated_executors(&TypeId::of::<E>())
            );
            // self.work_scheduler.print_debug_executors();
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    pub fn join(&self) {
        if !self.is_joining.swap(true, Ordering::Relaxed) {
            let mut handles = self.thread_handles.lock();
            for handle in handles.drain(..) {
                handle.join().unwrap();
            }
            self.work_scheduler.finalize()
        }
    }
}

impl ExecThreadPoolDataAddTrait for ExecThreadPool {
    fn add_data(&self, addr: WeakExecutorAddress, packet: PacketAny) {
        self.work_scheduler.add_input_packet(addr, packet);
    }

    fn add_executors_batch(&self, executors: Vec<ExecutorAddress>) {
        self.work_scheduler.register_executors_batch(executors);
    }
}

impl Drop for ExecThreadPool {
    fn drop(&mut self) {
        self.join();
    }
}
