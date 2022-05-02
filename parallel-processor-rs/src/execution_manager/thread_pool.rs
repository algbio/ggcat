use crate::execution_manager::executor_address::ExecutorAddress;
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::packet::PacketAny;
use crate::execution_manager::work_manager::WorkManager;
use parking_lot::{Mutex, RwLock};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

pub trait ExecThreadPoolDataAddTrait: Send + Sync {
    fn add_data(&self, addr: ExecutorAddress, packet: PacketAny);
}

pub struct ExecThreadPool {
    pub(crate) work_manager: RwLock<WorkManager>,
    threads_count: usize,
    is_joining: AtomicBool,
    thread_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl ExecThreadPool {
    pub fn new(threads_count: usize, executors_buffer_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            work_manager: RwLock::new(WorkManager::new(
                threads_count * 2,
                executors_buffer_capacity,
            )),
            threads_count,
            is_joining: AtomicBool::new(false),
            thread_handles: Mutex::new(Vec::new()),
        })
    }

    pub fn set_output(&self, target: &Arc<ExecThreadPool>) {
        self.work_manager.write().set_output(target.clone());
    }

    fn thread(&self) {
        let work_manager = self.work_manager.read();
        let mut executor = None;

        let mut self_ref = &mut (self,);

        while !self.is_joining.load(Ordering::Relaxed) {
            while let Some(packet) = work_manager.find_work(&mut executor) {
                executor.as_ref().unwrap().process_packet(packet);
            }
        }
    }

    pub fn start(self: &Arc<Self>) {
        let mut handles = self.thread_handles.lock();
        assert_eq!(handles.len(), 0);
        for _ in 0..self.threads_count {
            let self_ = self.clone();
            handles.push(std::thread::spawn(move || {
                self_.thread();
            }));
        }
    }

    pub fn join(&self) {
        self.is_joining.swap(true, Ordering::Relaxed);
        let mut handles = self.thread_handles.lock();
        for handle in handles.drain(..) {
            handle.join().unwrap();
        }
    }
}

impl ExecThreadPoolDataAddTrait for ExecThreadPool {
    fn add_data(&self, addr: ExecutorAddress, packet: PacketAny) {
        self.work_manager.read().add_input_packet(addr, packet);
    }
}

impl Drop for ExecThreadPool {
    fn drop(&mut self) {
        self.join();
    }
}
